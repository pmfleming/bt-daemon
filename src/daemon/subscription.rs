use std::sync::Arc;

use serde::Serialize;
use serde_json::{Value, json};
use shelllist_daemon_core::{
    ApiError,
    wire::{EventBody, Gap, GapReason},
};
use shelllist_daemon_tokio::{BroadcastEvent, WatchPhase, forward_broadcast, forward_watch};
use tokio::{
    sync::{broadcast, watch},
    task::JoinSet,
};
use zbus::{names::UniqueName, object_server::SignalEmitter};

use crate::{api, model::Snapshot, protocol};

use super::{
    AUDIO_STREAM, BluetoothDaemon, CHANGED_STREAM, INTERFACE, OBEX_STREAM, OPERATION_STREAM,
    PAIRING_STREAM, SCAN_STREAM, published::Published,
};

/// One captured state feeds both request responses and subscription events.
#[derive(Clone, PartialEq)]
pub(super) enum SharedSnapshot {
    Loading,
    Available(Arc<Snapshot>),
    Unavailable(Arc<String>),
}

impl From<anyhow::Result<Snapshot>> for SharedSnapshot {
    fn from(result: anyhow::Result<Snapshot>) -> Self {
        match result {
            Ok(snapshot) => Self::Available(Arc::new(snapshot)),
            Err(error) => Self::Unavailable(Arc::new(format!("{error:#}"))),
        }
    }
}

impl SharedSnapshot {
    pub(super) fn response(&self) -> Value {
        match self {
            Self::Loading => api::error("snapshot-loading", "Bluetooth snapshot is loading".into()),
            Self::Available(snapshot) => api::success(json!({ "snapshot": snapshot })),
            Self::Unavailable(error) => api::error("bluez-unavailable", error.to_string()),
        }
    }
}

impl Published<SharedSnapshot> {
    pub(super) fn response(&self) -> Value {
        self.stamp(self.value.response())
    }
}

fn valid_streams(streams: &[String]) -> bool {
    !streams.is_empty()
        && streams.iter().all(|requested| {
            protocol::STREAMS
                .iter()
                .any(|(supported, _)| requested == supported)
        })
}

#[derive(Clone)]
enum Target {
    Bus(SignalEmitter<'static>),
    Socket(shelllist_daemon_tokio::SocketSession),
}

pub(super) async fn start(
    daemon: &BluetoothDaemon,
    streams: Vec<String>,
    owner: UniqueName<'static>,
    emitter: SignalEmitter<'_>,
) -> String {
    let monitor = daemon.subscriptions.owner_monitor(emitter.connection());
    let watched = owner.to_string();
    let target = Target::Bus(emitter.set_destination(owner.clone().into()).to_owned());
    start_target(daemon, streams, owner.to_string(), target, async move {
        if let Err(error) = monitor.wait(&watched).await {
            tracing::warn!(%error, "subscription owner monitor failed");
        }
    })
    .await
}

pub(super) async fn start_socket(
    daemon: &BluetoothDaemon,
    streams: Vec<String>,
    session: &shelllist_daemon_tokio::SocketSession,
) -> String {
    let lifetime = session.clone();
    start_target(
        daemon,
        streams,
        session.owner().to_owned(),
        Target::Socket(session.clone()),
        async move { lifetime.closed().await },
    )
    .await
}

async fn start_target(
    daemon: &BluetoothDaemon,
    streams: Vec<String>,
    owner: String,
    signal_emitter: Target,
    stop: impl std::future::Future<Output = ()> + Send + 'static,
) -> String {
    if !valid_streams(&streams) {
        tracing::warn!(
            ?streams,
            "subscription rejected because it contains unsupported streams"
        );
        return api::error(
            "unsupported-stream",
            "Subscriptions require bluetooth.changed, pairing.request, bluetooth.operation, bluetooth.scan, bluetooth.audio.changed, and/or bluetooth.obex.transfer".to_string(),
        )
        .to_string();
    }

    let id = daemon.subscriptions.next_id("subscription");
    let subscription_id = id.clone();
    let snapshots = daemon.snapshots.subscribe();
    let audio_snapshots = daemon.audio_snapshots.subscribe();
    let pairing_events = daemon.pairing.subscribe();
    let operation_events = daemon.operations.subscribe();
    let scan_events = daemon.scans.subscribe();
    let obex_events = daemon.obex.subscribe();

    tracing::info!(%subscription_id, %owner, ?streams, "subscription started");
    let response =
        api::success(json!({ "subscription": { "id": id, "streams": streams } })).to_string();
    let events = async move {
        let wants = |target| streams.iter().any(|stream| stream == target);
        let mut forwarders = JoinSet::new();
        macro_rules! forward {
            ($stream:expr, $receiver:expr) => {
                forward!($stream, $receiver, forward_events, |event| &event.event);
            };
            ($stream:expr, $receiver:expr, $forward:ident, $fields:expr) => {
                if wants($stream) {
                    forwarders.spawn($forward(
                        $receiver,
                        signal_emitter.clone(),
                        subscription_id.clone(),
                        $stream,
                        $fields,
                    ));
                }
            };
        }
        forward!(
            CHANGED_STREAM,
            snapshots,
            forward_snapshots,
            snapshot_fields
        );
        forward!(
            AUDIO_STREAM,
            audio_snapshots,
            forward_snapshots,
            audio_fields
        );
        forward!(PAIRING_STREAM, pairing_events);
        forward!(OPERATION_STREAM, operation_events);
        forward!(SCAN_STREAM, scan_events);
        forward!(OBEX_STREAM, obex_events);
        if let Some(Err(error)) = forwarders.join_next().await {
            tracing::error!(%subscription_id, %error, "subscription forwarder task failed");
        }
        forwarders.abort_all();
        tracing::info!(%subscription_id, "subscription ended");
    };
    if let Err(error) = daemon
        .subscriptions
        .spawn_until(id, Some(owner), stop, events)
    {
        return api::error("subscription-unavailable", error.to_string()).to_string();
    }
    response
}

async fn forward_events<T>(
    receiver: broadcast::Receiver<T>,
    emitter: Target,
    subscription_id: String,
    stream: &'static str,
    event_name: fn(&T) -> &str,
) where
    T: Clone + Send + Serialize + 'static,
{
    let emitter = &emitter;
    let subscription_id = subscription_id.as_str();
    forward_broadcast(receiver, move |update| async move {
        if let BroadcastEvent::Lagged(skipped) = &update {
            tracing::warn!(%subscription_id, %stream, skipped, "subscription events were dropped");
        }
        let (event, fields) = broadcast_fields(&update, event_name);
        emit_event(emitter, stream, subscription_id, event, fields).await;
    })
    .await;
    tracing::warn!(%subscription_id, %stream, "subscription event source closed");
}

fn broadcast_fields<T: Serialize>(
    update: &BroadcastEvent<T>,
    event_name: fn(&T) -> &str,
) -> (&str, EventBody) {
    match update {
        BroadcastEvent::Item(event) => match serde_json::to_value(event) {
            Ok(data) => (event_name(event), EventBody::data(data)),
            Err(error) => {
                tracing::warn!(%error, event = event_name(event), "subscription event serialization failed; requesting recovery");
                gap_fields(Gap {
                    reason: GapReason::SourceReset,
                    dropped_events: "1".into(),
                })
            }
        },
        BroadcastEvent::Lagged(skipped) => gap_fields(Gap::lagged(*skipped)),
    }
}

fn gap_fields(gap: Gap) -> (&'static str, EventBody) {
    ("gap", EventBody::data(json!(gap)))
}

async fn forward_snapshots<T: Clone>(
    receiver: watch::Receiver<T>,
    emitter: Target,
    subscription_id: String,
    stream: &'static str,
    fields: fn(T, &'static str) -> (&'static str, EventBody),
) {
    forward_watch(receiver, |snapshot, phase| {
        let (event, payload) = fields(snapshot, watch_event(phase));
        emit_event(&emitter, stream, &subscription_id, event, payload)
    })
    .await;
}

fn snapshot_fields(
    state: Published<SharedSnapshot>,
    event: &'static str,
) -> (&'static str, EventBody) {
    let watermark = state.watermark();
    let (event, mut body) = match state.value {
        SharedSnapshot::Loading => ("loading", EventBody::data(json!({"status": "loading"}))),
        SharedSnapshot::Available(snapshot) => {
            (event, EventBody::data(json!({"snapshot": snapshot})))
        }
        SharedSnapshot::Unavailable(error) => (
            "unavailable",
            EventBody::error(ApiError::new("bluez-unavailable", error.as_str())),
        ),
    };
    body.data["state"] = watermark;
    (event, body)
}

fn audio_fields(state: Published<Value>, event: &'static str) -> (&'static str, EventBody) {
    let mut envelope = state.response();
    let (event, mut body) = if envelope["ok"].as_bool() == Some(true) {
        let devices = envelope
            .pointer_mut("/data/audio_devices")
            .map(Value::take)
            .unwrap_or_default();
        (event, EventBody::data(json!({"audio_devices": devices})))
    } else {
        let error = serde_json::from_value(
            envelope
                .get_mut("error")
                .map(Value::take)
                .unwrap_or(Value::Null),
        )
        .unwrap_or_else(|_| ApiError::new("audio-unavailable", "Invalid audio response"));
        ("unavailable", EventBody::error(error))
    };
    body.data["state"] = envelope["data"]["state"].take();
    (event, body)
}

/// Offline witnesses use the same captured state and serializers as both ingress paths.
/// Only generated epoch identities are normalized; no alternate fixture wire model.
pub(super) fn snapshot_ordering_fixture() -> Value {
    fn event(state: Published<SharedSnapshot>) -> Value {
        let (name, body) = snapshot_fields(state, "changed");
        body.envelope(
            shelllist_daemon_core::ApiIdentity::new(api::PROTOCOL, api::VERSION as u32),
            CHANGED_STREAM,
            name,
            shelllist_daemon_core::Correlation::Subscription("ordering-subscription"),
        )
        .into_value()
        .expect("serializable snapshot event")
    }
    let (sender, _) = watch::channel(Published::new(SharedSnapshot::Loading));
    let loading = event(sender.borrow().clone());
    let loading_read = sender.borrow().response();
    Published::update(
        &sender,
        SharedSnapshot::Available(Arc::new(Snapshot::default())),
    );
    let baseline = event(sender.borrow().clone());
    let old_read = sender.borrow().response();
    let mut next = Snapshot::default();
    next.radio.powered = true;
    Published::update(&sender, SharedSnapshot::Available(Arc::new(next)));
    let changed = event(sender.borrow().clone());
    let latest_read = sender.borrow().response();
    Published::update(
        &sender,
        SharedSnapshot::Unavailable(Arc::new("Unavailable".into())),
    );
    let unavailable = event(sender.borrow().clone());
    let unavailable_read = sender.borrow().response();
    let audio = Published::new(api::success(json!({"audio_devices": []})));
    let audio_read = audio.response();
    let (name, body) = audio_fields(audio, "subscribed");
    let audio_event = body
        .envelope(
            shelllist_daemon_core::ApiIdentity::new(api::PROTOCOL, api::VERSION as u32),
            AUDIO_STREAM,
            name,
            shelllist_daemon_core::Correlation::Subscription("ordering-subscription"),
        )
        .into_value()
        .expect("serializable audio event");
    let mut fixture = json!({
        "loading": loading, "loading_read": loading_read, "baseline": baseline,
        "old_read": old_read, "changed": changed, "latest_read": latest_read,
        "unavailable": unavailable, "unavailable_read": unavailable_read,
        "audio_read": audio_read, "audio_event": audio_event,
    });
    for (name, record) in fixture.as_object_mut().expect("object fixture") {
        record["data"]["state"]["epoch"] = json!(if name.starts_with("audio_") {
            "fixture-audio"
        } else {
            "fixture-radio"
        });
    }
    fixture
}

async fn emit_event(
    emitter: &Target,
    stream: &str,
    subscription_id: &str,
    event: &str,
    fields: EventBody,
) {
    let identity = shelllist_daemon_core::ApiIdentity::new(api::PROTOCOL, api::VERSION as u32);
    let correlation = shelllist_daemon_core::Correlation::Subscription(subscription_id);
    let result: anyhow::Result<()> = match emitter {
        Target::Bus(emitter) => shelllist_daemon_tokio::emit_event_body(
            emitter,
            INTERFACE,
            identity,
            stream,
            event,
            correlation,
            fields,
        )
        .await
        .map_err(Into::into),
        Target::Socket(session) => match fields
            .envelope(identity, stream, event, correlation)
            .into_value()
        {
            Ok(value) => session.emit(stream, value).await,
            Err(error) => Err(error.into()),
        },
    };
    if let Err(error) = result {
        tracing::warn!(%error, %stream, %subscription_id, "could not emit subscription event");
    }
}

fn watch_event(phase: WatchPhase) -> &'static str {
    match phase {
        WatchPhase::Initial => "subscribed",
        WatchPhase::Changed => "changed",
    }
}
