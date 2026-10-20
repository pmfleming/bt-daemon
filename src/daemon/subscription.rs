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

use crate::{api, protocol};

use super::{
    AUDIO_STREAM, BluetoothDaemon, CHANGED_STREAM, INTERFACE, OBEX_STREAM, OPERATION_STREAM,
    PAIRING_STREAM, SCAN_STREAM, SharedSnapshot,
};

fn valid_streams(streams: &[String]) -> bool {
    !streams.is_empty()
        && streams.iter().all(|requested| {
            protocol::STREAMS
                .iter()
                .any(|(supported, _)| requested == supported)
        })
}

pub(super) async fn start(
    daemon: &BluetoothDaemon,
    streams: Vec<String>,
    owner: UniqueName<'static>,
    emitter: SignalEmitter<'_>,
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
    let signal_emitter = emitter.set_destination(owner.clone().into()).to_owned();
    let connection = signal_emitter.connection().clone();
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
    if let Err(error) =
        daemon
            .subscriptions
            .spawn_for_owner(id, Some(owner.to_string()), &connection, events)
    {
        return api::error("subscription-unavailable", error.to_string()).to_string();
    }
    response
}

async fn forward_events<T>(
    receiver: broadcast::Receiver<T>,
    emitter: SignalEmitter<'static>,
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
    emitter: SignalEmitter<'static>,
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

fn snapshot_fields(snapshot: SharedSnapshot, event: &'static str) -> (&'static str, EventBody) {
    match snapshot {
        SharedSnapshot::Loading => ("loading", EventBody::data(json!({"status": "loading"}))),
        SharedSnapshot::Available(snapshot) => {
            (event, EventBody::data(json!({"snapshot": snapshot})))
        }
        SharedSnapshot::Unavailable(error) => (
            "unavailable",
            EventBody::error(ApiError::new("bluez-unavailable", error.as_str())),
        ),
    }
}

fn audio_fields(mut envelope: Value, event: &'static str) -> (&'static str, EventBody) {
    if envelope["ok"].as_bool() == Some(true) {
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
    }
}

async fn emit_event(
    emitter: &SignalEmitter<'_>,
    stream: &str,
    subscription_id: &str,
    event: &str,
    fields: EventBody,
) {
    let result = shelllist_daemon_tokio::emit_event_body(
        emitter,
        INTERFACE,
        shelllist_daemon_core::ApiIdentity::new(api::PROTOCOL, api::VERSION as u32),
        stream,
        event,
        shelllist_daemon_core::Correlation::Subscription(subscription_id),
        fields,
    )
    .await;
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
