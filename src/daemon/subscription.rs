use std::future::Future;

use serde::Serialize;
use serde_json::{Value, json};
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

#[derive(Clone, Copy)]
struct RequestedStreams {
    changes: bool,
    pairing: bool,
    operations: bool,
    scans: bool,
    audio: bool,
    obex: bool,
}

impl RequestedStreams {
    fn parse(streams: &[String]) -> Option<Self> {
        if streams.is_empty()
            || streams.iter().any(|requested| {
                !protocol::STREAMS
                    .iter()
                    .any(|(supported, _)| requested == supported)
            })
        {
            return None;
        }
        let wants = |target| streams.iter().any(|stream| stream == target);
        Some(Self {
            changes: wants(CHANGED_STREAM),
            pairing: wants(PAIRING_STREAM),
            operations: wants(OPERATION_STREAM),
            scans: wants(SCAN_STREAM),
            audio: wants(AUDIO_STREAM),
            obex: wants(OBEX_STREAM),
        })
    }
}

pub(super) async fn start(
    daemon: &BluetoothDaemon,
    streams: Vec<String>,
    owner: UniqueName<'static>,
    emitter: SignalEmitter<'_>,
) -> String {
    let Some(requested) = RequestedStreams::parse(&streams) else {
        tracing::warn!(
            ?streams,
            "subscription rejected because it contains unsupported streams"
        );
        return api::error(
            "unsupported-stream",
            "Subscriptions require bluetooth.changed, pairing.request, bluetooth.operation, bluetooth.scan, bluetooth.audio.changed, and/or bluetooth.obex.transfer".to_string(),
        )
        .to_string();
    };

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
    let events = async move {
        let mut forwarders = JoinSet::new();
        spawn_if(
            &mut forwarders,
            requested.changes,
            forward_snapshots(
                snapshots,
                signal_emitter.clone(),
                subscription_id.clone(),
                CHANGED_STREAM,
                snapshot_fields,
            ),
        );
        spawn_if(
            &mut forwarders,
            requested.audio,
            forward_snapshots(
                audio_snapshots,
                signal_emitter.clone(),
                subscription_id.clone(),
                AUDIO_STREAM,
                audio_fields,
            ),
        );
        macro_rules! forward {
            ($enabled:expr, $receiver:expr, $stream:expr) => {
                if $enabled {
                    forwarders.spawn(forward_events(
                        $receiver,
                        signal_emitter.clone(),
                        $stream,
                        subscription_id.clone(),
                        |event| &event.event,
                    ));
                }
            };
        }
        forward!(requested.pairing, pairing_events, PAIRING_STREAM);
        forward!(requested.operations, operation_events, OPERATION_STREAM);
        forward!(requested.scans, scan_events, SCAN_STREAM);
        forward!(requested.obex, obex_events, OBEX_STREAM);
        if let Some(Err(error)) = forwarders.join_next().await {
            tracing::error!(%subscription_id, %error, "subscription forwarder task failed");
        }
        forwarders.abort_all();
        tracing::info!(%subscription_id, "subscription ended");
    };
    if let Err(error) = daemon.subscriptions.spawn_for_owner(
        id.clone(),
        Some(owner.to_string()),
        &connection,
        events,
    ) {
        return api::error("subscription-unavailable", error.to_string()).to_string();
    }
    api::success(json!({ "subscription": { "id": id, "streams": streams } })).to_string()
}

fn spawn_if(
    tasks: &mut JoinSet<()>,
    enabled: bool,
    future: impl Future<Output = ()> + Send + 'static,
) {
    if enabled {
        tasks.spawn(future);
    }
}

async fn forward_events<T>(
    receiver: broadcast::Receiver<T>,
    emitter: SignalEmitter<'static>,
    stream: &'static str,
    subscription_id: String,
    event_name: fn(&T) -> &str,
) where
    T: Clone + Send + Serialize + 'static,
{
    let emitter = &emitter;
    let subscription_id = subscription_id.as_str();
    forward_broadcast(receiver, move |update| async move {
        match update {
            BroadcastEvent::Item(event) => {
                emit_event(
                    emitter,
                    stream,
                    subscription_id,
                    event_name(&event),
                    json!({ "data": event }),
                )
                .await;
            }
            BroadcastEvent::Lagged(skipped) => {
                tracing::warn!(%subscription_id, %stream, skipped, "subscription events were dropped");
                emit_event(
                    emitter,
                    stream,
                    subscription_id,
                    "lagged",
                    json!({ "data": { "skipped": skipped } }),
                )
                .await;
            }
        }
    }).await;
    tracing::warn!(%subscription_id, %stream, "subscription event source closed");
}

async fn forward_snapshots<T: Clone>(
    receiver: watch::Receiver<T>,
    emitter: SignalEmitter<'static>,
    subscription_id: String,
    stream: &'static str,
    fields: fn(T, &'static str) -> (&'static str, Value),
) {
    forward_watch(receiver, |snapshot, phase| {
        let (event, payload) = fields(snapshot, watch_event(phase));
        emit_event(&emitter, stream, &subscription_id, event, payload)
    })
    .await;
}

fn snapshot_fields(snapshot: SharedSnapshot, event: &'static str) -> (&'static str, Value) {
    match snapshot {
        SharedSnapshot::Loading => ("loading", json!({ "data": { "status": "loading" } })),
        SharedSnapshot::Available(snapshot) => (event, json!({ "data": { "snapshot": snapshot } })),
        SharedSnapshot::Unavailable(error) => (
            "unavailable",
            json!({ "error": { "code": "bluez-unavailable", "message": error } }),
        ),
    }
}

fn audio_fields(envelope: Value, event: &'static str) -> (&'static str, Value) {
    if envelope["ok"].as_bool() == Some(true) {
        (
            event,
            json!({ "data": { "audio_devices": envelope["data"]["audio_devices"] } }),
        )
    } else {
        ("unavailable", json!({ "error": envelope["error"] }))
    }
}

async fn emit_event(
    emitter: &SignalEmitter<'_>,
    stream: &str,
    subscription_id: &str,
    event: &str,
    fields: Value,
) {
    let result = shelllist_daemon_tokio::emit_json_event(
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

#[cfg(test)]
mod tests {
    #[test]
    fn snapshot_payloads_preserve_loading_failure_and_watch_phases() {
        use super::{RequestedStreams, SharedSnapshot, audio_fields, snapshot_fields};
        use serde_json::json;
        use std::sync::Arc;

        assert!(RequestedStreams::parse(&[]).is_none());
        assert!(RequestedStreams::parse(&["unsupported".into()]).is_none());
        assert_eq!(
            snapshot_fields(SharedSnapshot::Loading, "subscribed"),
            ("loading", json!({"data": {"status": "loading"}}))
        );
        let error = json!({"code": "bluez-unavailable", "message": "offline"});
        assert_eq!(
            snapshot_fields(
                SharedSnapshot::Unavailable(Arc::new("offline".into())),
                "changed"
            ),
            ("unavailable", json!({"error": error}))
        );
        for phase in ["subscribed", "changed"] {
            let snapshot = Arc::default();
            let expected = json!({"data": {"snapshot": snapshot}});
            assert_eq!(
                snapshot_fields(SharedSnapshot::Available(snapshot), phase),
                (phase, expected)
            );
            assert_eq!(
                audio_fields(crate::api::success(json!({"audio_devices": []})), phase),
                (phase, json!({"data": {"audio_devices": []}}))
            );
            assert_eq!(
                audio_fields(json!({"ok": false, "error": error}), phase),
                ("unavailable", json!({"error": error}))
            );
        }
        assert_eq!(
            audio_fields(json!(null), "changed"),
            ("unavailable", json!({"error": null}))
        );
    }
}
