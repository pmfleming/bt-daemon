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

fn audio_fields(mut envelope: Value, event: &'static str) -> (&'static str, Value) {
    if envelope["ok"].as_bool() == Some(true) {
        let mut fields = json!({ "data": {} });
        fields["data"]["audio_devices"] = envelope
            .pointer_mut("/data/audio_devices")
            .map(Value::take)
            .unwrap_or_default();
        (event, fields)
    } else {
        let mut fields = json!({});
        fields["error"] = envelope
            .get_mut("error")
            .map(Value::take)
            .unwrap_or_default();
        ("unavailable", fields)
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
        use super::{SharedSnapshot, audio_fields, snapshot_fields, valid_streams};
        use serde_json::json;
        use std::sync::Arc;

        assert!(!valid_streams(&[]));
        assert!(!valid_streams(&["unsupported".into()]));
        for (stream, _) in crate::protocol::STREAMS {
            assert!(valid_streams(&[stream.to_string(), stream.to_string()]));
            assert!(!valid_streams(&[stream.to_string(), "unsupported".into()]));
        }
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
                audio_fields(
                    crate::api::success(
                        json!({"audio_devices": [{"profiles": [1, 2]}], "ignored": true})
                    ),
                    phase
                ),
                (
                    phase,
                    json!({"data": {"audio_devices": [{"profiles": [1, 2]}]}})
                )
            );
            assert_eq!(
                audio_fields(json!({"ok": false, "error": error}), phase),
                ("unavailable", json!({"error": error}))
            );
        }
        for malformed in [json!(null), json!(false), json!([])] {
            assert_eq!(
                audio_fields(malformed, "changed"),
                ("unavailable", json!({"error": null}))
            );
        }
        for data in [json!(null), json!(false), json!([]), json!({})] {
            assert_eq!(
                audio_fields(json!({"ok": true, "data": data}), "changed"),
                ("changed", json!({"data": {"audio_devices": null}}))
            );
        }
    }
}
