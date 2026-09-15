use std::future::Future;

use serde::Serialize;
use serde_json::json;
use shelllist_daemon_tokio::{BroadcastEvent, WatchPhase, forward_broadcast, forward_watch};
use tokio::{
    sync::{broadcast, watch},
    task::JoinSet,
};
use zbus::{names::UniqueName, object_server::SignalEmitter};

use crate::{api, protocol};

use super::{
    AUDIO_STREAM, BluetoothDaemon, CHANGED_STREAM, OBEX_STREAM, OPERATION_STREAM, PAIRING_STREAM,
    SCAN_STREAM, SharedSnapshot, emit_audio, emit_snapshot, emit_stream,
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

    let id = daemon.next_id("subscription");
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
            forward_snapshots(snapshots, signal_emitter.clone(), subscription_id.clone()),
        );
        spawn_if(
            &mut forwarders,
            requested.audio,
            forward_audio(
                audio_snapshots,
                signal_emitter.clone(),
                subscription_id.clone(),
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
                emit_stream(
                    emitter,
                    stream,
                    subscription_id,
                    event_name(&event),
                    &event,
                )
                .await;
            }
            BroadcastEvent::Lagged(skipped) => {
                tracing::warn!(%subscription_id, %stream, skipped, "subscription events were dropped");
                emit_stream(
                    emitter,
                    stream,
                    subscription_id,
                    "lagged",
                    &json!({ "skipped": skipped }),
                )
                .await;
            }
        }
    }).await;
    tracing::warn!(%subscription_id, %stream, "subscription event source closed");
}

async fn forward_snapshots(
    receiver: watch::Receiver<SharedSnapshot>,
    emitter: SignalEmitter<'static>,
    subscription_id: String,
) {
    forward_watch(receiver, |snapshot, event| {
        let emitter = emitter.clone();
        let id = subscription_id.clone();
        async move { emit_snapshot(&emitter, &snapshot, &id, watch_event(event)).await }
    })
    .await;
}

async fn forward_audio(
    receiver: watch::Receiver<serde_json::Value>,
    emitter: SignalEmitter<'static>,
    subscription_id: String,
) {
    forward_watch(receiver, |snapshot, event| {
        let emitter = emitter.clone();
        let id = subscription_id.clone();
        async move { emit_audio(&emitter, &snapshot, &id, watch_event(event)).await }
    })
    .await;
}

fn watch_event(phase: WatchPhase) -> &'static str {
    match phase {
        WatchPhase::Initial => "subscribed",
        WatchPhase::Changed => "changed",
    }
}

#[cfg(test)]
mod tests {
    use super::RequestedStreams;

    #[tokio::test]
    async fn watched_values_are_emitted_once_and_forwarder_stops_when_closed() {
        let (updates, receiver) = tokio::sync::watch::channel(0);
        updates.send_replace(1);
        let (events, mut observed) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(super::forward_watch(receiver, move |value, event| {
            events.send((value, event)).unwrap();
            std::future::ready(())
        }));
        assert_eq!(observed.recv().await, Some((1, super::WatchPhase::Initial)));
        assert!(observed.try_recv().is_err());
        updates.send_replace(2);
        assert_eq!(observed.recv().await, Some((2, super::WatchPhase::Changed)));
        assert!(observed.try_recv().is_err());
        drop(updates);
        tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(observed.recv().await, None);
    }

    #[test]
    fn requested_streams_are_validated_and_deduplicated() {
        assert!(RequestedStreams::parse(&[]).is_none());
        assert!(RequestedStreams::parse(&["unsupported".to_string()]).is_none());
        let streams = RequestedStreams::parse(&[
            "bluetooth.changed".to_string(),
            "bluetooth.changed".to_string(),
            "bluetooth.operation".to_string(),
        ])
        .expect("supported streams");
        assert!(streams.changes);
        assert!(streams.operations);
        assert!(!streams.pairing);
    }
}
