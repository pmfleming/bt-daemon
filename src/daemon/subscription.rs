use std::{future::Future, sync::Arc};

use serde::Serialize;
use serde_json::json;
use tokio::{
    sync::{broadcast, oneshot, watch},
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
    let task_owner = owner.clone();
    let subscriptions = Arc::clone(&daemon.subscriptions);
    let (start_sender, start_receiver) = oneshot::channel();
    let task = crate::task::spawn("subscription", async move {
        if start_receiver.await.is_err() {
            return;
        }
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
        forwarders.spawn(async move {
            let _ = shelllist_daemon_tokio::wait_for_owner_loss(&connection, owner).await;
        });
        if let Some(Err(error)) = forwarders.join_next().await {
            tracing::error!(%subscription_id, %error, "subscription forwarder task failed");
        }
        forwarders.abort_all();
        subscriptions.remove(&subscription_id).await;
        tracing::info!(%subscription_id, "subscription ended");
    });
    daemon
        .subscriptions
        .insert(id.clone(), Some(task_owner.to_string()), task)
        .await;
    let _ = start_sender.send(());
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
    mut receiver: broadcast::Receiver<T>,
    emitter: SignalEmitter<'static>,
    stream: &'static str,
    subscription_id: String,
    event_name: fn(&T) -> &str,
) where
    T: Clone + Send + Serialize + 'static,
{
    loop {
        match receiver.recv().await {
            Ok(event) => {
                emit_stream(
                    &emitter,
                    stream,
                    &subscription_id,
                    event_name(&event),
                    &event,
                )
                .await;
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!(%subscription_id, %stream, skipped, "subscription events were dropped");
                emit_stream(
                    &emitter,
                    stream,
                    &subscription_id,
                    "lagged",
                    &json!({ "skipped": skipped }),
                )
                .await;
            }
            Err(broadcast::error::RecvError::Closed) => {
                tracing::warn!(%subscription_id, %stream, "subscription event source closed");
                break;
            }
        }
    }
}

async fn forward_snapshots(
    mut receiver: watch::Receiver<SharedSnapshot>,
    emitter: SignalEmitter<'static>,
    subscription_id: String,
) {
    let initial = receiver.borrow().clone();
    emit_snapshot(&emitter, &initial, &subscription_id, "subscribed").await;
    while receiver.changed().await.is_ok() {
        let snapshot = receiver.borrow().clone();
        emit_snapshot(&emitter, &snapshot, &subscription_id, "changed").await;
    }
}

async fn forward_audio(
    mut receiver: watch::Receiver<serde_json::Value>,
    emitter: SignalEmitter<'static>,
    subscription_id: String,
) {
    let initial = receiver.borrow().clone();
    emit_audio(&emitter, &initial, &subscription_id, "subscribed").await;
    while receiver.changed().await.is_ok() {
        let snapshot = receiver.borrow().clone();
        emit_audio(&emitter, &snapshot, &subscription_id, "changed").await;
    }
}

#[cfg(test)]
mod tests {
    use super::RequestedStreams;

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
