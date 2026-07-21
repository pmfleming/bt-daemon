use std::{future::Future, sync::Arc, time::Duration};

use futures::StreamExt;
use serde::Serialize;
use serde_json::json;
use tokio::{
    sync::{broadcast, oneshot},
    task::JoinSet,
};
use zbus::{names::UniqueName, object_server::SignalEmitter};

use crate::{api, backend::BluetoothBackend, pairing::PairingBroker, protocol};

use super::{
    AUDIO_STREAM, BluetoothDaemon, CHANGED_STREAM, OBEX_STREAM, OPERATION_STREAM, PAIRING_STREAM,
    SCAN_STREAM, emit_audio, emit_snapshot, emit_stream,
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
    let backend = Arc::clone(&daemon.backend);
    let pairing = Arc::clone(&daemon.pairing);
    let pairing_events = daemon.pairing.subscribe();
    let operation_events = daemon.operations.subscribe();
    let scan_events = daemon.scans.subscribe();
    let obex_events = daemon.obex_events.subscribe();
    let changes = daemon.backend.subscribe_changes();
    let audio_events = daemon.audio_events.subscribe();

    tracing::info!(%subscription_id, %owner, ?streams, "subscription started");
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
            forward_snapshots(
                changes,
                signal_emitter.clone(),
                Arc::clone(&backend),
                subscription_id.clone(),
            ),
        );
        spawn_if(
            &mut forwarders,
            requested.audio,
            forward_audio(
                audio_events,
                signal_emitter.clone(),
                Arc::clone(&pairing),
                subscription_id.clone(),
            ),
        );
        spawn_if(
            &mut forwarders,
            requested.pairing,
            forward_events(
                pairing_events,
                signal_emitter.clone(),
                PAIRING_STREAM,
                subscription_id.clone(),
                |event| &event.event,
            ),
        );
        spawn_if(
            &mut forwarders,
            requested.operations,
            forward_events(
                operation_events,
                signal_emitter.clone(),
                OPERATION_STREAM,
                subscription_id.clone(),
                |event| &event.event,
            ),
        );
        spawn_if(
            &mut forwarders,
            requested.scans,
            forward_events(
                scan_events,
                signal_emitter.clone(),
                SCAN_STREAM,
                subscription_id.clone(),
                |event| &event.event,
            ),
        );
        spawn_if(
            &mut forwarders,
            requested.obex,
            forward_events(
                obex_events,
                signal_emitter,
                OBEX_STREAM,
                subscription_id.clone(),
                |event| &event.event,
            ),
        );
        forwarders.spawn(wait_for_owner_loss(connection, owner));
        if let Some(Err(error)) = forwarders.join_next().await {
            tracing::error!(%subscription_id, %error, "subscription forwarder task failed");
        }
        forwarders.abort_all();
        subscriptions.lock().await.remove(&subscription_id);
        tracing::info!(%subscription_id, "subscription ended");
    });
    daemon.subscriptions.lock().await.insert(id.clone(), task);
    let _ = start_sender.send(());
    api::success(json!({ "subscription": { "id": id, "streams": streams } })).to_string()
}

async fn wait_for_owner_loss(connection: zbus::Connection, owner: UniqueName<'static>) {
    let result = async {
        let proxy = zbus::Proxy::new(
            &connection,
            "org.freedesktop.DBus",
            "/org/freedesktop/DBus",
            "org.freedesktop.DBus",
        )
        .await?;
        let mut changes = proxy.receive_signal("NameOwnerChanged").await?;
        let has_owner: bool = proxy.call("NameHasOwner", &(owner.as_str(),)).await?;
        if !has_owner {
            return Ok::<(), anyhow::Error>(());
        }
        while let Some(message) = changes.next().await {
            let (name, old_owner, new_owner): (String, String, String) =
                message.body().deserialize()?;
            if name == owner.as_str() && !old_owner.is_empty() && new_owner.is_empty() {
                return Ok::<(), anyhow::Error>(());
            }
        }
        anyhow::bail!("D-Bus owner watch ended")
    }
    .await;
    if let Err(error) = result {
        tracing::warn!(%owner, %error, "subscription owner watch ended");
    }
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
            }
            Err(broadcast::error::RecvError::Closed) => {
                tracing::warn!(%subscription_id, %stream, "subscription event source closed");
                break;
            }
        }
    }
}

async fn forward_snapshots(
    mut receiver: broadcast::Receiver<()>,
    emitter: SignalEmitter<'static>,
    backend: Arc<dyn BluetoothBackend>,
    subscription_id: String,
) {
    emit_snapshot(&emitter, &backend, &subscription_id, "subscribed").await;
    while receive_coalesced(&mut receiver, Duration::from_millis(80), CHANGED_STREAM).await {
        emit_snapshot(&emitter, &backend, &subscription_id, "changed").await;
    }
}

async fn forward_audio(
    mut receiver: broadcast::Receiver<()>,
    emitter: SignalEmitter<'static>,
    pairing: Arc<PairingBroker>,
    subscription_id: String,
) {
    emit_audio(&emitter, &pairing, &subscription_id, "subscribed").await;
    while receive_coalesced(&mut receiver, Duration::from_millis(150), AUDIO_STREAM).await {
        emit_audio(&emitter, &pairing, &subscription_id, "changed").await;
    }
}

async fn receive_coalesced(
    receiver: &mut broadcast::Receiver<()>,
    delay: Duration,
    stream: &'static str,
) -> bool {
    match receiver.recv().await {
        Ok(()) => {}
        Err(broadcast::error::RecvError::Lagged(skipped)) => {
            tracing::warn!(%stream, skipped, "refresh notifications were dropped");
        }
        Err(broadcast::error::RecvError::Closed) => {
            tracing::warn!(%stream, "refresh source closed");
            return false;
        }
    }
    tokio::time::sleep(delay).await;
    while receiver.try_recv().is_ok() {}
    true
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
