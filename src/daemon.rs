use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, broadcast},
    task::JoinHandle,
};
use zbus::{connection, object_server::SignalEmitter};

use crate::{
    api, audio as pipewire_audio, backend::BluetoothBackend, obex as bluez_obex,
    pairing::PairingBroker, protocol,
};

pub const BUS_NAME: &str = "org.laufan.BluetoothDaemon";
pub const OBJECT_PATH: &str = "/org/laufan/BluetoothDaemon";
pub const INTERFACE: &str = "org.laufan.BluetoothDaemon1";
pub use crate::protocol::stream::{
    AUDIO as AUDIO_STREAM, CHANGED as CHANGED_STREAM, OBEX as OBEX_STREAM,
    OPERATION as OPERATION_STREAM, PAIRING as PAIRING_STREAM, SCAN as SCAN_STREAM,
};

mod audio;
mod obex;
mod operation;
mod scan;

use self::{obex::OutgoingTransfers, operation::OperationCoordinator, scan::ScanCoordinator};

pub struct BluetoothDaemon {
    backend: Arc<dyn BluetoothBackend>,
    pairing: Arc<PairingBroker>,
    sequence: AtomicU64,
    subscriptions: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    operations: OperationCoordinator,
    scans: ScanCoordinator,
    audio_events: broadcast::Sender<()>,
    obex_events: broadcast::Sender<crate::obex::ObexEvent>,
    outgoing_obex: OutgoingTransfers,
    incoming_obex: Arc<crate::obex::IncomingBroker>,
}

impl BluetoothDaemon {
    fn next_id(&self, prefix: &str) -> String {
        format!("{prefix}-{}", self.sequence.fetch_add(1, Ordering::Relaxed))
    }

    async fn dispatch_call(&self, method: &str, params: Value) -> Value {
        match method {
            "bluetooth.scan" => self.scans.start(&params).await,
            "bluetooth.obex.send" => self.outgoing_obex.start(&params).await,
            "bluetooth.obex.respond" => obex::respond(&self.incoming_obex, &params).await,
            "bluetooth.obex.snapshot" => self.obex_snapshot().await,
            "bluetooth.audio.snapshot" => audio::snapshot(Arc::clone(&self.pairing)).await,
            "bluetooth.audio.setProfile" => audio::set_profile(&self.pairing, &params).await,
            "bluetooth.device.operation" => self.operations.start(params).await,
            "bluetooth.pairing.respond" => match self.pairing.respond(&params).await {
                Ok(accepted) => api::success(json!({ "result": { "accepted": accepted } })),
                Err(error) => api::error("pairing-response-rejected", format!("{error:#}")),
            },
            _ => api::dispatch(Arc::clone(&self.backend), method, params).await,
        }
    }

    async fn obex_snapshot(&self) -> Value {
        match bluez_obex::probe(self.incoming_obex.is_available()).await {
            Ok(capabilities) => api::success(json!({ "obex": capabilities })),
            Err(error) => api::success(json!({ "obex": {
                "available": false,
                "outgoing_object_push": false,
                "incoming_authorization": false,
                "transfer_progress": false,
                "cancellation": false,
                "reason": format!("{error:#}"),
            }})),
        }
    }
}

#[zbus::interface(name = "org.laufan.BluetoothDaemon1")]
impl BluetoothDaemon {
    async fn call(&self, method: &str, params_json: &str) -> String {
        let params = match serde_json::from_str(params_json) {
            Ok(params) => params,
            Err(error) => {
                return api::error("validation-error", format!("invalid params JSON: {error}"))
                    .to_string();
            }
        };
        self.dispatch_call(method, params).await.to_string()
    }

    async fn subscribe(
        &self,
        streams: Vec<String>,
        #[zbus(signal_emitter)] emitter: SignalEmitter<'_>,
    ) -> String {
        if streams.is_empty()
            || streams.iter().any(|requested| {
                !protocol::STREAMS
                    .iter()
                    .any(|(supported, _)| requested == supported)
            })
        {
            return api::error(
                "unsupported-stream",
                "Subscriptions require bluetooth.changed, pairing.request, bluetooth.operation, bluetooth.scan, bluetooth.audio.changed, and/or bluetooth.obex.transfer"
                    .to_string(),
            )
            .to_string();
        }
        let wants = |target| streams.iter().any(|stream| stream == target);
        let wants_changes = wants(CHANGED_STREAM);
        let wants_pairing = wants(PAIRING_STREAM);
        let wants_operations = wants(OPERATION_STREAM);
        let wants_scans = wants(SCAN_STREAM);
        let wants_audio = wants(AUDIO_STREAM);
        let wants_obex = wants(OBEX_STREAM);
        let id = self.next_id("subscription");
        let mut changes = self.backend.subscribe_changes();
        let mut pairing_events = self.pairing.subscribe();
        let mut operation_events = self.operations.subscribe();
        let mut scan_events = self.scans.subscribe();
        let mut audio_events = self.audio_events.subscribe();
        let mut obex_events = self.obex_events.subscribe();
        let backend = Arc::clone(&self.backend);
        let pairing = Arc::clone(&self.pairing);
        let signal_emitter = emitter.to_owned();
        let subscription_id = id.clone();
        let task = tokio::spawn(async move {
            if wants_changes {
                emit_snapshot(&signal_emitter, &backend, &subscription_id, "subscribed").await;
            }
            if wants_audio {
                emit_audio(&signal_emitter, &pairing, &subscription_id, "subscribed").await;
            }
            loop {
                tokio::select! {
                    result = changes.recv(), if wants_changes => match result {
                        Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            tokio::time::sleep(std::time::Duration::from_millis(80)).await;
                            while changes.try_recv().is_ok() {}
                            emit_snapshot(&signal_emitter, &backend, &subscription_id, "changed").await;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    },
                    result = pairing_events.recv(), if wants_pairing => match result {
                        Ok(event) => emit_stream(&signal_emitter, PAIRING_STREAM, &subscription_id, &event.event, &event).await,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    },
                    result = operation_events.recv(), if wants_operations => match result {
                        Ok(event) => emit_stream(&signal_emitter, OPERATION_STREAM, &subscription_id, &event.event, &event).await,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    },
                    result = scan_events.recv(), if wants_scans => match result {
                        Ok(event) => emit_stream(&signal_emitter, SCAN_STREAM, &subscription_id, &event.event, &event).await,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    },
                    result = audio_events.recv(), if wants_audio => match result {
                        Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                            while audio_events.try_recv().is_ok() {}
                            emit_audio(&signal_emitter, &pairing, &subscription_id, "changed").await;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    },
                    result = obex_events.recv(), if wants_obex => match result {
                        Ok(event) => emit_stream(&signal_emitter, OBEX_STREAM, &subscription_id, &event.event, &event).await,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        });
        self.subscriptions.lock().await.insert(id.clone(), task);
        api::success(json!({ "subscription": { "id": id, "streams": streams } })).to_string()
    }

    async fn cancel(&self, request_id: &str) -> String {
        if let Some(task) = self.subscriptions.lock().await.remove(request_id) {
            task.abort();
            return api::success(json!({ "cancelled": request_id, "kind": "subscription" }))
                .to_string();
        }
        if self.scans.contains(request_id).await {
            return self
                .scans
                .stop(Some(request_id), "cancelled")
                .await
                .to_string();
        }
        if self.operations.cancel(request_id).await {
            return api::success(json!({ "cancelled": request_id, "kind": "operation" }))
                .to_string();
        }
        if self.outgoing_obex.cancel(request_id).await {
            return api::success(json!({ "cancelled": request_id, "kind": "obex-transfer" }))
                .to_string();
        }
        if self.incoming_obex.cancel_transfer(request_id).await {
            return api::success(json!({ "cancelled": request_id, "kind": "obex-transfer" }))
                .to_string();
        }
        api::error(
            "request-not-found",
            format!("No active subscription or operation named {request_id}"),
        )
        .to_string()
    }

    #[zbus(signal)]
    async fn event(emitter: &SignalEmitter<'_>, stream: &str, event_json: &str)
    -> zbus::Result<()>;
}

async fn emit_snapshot(
    emitter: &SignalEmitter<'_>,
    backend: &Arc<dyn BluetoothBackend>,
    subscription_id: &str,
    event: &str,
) {
    let value = match backend.snapshot().await {
        Ok(snapshot) => json!({
            "protocol": api::PROTOCOL,
            "version": api::VERSION,
            "stream": CHANGED_STREAM,
            "event": event,
            "subscription_id": subscription_id,
            "data": { "snapshot": snapshot }
        }),
        Err(error) => json!({
            "protocol": api::PROTOCOL,
            "version": api::VERSION,
            "stream": CHANGED_STREAM,
            "event": "unavailable",
            "subscription_id": subscription_id,
            "error": { "code": "bluez-unavailable", "message": format!("{error:#}") }
        }),
    };
    if let Err(error) = BluetoothDaemon::event(emitter, CHANGED_STREAM, &value.to_string()).await {
        tracing::debug!(%error, "could not emit Bluetooth subscription event");
    }
}

async fn emit_stream<T: Serialize>(
    emitter: &SignalEmitter<'_>,
    stream: &str,
    subscription_id: &str,
    event: &str,
    data: &T,
) {
    let value = json!({
        "protocol": api::PROTOCOL,
        "version": api::VERSION,
        "stream": stream,
        "event": event,
        "subscription_id": subscription_id,
        "data": data,
    });
    if let Err(error) = BluetoothDaemon::event(emitter, stream, &value.to_string()).await {
        tracing::debug!(%error, %stream, "could not emit subscription event");
    }
}

async fn emit_audio(
    emitter: &SignalEmitter<'_>,
    pairing: &Arc<PairingBroker>,
    subscription_id: &str,
    event: &str,
) {
    let envelope = audio::snapshot(Arc::clone(pairing)).await;
    let value = if envelope["ok"].as_bool().unwrap_or(false) {
        json!({
            "protocol": api::PROTOCOL,
            "version": api::VERSION,
            "stream": AUDIO_STREAM,
            "event": event,
            "subscription_id": subscription_id,
            "data": { "audio_devices": envelope["data"]["audio_devices"].clone() }
        })
    } else {
        json!({
            "protocol": api::PROTOCOL,
            "version": api::VERSION,
            "stream": AUDIO_STREAM,
            "event": "unavailable",
            "subscription_id": subscription_id,
            "error": envelope["error"].clone()
        })
    };
    if let Err(error) = BluetoothDaemon::event(emitter, AUDIO_STREAM, &value.to_string()).await {
        tracing::debug!(%error, "could not emit Bluetooth audio event");
    }
}

pub async fn run(backend: Arc<dyn BluetoothBackend>, pairing: Arc<PairingBroker>) -> Result<()> {
    let (audio_events, _) = broadcast::channel(32);
    let (obex_events, _) = broadcast::channel(32);
    let operations = OperationCoordinator::new(Arc::clone(&backend));
    let scans = ScanCoordinator::new(Arc::clone(&backend));
    let outgoing_obex = OutgoingTransfers::new(Arc::clone(&backend), obex_events.clone());
    let incoming_obex = bluez_obex::IncomingBroker::new(Arc::clone(&backend), obex_events.clone());
    let monitor_events = audio_events.clone();
    std::thread::spawn(move || {
        loop {
            let events = monitor_events.clone();
            let notify = Arc::new(move || {
                let _ = events.send(());
            });
            if let Err(error) = pipewire_audio::monitor(notify) {
                tracing::warn!(%error, "PipeWire audio monitor is retrying");
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    });
    let connection = connection::Builder::session()
        .context("connect to session D-Bus")?
        .name(BUS_NAME)
        .context("claim bt-daemon bus name")?
        .serve_at(
            OBJECT_PATH,
            BluetoothDaemon {
                backend,
                pairing,
                sequence: AtomicU64::new(1),
                subscriptions: Arc::new(Mutex::new(HashMap::new())),
                operations,
                scans,
                audio_events,
                obex_events,
                outgoing_obex,
                incoming_obex: Arc::clone(&incoming_obex),
            },
        )
        .context("export bt-daemon D-Bus interface")?
        .serve_at(
            bluez_obex::AGENT_PATH,
            bluez_obex::ObexAgent::new(Arc::clone(&incoming_obex)),
        )
        .context("export incoming OBEX agent")?
        .build()
        .await
        .context("start bt-daemon D-Bus service")?;
    incoming_obex.set_connection(connection.clone());
    if let Err(error) = bluez_obex::register_agent(&connection, &incoming_obex).await {
        tracing::warn!(%error, "incoming OBEX authorization is unavailable");
    }
    bluez_obex::monitor_agent_owner(connection, incoming_obex);
    tracing::info!(
        bus_name = BUS_NAME,
        object_path = OBJECT_PATH,
        "bt-daemon started"
    );
    watch_bluez_owner().await
}

async fn watch_bluez_owner() -> Result<()> {
    let connection = zbus::Connection::system()
        .await
        .context("connect BlueZ owner watcher to system D-Bus")?;
    let proxy = zbus::Proxy::new(
        &connection,
        "org.freedesktop.DBus",
        "/org/freedesktop/DBus",
        "org.freedesktop.DBus",
    )
    .await
    .context("create system D-Bus owner proxy")?;
    let mut changes = proxy
        .receive_signal("NameOwnerChanged")
        .await
        .context("subscribe to system bus owner changes")?;
    while let Some(message) = changes.next().await {
        let (name, old_owner, new_owner): (String, String, String) =
            message
                .body()
                .deserialize()
                .context("decode system bus owner change")?;
        if name == "org.bluez" && old_owner != new_owner {
            bail!(
                "BlueZ owner changed; restarting bt-daemon to rebuild sessions and pairing agent"
            );
        }
    }
    bail!("BlueZ owner watch ended")
}

#[cfg(test)]
mod tests;
