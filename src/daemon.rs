use std::{collections::HashSet, sync::Arc};

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{Value, json};
use shelllist_daemon_tokio::OwnedTaskRegistry;
use tokio::sync::{Mutex, broadcast};
use zbus::{connection, message::Header, object_server::SignalEmitter};

use crate::{api, backend::BluetoothBackend, pairing::PairingBroker, protocol};

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
mod subscription;

use self::{obex::ObexCoordinator, operation::OperationCoordinator, scan::ScanCoordinator};

pub struct BluetoothDaemon {
    backend: Arc<dyn BluetoothBackend>,
    pairing: Arc<PairingBroker>,
    subscriptions: Arc<OwnedTaskRegistry>,
    scan_owner_watches: Arc<Mutex<HashSet<String>>>,
    operations: OperationCoordinator,
    scans: ScanCoordinator,
    audio_events: broadcast::Sender<()>,
    obex: Arc<ObexCoordinator>,
}

impl BluetoothDaemon {
    fn next_id(&self, prefix: &str) -> String {
        self.subscriptions.next_id(prefix)
    }

    async fn dispatch_call(
        &self,
        method: &str,
        params: Value,
        owner: Option<&str>,
        connection: Option<&zbus::Connection>,
    ) -> Value {
        match method {
            "bluetooth.protocol.describe" => {
                api::success(json!({ "registry": protocol::registry() }))
            }
            "bluetooth.scan" => {
                let owner = owner.unwrap_or("internal");
                let response = self.scans.start(&params, owner).await;
                if response["ok"].as_bool() == Some(true)
                    && let Some(connection) = connection
                {
                    self.watch_scan_owner(connection.clone(), owner.to_string())
                        .await;
                }
                response
            }
            "bluetooth.obex.send" => {
                self.obex
                    .outgoing
                    .start_owned(&params, owner.map(str::to_owned))
                    .await
            }
            "bluetooth.obex.respond" => obex::respond(&self.obex.incoming, &params).await,
            "bluetooth.obex.snapshot" => self.obex.snapshot().await,
            "bluetooth.audio.snapshot" => audio::snapshot(Arc::clone(&self.pairing)).await,
            "bluetooth.audio.setProfile" => audio::set_profile(&self.pairing, &params).await,
            "bluetooth.audio.setDefault" => audio::set_default(&self.pairing, &params).await,
            "bluetooth.requests.snapshot" => api::success(json!({
                "requests": {
                    "operations": self.operations.snapshot().await,
                    "scans": self.scans.snapshot().await,
                    "pairing": { "active": self.pairing.pending_events() },
                }
            })),
            "bluetooth.device.operation" => {
                self.operations
                    .start_owned(params, owner.map(str::to_owned))
                    .await
            }
            "bluetooth.pairing.respond" => match self.pairing.respond(&params).await {
                Ok(accepted) => api::success(json!({ "result": { "accepted": accepted } })),
                Err(error) => api::error("pairing-response-rejected", format!("{error:#}")),
            },
            _ => api::dispatch(Arc::clone(&self.backend), method, params).await,
        }
    }

    async fn watch_scan_owner(&self, connection: zbus::Connection, owner: String) {
        let mut watches = self.scan_owner_watches.lock().await;
        if !watches.insert(owner.clone()) {
            return;
        }
        drop(watches);
        let scans = self.scans.clone();
        let watches = Arc::clone(&self.scan_owner_watches);
        crate::task::spawn("scan-owner-watch", async move {
            let _ = shelllist_daemon_tokio::wait_for_owner_name_loss(&connection, &owner).await;
            tracing::info!(%owner, "D-Bus owner disappeared; releasing its Bluetooth scans");
            scans.stop_owner(&owner).await;
            watches.lock().await.remove(&owner);
        });
    }

    async fn cancel_owned(&self, request_id: &str, owner: Option<&str>) -> String {
        tracing::info!(%request_id, "cancellation requested");
        if self.subscriptions.cancel_owned(request_id, owner).await {
            return api::success(json!({ "cancelled": request_id, "kind": "subscription" }))
                .to_string();
        }
        if self
            .scans
            .is_owned_by(request_id, owner.unwrap_or("internal"))
            .await
        {
            return self
                .scans
                .stop(Some(request_id), "cancelled")
                .await
                .to_string();
        }
        if self.operations.cancel_owned(request_id, owner).await {
            return api::success(json!({ "cancelled": request_id, "kind": "operation" }))
                .to_string();
        }
        if self.obex.outgoing.cancel_owned(request_id, owner).await {
            return api::success(json!({ "cancelled": request_id, "kind": "obex-outgoing" }))
                .to_string();
        }
        if owner.is_none()
            && let Some(kind) = self.obex.cancel(request_id).await
        {
            return api::success(json!({ "cancelled": request_id, "kind": kind })).to_string();
        }
        tracing::warn!(%request_id, "cancellation target was not found");
        api::error(
            "request-not-found",
            format!("No active subscription or operation named {request_id}"),
        )
        .to_string()
    }
}

#[zbus::interface(name = "org.laufan.BluetoothDaemon1")]
impl BluetoothDaemon {
    async fn call(
        &self,
        method: &str,
        params_json: &str,
        #[zbus(header)] header: Header<'_>,
        #[zbus(connection)] connection: &zbus::Connection,
    ) -> String {
        tracing::info!(%method, "D-Bus request received");
        let params = match serde_json::from_str(params_json) {
            Ok(params) => params,
            Err(error) => {
                tracing::warn!(%method, %error, "D-Bus request contains invalid JSON");
                let response =
                    api::error("validation-error", format!("invalid params JSON: {error}"));
                api::log_response(method, &response);
                return response.to_string();
            }
        };
        let owner = header.sender().map(|sender| sender.as_str());
        let response = self
            .dispatch_call(method, params, owner, Some(connection))
            .await;
        api::log_response(method, &response);
        response.to_string()
    }

    async fn subscribe(
        &self,
        streams: Vec<String>,
        #[zbus(header)] header: Header<'_>,
        #[zbus(signal_emitter)] emitter: SignalEmitter<'_>,
    ) -> String {
        let Some(sender) = header.sender().map(|sender| sender.to_owned()) else {
            return api::error(
                "subscription-unavailable",
                "D-Bus caller identity is unavailable".to_string(),
            )
            .to_string();
        };
        subscription::start(self, streams, sender, emitter).await
    }

    async fn cancel(&self, request_id: &str, #[zbus(header)] header: Header<'_>) -> String {
        let owner = header.sender().map(ToString::to_string);
        self.cancel_owned(request_id, owner.as_deref()).await
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
        tracing::warn!(%error, %subscription_id, "could not emit Bluetooth subscription event");
    }
}

async fn emit_stream<T: Serialize>(
    emitter: &SignalEmitter<'_>,
    stream: &str,
    subscription_id: &str,
    event: &str,
    data: &T,
) {
    let value = shelllist_daemon_core::event_envelope(
        shelllist_daemon_core::ApiIdentity::new(api::PROTOCOL, api::VERSION as u32),
        stream,
        event,
        shelllist_daemon_core::Correlation::Subscription(subscription_id),
        json!({ "data": data }),
    );
    if let Err(error) = BluetoothDaemon::event(emitter, stream, &value.to_string()).await {
        tracing::warn!(%error, %stream, %subscription_id, "could not emit subscription event");
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
        tracing::warn!(%error, %subscription_id, "could not emit Bluetooth audio event");
    }
}

pub async fn run(backend: Arc<dyn BluetoothBackend>, pairing: Arc<PairingBroker>) -> Result<()> {
    let (audio_events, _) = broadcast::channel(32);
    let operations = OperationCoordinator::new(Arc::clone(&backend));
    let scans = ScanCoordinator::new(Arc::clone(&backend));
    let obex = ObexCoordinator::new(Arc::clone(&backend));
    audio::start_monitor(audio_events.clone())?;
    let connection = connection::Builder::session()
        .context("connect to session D-Bus")?
        .name(BUS_NAME)
        .context("claim bt-daemon bus name")?
        .serve_at(
            OBJECT_PATH,
            BluetoothDaemon {
                backend,
                pairing,
                subscriptions: Arc::new(OwnedTaskRegistry::default()),
                scan_owner_watches: Arc::new(Mutex::new(HashSet::new())),
                operations,
                scans,
                audio_events,
                obex: Arc::clone(&obex),
            },
        )
        .context("export bt-daemon D-Bus interface")?
        .serve_at(obex::AGENT_PATH, obex.agent())
        .context("export incoming OBEX agent")?
        .build()
        .await
        .context("start bt-daemon D-Bus service")?;
    obex.activate(connection).await;
    tracing::info!(
        bus_name = BUS_NAME,
        object_path = OBJECT_PATH,
        "bt-daemon started"
    );
    shelllist_daemon_tokio::wait_for_shutdown().await
}

#[cfg(test)]
mod tests;
