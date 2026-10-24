use std::{collections::HashSet, sync::Arc};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use shelllist_daemon_tokio::OwnedTaskRegistry;
use tokio::sync::{Mutex, broadcast, watch};
use zbus::{connection, message::Header, object_server::SignalEmitter};

use crate::{api, backend::BluetoothBackend, pairing::PairingBroker, protocol};

/// Well-known session bus name claimed while the service is running.
pub const BUS_NAME: &str = "org.laufan.BluetoothDaemon";
/// Object path exporting the bt-api D-Bus interface.
pub const OBJECT_PATH: &str = "/org/laufan/BluetoothDaemon";
/// Versioned D-Bus interface for calls, subscriptions and cancellation.
pub const INTERFACE: &str = "org.laufan.BluetoothDaemon2";
/// Stream names accepted by the subscription API.
pub use crate::protocol::stream::{
    AUDIO as AUDIO_STREAM, CHANGED as CHANGED_STREAM, OBEX as OBEX_STREAM,
    OPERATION as OPERATION_STREAM, PAIRING as PAIRING_STREAM, SCAN as SCAN_STREAM,
};

mod audio;
mod obex;
mod operation;
mod published;
mod scan;
mod socket;
mod subscription;

use self::{
    obex::ObexCoordinator, operation::OperationCoordinator, published::Published,
    scan::ScanCoordinator, subscription::SharedSnapshot,
};

/// Owns request coordinators and cached snapshots shared by session-bus clients.
pub struct BluetoothDaemon {
    boundary: shelllist_daemon_tokio::ServiceBoundary,
    backend: Arc<dyn BluetoothBackend>,
    pairing: Arc<PairingBroker>,
    subscriptions: Arc<OwnedTaskRegistry>,
    scan_owner_watches: Arc<Mutex<HashSet<String>>>,
    tasks: Arc<shelllist_daemon_tokio::TaskGroup>,
    operations: Arc<OperationCoordinator>,
    scans: ScanCoordinator,
    snapshots: watch::Sender<Published<SharedSnapshot>>,
    audio_snapshots: watch::Sender<Published<Value>>,
    obex: Arc<ObexCoordinator>,
}

impl BluetoothDaemon {
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
            "bluetooth.scan" => self.start_scan(&params, owner, connection).await,
            "bluetooth.obex.send" => {
                self.obex
                    .outgoing
                    .start_owned(&params, owner.map(str::to_owned))
                    .await
            }
            "bluetooth.obex.respond" => obex::respond(&self.obex.incoming, &params).await,
            "bluetooth.obex.snapshot" => self.obex.snapshot().await,
            "bluetooth.audio.snapshot" => self.audio_snapshots.borrow().response(),
            "bluetooth.audio.setProfile" => {
                let backend = Arc::clone(&self.backend);
                audio::apply_change(
                    &self.pairing,
                    &params,
                    audio::PROFILE,
                    move |device, profile| backend.remember_audio_profile(device, profile),
                )
                .await
            }
            "bluetooth.audio.setDefault" => {
                audio::apply_change(&self.pairing, &params, audio::DEFAULT, |_, _| Ok(())).await
            }
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
            "bluetooth.snapshot" => self.snapshots.borrow().response(),
            "bluetooth.pairing.respond" => match self.pairing.respond(&params).await {
                Ok(accepted) => api::success(json!({ "result": { "accepted": accepted } })),
                Err(error) => api::error("pairing-response-rejected", format!("{error:#}")),
            },
            _ => api::dispatch(Arc::clone(&self.backend), method, params).await,
        }
    }

    async fn start_scan(
        &self,
        params: &Value,
        owner: Option<&str>,
        connection: Option<&zbus::Connection>,
    ) -> Value {
        let owner = owner.unwrap_or("internal");
        let response = self.scans.start(params, owner).await;
        if response["ok"].as_bool() == Some(true)
            && let Some(connection) = connection
        {
            self.watch_scan_owner(connection, owner.to_owned()).await;
        }
        response
    }

    async fn watch_scan_owner(&self, connection: &zbus::Connection, owner: String) {
        let mut watches = self.scan_owner_watches.lock().await;
        if !watches.insert(owner.clone()) {
            return;
        }
        drop(watches);
        let scans = self.scans.clone();
        let watches = Arc::clone(&self.scan_owner_watches);
        let monitor = self.subscriptions.owner_monitor(connection);
        self.tasks.spawn("scan-owner-watch", async move {
            if let Err(error) = monitor.wait(&owner).await {
                tracing::warn!(%owner, %error, "scan owner monitoring failed; releasing scans");
            }
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
                .stop_owned(Some(request_id), owner.unwrap_or("internal"), "cancelled")
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

#[zbus::interface(name = "org.laufan.BluetoothDaemon2")]
impl BluetoothDaemon {
    async fn call(
        &self,
        method: &str,
        params_json: &str,
        #[zbus(header)] header: Header<'_>,
        #[zbus(connection)] connection: &zbus::Connection,
    ) -> String {
        let admitted =
            match self
                .boundary
                .call(header.sender().map(|s| s.as_str()), method, params_json)
            {
                Ok(admitted) => admitted,
                Err(response) => return response,
            };
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
        admitted.encode(&response)
    }

    async fn subscribe(
        &self,
        streams: Vec<String>,
        #[zbus(header)] header: Header<'_>,
        #[zbus(signal_emitter)] emitter: SignalEmitter<'_>,
    ) -> String {
        let admitted = match self
            .boundary
            .subscribe(header.sender().map(|s| s.as_str()), &streams)
        {
            Ok(admitted) => admitted,
            Err(response) => return response,
        };
        let Some(sender) = header.sender().map(|sender| sender.to_owned()) else {
            return api::error(
                "subscription-unavailable",
                "D-Bus caller identity is unavailable".to_string(),
            )
            .to_string();
        };
        admitted.encoded(subscription::start(self, streams, sender, emitter).await)
    }

    async fn cancel(&self, request_id: &str, #[zbus(header)] header: Header<'_>) -> String {
        let admitted = match self
            .boundary
            .cancel(header.sender().map(|s| s.as_str()), request_id)
        {
            Ok(admitted) => admitted,
            Err(response) => return response,
        };
        let owner = header.sender().map(ToString::to_string);
        admitted.encoded(self.cancel_owned(request_id, owner.as_deref()).await)
    }

    #[zbus(signal)]
    async fn event(emitter: &SignalEmitter<'_>, stream: &str, event_json: &str)
    -> zbus::Result<()>;
}

/// Export the session service and run until shutdown is requested.
/// Returns an error if service registration or monitor startup fails.
pub async fn run(backend: Arc<dyn BluetoothBackend>, pairing: Arc<PairingBroker>) -> Result<()> {
    let mut changes = backend.subscribe_changes();
    let (snapshots, _) = watch::channel(Published::new(SharedSnapshot::Loading));

    let (audio_events, _) = broadcast::channel(32);
    let operations = OperationCoordinator::new(Arc::clone(&backend));
    let scans = ScanCoordinator::new(Arc::clone(&backend));
    let obex = ObexCoordinator::new(Arc::clone(&backend));
    audio::start_monitor(audio_events.clone())?;
    let mut audio_changes = audio_events.subscribe();
    let (audio_snapshots, _) = watch::channel(Published::new(api::error(
        "snapshot-loading",
        "Bluetooth audio snapshot is loading".to_string(),
    )));
    let subscriptions = Arc::new(OwnedTaskRegistry::default());
    let tasks = Arc::new(shelllist_daemon_tokio::TaskGroup::default());
    let connection = connection::Builder::session()
        .context("connect to session D-Bus")?
        .name(BUS_NAME)
        .context("claim bt-daemon bus name")?
        .serve_at(
            OBJECT_PATH,
            BluetoothDaemon {
                boundary: shelllist_daemon_tokio::ServiceBoundary::new(
                    shelllist_daemon_core::ApiIdentity::new(
                        protocol::NAME,
                        protocol::VERSION as u32,
                    ),
                ),
                backend: Arc::clone(&backend),
                pairing: Arc::clone(&pairing),
                subscriptions: Arc::clone(&subscriptions),
                scan_owner_watches: Arc::new(Mutex::new(HashSet::new())),
                tasks: Arc::clone(&tasks),
                operations,
                scans,
                snapshots: snapshots.clone(),
                audio_snapshots: audio_snapshots.clone(),
                obex: Arc::clone(&obex),
            },
        )
        .context("export bt-daemon D-Bus interface")?
        .serve_at(obex::AGENT_PATH, obex.agent())
        .context("export incoming OBEX agent")?
        .build()
        .await
        .context("start bt-daemon D-Bus service")?;
    // The daemon owns this connection even when the optional OBEX agent is disabled.
    // Moving its last handle into activate() releases our bus name on that path.
    obex.activate(connection.clone()).await;

    // Claim the service name before probing BlueZ or PipeWire. Clients can
    // connect immediately and observe a typed loading state while snapshots
    // are populated in the background.
    tasks.spawn("bluetooth-snapshots", async move {
        Published::update(&snapshots, backend.snapshot().await.into());
        while receive_refresh(&mut changes, std::time::Duration::from_millis(80)).await {
            Published::update(&snapshots, backend.snapshot().await.into());
        }
    });
    tasks.spawn("bluetooth-audio-snapshots", async move {
        Published::update(&audio_snapshots, audio::snapshot(&pairing).await);
        while receive_refresh(&mut audio_changes, std::time::Duration::from_millis(150)).await {
            Published::update(&audio_snapshots, audio::snapshot(&pairing).await);
        }
    });
    tracing::info!(
        bus_name = BUS_NAME,
        object_path = OBJECT_PATH,
        "bt-daemon started"
    );
    let (socket_stop, stopped) = tokio::sync::oneshot::channel();
    let mut socket_task = tokio::spawn(socket::listen(connection.clone(), async {
        let _ = stopped.await;
    }));
    let (mut result, joined) = tokio::select! {
        result = shelllist_daemon_tokio::wait_for_shutdown() => (result, false),
        result = &mut socket_task => (result.context("join Bluetooth socket listener").and_then(|result| result), true),
    };
    let _ = socket_stop.send(());
    if !joined {
        result = result.and(
            socket_task
                .await
                .context("join Bluetooth socket shutdown")
                .and_then(|result| result),
        );
    }
    subscriptions.shutdown().await;
    tasks.shutdown().await;
    drop(connection);
    result
}

pub(crate) fn snapshot_ordering_fixture() -> Value {
    subscription::snapshot_ordering_fixture()
}

async fn receive_refresh(
    receiver: &mut broadcast::Receiver<()>,
    delay: std::time::Duration,
) -> bool {
    match receiver.recv().await {
        Ok(()) | Err(broadcast::error::RecvError::Lagged(_)) => {}
        Err(broadcast::error::RecvError::Closed) => return false,
    }
    tokio::time::sleep(delay).await;
    while receiver.try_recv().is_ok() {}
    true
}

#[cfg(test)]
mod tests;
