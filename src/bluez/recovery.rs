use std::{
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bluer::agent::AgentHandle;
use futures::StreamExt;
use serde_json::Value;
use tokio::sync::{Mutex, broadcast};

use crate::{
    backend::{
        AdapterOperation, BackendError, BackendErrorKind, BluetoothBackend, DeviceOperation,
        ObexRemote, ObexTarget, OperationProgress,
    },
    model::Snapshot,
    pairing::PairingBroker,
};

use super::BluezBackend;

mod owner;

// A candidate owns workers before publication. Cancellation while registering
// its agent or restoring startup policy must not leave Arc-owned workers alive.
struct Candidate {
    backend: Arc<BluezBackend>,
    installed: bool,
}

impl Drop for Candidate {
    fn drop(&mut self) {
        if !self.installed {
            self.backend.tasks.abort();
            if let Some(provider) = &self.backend.fast_pair {
                provider.abort();
            }
        }
    }
}

/// Rebuilds the BlueZ backend and pairing agent when the BlueZ bus owner changes.
/// Callers receive typed unavailability errors while recovery is in progress.
pub struct RecoveringBackend {
    current: RwLock<Arc<BluezBackend>>,
    changes: broadcast::Sender<()>,
    agent: Mutex<Option<AgentHandle>>,
    available: AtomicBool,
}

impl RecoveringBackend {
    pub fn new(initial: Arc<BluezBackend>) -> Arc<Self> {
        let (changes, _) = broadcast::channel(64);
        let backend = Arc::new(Self {
            current: RwLock::new(Arc::clone(&initial)),
            changes,
            agent: Mutex::new(None),
            available: AtomicBool::new(true),
        });
        backend.forward_changes(initial);
        backend
    }

    pub async fn set_agent(&self, agent: AgentHandle) {
        *self.agent.lock().await = Some(agent);
    }

    fn current(&self) -> Arc<BluezBackend> {
        self.current
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn forward_changes(&self, backend: Arc<BluezBackend>) {
        let mut receiver = backend.subscribe_changes();
        let changes = self.changes.clone();
        backend
            .tasks
            .spawn("recovering-bluez-change-forwarder", async move {
                loop {
                    match receiver.recv().await {
                        Ok(()) | Err(broadcast::error::RecvError::Lagged(_)) => {
                            let _ = changes.send(());
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    }
                }
            });
    }

    pub fn start_recovery(self: &Arc<Self>, pairing: Arc<PairingBroker>) {
        let backend = Arc::clone(self);
        crate::task::spawn("bluez-session-recovery", async move {
            if let Err(error) = backend.monitor_owner(Arc::clone(&pairing)).await {
                // Setup/stream failure is not proof that the last backend is live.
                backend.invalidate(&pairing);
                backend.stop_generation().await;
                tracing::error!(%error, "BlueZ recovery monitor stopped");
            }
        });
    }

    fn invalidate(&self, pairing: &PairingBroker) {
        self.available.store(false, Ordering::Release);
        pairing.cancel_all("bluez-unavailable");
        let _ = self.changes.send(());
    }

    async fn stop_generation(&self) {
        self.current().shutdown().await;
        self.agent.lock().await.take();
    }

    async fn monitor_owner(&self, pairing: Arc<PairingBroker>) -> Result<()> {
        let connection = zbus::Connection::system()
            .await
            .context("connect BlueZ recovery monitor to system D-Bus")?;
        let proxy = zbus::Proxy::new(
            &connection,
            "org.freedesktop.DBus",
            "/org/freedesktop/DBus",
            "org.freedesktop.DBus",
        )
        .await
        .context("create BlueZ recovery owner proxy")?;
        let changes = proxy
            .receive_signal("NameOwnerChanged")
            .await
            .context("subscribe to BlueZ owner changes")?;
        let events = changes.map(|message| {
            message
                .body()
                .deserialize()
                .context("decode BlueZ owner change")
        });
        owner::drive(events, |new_owner| {
            self.invalidate(&pairing);
            let pairing = &pairing;
            async move {
                self.stop_generation().await;
                if new_owner.is_empty() {
                    tracing::warn!(
                        "BlueZ disappeared; retaining daemon API while waiting for recovery"
                    );
                } else {
                    self.recover(pairing).await;
                }
            }
        })
        .await
    }

    async fn recover(&self, pairing: &Arc<PairingBroker>) {
        tracing::info!("BlueZ appeared; rebuilding Bluetooth backend");
        loop {
            let previous = self.current();
            match Self::replacement(pairing, &previous).await {
                Ok((mut replacement, agent)) => {
                    *self.agent.lock().await = Some(agent);
                    *self
                        .current
                        .write()
                        .unwrap_or_else(|poison| poison.into_inner()) =
                        Arc::clone(&replacement.backend);
                    self.forward_changes(Arc::clone(&replacement.backend));
                    replacement.installed = true;
                    self.available.store(true, Ordering::Release);
                    let _ = self.changes.send(());
                    tracing::info!("Bluetooth backend recovered without restarting bt-daemon");
                    return;
                }
                Err(error) => {
                    tracing::warn!(%error, "Bluetooth backend recovery is retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn replacement(
        pairing: &Arc<PairingBroker>,
        previous: &BluezBackend,
    ) -> Result<(Candidate, AgentHandle)> {
        let replacement = Candidate {
            backend: Arc::new(
                BluezBackend::with_state(
                    Arc::clone(&previous.identities),
                    Arc::clone(&previous.management),
                )
                .await?,
            ),
            installed: false,
        };
        let agent = replacement
            .backend
            .register_agent(pairing.agent())
            .await
            .context("restore the Bluetooth pairing agent")?;
        replacement.backend.apply_startup_policy().await;
        replacement.backend.start_monitoring();
        replacement.backend.start_lifecycle_monitoring();
        Ok((replacement, agent))
    }
}

macro_rules! impl_recovering_backend {
    ($(fn $name:ident($($argument:ident: $type:ty),*) -> $output:ty;)+) => {
        #[async_trait]
        impl BluetoothBackend for RecoveringBackend {
            fn subscribe_changes(&self) -> broadcast::Receiver<()> {
                self.changes.subscribe()
            }

            $(async fn $name(&self, $($argument: $type),*) -> Result<$output> {
                if !self.available.load(Ordering::Acquire) {
                    return Err(BackendError::new(BackendErrorKind::Unavailable, "BlueZ is recovering").into());
                }
                self.current().$name($($argument),*).await
            })+
        }
    };
}

impl_recovering_backend! {
    fn snapshot() -> Snapshot;
    fn set_powered(adapter_key: Option<&str>, powered: bool) -> Snapshot;
    fn set_scanning(adapter_key: Option<&str>, enabled: bool) -> Snapshot;
    fn adapter_operation(adapter_key: &str, operation: AdapterOperation, params: &Value) -> Snapshot;
    fn update_management(params: &Value) -> Snapshot;
    fn update_device_policy(device_key: &str, params: &Value) -> Snapshot;
    fn obex_target(device_key: &str) -> ObexTarget;
    fn obex_remote(source: &str, destination: &str) -> ObexRemote;
    fn device_operation(device_key: &str, operation: DeviceOperation, params: &Value, progress: OperationProgress) -> Snapshot;
}
