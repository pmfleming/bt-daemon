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
    identity::DeviceIdentityRegistry,
    management::ManagementStore,
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
            if let Some(provider) = &self.backend.sony {
                provider.abort();
            }
        }
    }
}

/// Rebuilds the BlueZ backend and pairing agent when the BlueZ bus owner changes.
/// Callers receive typed unavailability errors while recovery is in progress.
pub struct RecoveringBackend {
    current: RwLock<Option<Arc<BluezBackend>>>,
    identities: Arc<DeviceIdentityRegistry>,
    management: Arc<ManagementStore>,
    changes: broadcast::Sender<()>,
    agent: Mutex<Option<AgentHandle>>,
    available: AtomicBool,
}

impl RecoveringBackend {
    /// Load durable state without contacting BlueZ. Recovery initializes the first generation.
    pub fn load_default() -> Result<Arc<Self>> {
        let (changes, _) = broadcast::channel(64);
        Ok(Arc::new(Self {
            current: RwLock::new(None),
            identities: DeviceIdentityRegistry::load_default()?,
            management: Arc::new(ManagementStore::load_default()?),
            changes,
            agent: Mutex::new(None),
            available: AtomicBool::new(false),
        }))
    }

    pub fn identity_registry(&self) -> Arc<DeviceIdentityRegistry> {
        Arc::clone(&self.identities)
    }

    fn current(&self) -> Result<Arc<BluezBackend>> {
        let current = self
            .current
            .read()
            .unwrap_or_else(|poison| poison.into_inner());
        if self.available.load(Ordering::Acquire)
            && let Some(backend) = current.as_ref()
        {
            return Ok(Arc::clone(backend));
        }
        Err(BackendError::new(BackendErrorKind::Unavailable, "BlueZ is recovering").into())
    }

    fn forward_changes(&self, backend: Arc<BluezBackend>) {
        let receiver = backend.subscribe_changes();
        let changes = self.changes.clone();
        backend.tasks.spawn(
            "recovering-bluez-change-forwarder",
            shelllist_daemon_tokio::forward_broadcast(receiver, move |_| {
                // Both a change and a gap require a fresh snapshot.
                let _ = changes.send(());
                std::future::ready(())
            }),
        );
    }

    pub fn start_recovery(self: &Arc<Self>, pairing: Arc<PairingBroker>) {
        let backend = Arc::clone(self);
        crate::task::spawn("bluez-session-recovery", async move {
            loop {
                if let Err(error) = backend.monitor_owner(Arc::clone(&pairing)).await {
                    // Setup/stream failure is not proof that the last backend is live.
                    backend.invalidate(&pairing);
                    backend.stop_generation().await;
                    tracing::warn!(%error, "BlueZ recovery monitor is retrying");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    fn invalidate(&self, pairing: &PairingBroker) {
        self.available.store(false, Ordering::Release);
        pairing.cancel_all("bluez-unavailable");
        let _ = self.changes.send(());
    }

    async fn stop_generation(&self) {
        // Keep the retiring generation reachable until shutdown completes. An
        // owner change can cancel this future; the next pass must finish cleanup.
        let previous = self
            .current
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        if let Some(previous) = previous {
            previous.shutdown().await;
        }
        self.current
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
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
        // Subscribe before checking current ownership so startup does not depend on
        // a future NameOwnerChanged signal, and owner transitions cannot be missed.
        let present: bool = proxy
            .call("NameHasOwner", &("org.bluez",))
            .await
            .context("query initial BlueZ owner")?;
        let initial_owner = if present { "initial" } else { "" }.to_string();
        let events = changes.map(|message| {
            message
                .body()
                .deserialize()
                .context("decode BlueZ owner change")
        });
        owner::drive(events, Some(initial_owner), |new_owner| {
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
            match self.replacement(pairing).await {
                Ok((mut replacement, agent)) => {
                    *self.agent.lock().await = Some(agent);
                    *self
                        .current
                        .write()
                        .unwrap_or_else(|poison| poison.into_inner()) =
                        Some(Arc::clone(&replacement.backend));
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

    async fn replacement(&self, pairing: &Arc<PairingBroker>) -> Result<(Candidate, AgentHandle)> {
        let replacement = Candidate {
            backend: Arc::new(
                BluezBackend::with_state(
                    Arc::clone(&self.identities),
                    Arc::clone(&self.management),
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

            fn remember_audio_profile(&self, device_key: &str, profile_key: &str) -> Result<()> {
                self.current()?.remember_audio_profile(device_key, profile_key)
            }

            $(async fn $name(&self, $($argument: $type),*) -> Result<$output> {
                self.current()?.$name($($argument),*).await
            })+
        }
    };
}

impl_recovering_backend! {
    fn snapshot() -> Snapshot;
    fn set_powered(adapter_key: Option<&str>, powered: bool) -> Snapshot;
    fn set_scanning(adapter_key: Option<&str>, enabled: bool) -> Snapshot;
    fn adapter_operation(adapter_key: &str, operation: AdapterOperation, params: &Value) -> ();
    fn update_management(params: &Value) -> Snapshot;
    fn update_device_policy(device_key: &str, params: &Value) -> Snapshot;
    fn obex_target(device_key: &str) -> ObexTarget;
    fn obex_remote(source: &str, destination: &str) -> ObexRemote;
    fn device_operation(device_key: &str, operation: DeviceOperation, params: &Value, progress: OperationProgress) -> Snapshot;
}
