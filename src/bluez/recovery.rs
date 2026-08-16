use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use bluer::agent::AgentHandle;
use futures::StreamExt;
use serde_json::Value;
use tokio::sync::{Mutex, broadcast};

use crate::{
    backend::{
        AdapterOperation, BluetoothBackend, DeviceOperation, ObexRemote, ObexTarget,
        OperationProgress,
    },
    model::Snapshot,
    pairing::PairingBroker,
};

use super::BluezBackend;

pub struct RecoveringBackend {
    current: RwLock<Arc<BluezBackend>>,
    changes: broadcast::Sender<()>,
    agent: Mutex<Option<AgentHandle>>,
}

impl RecoveringBackend {
    pub fn new(initial: Arc<BluezBackend>) -> Arc<Self> {
        let (changes, _) = broadcast::channel(64);
        let backend = Arc::new(Self {
            current: RwLock::new(Arc::clone(&initial)),
            changes,
            agent: Mutex::new(None),
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
        crate::task::spawn("recovering-bluez-change-forwarder", async move {
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
            if let Err(error) = backend.monitor_owner(pairing).await {
                tracing::error!(%error, "BlueZ recovery monitor stopped");
            }
        });
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
        let mut changes = proxy
            .receive_signal("NameOwnerChanged")
            .await
            .context("subscribe to BlueZ owner changes")?;
        while let Some(message) = changes.next().await {
            let (name, old_owner, new_owner): (String, String, String) = message
                .body()
                .deserialize()
                .context("decode BlueZ owner change")?;
            if name != "org.bluez" || old_owner == new_owner {
                continue;
            }
            let _ = self.changes.send(());
            if new_owner.is_empty() {
                tracing::warn!(
                    "BlueZ disappeared; retaining daemon API while waiting for recovery"
                );
            } else {
                self.recover(&pairing).await;
            }
        }
        bail!("BlueZ recovery owner stream ended")
    }

    async fn recover(&self, pairing: &Arc<PairingBroker>) {
        tracing::info!("BlueZ appeared; rebuilding Bluetooth backend");
        loop {
            match Self::replacement(pairing).await {
                Ok((replacement, agent)) => {
                    *self.agent.lock().await = Some(agent);
                    *self
                        .current
                        .write()
                        .unwrap_or_else(|poison| poison.into_inner()) = Arc::clone(&replacement);
                    self.forward_changes(replacement);
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

    async fn replacement(pairing: &Arc<PairingBroker>) -> Result<(Arc<BluezBackend>, AgentHandle)> {
        let replacement = Arc::new(BluezBackend::new().await?);
        replacement.apply_startup_policy().await;
        replacement.start_monitoring();
        replacement.start_lifecycle_monitoring();
        let agent = replacement
            .register_agent(pairing.agent())
            .await
            .context("restore the Bluetooth pairing agent")?;
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
