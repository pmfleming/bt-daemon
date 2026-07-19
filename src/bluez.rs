use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use bluer::{Adapter as BluezAdapter, Device as BluezDevice, Session};
use futures::StreamExt;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::{sync::Mutex, task::JoinHandle};

use crate::{
    backend::BluetoothBackend,
    model::{Adapter, Battery, Device, DeviceCapabilities, Snapshot},
};

pub struct BluezBackend {
    session: Session,
    discovery_tasks: Mutex<HashMap<String, JoinHandle<()>>>,
}

impl BluezBackend {
    pub async fn new() -> Result<Self> {
        Ok(Self {
            session: Session::new().await.context("open BlueZ session")?,
            discovery_tasks: Mutex::new(HashMap::new()),
        })
    }

    async fn adapters(&self) -> Result<Vec<BluezAdapter>> {
        let mut adapters = Vec::new();
        for name in self
            .session
            .adapter_names()
            .await
            .context("list BlueZ adapters")?
        {
            adapters.push(
                self.session
                    .adapter(&name)
                    .with_context(|| format!("open adapter {name}"))?,
            );
        }
        Ok(adapters)
    }

    async fn find_adapter(&self, key: &str) -> Result<BluezAdapter> {
        for adapter in self.adapters().await? {
            if adapter_key(&adapter).await? == key {
                return Ok(adapter);
            }
        }
        bail!("Bluetooth adapter is no longer available")
    }

    async fn find_device(&self, key: &str) -> Result<(BluezAdapter, BluezDevice)> {
        for adapter in self.adapters().await? {
            for address in adapter
                .device_addresses()
                .await
                .context("list adapter devices")?
            {
                let device = adapter.device(address).context("open BlueZ device")?;
                if device_key(&adapter, &device) == key {
                    return Ok((adapter, device));
                }
            }
        }
        bail!("Bluetooth device is no longer available")
    }

    async fn start_discovery(&self, adapter: BluezAdapter) -> Result<()> {
        let name = adapter.name().to_string();
        let mut tasks = self.discovery_tasks.lock().await;
        if tasks.get(&name).is_some_and(|task| !task.is_finished()) {
            return Ok(());
        }
        let mut events = adapter
            .discover_devices_with_changes()
            .await
            .with_context(|| format!("start discovery on {name}"))?;
        tasks.insert(
            name,
            tokio::spawn(async move { while events.next().await.is_some() {} }),
        );
        Ok(())
    }

    async fn stop_discovery(&self) {
        let mut tasks = self.discovery_tasks.lock().await;
        for (_, task) in tasks.drain() {
            task.abort();
        }
    }
}

#[async_trait]
impl BluetoothBackend for BluezBackend {
    async fn snapshot(&self) -> Result<Snapshot> {
        let mut adapters_out = Vec::new();
        let mut devices_out = Vec::new();
        for adapter in self.adapters().await? {
            let adapter_key = adapter_key(&adapter).await?;
            adapters_out.push(Adapter {
                key: adapter_key.clone(),
                name: adapter.name().to_string(),
                alias: adapter
                    .alias()
                    .await
                    .unwrap_or_else(|_| adapter.name().to_string()),
                powered: adapter.is_powered().await.unwrap_or(false),
                discovering: adapter.is_discovering().await.unwrap_or(false),
                pairable: adapter.is_pairable().await.unwrap_or(false),
            });
            for address in adapter.device_addresses().await.unwrap_or_default() {
                let Ok(device) = adapter.device(address) else {
                    continue;
                };
                if let Some(value) = device_snapshot(&adapter, &device, &adapter_key).await {
                    devices_out.push(value);
                }
            }
        }
        devices_out.sort_by_key(|device| {
            (
                !device.connected,
                !device.paired,
                device.name.to_lowercase(),
            )
        });
        Ok(Snapshot {
            adapters: adapters_out,
            devices: devices_out,
        })
    }

    async fn set_powered(&self, adapter_key: Option<&str>, powered: bool) -> Result<Snapshot> {
        if let Some(key) = adapter_key {
            self.find_adapter(key)
                .await?
                .set_powered(powered)
                .await
                .context("set adapter power")?;
        } else {
            for adapter in self.adapters().await? {
                adapter
                    .set_powered(powered)
                    .await
                    .context("set adapter power")?;
            }
        }
        if !powered {
            self.stop_discovery().await;
        }
        self.snapshot().await
    }

    async fn set_scanning(&self, enabled: bool) -> Result<Snapshot> {
        if enabled {
            for adapter in self.adapters().await? {
                if adapter.is_powered().await.unwrap_or(false) {
                    self.start_discovery(adapter).await?;
                }
            }
        } else {
            self.stop_discovery().await;
        }
        self.snapshot().await
    }

    async fn device_operation(
        &self,
        device_key: &str,
        operation: &str,
        params: &Value,
    ) -> Result<Snapshot> {
        let (adapter, device) = self.find_device(device_key).await?;
        match operation {
            "pair" => device.pair().await.context("pair Bluetooth device")?,
            "connect" => device.connect().await.context("connect Bluetooth device")?,
            "disconnect" => device
                .disconnect()
                .await
                .context("disconnect Bluetooth device")?,
            "remove" => adapter
                .remove_device(device.address())
                .await
                .context("remove Bluetooth device")?,
            "set-trusted" => device
                .set_trusted(required_bool(params, "trusted")?)
                .await
                .context("set trusted state")?,
            "set-blocked" => device
                .set_blocked(required_bool(params, "blocked")?)
                .await
                .context("set blocked state")?,
            "set-wake-allowed" => device
                .set_wake_allowed(required_bool(params, "wake_allowed")?)
                .await
                .context("set wake permission")?,
            "set-alias" => device
                .set_alias(required_string(params, "alias")?.to_string())
                .await
                .context("set device alias")?,
            _ => bail!("unsupported Bluetooth device operation: {operation}"),
        }
        self.snapshot().await
    }
}

async fn adapter_key(adapter: &BluezAdapter) -> Result<String> {
    let address = adapter.address().await.context("read adapter identity")?;
    Ok(opaque_key(
        "adapter",
        &format!("{}:{address}", adapter.name()),
    ))
}

fn device_key(adapter: &BluezAdapter, device: &BluezDevice) -> String {
    opaque_key(
        "device",
        &format!("{}:{}", adapter.name(), device.address()),
    )
}

fn opaque_key(kind: &str, identity: &str) -> String {
    let digest = Sha256::digest(identity.as_bytes());
    format!("{kind}-{}", hex::encode(&digest[..12]))
}

async fn device_snapshot(
    adapter: &BluezAdapter,
    device: &BluezDevice,
    adapter_key: &str,
) -> Option<Device> {
    let paired = device.is_paired().await.ok()?;
    let connected = device.is_connected().await.unwrap_or(false);
    let rssi = device.rssi().await.unwrap_or(None);
    if !paired && !connected && rssi.is_none() {
        return None;
    }
    let trusted = device.is_trusted().await.unwrap_or(false);
    let blocked = device.is_blocked().await.unwrap_or(false);
    let wake_allowed = device.is_wake_allowed().await.unwrap_or(None);
    let battery = device
        .battery_percentage()
        .await
        .unwrap_or(None)
        .map(|percentage| {
            vec![Battery {
                component: "main".to_string(),
                percentage,
            }]
        })
        .unwrap_or_default();
    Some(Device {
        key: device_key(adapter, device),
        adapter_key: adapter_key.to_string(),
        name: device
            .alias()
            .await
            .unwrap_or_else(|_| "Unknown device".to_string()),
        icon: device.icon().await.unwrap_or(None),
        paired,
        connected,
        trusted,
        blocked,
        wake_allowed,
        battery,
        signal_strength: rssi.map(signal_strength),
        present: rssi.is_some() || connected,
        capabilities: DeviceCapabilities {
            can_pair: !paired && !blocked,
            can_connect: !connected && !blocked,
            can_disconnect: connected,
            can_remove: paired,
            can_trust: paired,
            can_block: true,
            can_wake: wake_allowed.is_some(),
            can_rename: true,
        },
    })
}

fn signal_strength(rssi: i16) -> u8 {
    (((i32::from(rssi) + 100) * 100) / 60).clamp(0, 100) as u8
}

fn required_bool(params: &Value, name: &str) -> Result<bool> {
    params
        .get(name)
        .and_then(Value::as_bool)
        .with_context(|| format!("missing boolean parameter '{name}'"))
}

fn required_string<'a>(params: &'a Value, name: &str) -> Result<&'a str> {
    params
        .get(name)
        .and_then(Value::as_str)
        .with_context(|| format!("missing string parameter '{name}'"))
}

#[cfg(test)]
mod tests {
    use super::{opaque_key, signal_strength};

    #[test]
    fn keys_are_opaque_and_deterministic() {
        assert_eq!(
            opaque_key("device", "hci0:AA"),
            opaque_key("device", "hci0:AA")
        );
        assert!(!opaque_key("device", "hci0:AA").contains("AA"));
    }

    #[test]
    fn signal_is_clamped() {
        assert_eq!(signal_strength(-120), 0);
        assert_eq!(signal_strength(-100), 0);
        assert_eq!(signal_strength(-70), 50);
        assert_eq!(signal_strength(-40), 100);
        assert_eq!(signal_strength(-10), 100);
    }
}
