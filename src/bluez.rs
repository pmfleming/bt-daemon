use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bluer::{
    Adapter as BluezAdapter, Device as BluezDevice, Session,
    agent::{Agent, AgentHandle},
};
use futures::{
    StreamExt,
    stream::{BoxStream, SelectAll},
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::{
    sync::{Mutex, broadcast},
    task::JoinHandle,
};

use crate::{
    backend::{BackendError, BackendErrorKind, BluetoothBackend, ObexRemote, ObexTarget},
    fast_pair::{FastPairBatteryProvider, MESSAGE_STREAM_UUID},
    identity::DeviceIdentityRegistry,
    model::{Adapter, Battery, Device, DeviceCapabilities, Service, Snapshot},
    params::Params,
};

pub struct BluezBackend {
    session: Session,
    discovery_tasks: Mutex<HashMap<String, JoinHandle<()>>>,
    changes: broadcast::Sender<()>,
    identities: Arc<DeviceIdentityRegistry>,
    last_seen: Mutex<HashMap<String, u64>>,
    system_bus: zbus::Connection,
    fast_pair: Option<Arc<FastPairBatteryProvider>>,
}

impl BluezBackend {
    pub async fn new() -> Result<Self> {
        let (changes, _) = broadcast::channel(64);
        let session = Session::new().await.backend_context("open BlueZ session")?;
        let system_bus = zbus::Connection::system()
            .await
            .context("open system D-Bus for BlueZ compatibility properties")?;
        let fast_pair = match FastPairBatteryProvider::start(session.clone(), changes.clone()).await
        {
            Ok(provider) => Some(provider),
            Err(error) => {
                tracing::warn!(error = %error, "Fast Pair component battery provider is unavailable");
                None
            }
        };
        Ok(Self {
            session,
            discovery_tasks: Mutex::new(HashMap::new()),
            changes,
            identities: DeviceIdentityRegistry::load_default()?,
            last_seen: Mutex::new(HashMap::new()),
            system_bus,
            fast_pair,
        })
    }

    pub fn identity_registry(&self) -> Arc<DeviceIdentityRegistry> {
        Arc::clone(&self.identities)
    }

    pub async fn register_agent(&self, agent: Agent) -> Result<AgentHandle> {
        self.session
            .register_agent(agent)
            .await
            .backend_context("register BlueZ pairing agent")
    }

    pub fn start_monitoring(self: &Arc<Self>) {
        let backend = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                if let Err(error) = backend.monitor_one_change().await {
                    tracing::warn!(error = %error, "BlueZ event monitor is retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                let _ = backend.changes.send(());
            }
        });
    }

    async fn monitor_one_change(&self) -> Result<()> {
        let mut streams: SelectAll<BoxStream<'static, ()>> = SelectAll::new();
        streams.push(
            self.session
                .events()
                .await
                .backend_context("watch adapter hotplug")?
                .map(|_| ())
                .boxed(),
        );
        for adapter in self.adapters().await? {
            streams.push(
                adapter
                    .events()
                    .await
                    .backend_context("watch adapter changes")?
                    .map(|_| ())
                    .boxed(),
            );
            for address in adapter.device_addresses().await.unwrap_or_default() {
                let Ok(device) = adapter.device(address) else {
                    continue;
                };
                if let Ok(events) = device.events().await {
                    streams.push(events.map(|_| ()).boxed());
                }
            }
        }
        streams.next().await.context("BlueZ event streams ended")
    }

    async fn adapters(&self) -> Result<Vec<BluezAdapter>> {
        let mut adapters = Vec::new();
        for name in self
            .session
            .adapter_names()
            .await
            .backend_context("list BlueZ adapters")?
        {
            adapters.push(
                self.session
                    .adapter(&name)
                    .backend_context(&format!("open adapter {name}"))?,
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
        Err(BackendError::new(
            BackendErrorKind::DeviceUnavailable,
            "Bluetooth adapter is no longer available",
        )
        .into())
    }

    async fn find_device(&self, key: &str) -> Result<(BluezAdapter, BluezDevice)> {
        for adapter in self.adapters().await? {
            for address in adapter
                .device_addresses()
                .await
                .backend_context("list adapter devices")?
            {
                let device = adapter
                    .device(address)
                    .backend_context("open BlueZ device")?;
                if self.identities.device_key(adapter.name(), device.address()) == key {
                    return Ok((adapter, device));
                }
            }
        }
        Err(BackendError::new(
            BackendErrorKind::DeviceUnavailable,
            "Bluetooth device is no longer available",
        )
        .into())
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
            .backend_context(&format!("start discovery on {name}"))?;
        tasks.insert(
            name,
            tokio::spawn(async move { while events.next().await.is_some() {} }),
        );
        Ok(())
    }

    async fn stop_discovery(&self, adapter_name: Option<&str>) {
        let mut tasks = self.discovery_tasks.lock().await;
        if let Some(name) = adapter_name {
            if let Some(task) = tasks.remove(name) {
                task.abort();
            }
        } else {
            for (_, task) in tasks.drain() {
                task.abort();
            }
        }
    }
}

#[async_trait]
impl BluetoothBackend for BluezBackend {
    fn subscribe_changes(&self) -> broadcast::Receiver<()> {
        self.changes.subscribe()
    }

    async fn snapshot(&self) -> Result<Snapshot> {
        let mut adapters_out = Vec::new();
        let mut devices_out = Vec::new();
        for adapter in self.adapters().await? {
            let adapter_key = adapter_key(&adapter).await?;
            adapters_out.push(Adapter {
                key: adapter_key.clone(),
                name: adapter.name().to_string(),
                alias: bluez_result(adapter.alias().await, "read adapter alias")?,
                address: bluez_result(adapter.address().await, "read adapter address")?.to_string(),
                address_type: bluez_result(
                    adapter.address_type().await,
                    "read adapter address type",
                )?
                .to_string(),
                powered: bluez_result(adapter.is_powered().await, "read adapter power")?,
                discovering: bluez_result(
                    adapter.is_discovering().await,
                    "read adapter discovery state",
                )?,
                discoverable: bluez_result(
                    adapter.is_discoverable().await,
                    "read adapter discoverable state",
                )?,
                pairable: bluez_result(adapter.is_pairable().await, "read adapter pairable state")?,
                discoverable_timeout: bluez_result(
                    adapter.discoverable_timeout().await,
                    "read adapter discoverable timeout",
                )?,
                pairable_timeout: bluez_result(
                    adapter.pairable_timeout().await,
                    "read adapter pairable timeout",
                )?,
                modalias: bluez_result(adapter.modalias().await, "read adapter modalias")?
                    .map(modalias_string),
            });
            for address in adapter
                .device_addresses()
                .await
                .backend_context("list adapter devices")?
            {
                let device = adapter
                    .device(address)
                    .backend_context("open BlueZ device")?;
                if let Some(value) = device_snapshot(
                    &adapter,
                    &device,
                    &adapter_key,
                    &self.identities,
                    &self.last_seen,
                    &self.system_bus,
                    self.fast_pair.as_deref(),
                )
                .await?
                {
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
                .backend_context("set adapter power")?;
        } else {
            for adapter in self.adapters().await? {
                adapter
                    .set_powered(powered)
                    .await
                    .backend_context("set adapter power")?;
            }
        }
        if !powered {
            self.stop_discovery(None).await;
        }
        self.snapshot().await
    }

    async fn set_scanning(&self, adapter_key: Option<&str>, enabled: bool) -> Result<Snapshot> {
        if let Some(key) = adapter_key {
            let adapter = self.find_adapter(key).await?;
            let name = adapter.name().to_string();
            if enabled {
                if bluez_result(adapter.is_powered().await, "read adapter power")? {
                    self.start_discovery(adapter).await?;
                }
            } else {
                self.stop_discovery(Some(&name)).await;
            }
        } else if enabled {
            for adapter in self.adapters().await? {
                if bluez_result(adapter.is_powered().await, "read adapter power")? {
                    self.start_discovery(adapter).await?;
                }
            }
        } else {
            self.stop_discovery(None).await;
        }
        self.snapshot().await
    }

    async fn adapter_operation(
        &self,
        adapter_key: &str,
        operation: &str,
        params: &Value,
    ) -> Result<Snapshot> {
        let adapter = self.find_adapter(adapter_key).await?;
        match operation {
            "set-alias" => adapter
                .set_alias(params.require_string("alias")?.to_string())
                .await
                .backend_context("set adapter alias")?,
            "set-discoverable" => adapter
                .set_discoverable(params.require_bool("discoverable")?)
                .await
                .backend_context("set adapter discoverable state")?,
            "set-pairable" => adapter
                .set_pairable(params.require_bool("pairable")?)
                .await
                .backend_context("set adapter pairable state")?,
            "set-discoverable-timeout" => adapter
                .set_discoverable_timeout(params.require_u32("timeout")?)
                .await
                .backend_context("set adapter discoverable timeout")?,
            "set-pairable-timeout" => adapter
                .set_pairable_timeout(params.require_u32("timeout")?)
                .await
                .backend_context("set adapter pairable timeout")?,
            _ => {
                return Err(BackendError::new(
                    BackendErrorKind::InvalidInput,
                    format!("unsupported Bluetooth adapter operation: {operation}"),
                )
                .into());
            }
        }
        self.snapshot().await
    }

    async fn obex_target(&self, device_key: &str) -> Result<ObexTarget> {
        let (adapter, device) = self.find_device(device_key).await?;
        Ok(ObexTarget {
            source: adapter
                .address()
                .await
                .backend_context("read OBEX source address")?
                .to_string(),
            destination: device.address().to_string(),
        })
    }

    async fn obex_remote(&self, source: &str, destination: &str) -> Result<ObexRemote> {
        let source = source.parse().map_err(|error| {
            BackendError::new(
                BackendErrorKind::InvalidInput,
                format!("parse incoming OBEX adapter address: {error}"),
            )
        })?;
        let destination = destination.parse().map_err(|error| {
            BackendError::new(
                BackendErrorKind::InvalidInput,
                format!("parse incoming OBEX device address: {error}"),
            )
        })?;
        for adapter in self.adapters().await? {
            if adapter
                .address()
                .await
                .backend_context("read adapter address")?
                != source
            {
                continue;
            }
            let device = adapter
                .device(destination)
                .backend_context("open incoming OBEX device")?;
            if !bluez_result(device.is_paired().await, "read device paired state")? {
                return Err(BackendError::new(
                    BackendErrorKind::Rejected,
                    "incoming transfers require a paired Bluetooth device",
                )
                .into());
            }
            if bluez_result(device.is_blocked().await, "read device blocked state")? {
                return Err(BackendError::new(
                    BackendErrorKind::Rejected,
                    "incoming transfers are disabled for blocked Bluetooth devices",
                )
                .into());
            }
            return Ok(ObexRemote {
                device_key: self.identities.device_key(adapter.name(), device.address()),
                name: bluez_result(device.alias().await, "read device alias")?,
            });
        }
        Err(BackendError::new(
            BackendErrorKind::DeviceUnavailable,
            "incoming OBEX adapter is unavailable",
        )
        .into())
    }

    async fn device_operation(
        &self,
        device_key: &str,
        operation: &str,
        params: &Value,
    ) -> Result<Snapshot> {
        let (adapter, device) = self.find_device(device_key).await?;
        match operation {
            "pair" => {
                operation_timeout(
                    Duration::from_secs(75),
                    "pair Bluetooth device",
                    device.pair(),
                )
                .await?;
                if params
                    .get("trust_after_pair")
                    .and_then(Value::as_bool)
                    .unwrap_or(true)
                {
                    device
                        .set_trusted(true)
                        .await
                        .backend_context("trust paired Bluetooth device")?;
                }
                if !bluez_result(device.is_connected().await, "read device connected state")? {
                    operation_timeout(
                        Duration::from_secs(25),
                        "connect paired Bluetooth device",
                        device.connect(),
                    )
                    .await?;
                }
            }
            "connect" => {
                operation_timeout(
                    Duration::from_secs(25),
                    "connect Bluetooth device",
                    device.connect(),
                )
                .await?
            }
            "disconnect" => {
                operation_timeout(
                    Duration::from_secs(12),
                    "disconnect Bluetooth device",
                    device.disconnect(),
                )
                .await?
            }
            "remove" => {
                if bluez_result(device.is_connected().await, "read device connected state")? {
                    operation_timeout(
                        Duration::from_secs(12),
                        "disconnect Bluetooth device before removal",
                        device.disconnect(),
                    )
                    .await?;
                }
                adapter
                    .remove_device(device.address())
                    .await
                    .backend_context("remove Bluetooth device")?;
            }
            "set-trusted" => device
                .set_trusted(params.require_bool("trusted")?)
                .await
                .backend_context("set trusted state")?,
            "set-blocked" => device
                .set_blocked(params.require_bool("blocked")?)
                .await
                .backend_context("set blocked state")?,
            "set-wake-allowed" => device
                .set_wake_allowed(params.require_bool("wake_allowed")?)
                .await
                .backend_context("set wake permission")?,
            "set-alias" => device
                .set_alias(params.require_string("alias")?.to_string())
                .await
                .backend_context("set device alias")?,
            _ => {
                return Err(BackendError::new(
                    BackendErrorKind::InvalidInput,
                    format!("unsupported Bluetooth device operation: {operation}"),
                )
                .into());
            }
        }
        self.snapshot().await
    }
}

async fn adapter_key(adapter: &BluezAdapter) -> Result<String> {
    let address = adapter
        .address()
        .await
        .backend_context("read adapter identity")?;
    Ok(opaque_key(
        "adapter",
        &format!("{}:{address}", adapter.name()),
    ))
}

fn opaque_key(kind: &str, identity: &str) -> String {
    let digest = Sha256::digest(identity.as_bytes());
    format!("{kind}-{}", hex::encode(&digest[..12]))
}

async fn device_snapshot(
    adapter: &BluezAdapter,
    device: &BluezDevice,
    adapter_key: &str,
    identities: &DeviceIdentityRegistry,
    last_seen: &Mutex<HashMap<String, u64>>,
    system_bus: &zbus::Connection,
    fast_pair: Option<&FastPairBatteryProvider>,
) -> Result<Option<Device>> {
    let identity = device.address();
    let paired = bluez_result(device.is_paired().await, "read device paired state")?;
    let connected = bluez_result(device.is_connected().await, "read device connected state")?;
    let rssi = bluez_result(device.rssi().await, "read device signal strength")?;
    if !paired && !connected && rssi.is_none() {
        return Ok(None);
    }
    let trusted = bluez_result(device.is_trusted().await, "read device trusted state")?;
    let blocked = bluez_result(device.is_blocked().await, "read device blocked state")?;
    let wake_allowed = bluez_result(
        device.is_wake_allowed().await,
        "read device wake permission",
    )?;
    let alias = bluez_result(device.alias().await, "read device alias")?;
    let mut uuids = bluez_result(device.uuids().await, "read device UUIDs")?
        .unwrap_or_default()
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    uuids.sort();
    let services = uuids
        .iter()
        .map(|uuid| Service {
            uuid: uuid.clone(),
            label: service_label(uuid).to_string(),
        })
        .collect();
    let key = identities.device_key(adapter.name(), identity);
    let last_seen_ms = {
        let mut seen = last_seen.lock().await;
        if rssi.is_some() || connected {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            seen.insert(key.clone(), now);
            Some(now)
        } else {
            seen.get(&key).copied()
        }
    };
    let component_battery = if connected {
        match fast_pair {
            Some(provider) => provider.batteries(identity).await,
            None => Vec::new(),
        }
    } else {
        Vec::new()
    };
    let battery = if component_battery.is_empty() {
        bluez_result(device.battery_percentage().await, "read device battery")?
            .map(|percentage| {
                vec![Battery {
                    id: "aggregate".to_string(),
                    label: "Battery".to_string(),
                    component: "main".to_string(),
                    percentage,
                    source: "bluez".to_string(),
                    confidence: "standard".to_string(),
                }]
            })
            .unwrap_or_default()
    } else {
        component_battery
    };
    let capabilities = device_capabilities(paired, connected, blocked, wake_allowed);
    Ok(Some(Device {
        key,
        adapter_key: adapter_key.to_string(),
        name: alias.clone(),
        alias,
        address: identity.to_string(),
        address_type: bluez_result(device.address_type().await, "read device address type")?
            .to_string(),
        icon: bluez_result(device.icon().await, "read device icon")?,
        paired,
        bonded: bonded_property(system_bus, adapter.name(), identity).await,
        connected,
        services_resolved: bluez_result(
            device.is_services_resolved().await,
            "read device services state",
        )?,
        trusted,
        blocked,
        wake_allowed,
        legacy_pairing: bluez_result(
            device.is_legacy_pairing().await,
            "read device legacy-pairing state",
        )?,
        modalias: bluez_result(device.modalias().await, "read device modalias")?
            .map(modalias_string),
        uuids,
        services,
        battery,
        rssi,
        signal_strength: rssi.map(signal_strength),
        present: rssi.is_some() || connected,
        last_seen_ms,
        capabilities,
    }))
}

fn device_capabilities(
    paired: bool,
    connected: bool,
    blocked: bool,
    wake_allowed: Option<bool>,
) -> DeviceCapabilities {
    let mut unsupported_reasons = HashMap::new();
    if paired {
        unsupported_reasons.insert("pair".into(), "Device is already paired".into());
    }
    if connected {
        unsupported_reasons.insert("connect".into(), "Device is already connected".into());
    } else {
        unsupported_reasons.insert("disconnect".into(), "Device is not connected".into());
    }
    if wake_allowed.is_none() {
        unsupported_reasons.insert(
            "wake".into(),
            "BlueZ does not expose wake control for this device".into(),
        );
    }
    if !paired {
        unsupported_reasons.insert(
            "send_file".into(),
            "Pair the device before sending files".into(),
        );
    }
    DeviceCapabilities {
        can_pair: !paired && !blocked,
        can_connect: !connected && !blocked,
        can_disconnect: connected,
        can_remove: paired,
        can_trust: paired,
        can_block: true,
        can_wake: wake_allowed.is_some(),
        can_rename: true,
        can_send_file: paired && !blocked,
        unsupported_reasons,
    }
}

trait BluezResultExt<T> {
    fn backend_context(self, operation: &str) -> Result<T>;
}

impl<T> BluezResultExt<T> for bluer::Result<T> {
    fn backend_context(self, operation: &str) -> Result<T> {
        bluez_result(self, operation)
    }
}

fn bluez_result<T>(result: bluer::Result<T>, operation: &str) -> Result<T> {
    result.map_err(|error| {
        let kind = match error.kind {
            bluer::ErrorKind::AuthenticationCanceled
            | bluer::ErrorKind::AuthenticationFailed
            | bluer::ErrorKind::AuthenticationRejected
            | bluer::ErrorKind::NotAuthorized
            | bluer::ErrorKind::NotPermitted => BackendErrorKind::Rejected,
            bluer::ErrorKind::AuthenticationTimeout => BackendErrorKind::Timeout,
            bluer::ErrorKind::DoesNotExist | bluer::ErrorKind::NotFound => {
                BackendErrorKind::DeviceUnavailable
            }
            bluer::ErrorKind::NotAvailable
            | bluer::ErrorKind::NotReady
            | bluer::ErrorKind::Internal(_) => BackendErrorKind::Unavailable,
            bluer::ErrorKind::InvalidArguments
            | bluer::ErrorKind::InvalidLength
            | bluer::ErrorKind::InvalidOffset
            | bluer::ErrorKind::InvalidAddress(_)
            | bluer::ErrorKind::InvalidName(_) => BackendErrorKind::InvalidInput,
            _ => BackendErrorKind::OperationFailed,
        };
        BackendError::new(kind, format!("{operation}: {error}")).into()
    })
}

async fn operation_timeout<T>(
    duration: Duration,
    operation: &'static str,
    future: impl std::future::Future<Output = bluer::Result<T>>,
) -> Result<T> {
    match tokio::time::timeout(duration, future).await {
        Ok(result) => bluez_result(result, operation),
        Err(_) => Err(BackendError::new(
            BackendErrorKind::Timeout,
            format!("{operation} timed out"),
        )
        .into()),
    }
}

fn service_label(uuid: &str) -> &'static str {
    if uuid.eq_ignore_ascii_case(MESSAGE_STREAM_UUID) {
        return "Fast Pair Message Stream";
    }
    if uuid.eq_ignore_ascii_case("0000fe2c-0000-1000-8000-00805f9b34fb") {
        return "Fast Pair Service";
    }
    match uuid
        .get(4..8)
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "1105" => "Object Push",
        "1108" => "Headset",
        "110a" => "Audio Source",
        "110b" => "Audio Sink",
        "110c" => "A/V Remote Control Target",
        "110d" => "Advanced Audio Distribution",
        "110e" => "A/V Remote Control",
        "1115" => "Personal Area Network",
        "1116" => "Network Access Point",
        "1117" => "Group Network",
        "111e" => "Handsfree",
        "1124" => "Human Interface Device",
        "1200" => "Device Information",
        "180f" => "Battery Service",
        _ => "Bluetooth service",
    }
}

async fn bonded_property(
    connection: &zbus::Connection,
    adapter: &str,
    address: bluer::Address,
) -> Option<bool> {
    let path = format!(
        "/org/bluez/{adapter}/dev_{}",
        address.to_string().replace(':', "_")
    );
    let proxy = zbus::Proxy::new(connection, "org.bluez", path, "org.bluez.Device1")
        .await
        .ok()?;
    proxy.get_property::<bool>("Bonded").await.ok()
}

fn modalias_string(value: bluer::Modalias) -> String {
    format!(
        "{}:v{:04X}p{:04X}d{:04X}",
        value.source, value.vendor, value.product, value.device
    )
}

fn signal_strength(rssi: i16) -> u8 {
    (((i32::from(rssi) + 100) * 100) / 60).clamp(0, 100) as u8
}

#[cfg(test)]
mod tests {
    use crate::backend::{BackendError, BackendErrorKind};

    use super::{bluez_result, device_capabilities, opaque_key, service_label, signal_strength};

    #[test]
    fn adapter_keys_are_opaque_and_deterministic() {
        assert_eq!(
            opaque_key("adapter", "hci0:AA"),
            opaque_key("adapter", "hci0:AA")
        );
        assert!(!opaque_key("adapter", "hci0:AA").contains("AA"));
    }

    #[test]
    fn fast_pair_service_labels_distinguish_gatt_and_message_stream() {
        assert_eq!(
            service_label("0000fe2c-0000-1000-8000-00805f9b34fb"),
            "Fast Pair Service"
        );
        assert_eq!(
            service_label("df21fe2c-2515-4fdb-8886-f12c4d67927c"),
            "Fast Pair Message Stream"
        );
    }

    #[test]
    fn signal_is_clamped() {
        assert_eq!(signal_strength(-120), 0);
        assert_eq!(signal_strength(-100), 0);
        assert_eq!(signal_strength(-70), 50);
        assert_eq!(signal_strength(-40), 100);
        assert_eq!(signal_strength(-10), 100);
    }

    #[test]
    fn false_properties_are_values_but_unavailable_properties_are_errors() {
        assert!(!bluez_result(Ok(false), "read paired state").unwrap());
        let error = bluez_result::<bool>(
            Err(bluer::Error {
                kind: bluer::ErrorKind::NotReady,
                message: "adapter restarting".into(),
            }),
            "read paired state",
        )
        .unwrap_err();
        assert_eq!(
            error.downcast_ref::<BackendError>().unwrap().kind,
            BackendErrorKind::Unavailable
        );
    }

    #[test]
    fn capabilities_preserve_false_state_without_claiming_unavailability() {
        let capabilities = device_capabilities(false, false, false, None);
        assert!(capabilities.can_pair);
        assert!(capabilities.can_connect);
        assert!(!capabilities.can_disconnect);
        assert_eq!(
            capabilities.unsupported_reasons["disconnect"],
            "Device is not connected"
        );
    }
}
