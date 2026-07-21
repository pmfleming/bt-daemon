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
    backend::{
        AdapterOperation, BackendError, BackendErrorKind, BluetoothBackend, DeviceOperation,
        ObexRemote, ObexTarget,
    },
    fast_pair::{FAST_PAIR_SERVICE_UUID, FastPairBatteryProvider, MESSAGE_STREAM_UUID},
    identity::DeviceIdentityRegistry,
    model::{Adapter, Battery, Device, DeviceCapabilities, Service, Snapshot},
    params::Params,
};

const DISCOVERED_DEVICE_CACHE_TTL: Duration = Duration::from_secs(5 * 60);

#[derive(Clone)]
struct CachedDevice {
    device: Device,
    observed_at_ms: u64,
}

pub struct BluezBackend {
    session: Session,
    discovery_tasks: Mutex<HashMap<String, JoinHandle<()>>>,
    changes: broadcast::Sender<()>,
    identities: Arc<DeviceIdentityRegistry>,
    device_cache: Mutex<HashMap<String, CachedDevice>>,
    system_bus: zbus::Connection,
    fast_pair: Option<Arc<FastPairBatteryProvider>>,
}

impl BluezBackend {
    pub async fn new() -> Result<Self> {
        tracing::info!("initializing BlueZ backend");
        let (changes, _) = broadcast::channel(64);
        let session = Session::new().await.backend_context("open BlueZ session")?;
        let system_bus = zbus::Connection::system()
            .await
            .context("open system D-Bus for BlueZ compatibility properties")?;
        let identities = DeviceIdentityRegistry::load_default()?;
        for name in session
            .adapter_names()
            .await
            .backend_context("list adapters for identity initialization")?
        {
            let adapter = session
                .adapter(&name)
                .backend_context(&format!("open adapter {name} for identity initialization"))?;
            let address = adapter
                .address()
                .await
                .backend_context("read stable adapter identity")?;
            identities.register_adapter(&name, &address.to_string());
        }
        let fast_pair = match FastPairBatteryProvider::start(
            session.clone(),
            Arc::clone(&identities),
            changes.clone(),
        )
        .await
        {
            Ok(provider) => Some(provider),
            Err(error) => {
                tracing::warn!(error = %error, "Fast Pair component battery provider is unavailable");
                None
            }
        };
        tracing::info!(fast_pair = fast_pair.is_some(), "BlueZ backend initialized");
        Ok(Self {
            session,
            discovery_tasks: Mutex::new(HashMap::new()),
            changes,
            identities,
            device_cache: Mutex::new(HashMap::new()),
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
        tracing::info!("BlueZ event monitor started");
        let backend = Arc::clone(self);
        crate::task::spawn("bluez-monitor", async move {
            loop {
                if let Err(error) = backend.monitor_one_change().await {
                    tracing::warn!(error = %error, "BlueZ event monitor is retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                tracing::trace!("BlueZ change notification received");
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
            if let Err(error) = append_adapter_event_streams(&adapter, &mut streams).await {
                tracing::warn!(adapter = adapter.name(), error = %error, error_chain = %format!("{error:#}"), "could not monitor BlueZ adapter");
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
            crate::task::spawn("bluez-discovery", async move {
                while events.next().await.is_some() {}
            }),
        );
        Ok(())
    }

    async fn stop_discovery(&self, adapter_name: Option<&str>) {
        let mut tasks = self.discovery_tasks.lock().await;
        let stopped = if let Some(name) = adapter_name {
            tasks
                .remove(name)
                .map(|task| {
                    task.abort();
                    true
                })
                .unwrap_or(false)
        } else {
            let stopped = !tasks.is_empty();
            for (_, task) in tasks.drain() {
                task.abort();
            }
            stopped
        };
        drop(tasks);
        if stopped {
            let changes = self.changes.clone();
            crate::task::spawn("bluetooth-cache-expiry", async move {
                tokio::time::sleep(DISCOVERED_DEVICE_CACHE_TTL).await;
                let _ = changes.send(());
            });
        }
    }
}

#[async_trait]
impl BluetoothBackend for BluezBackend {
    fn subscribe_changes(&self) -> broadcast::Receiver<()> {
        self.changes.subscribe()
    }

    async fn snapshot(&self) -> Result<Snapshot> {
        let now_ms = unix_time_ms();
        self.device_cache
            .lock()
            .await
            .retain(|_, cached| cache_entry_is_fresh(cached.observed_at_ms, now_ms));
        let mut snapshot = Snapshot::default();
        for adapter in self.adapters().await? {
            let address = adapter
                .address()
                .await
                .backend_context("read stable adapter identity")?;
            self.identities
                .register_adapter(adapter.name(), &address.to_string());
            let adapter_key = opaque_key("adapter", &address.to_string());
            snapshot
                .adapters
                .push(adapter_snapshot(&adapter, &adapter_key).await?);
            snapshot
                .devices
                .extend(adapter_devices(self, &adapter, &adapter_key).await?);
        }
        snapshot.devices.sort_by_key(|device| {
            (
                !device.connected,
                !device.paired,
                device.name.to_lowercase(),
            )
        });
        tracing::debug!(
            adapters = snapshot.adapters.len(),
            devices = snapshot.devices.len(),
            "BlueZ snapshot completed"
        );
        Ok(snapshot)
    }

    async fn set_powered(&self, adapter_key: Option<&str>, powered: bool) -> Result<Snapshot> {
        tracing::info!(?adapter_key, powered, "setting Bluetooth adapter power");
        if let Some(key) = adapter_key {
            let adapter = self.find_adapter(key).await?;
            adapter
                .set_powered(powered)
                .await
                .backend_context("set adapter power")?;
            if !powered {
                self.stop_discovery(Some(adapter.name())).await;
            }
        } else {
            for adapter in self.adapters().await? {
                adapter
                    .set_powered(powered)
                    .await
                    .backend_context("set adapter power")?;
            }
        }
        if !powered && adapter_key.is_none() {
            self.stop_discovery(None).await;
        }
        self.snapshot().await
    }

    async fn set_scanning(&self, adapter_key: Option<&str>, enabled: bool) -> Result<Snapshot> {
        tracing::info!(?adapter_key, enabled, "setting Bluetooth discovery state");
        if let Some(key) = adapter_key {
            set_adapter_scanning(self, self.find_adapter(key).await?, enabled).await?;
        } else if enabled {
            for adapter in self.adapters().await? {
                set_adapter_scanning(self, adapter, true).await?;
            }
        } else {
            self.stop_discovery(None).await;
        }
        self.snapshot().await
    }

    async fn adapter_operation(
        &self,
        adapter_key: &str,
        operation: AdapterOperation,
        params: &Value,
    ) -> Result<Snapshot> {
        tracing::info!(%adapter_key, %operation, "Bluetooth adapter operation started");
        let adapter = self.find_adapter(adapter_key).await?;
        match operation {
            AdapterOperation::SetAlias => adapter
                .set_alias(params.require_string("alias")?.to_string())
                .await
                .backend_context("set adapter alias")?,
            AdapterOperation::SetDiscoverable => adapter
                .set_discoverable(params.require_bool("discoverable")?)
                .await
                .backend_context("set adapter discoverable state")?,
            AdapterOperation::SetPairable => adapter
                .set_pairable(params.require_bool("pairable")?)
                .await
                .backend_context("set adapter pairable state")?,
            AdapterOperation::SetDiscoverableTimeout => adapter
                .set_discoverable_timeout(params.require_u32("timeout")?)
                .await
                .backend_context("set adapter discoverable timeout")?,
            AdapterOperation::SetPairableTimeout => adapter
                .set_pairable_timeout(params.require_u32("timeout")?)
                .await
                .backend_context("set adapter pairable timeout")?,
        }
        self.snapshot().await
    }

    async fn obex_target(&self, device_key: &str) -> Result<ObexTarget> {
        tracing::debug!(%device_key, "resolving outgoing OBEX target");
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
        tracing::debug!("validating incoming OBEX remote");
        let source = parse_obex_address(source, "adapter")?;
        let destination = parse_obex_address(destination, "device")?;
        let adapter = find_adapter_address(self, source).await?;
        validated_obex_remote(&self.identities, &adapter, destination).await
    }

    async fn device_operation(
        &self,
        device_key: &str,
        operation: DeviceOperation,
        params: &Value,
    ) -> Result<Snapshot> {
        tracing::info!(%device_key, %operation, "Bluetooth device operation started");
        let (adapter, device) = self.find_device(device_key).await?;
        run_device_operation(
            &adapter,
            &device,
            operation,
            params,
            self.fast_pair.as_deref(),
        )
        .await?;
        if operation == DeviceOperation::Remove {
            self.device_cache.lock().await.remove(device_key);
        }
        self.snapshot().await
    }
}

async fn find_adapter_address(
    backend: &BluezBackend,
    address: bluer::Address,
) -> Result<BluezAdapter> {
    for adapter in backend.adapters().await? {
        if adapter
            .address()
            .await
            .backend_context("read adapter address")?
            == address
        {
            return Ok(adapter);
        }
    }
    Err(BackendError::new(
        BackendErrorKind::DeviceUnavailable,
        "incoming OBEX adapter is unavailable",
    )
    .into())
}

fn parse_obex_address(value: &str, role: &str) -> Result<bluer::Address> {
    value.parse().map_err(|error| {
        BackendError::new(
            BackendErrorKind::InvalidInput,
            format!("parse incoming OBEX {role} address: {error}"),
        )
        .into()
    })
}

async fn validated_obex_remote(
    identities: &DeviceIdentityRegistry,
    adapter: &BluezAdapter,
    address: bluer::Address,
) -> Result<ObexRemote> {
    let device = adapter
        .device(address)
        .backend_context("open incoming OBEX device")?;
    let paired = bluez_result(device.is_paired().await, "read device paired state")?;
    let blocked = bluez_result(device.is_blocked().await, "read device blocked state")?;
    if !paired || blocked {
        let message = if blocked {
            "incoming transfers are disabled for blocked Bluetooth devices"
        } else {
            "incoming transfers require a paired Bluetooth device"
        };
        return Err(BackendError::new(BackendErrorKind::Rejected, message).into());
    }
    Ok(ObexRemote {
        device_key: identities.device_key(adapter.name(), device.address()),
        name: bluez_result(device.alias().await, "read device alias")?,
    })
}

async fn append_adapter_event_streams(
    adapter: &BluezAdapter,
    streams: &mut SelectAll<BoxStream<'static, ()>>,
) -> Result<()> {
    streams.push(
        adapter
            .events()
            .await
            .backend_context("watch adapter changes")?
            .map(|_| ())
            .boxed(),
    );
    for address in adapter
        .device_addresses()
        .await
        .backend_context("list monitored adapter devices")?
    {
        let device = adapter
            .device(address)
            .backend_context("open monitored BlueZ device")?;
        match device.events().await {
            Ok(events) => streams.push(events.map(|_| ()).boxed()),
            Err(error) => tracing::warn!(%address, %error, "could not monitor BlueZ device events"),
        }
    }
    Ok(())
}

async fn set_adapter_scanning(
    backend: &BluezBackend,
    adapter: BluezAdapter,
    enabled: bool,
) -> Result<()> {
    if enabled {
        if bluez_result(adapter.is_powered().await, "read adapter power")? {
            backend.start_discovery(adapter).await?;
        }
    } else {
        backend.stop_discovery(Some(adapter.name())).await;
    }
    Ok(())
}

async fn adapter_devices(
    backend: &BluezBackend,
    adapter: &BluezAdapter,
    adapter_key: &str,
) -> Result<Vec<Device>> {
    let mut devices = Vec::new();
    let mut included = std::collections::HashSet::new();
    for address in adapter
        .device_addresses()
        .await
        .backend_context("list adapter devices")?
    {
        let device = adapter
            .device(address)
            .backend_context("open BlueZ device")?;
        if let Some(snapshot) = device_snapshot(
            adapter,
            &device,
            adapter_key,
            &backend.identities,
            &backend.device_cache,
            &backend.system_bus,
            backend.fast_pair.as_deref(),
        )
        .await?
        {
            included.insert(snapshot.key.clone());
            devices.push(snapshot);
        }
    }

    let now_ms = unix_time_ms();
    let cached = backend.device_cache.lock().await;
    devices.extend(
        cached
            .values()
            .filter(|cached| {
                cached.device.adapter_key == adapter_key
                    && !included.contains(&cached.device.key)
                    && cache_entry_is_fresh(cached.observed_at_ms, now_ms)
            })
            .map(cached_device_view),
    );
    Ok(devices)
}

async fn adapter_snapshot(adapter: &BluezAdapter, key: &str) -> Result<Adapter> {
    Ok(Adapter {
        key: key.to_string(),
        name: adapter.name().to_string(),
        alias: bluez_result(adapter.alias().await, "read adapter alias")?,
        address: bluez_result(adapter.address().await, "read adapter address")?.to_string(),
        address_type: bluez_result(adapter.address_type().await, "read adapter address type")?
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
    })
}

async fn run_device_operation(
    adapter: &BluezAdapter,
    device: &BluezDevice,
    operation: DeviceOperation,
    params: &Value,
    fast_pair: Option<&FastPairBatteryProvider>,
) -> Result<()> {
    match operation {
        DeviceOperation::Pair => {
            pair_device(device, params).await?;
            if let Some(public_key) = params
                .get("fast_pair_anti_spoofing_public_key")
                .and_then(Value::as_str)
            {
                fast_pair
                    .context("Fast Pair provider is unavailable")?
                    .provision_account_key(adapter, device, public_key)
                    .await?;
            }
        }
        DeviceOperation::Connect => connect_device(device, "connect Bluetooth device").await?,
        DeviceOperation::Disconnect => {
            disconnect_device(device, "disconnect Bluetooth device").await?
        }
        DeviceOperation::Remove => remove_device(adapter, device).await?,
        DeviceOperation::SetTrusted => device
            .set_trusted(params.require_bool("trusted")?)
            .await
            .backend_context("set trusted state")?,
        DeviceOperation::SetBlocked => device
            .set_blocked(params.require_bool("blocked")?)
            .await
            .backend_context("set blocked state")?,
        DeviceOperation::SetWakeAllowed => device
            .set_wake_allowed(params.require_bool("wake_allowed")?)
            .await
            .backend_context("set wake permission")?,
        DeviceOperation::SetAlias => device
            .set_alias(params.require_string("alias")?.to_string())
            .await
            .backend_context("set device alias")?,
        DeviceOperation::ProvisionFastPair => {
            fast_pair
                .context("Fast Pair provider is unavailable")?
                .provision_account_key(
                    adapter,
                    device,
                    params.require_string("anti_spoofing_public_key")?,
                )
                .await?;
        }
        DeviceOperation::SetMultipoint => {
            fast_pair
                .context("Fast Pair provider is unavailable")?
                .set_multipoint(device, params.require_bool("enabled")?)
                .await?;
        }
        DeviceOperation::SetNoiseControl => {
            fast_pair
                .context("Fast Pair provider is unavailable")?
                .set_noise_control(device, params.require_string("mode")?)
                .await?;
        }
    }
    Ok(())
}

async fn pair_device(device: &BluezDevice, params: &Value) -> Result<()> {
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
        connect_device(device, "connect paired Bluetooth device").await?;
    }
    Ok(())
}

async fn connect_device(device: &BluezDevice, operation: &'static str) -> Result<()> {
    operation_timeout(Duration::from_secs(25), operation, device.connect()).await
}

async fn disconnect_device(device: &BluezDevice, operation: &'static str) -> Result<()> {
    operation_timeout(Duration::from_secs(12), operation, device.disconnect()).await
}

async fn remove_device(adapter: &BluezAdapter, device: &BluezDevice) -> Result<()> {
    if bluez_result(device.is_connected().await, "read device connected state")? {
        disconnect_device(device, "disconnect Bluetooth device before removal").await?;
    }
    adapter
        .remove_device(device.address())
        .await
        .backend_context("remove Bluetooth device")
}

async fn adapter_key(adapter: &BluezAdapter) -> Result<String> {
    let address = adapter
        .address()
        .await
        .backend_context("read adapter identity")?;
    Ok(opaque_key("adapter", &address.to_string()))
}

fn opaque_key(kind: &str, identity: &str) -> String {
    let digest = Sha256::digest(identity.as_bytes());
    format!("{kind}-{}", hex::encode(&digest[..12]))
}

struct DeviceMetadata {
    alias: String,
    address_type: String,
    icon: Option<String>,
    services_resolved: bool,
    legacy_pairing: bool,
    modalias: Option<String>,
    uuids: Vec<String>,
}

impl DeviceMetadata {
    async fn read(device: &BluezDevice) -> Result<Self> {
        let mut uuids = bluez_result(device.uuids().await, "read device UUIDs")?
            .unwrap_or_default()
            .into_iter()
            .map(|uuid| uuid.to_string())
            .collect::<Vec<_>>();
        uuids.sort();
        Ok(Self {
            alias: bluez_result(device.alias().await, "read device alias")?,
            address_type: bluez_result(device.address_type().await, "read device address type")?
                .to_string(),
            icon: bluez_result(device.icon().await, "read device icon")?,
            services_resolved: bluez_result(
                device.is_services_resolved().await,
                "read device services state",
            )?,
            legacy_pairing: bluez_result(
                device.is_legacy_pairing().await,
                "read device legacy-pairing state",
            )?,
            modalias: bluez_result(device.modalias().await, "read device modalias")?
                .map(modalias_string),
            uuids,
        })
    }
}

async fn device_snapshot(
    adapter: &BluezAdapter,
    device: &BluezDevice,
    adapter_key: &str,
    identities: &DeviceIdentityRegistry,
    device_cache: &Mutex<HashMap<String, CachedDevice>>,
    system_bus: &zbus::Connection,
    fast_pair: Option<&FastPairBatteryProvider>,
) -> Result<Option<Device>> {
    let paired = bluez_result(device.is_paired().await, "read device paired state")?;
    let connected = bluez_result(device.is_connected().await, "read device connected state")?;
    let live_rssi = bluez_result(device.rssi().await, "read device signal strength")?;
    let present = live_rssi.is_some() || connected;
    let identity = device.address();
    let key = identities.device_key(adapter.name(), identity);
    let now_ms = unix_time_ms();
    let cached = device_cache
        .lock()
        .await
        .get(&key)
        .filter(|cached| cache_entry_is_fresh(cached.observed_at_ms, now_ms))
        .cloned();
    if !should_include_device(paired, present, cached.is_some()) {
        return Ok(None);
    }
    let trusted = bluez_result(device.is_trusted().await, "read device trusted state")?;
    let blocked = bluez_result(device.is_blocked().await, "read device blocked state")?;
    let wake_allowed = bluez_result(
        device.is_wake_allowed().await,
        "read device wake permission",
    )?;
    let metadata = DeviceMetadata::read(device).await?;
    let services = metadata
        .uuids
        .iter()
        .map(|uuid| Service {
            uuid: uuid.clone(),
            label: service_label(uuid).to_string(),
        })
        .collect();
    let last_seen_ms = if present {
        Some(now_ms)
    } else {
        cached
            .as_ref()
            .and_then(|cached| cached.device.last_seen_ms)
    };
    let rssi = live_rssi.or_else(|| cached.as_ref().and_then(|cached| cached.device.rssi));
    let battery = device_batteries(device, identity, connected, fast_pair).await?;
    let fast_pair_features = match (connected, fast_pair) {
        (true, Some(provider)) => provider.features(device).await,
        _ => None,
    };
    let has_fast_pair = metadata.uuids.iter().any(|uuid| {
        uuid.eq_ignore_ascii_case(FAST_PAIR_SERVICE_UUID)
            || uuid.eq_ignore_ascii_case(MESSAGE_STREAM_UUID)
    });
    let capabilities = device_capabilities(
        paired,
        connected,
        blocked,
        wake_allowed,
        has_fast_pair,
        fast_pair_features.as_ref(),
    );
    let snapshot = Device {
        key: key.clone(),
        adapter_key: adapter_key.to_string(),
        name: metadata.alias.clone(),
        alias: metadata.alias,
        address: identity.to_string(),
        address_type: metadata.address_type,
        icon: metadata.icon,
        paired,
        bonded: bonded_property(system_bus, adapter.name(), identity).await,
        connected,
        services_resolved: metadata.services_resolved,
        trusted,
        blocked,
        wake_allowed,
        legacy_pairing: metadata.legacy_pairing,
        modalias: metadata.modalias,
        uuids: metadata.uuids,
        services,
        battery,
        fast_pair: fast_pair_features,
        rssi,
        signal_strength: rssi.map(signal_strength),
        signal_live: live_rssi.is_some(),
        present,
        last_seen_ms,
        capabilities,
    };
    if present {
        device_cache.lock().await.insert(
            key,
            CachedDevice {
                device: snapshot.clone(),
                observed_at_ms: now_ms,
            },
        );
    }
    Ok(Some(snapshot))
}

fn cached_device_view(cached: &CachedDevice) -> Device {
    let mut device = cached.device.clone();
    device.connected = false;
    device.present = false;
    device.signal_live = false;
    let has_fast_pair = device.uuids.iter().any(|uuid| {
        uuid.eq_ignore_ascii_case(FAST_PAIR_SERVICE_UUID)
            || uuid.eq_ignore_ascii_case(MESSAGE_STREAM_UUID)
    });
    device.capabilities = device_capabilities(
        device.paired,
        false,
        device.blocked,
        device.wake_allowed,
        has_fast_pair,
        device.fast_pair.as_ref(),
    );
    device
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn cache_entry_is_fresh(observed_at_ms: u64, now_ms: u64) -> bool {
    now_ms.saturating_sub(observed_at_ms) <= DISCOVERED_DEVICE_CACHE_TTL.as_millis() as u64
}

const fn should_include_device(paired: bool, present: bool, cached: bool) -> bool {
    paired || present || cached
}

async fn device_batteries(
    device: &BluezDevice,
    identity: bluer::Address,
    connected: bool,
    fast_pair: Option<&FastPairBatteryProvider>,
) -> Result<Vec<Battery>> {
    let component_battery = match (connected, fast_pair) {
        (true, Some(provider)) => provider.batteries(identity).await,
        _ => Vec::new(),
    };
    if !component_battery.is_empty() {
        return Ok(component_battery);
    }
    Ok(
        bluez_result(device.battery_percentage().await, "read device battery")?
            .map(aggregate_battery)
            .into_iter()
            .collect(),
    )
}

fn aggregate_battery(percentage: u8) -> Battery {
    Battery {
        id: "aggregate".into(),
        label: "Battery".into(),
        component: "main".into(),
        percentage,
        source: "bluez".into(),
        confidence: "standard".into(),
    }
}

fn device_capabilities(
    paired: bool,
    connected: bool,
    blocked: bool,
    wake_allowed: Option<bool>,
    has_fast_pair: bool,
    fast_pair: Option<&crate::model::FastPairFeatures>,
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
    let can_provision_fast_pair = paired
        && connected
        && has_fast_pair
        && fast_pair.is_some_and(|features| {
            features.model_id.is_some() && !features.authenticated_controls
        });
    if !can_provision_fast_pair {
        unsupported_reasons.insert(
            "provision_fast_pair".into(),
            "Fast Pair provisioning requires recent-pairing model metadata from a connected device"
                .into(),
        );
    }
    let authenticated = fast_pair.is_some_and(|features| features.authenticated_controls);
    let can_set_multipoint = authenticated
        && fast_pair
            .and_then(|features| features.multipoint)
            .is_some_and(|multipoint| multipoint.supported && multipoint.configurable);
    if !can_set_multipoint {
        unsupported_reasons.insert(
            "set_multipoint".into(),
            "Authenticated configurable Fast Pair multipoint is unavailable".into(),
        );
    }
    let can_set_noise_control = authenticated
        && fast_pair
            .and_then(|features| features.noise_control.as_ref())
            .is_some_and(|noise| !noise.settable_modes.is_empty());
    if !can_set_noise_control {
        unsupported_reasons.insert(
            "set_noise_control".into(),
            "Authenticated Fast Pair noise control is unavailable".into(),
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
        can_provision_fast_pair,
        can_set_multipoint,
        can_set_noise_control,
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
        tracing::warn!(%operation, code = kind.code(), %error, "BlueZ operation failed");
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
    if uuid.eq_ignore_ascii_case(FAST_PAIR_SERVICE_UUID) {
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
    let proxy = match zbus::Proxy::new(connection, "org.bluez", path, "org.bluez.Device1").await {
        Ok(proxy) => proxy,
        Err(error) => {
            tracing::debug!(%adapter, %address, %error, "BlueZ Bonded compatibility proxy is unavailable");
            return None;
        }
    };
    match proxy.get_property::<bool>("Bonded").await {
        Ok(bonded) => Some(bonded),
        Err(error) => {
            tracing::debug!(%adapter, %address, %error, "BlueZ Bonded compatibility property is unavailable");
            None
        }
    }
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

    use super::{
        DISCOVERED_DEVICE_CACHE_TTL, bluez_result, cache_entry_is_fresh, device_capabilities,
        opaque_key, service_label, should_include_device, signal_strength,
    };

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
    fn discovered_device_cache_has_a_bounded_lifetime() {
        let ttl_ms = DISCOVERED_DEVICE_CACHE_TTL.as_millis() as u64;
        assert!(cache_entry_is_fresh(1_000, 1_000 + ttl_ms));
        assert!(!cache_entry_is_fresh(1_000, 1_001 + ttl_ms));
        assert!(cache_entry_is_fresh(2_000, 1_000));
    }

    #[test]
    fn cached_unpaired_device_survives_loss_of_live_rssi() {
        assert!(should_include_device(false, true, false));
        assert!(should_include_device(false, false, true));
        assert!(!should_include_device(false, false, false));
        assert!(should_include_device(true, false, false));
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
        let capabilities = device_capabilities(false, false, false, None, false, None);
        assert!(capabilities.can_pair);
        assert!(capabilities.can_connect);
        assert!(!capabilities.can_disconnect);
        assert_eq!(
            capabilities.unsupported_reasons["disconnect"],
            "Device is not connected"
        );
    }
}
