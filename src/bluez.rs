use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
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
        ObexRemote, ObexTarget, Params,
    },
    fast_pair::FastPairBatteryProvider,
    identity::DeviceIdentityRegistry,
    management::ManagementStore,
    model::{Device, Snapshot},
    rfkill,
};

mod snapshot;

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
    management: ManagementStore,
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
        let management = ManagementStore::load_default()?;
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
            management,
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

    pub async fn apply_startup_policy(&self) {
        let policy = self.management.policy();
        let result = match policy.launch_state.as_str() {
            "enable" => self.set_powered(None, true).await.map(|_| ()),
            "disable" => self.set_powered(None, false).await.map(|_| ()),
            _ => self.restore_runtime_state().await,
        };
        if let Err(error) = result {
            tracing::warn!(%error, launch_state = %policy.launch_state, "could not apply Bluetooth startup policy");
        }
    }

    pub fn start_lifecycle_monitoring(self: &Arc<Self>) {
        let backend = Arc::clone(self);
        crate::task::spawn("logind-bluetooth-lifecycle", async move {
            if let Err(error) = backend.monitor_sleep_events().await {
                tracing::warn!(%error, "Bluetooth suspend/resume monitoring stopped");
            }
        });
    }

    async fn monitor_sleep_events(&self) -> Result<()> {
        let connection = zbus::Connection::system()
            .await
            .context("connect Bluetooth lifecycle monitor to system D-Bus")?;
        let proxy = zbus::Proxy::new(
            &connection,
            "org.freedesktop.login1",
            "/org/freedesktop/login1",
            "org.freedesktop.login1.Manager",
        )
        .await
        .context("create logind Bluetooth lifecycle proxy")?;
        let mut events = proxy
            .receive_signal("PrepareForSleep")
            .await
            .context("subscribe to logind sleep events")?;
        while let Some(message) = events.next().await {
            let (sleeping,): (bool,) = message
                .body()
                .deserialize()
                .context("decode logind sleep event")?;
            if sleeping {
                self.remember_runtime_state().await;
            } else if let Err(error) = self.restore_runtime_state().await {
                tracing::warn!(%error, "could not restore Bluetooth state after resume");
            }
        }
        bail!("logind sleep event stream ended")
    }

    async fn remember_runtime_state(&self) {
        match snapshot::build(self).await {
            Ok(snapshot) => self.management.remember_snapshot(&snapshot),
            Err(error) => tracing::warn!(%error, "could not capture Bluetooth runtime state"),
        }
    }

    async fn restore_runtime_state(&self) -> Result<()> {
        let runtime = self.management.runtime();
        self.restore_adapter_power(runtime.adapter_power()).await?;
        if self.management.policy().reconnect_on_resume {
            for device_key in runtime.connected_device_keys() {
                self.reconnect_device(device_key).await?;
            }
        }
        Ok(())
    }

    async fn restore_adapter_power(&self, power: &HashMap<String, bool>) -> Result<()> {
        for adapter in self.adapters().await? {
            let address = adapter
                .address()
                .await
                .backend_context("read adapter identity during restore")?;
            if let Some(powered) = power.get(&opaque_key("adapter", &address.to_string())) {
                adapter
                    .set_powered(*powered)
                    .await
                    .backend_context("restore adapter power")?;
            }
        }
        Ok(())
    }

    async fn reconnect_device(&self, device_key: &str) -> Result<()> {
        let Ok((_, device)) = self.find_device(device_key).await else {
            return Ok(());
        };
        if bluez_result(device.is_connected().await, "read reconnect device state")? {
            return Ok(());
        }
        match tokio::time::timeout(Duration::from_secs(15), device.connect()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(%error, %device_key, "could not reconnect Bluetooth device")
            }
            Err(_) => tracing::warn!(%device_key, "Bluetooth reconnect timed out"),
        }
        Ok(())
    }

    async fn set_one_adapter_power(&self, key: &str, powered: bool) -> Result<()> {
        let adapter = self.find_adapter(key).await?;
        adapter
            .set_powered(powered)
            .await
            .backend_context("set adapter power")?;
        if !powered {
            self.stop_discovery(Some(adapter.name())).await;
        }
        Ok(())
    }

    async fn set_all_adapter_power(&self, powered: bool) -> Result<()> {
        let before = self.snapshot().await?;
        if powered && before.radio.hard_blocked {
            return Err(BackendError::new(
                BackendErrorKind::Rejected,
                "Bluetooth is disabled by a hardware radio switch",
            )
            .into());
        }
        let adapters = self.adapters().await?;
        if powered {
            unblock_rfkill(&before, adapters.is_empty())?;
        }
        set_adapters_power(adapters, powered).await?;
        update_rfkill(&before, powered);
        if !powered {
            self.stop_discovery(None).await;
        }
        Ok(())
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

    async fn start_discovery(&self, adapter: BluezAdapter) -> Result<bool> {
        let name = adapter.name().to_string();
        let mut tasks = self.discovery_tasks.lock().await;
        if tasks.get(&name).is_some_and(|task| !task.is_finished()) {
            return Ok(false);
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
        Ok(true)
    }

    async fn rollback_discovery(&self, adapter_names: &[String]) {
        for name in adapter_names {
            self.stop_discovery(Some(name)).await;
        }
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
        snapshot::build(self).await
    }

    async fn set_powered(&self, adapter_key: Option<&str>, powered: bool) -> Result<Snapshot> {
        tracing::info!(?adapter_key, powered, "setting Bluetooth adapter power");
        match adapter_key {
            Some(key) => self.set_one_adapter_power(key, powered).await?,
            None => self.set_all_adapter_power(powered).await?,
        }
        let snapshot = self.snapshot().await?;
        self.management.remember_snapshot(&snapshot);
        Ok(snapshot)
    }

    async fn set_scanning(&self, adapter_key: Option<&str>, enabled: bool) -> Result<Snapshot> {
        tracing::info!(?adapter_key, enabled, "setting Bluetooth discovery state");
        if let Some(key) = adapter_key {
            set_adapter_scanning(self, self.find_adapter(key).await?, enabled).await?;
        } else if enabled {
            let mut started = Vec::new();
            let mut eligible = 0_u32;
            for adapter in self.adapters().await? {
                let name = adapter.name().to_string();
                let powered = match bluez_result(adapter.is_powered().await, "read adapter power") {
                    Ok(powered) => powered,
                    Err(error) => {
                        self.rollback_discovery(&started).await;
                        return Err(error);
                    }
                };
                if powered {
                    eligible += 1;
                    match self.start_discovery(adapter).await {
                        Ok(true) => started.push(name),
                        Ok(false) => {}
                        Err(error) => {
                            self.rollback_discovery(&started).await;
                            return Err(error);
                        }
                    }
                }
            }
            if eligible == 0 {
                return Err(BackendError::new(
                    BackendErrorKind::Rejected,
                    "Bluetooth discovery requires at least one powered adapter",
                )
                .into());
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

    async fn update_management(&self, params: &Value) -> Result<Snapshot> {
        self.management.update(params)?;
        self.snapshot().await
    }

    async fn obex_target(&self, device_key: &str) -> Result<ObexTarget> {
        tracing::debug!(%device_key, "resolving outgoing OBEX target");
        let (adapter, device) = self.find_device(device_key).await?;
        let paired = bluez_result(device.is_paired().await, "read outgoing OBEX paired state")?;
        let blocked = bluez_result(
            device.is_blocked().await,
            "read outgoing OBEX blocked state",
        )?;
        validate_obex_send_state(paired, blocked)?;
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
        let trust_after_pair = self.management.policy().trust_after_pair;
        run_device_operation(
            &adapter,
            &device,
            operation,
            params,
            self.fast_pair.as_deref(),
            trust_after_pair,
        )
        .await?;
        if operation == DeviceOperation::Remove {
            self.device_cache.lock().await.remove(device_key);
            self.identities.forget_presentation(device_key);
        }
        let snapshot = self.snapshot().await?;
        self.management.remember_snapshot(&snapshot);
        Ok(snapshot)
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

fn validate_obex_send_state(paired: bool, blocked: bool) -> Result<()> {
    if !paired {
        return Err(BackendError::new(
            BackendErrorKind::Rejected,
            "outgoing transfers require a paired Bluetooth device",
        )
        .into());
    }
    if blocked {
        return Err(BackendError::new(
            BackendErrorKind::Rejected,
            "outgoing transfers are disabled for blocked Bluetooth devices",
        )
        .into());
    }
    Ok(())
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

fn unblock_rfkill(before: &Snapshot, no_adapters: bool) -> Result<()> {
    if !before.radio.rfkill_present || !before.radio.soft_blocked {
        return Ok(());
    }
    let Err(error) = rfkill::set_bluetooth_soft_blocked(false) else {
        return Ok(());
    };
    if no_adapters {
        return Err(BackendError::new(
            BackendErrorKind::Rejected,
            format!("could not unblock Bluetooth through rfkill: {error:#}"),
        )
        .into());
    }
    tracing::warn!(%error, "direct rfkill unblock failed; asking BlueZ to power available adapters");
    Ok(())
}

async fn set_adapters_power(adapters: Vec<BluezAdapter>, powered: bool) -> Result<()> {
    let operation = if powered {
        "enable adapter power"
    } else {
        "disable adapter power"
    };
    for adapter in adapters {
        adapter
            .set_powered(powered)
            .await
            .backend_context(operation)?;
    }
    Ok(())
}

fn update_rfkill(before: &Snapshot, powered: bool) {
    if powered || !before.radio.rfkill_present || before.radio.soft_blocked {
        return;
    }
    if let Err(error) = rfkill::set_bluetooth_soft_blocked(true) {
        tracing::warn!(%error, "could not soft-block Bluetooth; adapters are powered off");
    }
}

async fn set_adapter_scanning(
    backend: &BluezBackend,
    adapter: BluezAdapter,
    enabled: bool,
) -> Result<()> {
    if enabled {
        if !bluez_result(adapter.is_powered().await, "read adapter power")? {
            return Err(BackendError::new(
                BackendErrorKind::Rejected,
                "Bluetooth discovery requires a powered adapter",
            )
            .into());
        }
        backend.start_discovery(adapter).await?;
    } else {
        backend.stop_discovery(Some(adapter.name())).await;
    }
    Ok(())
}

async fn run_device_operation(
    adapter: &BluezAdapter,
    device: &BluezDevice,
    operation: DeviceOperation,
    params: &Value,
    fast_pair: Option<&FastPairBatteryProvider>,
    default_trust_after_pair: bool,
) -> Result<()> {
    use DeviceOperation::{
        ProvisionFastPair, ResetAlias, SetAlias, SetBlocked, SetMultipoint, SetNoiseControl,
        SetTrusted, SetWakeAllowed,
    };
    match operation {
        DeviceOperation::Pair => {
            let trust_after_pair = trust_after_pair(params, default_trust_after_pair)?;
            let public_key = params.optional_string("fast_pair_anti_spoofing_public_key")?;
            if public_key.is_some() && fast_pair.is_none() {
                bail!("Fast Pair provider is unavailable");
            }
            pair_device(device, trust_after_pair).await?;
            provision_after_pair(adapter, device, public_key, fast_pair).await
        }
        DeviceOperation::Connect => connect_device(device, "connect Bluetooth device").await,
        DeviceOperation::Disconnect => {
            disconnect_device(device, "disconnect Bluetooth device").await
        }
        DeviceOperation::Remove => remove_device(adapter, device).await,
        SetTrusted | SetBlocked | SetWakeAllowed | SetAlias | ResetAlias => {
            run_property_operation(device, operation, params).await
        }
        ProvisionFastPair => {
            fast_pair
                .context("Fast Pair provider is unavailable")?
                .provision_account_key(
                    adapter,
                    device,
                    params.require_string("anti_spoofing_public_key")?,
                )
                .await
        }
        SetMultipoint => {
            fast_pair
                .context("Fast Pair provider is unavailable")?
                .set_multipoint(device, params.require_bool("enabled")?)
                .await
        }
        SetNoiseControl => {
            fast_pair
                .context("Fast Pair provider is unavailable")?
                .set_noise_control(device, params.require_string("mode")?)
                .await
        }
    }
}

async fn run_property_operation(
    device: &BluezDevice,
    operation: DeviceOperation,
    params: &Value,
) -> Result<()> {
    match operation {
        DeviceOperation::SetTrusted => device
            .set_trusted(params.require_bool("trusted")?)
            .await
            .backend_context("set trusted state"),
        DeviceOperation::SetBlocked => device
            .set_blocked(params.require_bool("blocked")?)
            .await
            .backend_context("set blocked state"),
        DeviceOperation::SetWakeAllowed => device
            .set_wake_allowed(params.require_bool("wake_allowed")?)
            .await
            .backend_context("set wake permission"),
        DeviceOperation::SetAlias => device
            .set_alias(params.require_string("alias")?.to_string())
            .await
            .backend_context("set device alias"),
        DeviceOperation::ResetAlias => device
            .set_alias(String::new())
            .await
            .backend_context("reset device alias"),
        _ => bail!("invalid property operation group"),
    }
}

async fn provision_after_pair(
    adapter: &BluezAdapter,
    device: &BluezDevice,
    public_key: Option<&str>,
    fast_pair: Option<&FastPairBatteryProvider>,
) -> Result<()> {
    if let Some(public_key) = public_key {
        fast_pair
            .context("Fast Pair provider is unavailable")?
            .provision_account_key(adapter, device, public_key)
            .await?;
    }
    Ok(())
}

fn trust_after_pair(params: &Value, default: bool) -> Result<bool> {
    Ok(params.optional_bool("trust_after_pair")?.unwrap_or(default))
}

async fn pair_device(device: &BluezDevice, trust_after_pair: bool) -> Result<()> {
    operation_timeout(
        Duration::from_secs(75),
        "pair Bluetooth device",
        device.pair(),
    )
    .await?;
    if trust_after_pair {
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

#[cfg(test)]
mod tests {
    use crate::backend::{BackendError, BackendErrorKind};

    use super::{bluez_result, opaque_key, trust_after_pair, validate_obex_send_state};

    #[test]
    fn adapter_keys_are_opaque_and_deterministic() {
        assert_eq!(
            opaque_key("adapter", "hci0:AA"),
            opaque_key("adapter", "hci0:AA")
        );
        assert!(!opaque_key("adapter", "hci0:AA").contains("AA"));
    }

    #[test]
    fn pairing_defaults_and_obex_policy_are_enforced() {
        assert!(!trust_after_pair(&serde_json::json!({}), false).unwrap());
        assert!(trust_after_pair(&serde_json::json!({ "trust_after_pair": true }), false).unwrap());
        assert!(
            trust_after_pair(&serde_json::json!({ "trust_after_pair": "false" }), true).is_err()
        );

        assert!(validate_obex_send_state(true, false).is_ok());
        assert!(validate_obex_send_state(false, false).is_err());
        assert!(validate_obex_send_state(true, true).is_err());
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
}
