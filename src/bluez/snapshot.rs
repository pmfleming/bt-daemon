use std::{collections::HashSet, time::SystemTime};

use anyhow::Result;
use bluer::{Adapter as BluezAdapter, Device as BluezDevice};

use crate::{
    fast_pair::{FAST_PAIR_SERVICE_UUID, FastPairBatteryProvider, MESSAGE_STREAM_UUID},
    identity::DeviceIdentityRegistry,
    model::{
        Adapter, Battery, Device, DeviceCapabilities, DeviceIdentity, DevicePresentation,
        DeviceServices, DeviceState, Service, Snapshot, presentation_components, presentation_type,
    },
};

use super::{
    BluezBackend, BluezResultExt, CachedDevice, DISCOVERED_DEVICE_CACHE_TTL, bluez_result,
    opaque_key,
};

pub(super) async fn build(backend: &BluezBackend) -> Result<Snapshot> {
    let now_ms = unix_time_ms();
    backend
        .device_cache
        .lock()
        .await
        .retain(|_, cached| cache_entry_is_fresh(cached.observed_at_ms, now_ms));
    let mut snapshot = Snapshot::default();
    for adapter in backend.adapters().await? {
        let address = adapter
            .address()
            .await
            .backend_context("read stable adapter identity")?;
        backend
            .identities
            .register_adapter(adapter.name(), &address.to_string());
        let adapter_key = opaque_key("adapter", &address.to_string());
        snapshot
            .adapters
            .push(adapter_snapshot(&adapter, &adapter_key).await?);
        snapshot
            .devices
            .extend(adapter_devices(backend, &adapter, &adapter_key).await?);
    }
    snapshot.devices.sort_by_key(|device| {
        (
            !device.state.connected,
            !device.state.paired,
            device.identity.name.to_lowercase(),
        )
    });
    let powered = snapshot.adapters.iter().any(|adapter| adapter.powered);
    snapshot.radio = crate::rfkill::radio_state(snapshot.adapters.len(), powered);
    snapshot.management = backend.management.policy();
    tracing::debug!(
        adapters = snapshot.adapters.len(),
        devices = snapshot.devices.len(),
        "BlueZ snapshot completed"
    );
    Ok(snapshot)
}

async fn adapter_devices(
    backend: &BluezBackend,
    adapter: &BluezAdapter,
    adapter_key: &str,
) -> Result<Vec<Device>> {
    let mut devices = Vec::new();
    let mut included = HashSet::new();
    for address in adapter
        .device_addresses()
        .await
        .backend_context("list adapter devices")?
    {
        let device = adapter
            .device(address)
            .backend_context("open BlueZ device")?;
        if let Some(snapshot) = device_snapshot(backend, adapter, &device, adapter_key).await? {
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

struct DeviceMetadata {
    alias: String,
    remote_name: Option<String>,
    address_type: String,
    icon: Option<String>,
    services_resolved: bool,
    legacy_pairing: bool,
    modalias: Option<String>,
    uuids: Vec<String>,
}

struct BluezDeviceState {
    paired: bool,
    connected: bool,
    trusted: bool,
    blocked: bool,
    wake_allowed: Option<bool>,
    live_rssi: Option<i16>,
}

impl BluezDeviceState {
    async fn read(device: &BluezDevice) -> Result<Self> {
        Ok(Self {
            paired: bluez_result(device.is_paired().await, "read device paired state")?,
            connected: bluez_result(device.is_connected().await, "read device connected state")?,
            trusted: bluez_result(device.is_trusted().await, "read device trusted state")?,
            blocked: bluez_result(device.is_blocked().await, "read device blocked state")?,
            wake_allowed: bluez_result(
                device.is_wake_allowed().await,
                "read device wake permission",
            )?,
            live_rssi: bluez_result(device.rssi().await, "read device signal strength")?,
        })
    }

    fn present(&self) -> bool {
        self.live_rssi.is_some() || self.connected
    }
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
            remote_name: bluez_result(device.name().await, "read remote device name")?,
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
    backend: &BluezBackend,
    adapter: &BluezAdapter,
    device: &BluezDevice,
    adapter_key: &str,
) -> Result<Option<Device>> {
    let state = BluezDeviceState::read(device).await?;
    let present = state.present();
    let identity = device.address();
    let key = backend.identities.device_key(adapter.name(), identity);
    let now_ms = unix_time_ms();
    let cached = backend
        .device_cache
        .lock()
        .await
        .get(&key)
        .filter(|cached| cache_entry_is_fresh(cached.observed_at_ms, now_ms))
        .cloned();
    if !should_include_device(state.paired, present, cached.is_some()) {
        tracing::trace!(device_key = %key, paired = state.paired, present, "device omitted from snapshot");
        return Ok(None);
    }
    let metadata = DeviceMetadata::read(device).await?;
    let services = metadata
        .uuids
        .iter()
        .map(|uuid| Service {
            uuid: uuid.clone(),
            label: service_label(uuid).to_string(),
        })
        .collect();
    let last_seen_ms = present.then_some(now_ms).or_else(|| {
        cached
            .as_ref()
            .and_then(|cached| cached.device.presentation.last_seen_ms)
    });
    let rssi = state.live_rssi.or_else(|| {
        cached
            .as_ref()
            .and_then(|cached| cached.device.presentation.rssi)
    });
    let observed_battery = device_batteries(
        device,
        identity,
        state.connected,
        backend.fast_pair.as_deref(),
    )
    .await?;
    let fast_pair_features = match (state.connected, backend.fast_pair.as_deref()) {
        (true, Some(provider)) => provider.features(device).await,
        _ => None,
    };
    let presentation = presentation(
        &backend.identities,
        &key,
        state.paired,
        state.connected,
        metadata.icon,
        fast_pair_features
            .as_ref()
            .and_then(|features| features.model_id.as_deref()),
        observed_battery,
    );
    let has_fast_pair = metadata.uuids.iter().any(|uuid| {
        uuid.eq_ignore_ascii_case(FAST_PAIR_SERVICE_UUID)
            || uuid.eq_ignore_ascii_case(MESSAGE_STREAM_UUID)
    });
    let capabilities = device_capabilities(
        state.paired,
        state.connected,
        state.blocked,
        state.wake_allowed,
        has_fast_pair,
        fast_pair_features.as_ref(),
    );
    let snapshot = Device {
        key: key.clone(),
        adapter_key: adapter_key.to_string(),
        identity: DeviceIdentity {
            name: metadata.alias.clone(),
            alias: metadata.alias,
            remote_name: metadata.remote_name,
            device_type: presentation.device_type,
            address: identity.to_string(),
            address_type: metadata.address_type,
            icon: presentation.icon,
            modalias: metadata.modalias,
        },
        state: DeviceState {
            paired: state.paired,
            bonded: bonded_property(&backend.system_bus, adapter.name(), identity).await,
            connected: state.connected,
            trusted: state.trusted,
            blocked: state.blocked,
            wake_allowed: state.wake_allowed,
            legacy_pairing: metadata.legacy_pairing,
        },
        services: DeviceServices {
            services_resolved: metadata.services_resolved,
            uuids: metadata.uuids,
            services,
        },
        presentation: DevicePresentation {
            battery: presentation.battery,
            components: presentation.components,
            model_id: presentation.model_id,
            fast_pair: fast_pair_features,
            rssi,
            signal_strength: rssi.map(signal_strength),
            signal_live: state.live_rssi.is_some(),
            present,
            last_seen_ms,
        },
        policy: backend.management.device_policy(&key),
        capabilities,
    };
    if present {
        backend.device_cache.lock().await.insert(
            key,
            CachedDevice {
                device: snapshot.clone(),
                observed_at_ms: now_ms,
            },
        );
    }
    Ok(Some(snapshot))
}

struct ResolvedPresentation {
    icon: Option<String>,
    device_type: String,
    model_id: Option<String>,
    components: Vec<String>,
    battery: Vec<Battery>,
}

fn presentation(
    identities: &DeviceIdentityRegistry,
    key: &str,
    paired: bool,
    connected: bool,
    icon: Option<String>,
    model_id: Option<&str>,
    battery: Vec<Battery>,
) -> ResolvedPresentation {
    if !paired {
        identities.forget_presentation(key);
        return ResolvedPresentation {
            device_type: presentation_type(icon.as_deref(), &battery).into(),
            components: presentation_components(&battery),
            model_id: model_id.map(Into::into),
            icon,
            battery,
        };
    }
    let remembered = identities.remember_presentation(key, icon.as_deref(), model_id, &battery);
    let icon = icon.filter(|icon| !icon.trim().is_empty());
    let restored_icon = icon.is_none() && remembered.icon.is_some();
    let restored_battery = !connected && battery.is_empty() && !remembered.battery.is_empty();
    tracing::trace!(
        device_key = key,
        restored_icon,
        restored_battery,
        restored_model = model_id.is_none() && remembered.model_id.is_some(),
        restored_components = battery.is_empty() && !remembered.components.is_empty(),
        "resolved snapshot presentation"
    );
    ResolvedPresentation {
        icon: icon.or(remembered.icon),
        device_type: remembered.device_type,
        model_id: remembered.model_id,
        components: remembered.components,
        battery: if restored_battery {
            remembered.battery
        } else {
            battery
        },
    }
}

fn cached_device_view(cached: &CachedDevice) -> Device {
    let mut device = cached.device.clone();
    device.state.connected = false;
    device.presentation.present = false;
    device.presentation.signal_live = false;
    let has_fast_pair = device.services.uuids.iter().any(|uuid| {
        uuid.eq_ignore_ascii_case(FAST_PAIR_SERVICE_UUID)
            || uuid.eq_ignore_ascii_case(MESSAGE_STREAM_UUID)
    });
    device.capabilities = device_capabilities(
        device.state.paired,
        false,
        device.state.blocked,
        device.state.wake_allowed,
        has_fast_pair,
        device.presentation.fast_pair.as_ref(),
    );
    device
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
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
            .map(Battery::bluez_aggregate)
            .into_iter()
            .collect(),
    )
}

fn device_capabilities(
    paired: bool,
    connected: bool,
    blocked: bool,
    wake_allowed: Option<bool>,
    has_fast_pair: bool,
    fast_pair: Option<&crate::model::FastPairFeatures>,
) -> DeviceCapabilities {
    let (can_provision_fast_pair, can_set_multipoint, can_set_noise_control) =
        fast_pair_capabilities(paired, connected, has_fast_pair, fast_pair);
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
        unsupported_reasons: unsupported_reasons(
            paired,
            connected,
            wake_allowed,
            can_provision_fast_pair,
            can_set_multipoint,
            can_set_noise_control,
        ),
    }
}

fn fast_pair_capabilities(
    paired: bool,
    connected: bool,
    has_fast_pair: bool,
    features: Option<&crate::model::FastPairFeatures>,
) -> (bool, bool, bool) {
    let authenticated = features.is_some_and(|features| features.authenticated_controls);
    let provision = paired
        && connected
        && has_fast_pair
        && features.is_some_and(|features| {
            features.model_id.is_some() && !features.authenticated_controls
        });
    let multipoint = authenticated
        && features
            .and_then(|features| features.multipoint)
            .is_some_and(|multipoint| multipoint.supported && multipoint.configurable);
    let noise_control = authenticated
        && features
            .and_then(|features| features.noise_control.as_ref())
            .is_some_and(|noise| !noise.settable_modes.is_empty());
    (provision, multipoint, noise_control)
}

fn unsupported_reasons(
    paired: bool,
    connected: bool,
    wake_allowed: Option<bool>,
    can_provision_fast_pair: bool,
    can_set_multipoint: bool,
    can_set_noise_control: bool,
) -> std::collections::HashMap<String, String> {
    [
        (paired, "pair", "Device is already paired"),
        (connected, "connect", "Device is already connected"),
        (!connected, "disconnect", "Device is not connected"),
        (
            wake_allowed.is_none(),
            "wake",
            "BlueZ does not expose wake control for this device",
        ),
        (!paired, "send_file", "Pair the device before sending files"),
        (
            !can_provision_fast_pair,
            "provision_fast_pair",
            "Fast Pair provisioning requires recent-pairing model metadata from a connected device",
        ),
        (
            !can_set_multipoint,
            "set_multipoint",
            "Authenticated configurable Fast Pair multipoint is unavailable",
        ),
        (
            !can_set_noise_control,
            "set_noise_control",
            "Authenticated Fast Pair noise control is unavailable",
        ),
    ]
    .into_iter()
    .filter(|(unsupported, _, _)| *unsupported)
    .map(|(_, operation, reason)| (operation.into(), reason.into()))
    .collect()
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
    use crate::fast_pair::{FAST_PAIR_SERVICE_UUID, MESSAGE_STREAM_UUID};

    use super::{
        DISCOVERED_DEVICE_CACHE_TTL, cache_entry_is_fresh, device_capabilities, service_label,
        should_include_device, signal_strength,
    };

    // During development phases, structured logs and error capture are preferred to broad tests,
    // which may lock in incorrect Bluetooth behavior. Keep these tests minimal and limited to
    // stable, hardware-independent snapshot transformations.

    #[test]
    fn stable_snapshot_transformations_are_bounded() {
        assert_eq!(service_label(FAST_PAIR_SERVICE_UUID), "Fast Pair Service");
        assert_eq!(
            service_label(MESSAGE_STREAM_UUID),
            "Fast Pair Message Stream"
        );
        assert_eq!(
            (
                signal_strength(-120),
                signal_strength(-70),
                signal_strength(-10)
            ),
            (0, 50, 100)
        );
        let ttl_ms = DISCOVERED_DEVICE_CACHE_TTL.as_millis() as u64;
        assert!(cache_entry_is_fresh(1_000, 1_000 + ttl_ms));
        assert!(!cache_entry_is_fresh(1_000, 1_001 + ttl_ms));
        assert!(should_include_device(false, false, true));
        assert!(!should_include_device(false, false, false));
    }

    #[test]
    fn capabilities_follow_pairing_and_connection_state() {
        let unknown = device_capabilities(false, false, false, None, false, None);
        assert!(unknown.can_pair && unknown.can_connect);
        assert!(!unknown.can_disconnect);
        assert_eq!(
            unknown.unsupported_reasons["disconnect"],
            "Device is not connected"
        );

        let connected = device_capabilities(true, true, false, Some(true), false, None);
        assert!(connected.can_disconnect && connected.can_remove && connected.can_send_file);
        assert!(!connected.can_pair && !connected.can_connect);
    }
}
