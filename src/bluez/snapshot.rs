use std::{collections::HashSet, time::SystemTime};

use anyhow::Result;
use bluer::{Adapter as BluezAdapter, Device as BluezDevice};

use crate::{
    fast_pair::{FAST_PAIR_SERVICE_UUID, FastPairBatteryProvider, MESSAGE_STREAM_UUID},
    identity::DeviceIdentityRegistry,
    model::{Adapter, Battery, Device, DeviceCapabilities, Service, Snapshot},
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
    backend: &BluezBackend,
    adapter: &BluezAdapter,
    device: &BluezDevice,
    adapter_key: &str,
) -> Result<Option<Device>> {
    let paired = bluez_result(device.is_paired().await, "read device paired state")?;
    let connected = bluez_result(device.is_connected().await, "read device connected state")?;
    let live_rssi = bluez_result(device.rssi().await, "read device signal strength")?;
    let present = live_rssi.is_some() || connected;
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
    if !should_include_device(paired, present, cached.is_some()) {
        tracing::trace!(device_key = %key, paired, present, "device omitted from snapshot");
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
    let last_seen_ms = present.then_some(now_ms).or_else(|| {
        cached
            .as_ref()
            .and_then(|cached| cached.device.last_seen_ms)
    });
    let rssi = live_rssi.or_else(|| cached.as_ref().and_then(|cached| cached.device.rssi));
    let observed_battery =
        device_batteries(device, identity, connected, backend.fast_pair.as_deref()).await?;
    let (icon, battery) = presentation(
        &backend.identities,
        &key,
        paired,
        connected,
        metadata.icon,
        observed_battery,
    );
    let fast_pair_features = match (connected, backend.fast_pair.as_deref()) {
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
        icon,
        paired,
        bonded: bonded_property(&backend.system_bus, adapter.name(), identity).await,
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

fn presentation(
    identities: &DeviceIdentityRegistry,
    key: &str,
    paired: bool,
    connected: bool,
    icon: Option<String>,
    battery: Vec<Battery>,
) -> (Option<String>, Vec<Battery>) {
    if !paired {
        identities.forget_presentation(key);
        return (icon, battery);
    }
    let (remembered_icon, remembered_battery) =
        identities.remember_presentation(key, icon.as_deref(), &battery);
    let icon = icon.filter(|icon| !icon.trim().is_empty());
    let restored_icon = icon.is_none() && remembered_icon.is_some();
    let restored_battery = !connected && battery.is_empty() && !remembered_battery.is_empty();
    tracing::trace!(
        device_key = key,
        restored_icon,
        restored_battery,
        "resolved snapshot presentation"
    );
    let battery = if restored_battery {
        remembered_battery
    } else {
        battery
    };
    (icon.or(remembered_icon), battery)
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
    let can_provision_fast_pair = paired
        && connected
        && has_fast_pair
        && fast_pair.is_some_and(|features| {
            features.model_id.is_some() && !features.authenticated_controls
        });
    let authenticated = fast_pair.is_some_and(|features| features.authenticated_controls);
    let can_set_multipoint = authenticated
        && fast_pair
            .and_then(|features| features.multipoint)
            .is_some_and(|multipoint| multipoint.supported && multipoint.configurable);
    let can_set_noise_control = authenticated
        && fast_pair
            .and_then(|features| features.noise_control.as_ref())
            .is_some_and(|noise| !noise.settable_modes.is_empty());
    let unsupported_reasons = [
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
    .collect();
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
