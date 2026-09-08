//! Snapshot construction from captured evidence; no clocks, locks or hardware I/O.
use super::{
    BluezDeviceState, DeviceMetadata, ResolvedPresentation, device_capabilities, service_label,
    signal_strength,
};
use crate::fast_pair::{FAST_PAIR_SERVICE_UUID, MESSAGE_STREAM_UUID};
use crate::{
    bluez::{CachedDevice, observations::Observation},
    management::DevicePolicy,
    model::{
        Device, DeviceIdentity, DevicePresentation, DeviceServices, DeviceState, FastPairFeatures,
        Service,
    },
};

pub(super) struct Context {
    pub key: String,
    pub adapter_key: String,
    pub address: String,
    pub policy: DevicePolicy,
}

pub(super) struct Reading {
    pub state: BluezDeviceState,
    pub metadata: DeviceMetadata,
    pub bonded: Option<bool>,
    pub features: Option<FastPairFeatures>,
}

pub(super) struct Signal {
    pub rssi: Option<i16>,
    pub last_seen_ms: Option<u64>,
    pub live: bool,
}

impl Signal {
    pub fn resolve(
        observed: Option<&Observation>,
        cached: Option<&CachedDevice>,
        live: bool,
    ) -> Self {
        Self {
            rssi: observed
                .and_then(|seen| seen.rssi)
                .or_else(|| cached.and_then(|cache| cache.device.presentation.rssi)),
            last_seen_ms: observed
                .map(|seen| seen.last_seen_ms)
                .or_else(|| cached.and_then(|cache| cache.device.presentation.last_seen_ms)),
            live,
        }
    }
}

pub(super) fn build(
    context: Context,
    reading: Reading,
    presentation: ResolvedPresentation,
    signal: Signal,
) -> Device {
    let Reading {
        state,
        metadata,
        bonded,
        features,
    } = reading;
    let services = metadata
        .uuids
        .iter()
        .map(|uuid| Service {
            uuid: uuid.clone(),
            label: service_label(uuid).into(),
        })
        .collect();
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
        features.as_ref(),
    );
    Device {
        key: context.key,
        adapter_key: context.adapter_key,
        identity: DeviceIdentity {
            name: metadata.alias.clone(),
            alias: metadata.alias,
            remote_name: metadata.remote_name,
            device_type: presentation.device_type,
            address: context.address,
            address_type: metadata.address_type,
            icon: presentation.icon,
            modalias: metadata.modalias,
        },
        state: DeviceState {
            paired: state.paired,
            bonded,
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
            battery_live: presentation.battery_live,
            battery_last_known: presentation.battery_last_known,
            components: presentation.components,
            model_id: presentation.model_id,
            fast_pair: features,
            rssi: signal.rssi,
            signal_strength: signal.rssi.map(signal_strength),
            signal_live: signal.live,
            present: state.connected || signal.live,
            last_seen_ms: signal.last_seen_ms,
        },
        policy: context.policy,
        capabilities,
    }
}

pub(super) fn cache_entry(device: &Device, now_ms: u64) -> Option<CachedDevice> {
    device.presentation.present.then(|| CachedDevice {
        device: device.clone(),
        observed_at_ms: if device.state.connected {
            now_ms
        } else {
            device.presentation.last_seen_ms.unwrap_or(now_ms)
        },
    })
}

#[cfg(test)]
mod tests {
    use super::{Context, Reading, Signal, build, cache_entry};
    use crate::{
        bluez::{
            observations::Observations,
            snapshot::{BluezDeviceState, DeviceMetadata, ResolvedPresentation},
        },
        management::ManagementStore,
    };

    fn reading() -> Reading {
        Reading {
            state: BluezDeviceState {
                paired: false,
                connected: false,
                trusted: false,
                blocked: false,
                wake_allowed: None,
            },
            metadata: DeviceMetadata {
                alias: "Buds".into(),
                remote_name: None,
                address_type: "public".into(),
                icon: None,
                services_resolved: false,
                legacy_pairing: false,
                modalias: None,
                uuids: vec![],
            },
            bonded: None,
            features: None,
        }
    }
    fn assemble(reading: Reading, signal: Signal) -> crate::model::Device {
        build(
            Context {
                key: "opaque-peer".into(),
                adapter_key: "opaque-adapter".into(),
                address: "AA:BB:CC:DD:EE:FF".into(),
                policy: ManagementStore::in_memory().device_policy("opaque-peer"),
            },
            reading,
            ResolvedPresentation {
                icon: None,
                device_type: "unknown".into(),
                model_id: None,
                components: vec![],
                battery: vec![],
                battery_live: false,
                battery_last_known: false,
            },
            signal,
        )
    }

    #[test]
    fn missing_properties_remain_unknown_not_fabricated() {
        let device = assemble(reading(), Signal::resolve(None, None, false));
        assert_eq!(device.identity.name, "Buds");
        assert_eq!(device.key, "opaque-peer");
        assert!(device.identity.remote_name.is_none());
        assert!(device.state.bonded.is_none());
        assert!(device.state.wake_allowed.is_none());
        assert!(device.services.services.is_empty());
        assert!(device.presentation.rssi.is_none());
        assert!(device.presentation.signal_strength.is_none());
        assert!(!device.presentation.present);
        assert!(cache_entry(&device, 100).is_none());
        assert!(!device.capabilities.can_wake);
    }

    #[test]
    fn captured_connection_and_block_state_determine_capabilities() {
        for (connected, blocked, can_connect, can_disconnect) in [
            (false, false, true, false),
            (true, false, false, true),
            (false, true, false, false),
            (true, true, false, true),
        ] {
            let mut input = reading();
            input.state.connected = connected;
            input.state.blocked = blocked;
            input.state.paired = true;
            input.bonded = Some(true);
            let device = assemble(input, Signal::resolve(None, None, false));
            assert_eq!(device.presentation.present, connected);
            assert_eq!(device.capabilities.can_connect, can_connect);
            assert_eq!(device.capabilities.can_disconnect, can_disconnect);
            assert_eq!(device.capabilities.can_send_file, !blocked);
            assert_eq!(device.state.bonded, Some(true));
        }
    }

    #[test]
    fn stale_signal_is_retained_without_becoming_live_or_refreshing_cache_age() {
        let device = assemble(
            reading(),
            Signal {
                rssi: Some(-70),
                last_seen_ms: Some(10),
                live: true,
            },
        );
        assert_eq!(device.presentation.signal_strength, Some(50));
        let cached = cache_entry(&device, 999).unwrap();
        assert_eq!(cached.observed_at_ms, 10);
        let stale = assemble(reading(), Signal::resolve(None, Some(&cached), false));
        assert_eq!(stale.presentation.rssi, Some(-70));
        assert_eq!(stale.presentation.last_seen_ms, Some(10));
        assert!(!stale.presentation.signal_live);
        assert!(!stale.presentation.present);
        assert!(cache_entry(&stale, 1000).is_none());
        let mut connected = reading();
        connected.state.connected = true;
        let current = assemble(connected, Signal::resolve(None, Some(&cached), false));
        assert_eq!(cache_entry(&current, 1000).unwrap().observed_at_ms, 1000);
    }

    #[test]
    fn explicit_observations_override_cached_signal_but_missing_rssi_can_fall_back() {
        let cached = cache_entry(
            &assemble(
                reading(),
                Signal {
                    rssi: Some(-90),
                    last_seen_ms: Some(1),
                    live: true,
                },
            ),
            1,
        )
        .unwrap();
        let mut observations = Observations::default();
        let peer = bluer::Address::default();
        observations.record("hci0", peer, Some(-40));
        let seen = observations.get("hci0", peer).unwrap();
        let signal = Signal::resolve(Some(&seen), Some(&cached), true);
        assert_eq!(signal.rssi, Some(-40));
        assert_eq!(signal.last_seen_ms, Some(seen.last_seen_ms));
        observations.record("hci0", peer, None);
        let missing = observations.get("hci0", peer).unwrap();
        assert_eq!(
            Signal::resolve(Some(&missing), Some(&cached), false).rssi,
            Some(-90)
        );
    }
}
