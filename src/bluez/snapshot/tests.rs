use super::{
    SERVICE_LABELS, cache_entry_is_fresh, cached_device_view, device_capabilities,
    fast_pair_capabilities, presentation, service_label, should_include_device, signal_strength,
};
use crate::bluez::{CachedDevice, DISCOVERED_DEVICE_CACHE_TTL};
use crate::fast_pair::{FAST_PAIR_SERVICE_UUID, MESSAGE_STREAM_UUID};
use crate::identity::DeviceIdentityRegistry;
use crate::model::{
    Battery, Device, DeviceIdentity, DevicePresentation, DeviceServices, DeviceState,
    FastPairFeatures, FastPairMultipoint, FastPairNoiseControl,
};

fn features() -> FastPairFeatures {
    FastPairFeatures {
        model_id: Some("aabbcc".into()),
        ble_address: None,
        authenticated_controls: true,
        account_key_available: true,
        provisioning_available: true,
        provisioning_reason: None,
        trusted_model_name: None,
        multipoint: Some(FastPairMultipoint {
            version: 1,
            supported: true,
            configurable: true,
            enabled: false,
            audio_switch_enabled: false,
        }),
        noise_control: Some(FastPairNoiseControl {
            version: 2,
            available_modes: vec!["off".into()],
            settable_modes: vec!["off".into()],
            active_mode: None,
        }),
        last_switch: None,
        audio_switch_seeker_supported: false,
    }
}

#[test]
fn assigned_service_labels_require_the_bluetooth_base_uuid() {
    for (id, expected) in SERVICE_LABELS {
        let uuid = format!("0000{id}-0000-1000-8000-00805f9b34fb");
        assert_eq!(service_label(&uuid), *expected);
        assert_eq!(service_label(&uuid.to_uppercase()), *expected);
    }
    assert_eq!(
        service_label(MESSAGE_STREAM_UUID),
        "Fast Pair Message Stream"
    );
    assert_eq!(service_label(FAST_PAIR_SERVICE_UUID), "Fast Pair Service");
    for uuid in [
        "",
        "1105",
        "xxxx1105",
        "éé1105",
        "ffff1105-0000-1000-8000-00805f9b34fb",
        "00001105-0000-1000-8000-ffffffffffff",
    ] {
        assert_eq!(service_label(uuid), "Bluetooth service", "{uuid}");
    }
}

#[test]
fn visibility_cache_and_signal_boundaries_are_explicit() {
    for paired in [false, true] {
        for present in [false, true] {
            for cached in [false, true] {
                assert_eq!(
                    should_include_device(paired, present, cached),
                    paired || present || cached
                );
            }
        }
    }
    let ttl = DISCOVERED_DEVICE_CACHE_TTL.as_millis() as u64;
    assert!(cache_entry_is_fresh(100, 100 + ttl));
    assert!(!cache_entry_is_fresh(100, 101 + ttl));
    assert!(cache_entry_is_fresh(100, 99));
    for (rssi, expected) in [
        (i16::MIN, 0),
        (-100, 0),
        (-70, 50),
        (-40, 100),
        (i16::MAX, 100),
    ] {
        assert_eq!(signal_strength(rssi), expected);
    }
}

#[test]
fn fast_pair_controls_require_live_authenticated_supported_features() {
    let mut features = features();
    for (paired, connected, advertised) in [
        (false, true, true),
        (true, false, true),
        (true, true, false),
    ] {
        assert_eq!(
            fast_pair_capabilities(paired, connected, advertised, Some(&features)),
            (false, false, false)
        );
    }
    assert_eq!(
        fast_pair_capabilities(true, true, true, None),
        (false, false, false)
    );
    assert_eq!(
        fast_pair_capabilities(true, true, true, Some(&features)),
        (true, true, true)
    );
    features.authenticated_controls = false;
    assert_eq!(
        fast_pair_capabilities(true, true, true, Some(&features)),
        (true, false, false)
    );
    features.authenticated_controls = true;
    features.provisioning_available = false;
    features.multipoint = None;
    features.noise_control = None;
    assert_eq!(
        fast_pair_capabilities(true, true, true, Some(&features)),
        (false, false, false)
    );
}

#[test]
fn blocked_devices_cannot_connect_pair_send_or_use_fast_pair_controls() {
    let features = features();
    for paired in [false, true] {
        for connected in [false, true] {
            let blocked = device_capabilities(paired, connected, true, None, true, Some(&features));
            assert!(!blocked.can_pair && !blocked.can_connect && !blocked.can_send_file);
            assert!(
                !blocked.can_provision_fast_pair
                    && !blocked.can_set_multipoint
                    && !blocked.can_set_noise_control
            );
            assert_eq!(blocked.can_disconnect, connected);
            assert_eq!(blocked.can_remove, paired);
            assert!(!blocked.can_wake);
            assert!(blocked.unsupported_reasons.contains_key("wake"));
            let allowed = device_capabilities(paired, connected, false, Some(false), false, None);
            assert_eq!(allowed.can_pair, !paired);
            assert_eq!(allowed.can_connect, !connected);
            assert_eq!(allowed.can_send_file, paired);
            assert!(allowed.can_wake);
            assert!(!allowed.unsupported_reasons.contains_key("wake"));
        }
    }
}

#[test]
fn presentation_restores_paired_history_but_not_live_or_unpaired_batteries() {
    let identities = DeviceIdentityRegistry::in_memory();
    let key = identities.device_key("hci0", bluer::Address::default());
    let battery = vec![Battery::bluez_aggregate(75)];
    let live = presentation(
        &identities,
        &key,
        true,
        true,
        Some("audio-headphones".into()),
        Some("aabbcc"),
        battery.clone(),
    );
    assert!(live.battery_live && !live.battery_last_known);
    assert_eq!(live.device_type, "Headphones");
    let remembered = presentation(
        &identities,
        &key,
        true,
        false,
        Some(" ".into()),
        None,
        vec![],
    );
    assert_eq!(remembered.battery, battery);
    assert_eq!(remembered.icon.as_deref(), Some("audio-headphones"));
    assert_eq!(remembered.model_id.as_deref(), Some("aabbcc"));
    assert!(!remembered.battery_live && remembered.battery_last_known);
    let connected_without_battery = presentation(&identities, &key, true, true, None, None, vec![]);
    assert!(connected_without_battery.battery.is_empty());
    assert!(
        !connected_without_battery.battery_live && !connected_without_battery.battery_last_known
    );
    let unpaired = presentation(&identities, &key, false, false, None, None, vec![]);
    assert_eq!(unpaired.device_type, "Bluetooth device");
    assert!(unpaired.battery.is_empty() && unpaired.model_id.is_none());
    let repaired = presentation(&identities, &key, true, false, None, None, vec![]);
    assert!(repaired.battery.is_empty() && repaired.model_id.is_none());
}

#[test]
fn cached_device_views_remove_live_state_and_authenticated_controls() {
    let device = Device {
        key: "device-test".into(),
        adapter_key: "adapter-test".into(),
        identity: DeviceIdentity {
            name: "Headphones".into(),
            alias: "Headphones".into(),
            remote_name: None,
            device_type: "Headphones".into(),
            address: "00:00:00:00:00:00".into(),
            address_type: "public".into(),
            icon: None,
            modalias: None,
        },
        state: DeviceState {
            paired: true,
            bonded: Some(true),
            connected: true,
            trusted: true,
            blocked: false,
            wake_allowed: None,
            legacy_pairing: false,
        },
        services: DeviceServices {
            services_resolved: true,
            uuids: vec![FAST_PAIR_SERVICE_UUID.into()],
            services: vec![],
        },
        presentation: DevicePresentation {
            battery: vec![Battery::bluez_aggregate(80)],
            battery_live: true,
            battery_last_known: false,
            components: vec![],
            model_id: None,
            fast_pair: Some(features()),
            rssi: Some(-40),
            signal_strength: Some(100),
            signal_live: true,
            present: true,
            last_seen_ms: Some(42),
        },
        policy: crate::management::ManagementStore::in_memory().device_policy("device-test"),
        capabilities: device_capabilities(true, true, false, None, true, Some(&features())),
    };
    let cached = CachedDevice {
        device,
        observed_at_ms: 42,
    };
    let view = cached_device_view(&cached);
    assert!(!view.state.connected && !view.presentation.present && !view.presentation.signal_live);
    assert!(!view.presentation.battery_live && view.presentation.battery_last_known);
    assert_eq!(view.presentation.last_seen_ms, Some(42));
    assert!(view.capabilities.can_connect && !view.capabilities.can_disconnect);
    assert!(!view.capabilities.can_set_multipoint && !view.capabilities.can_set_noise_control);
    assert!(cached.device.state.connected);
}
