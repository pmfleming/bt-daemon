use super::{device_capabilities, fast_pair_capabilities, signal_level, supplement_components};
use crate::model::{
    Battery, FastPairFeatures, FastPairMultipoint, FastPairNoiseControl, NoiseControl, RssiKind,
};

fn features() -> FastPairFeatures {
    FastPairFeatures {
        model_id: Some("aabbcc".into()),
        advertised_capabilities: None,
        ble_address: None,
        controls_available: true,
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
fn noise_control_capability_accepts_versions_one_and_two_without_credentials() {
    let mut features = features();
    features.authenticated_controls = false;
    features.account_key_available = false;
    for version in [0, 1, 2, 3, 255] {
        features.noise_control.as_mut().unwrap().version = version;
        assert_eq!(
            fast_pair_capabilities(true, true, true, Some(&features)).2,
            matches!(version, 1 | 2),
            "ANC version {version}"
        );
    }
    for version in [1, 2] {
        let noise = features.noise_control.as_mut().unwrap();
        noise.version = version;
        noise.settable_modes.clear();
        assert!(!fast_pair_capabilities(true, true, true, Some(&features)).2);
    }
}

#[test]
fn device_capabilities_enforce_connection_authentication_and_block_policy() {
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
    features.account_key_available = false;
    assert_eq!(
        fast_pair_capabilities(true, true, true, Some(&features)),
        (true, false, true),
        "ANC does not require account keys or a nonce"
    );
    features.controls_available = false;
    assert_eq!(
        fast_pair_capabilities(true, true, true, Some(&features)),
        (true, false, false),
        "disabled controls or a missing stream must still disable ANC"
    );
    features.authenticated_controls = true;
    features.provisioning_available = false;
    features.multipoint = None;
    features.noise_control = None;
    assert_eq!(
        fast_pair_capabilities(true, true, true, Some(&features)),
        (false, false, false)
    );
    let features = self::features();
    for paired in [false, true] {
        for connected in [false, true] {
            let blocked =
                device_capabilities(paired, connected, true, None, true, Some(&features), None);
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
            let allowed =
                device_capabilities(paired, connected, false, Some(false), false, None, None);
            assert_eq!(allowed.can_pair, !paired);
            assert_eq!(allowed.can_connect, !connected);
            assert_eq!(allowed.can_send_file, paired);
            assert!(allowed.can_wake);
            assert!(!allowed.unsupported_reasons.contains_key("wake"));
        }
    }
}

#[test]
fn vendor_noise_control_needs_a_paired_live_link_without_fast_pair() {
    let mut sony = NoiseControl {
        source: "sony-headphones-control".into(),
        available_modes: vec!["off".into(), "noise-cancelling".into()],
        settable_modes: vec!["off".into(), "noise-cancelling".into()],
        active_mode: Some("off".into()),
    };
    for (paired, connected, blocked, expected) in [
        (true, true, false, true),
        (false, true, false, false),
        (true, false, false, false),
        (true, true, true, false),
    ] {
        let capabilities =
            device_capabilities(paired, connected, blocked, None, false, None, Some(&sony));
        assert_eq!(capabilities.can_set_noise_control, expected);
        assert_eq!(
            capabilities
                .unsupported_reasons
                .contains_key("set_noise_control"),
            !(paired && connected)
        );
    }
    sony.settable_modes.clear();
    assert!(
        !device_capabilities(true, true, false, None, false, None, Some(&sony))
            .can_set_noise_control
    );
}

#[test]
fn signal_level_buckets_both_units_without_treating_relative_zero_as_missing() {
    let relative = Some(RssiKind::BrEdrRelative);
    for (rssi, expected) in [(5, 3), (0, 3), (-1, 2), (-10, 2), (-11, 1), (-60, 1)] {
        assert_eq!(signal_level(rssi, relative), expected, "relative {rssi}");
    }
    // dBm keeps the strength heuristic's 67/34% boundaries.
    for (rssi, expected) in [(-40, 3), (-59, 3), (-60, 2), (-79, 2), (-80, 1), (-100, 1)] {
        assert_eq!(
            signal_level(rssi, Some(RssiKind::Dbm)),
            expected,
            "dbm {rssi}"
        );
        assert_eq!(signal_level(rssi, None), expected, "untyped {rssi}");
    }
}

fn component(source: &str, component: &str, percentage: u8) -> Battery {
    Battery {
        id: format!("{source}-{component}"),
        label: component.into(),
        component: component.into(),
        percentage,
        charging: None,
        source: source.into(),
        confidence: "high".into(),
    }
}

#[test]
fn vendor_batteries_only_fill_components_fast_pair_left_unknown() {
    let mut readings = vec![
        component("fast-pair", "left", 90),
        component("fast-pair", "right", 80),
    ];
    supplement_components(
        &mut readings,
        vec![
            component("maestro", "left", 89),
            component("maestro", "right", 81),
            component("maestro", "case", 43),
        ],
    );
    assert_eq!(
        readings
            .iter()
            .map(|b| (b.id.as_str(), b.percentage))
            .collect::<Vec<_>>(),
        [
            ("fast-pair-left", 90),
            ("fast-pair-right", 80),
            ("maestro-case", 43)
        ]
    );

    let mut without_fast_pair = Vec::new();
    supplement_components(
        &mut without_fast_pair,
        vec![component("maestro", "case", 43)],
    );
    assert_eq!(without_fast_pair.len(), 1);
}
