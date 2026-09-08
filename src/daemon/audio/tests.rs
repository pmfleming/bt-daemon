use super::{device_snapshot, endpoint_snapshot, select_default, select_profile};
use crate::{
    audio::{self, AudioDevice, AudioEndpoint, AudioProfile},
    identity::DeviceIdentityRegistry,
    pairing::PairingBroker,
};

fn device() -> AudioDevice {
    AudioDevice {
        pipewire_id: 10,
        address: "AA:BB:CC:DD:EE:FF".into(),
        adapter: "hci0".into(),
        name: "Headphones".into(),
        active_profile: Some(2),
        profiles: vec![AudioProfile {
            index: 2,
            name: "a2dp-sink".into(),
            description: "High Fidelity".into(),
            available: true,
            priority: 100,
            mode: "high-fidelity".into(),
            codec: Some("AAC".into()),
        }],
        sink: Some(AudioEndpoint {
            name: "private-pipewire-node".into(),
            state: "running".into(),
            is_default: true,
        }),
        source: None,
    }
}

#[test]
fn snapshot_uses_opaque_device_profile_and_endpoint_keys() {
    let identities = DeviceIdentityRegistry::in_memory();
    let input = device();
    let key = identities.device_key(&input.adapter, input.address.parse().unwrap());
    let pairing = PairingBroker::new(identities);
    let value = device_snapshot(&pairing, input).unwrap();
    assert_eq!(value["device_key"], key);
    assert_eq!(
        value["active_profile_key"],
        audio::profile_key(&key, "a2dp-sink")
    );
    assert_eq!(value["profiles"][0]["codec"], "AAC");
    assert_eq!(value["sink"]["key"], audio::endpoint_key(&key, "sink"));
    assert_eq!(value["sink"]["ready"], true);
    assert_eq!(value["sink"]["is_default"], true);
    assert!(value["source"].is_null());
    assert!(!value.to_string().contains("AA:BB:CC:DD:EE:FF"));
    assert!(!value.to_string().contains("private-pipewire-node"));
}

#[test]
fn snapshots_reject_unresolvable_devices_and_tolerate_unknown_profiles() {
    let pairing = PairingBroker::new(DeviceIdentityRegistry::in_memory());
    let mut input = device();
    input.address = "not-an-address".into();
    assert!(device_snapshot(&pairing, input).is_none());
    let mut input = device();
    input.adapter.clear();
    assert!(device_snapshot(&pairing, input).is_none());
    let mut input = device();
    input.active_profile = Some(999);
    assert!(device_snapshot(&pairing, input).unwrap()["active_profile_key"].is_null());
}

#[test]
fn endpoint_readiness_distinguishes_initializing_failed_and_ready_states() {
    assert!(endpoint_snapshot("key", "sink", None).is_none());
    for (state, expected) in [
        ("creating", false),
        ("error", false),
        ("suspended", true),
        ("running", true),
    ] {
        let endpoint = AudioEndpoint {
            name: "node".into(),
            state: state.into(),
            is_default: false,
        };
        assert_eq!(
            endpoint_snapshot("key", "sink", Some(endpoint)).unwrap()["ready"],
            expected
        );
    }
}

#[test]
fn selection_checks_availability_and_device_scoped_keys_without_running_hardware_operations() {
    let key = "device-key";
    let profile = audio::profile_key(key, "a2dp-sink");
    assert!(select_profile(key, &profile, device()).is_some());
    assert!(select_profile("other-device", &profile, device()).is_none());
    let mut unavailable = device();
    unavailable.profiles[0].available = false;
    assert!(select_profile(key, &profile, unavailable).is_none());
    assert!(select_default(key, &audio::endpoint_key(key, "sink"), device()).is_some());
    assert!(select_default(key, &audio::endpoint_key(key, "source"), device()).is_none());
    assert!(select_default("other-device", &audio::endpoint_key(key, "sink"), device()).is_none());
    let mut source = device();
    source.source = source.sink.take();
    assert!(select_default(key, &audio::endpoint_key(key, "source"), source).is_some());
}
