use super::{device_snapshot, select_default, select_profile};
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
fn snapshots_expose_only_resolvable_opaque_devices_and_endpoint_readiness() {
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

    let mut input = device();
    input.address = "not-an-address".into();
    assert!(device_snapshot(&pairing, input).is_none());
    let mut input = device();
    input.adapter.clear();
    assert!(device_snapshot(&pairing, input).is_none());
    let mut input = device();
    input.active_profile = Some(999);
    assert!(device_snapshot(&pairing, input).unwrap()["active_profile_key"].is_null());

    for (state, expected) in [
        ("creating", false),
        ("error", false),
        ("suspended", true),
        ("running", true),
    ] {
        let mut input = device();
        input.sink.as_mut().unwrap().state = state.into();
        assert_eq!(
            device_snapshot(&pairing, input).unwrap()["sink"]["ready"],
            expected
        );
    }
}

#[tokio::test]
async fn audio_changes_validate_their_own_parameter_before_probing() {
    let pairing = PairingBroker::new(DeviceIdentityRegistry::in_memory());
    for change in [super::DEFAULT, super::PROFILE] {
        let parameter = change.parameter;
        let result =
            super::apply_change(&pairing, &serde_json::json!({"device_key": "peer"}), change).await;
        assert_eq!(result["error"]["code"], "validation-error");
        assert!(
            result["error"]["message"]
                .as_str()
                .unwrap()
                .contains(parameter)
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
