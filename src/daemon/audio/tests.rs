use super::{device_snapshot, select_default};
use crate::{
    audio::{self, AudioDevice, AudioEndpoint, AudioProfile},
    identity::DeviceIdentityRegistry,
    pairing::PairingBroker,
};
use serde_json::json;

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
    let profile = value["active_profile_key"].as_str().unwrap();
    assert!(super::select_profile(&key, profile, device()).is_some());
    assert!(super::select_profile("other-device", profile, device()).is_none());
    let sink = value["sink"]["key"].as_str().unwrap();
    assert!(select_default(&key, sink, device()).is_some());
    assert!(select_default("other-device", sink, device()).is_none());
    assert!(select_default(&key, &audio::endpoint_key(&key, "source"), device()).is_none());

    let mut source = device();
    source.source = source.sink.take();
    assert!(select_default(&key, &audio::endpoint_key(&key, "source"), source).is_some());

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

#[test]
fn applied_profiles_report_effect_persistence_and_refresh_failures_independently() {
    assert!(
        super::PROFILE
            .request(&json!({"device_key": "peer", "profile_key": "selection", "remember": true}))
            .unwrap()
            .2
    );
    for (apply_ok, persist_ok) in [(false, true), (true, false), (true, true)] {
        let mut persisted = false;
        let result = super::apply_then_remember(
            || {
                anyhow::ensure!(apply_ok, "unavailable");
                Ok(())
            },
            || {
                persisted = true;
                anyhow::ensure!(persist_ok, "disk full");
                Ok(())
            },
        );
        assert_eq!(persisted, apply_ok);
        assert_eq!(result.is_ok(), apply_ok);
        let Ok(error) = result else {
            continue;
        };
        assert_eq!(error.as_deref(), (!persist_ok).then_some("disk full"));
        for snapshot in [
            crate::api::success(json!({"audio_devices": []})),
            crate::api::error("audio-unavailable", "offline".into()),
        ] {
            assert_eq!(
                super::applied_response(snapshot.clone(), false, None),
                snapshot
            );
            let response = super::applied_response(snapshot.clone(), true, error.clone());
            assert_eq!(response["ok"], true);
            assert_eq!(
                response["data"]["profile_outcome"],
                json!({
                    "applied": true, "remembered": persist_ok, "persistence_error": error
                })
            );
            if snapshot["ok"] == true {
                assert_eq!(response["data"]["audio_devices"], json!([]));
                assert!(response["data"].get("refresh_error").is_none());
            } else {
                assert_eq!(response["data"]["refresh_error"], snapshot["error"]);
            }
        }
    }
}
