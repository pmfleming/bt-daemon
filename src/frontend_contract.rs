use crate::{
    model::{Snapshot, audio_wire::Device as AudioDevice},
    protocol,
};
use serde_json::json;
#[test]
fn frontend_contract_owned_snapshot_audio_and_adapter_outcomes() {
    let f = protocol::contract_fixture().unwrap();
    let s: Snapshot = serde_json::from_value(f["snapshot"]["data"]["snapshot"].clone()).unwrap();
    assert_eq!(s.management.version, 1);
    assert!(!s.devices.is_empty());
    let audio: Vec<AudioDevice> =
        serde_json::from_value(f["audio_snapshot"]["data"]["audio_devices"].clone()).unwrap();
    assert!(!audio.is_empty());
    assert!(
        !audio[0]
            .sink
            .as_ref()
            .expect("fixture must exercise a selectable sink")
            .key
            .is_empty()
    );
    let ordering = &f["snapshot_ordering"];
    for (event, read) in [
        ("baseline", "old_read"),
        ("changed", "latest_read"),
        ("unavailable", "unavailable_read"),
        ("loading", "loading_read"),
        ("audio_event", "audio_read"),
    ] {
        assert_eq!(
            ordering[event]["data"]["state"],
            ordering[read]["data"]["state"]
        );
    }
    assert_eq!(ordering["old_read"]["data"]["state"]["revision"], 2);
    assert_eq!(ordering["latest_read"]["data"]["state"]["revision"], 3);
    assert_eq!(ordering["unavailable_read"]["ok"], false);
    assert_eq!(
        f["snapshot"]["data"]["state"],
        ordering["old_read"]["data"]["state"]
    );
    assert_ne!(
        f["snapshot"]["data"]["state"]["epoch"],
        f["audio_snapshot"]["data"]["state"]["epoch"]
    );
    let methods = f["registry"]["methods"].as_array().unwrap();
    assert!(
        methods
            .iter()
            .any(|m| m["name"] == "bluetooth.requests.snapshot")
    );
    assert!(
        methods
            .iter()
            .any(|m| m["name"] == "bluetooth.device.policy.update"
                && m["params_schema"]["properties"]["fast_pair_controls_enabled"]["type"]
                    == json!(["boolean", "null"]))
    );
    assert!(
        methods
            .iter()
            .any(|m| m["name"] == "bluetooth.adapter.update"
                && m["response_key"] == "adapter_batch"
                && m["cancellable"] == false
                && m["params_schema"]["properties"]["changes"]["additionalProperties"] == false)
    );
    // Adapter batches are presently dynamic wire projections, not a second DTO.
    // Keep their semantic witnesses rather than inventing a test-only mirror type.
    let batch = &f["adapter_update"]["data"]["adapter_batch"];
    assert_eq!(f["adapter_update"]["ok"], true);
    assert_eq!(batch["key"], "adapter-opaque");
    assert!(batch["snapshot_error"]["message"].is_string());
    for (index, (state, field)) in [
        ("applied", "alias"),
        ("unknown", "discoverable_timeout"),
        ("not-attempted", "pairable_timeout"),
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(batch["outcomes"][index]["state"], *state);
        assert_eq!(batch["outcomes"][index]["field"], *field);
    }
    for path in [
        "/radio/operational",
        "/devices/0/policy/wait_for_services",
        "/devices/0/signal_live",
        "/devices/0/components",
        "/devices/0/rssi",
    ] {
        let mut bad = f["snapshot"]["data"]["snapshot"].clone();
        *bad.pointer_mut(path).unwrap() = json!({});
        assert!(serde_json::from_value::<Snapshot>(bad).is_err(), "{path}");
    }
}
