use serde_json::{Value, json};

pub const NAME: &str = "bt-api";
pub const VERSION: u8 = 1;
pub mod stream {
    pub const CHANGED: &str = "bluetooth.changed";
    pub const PAIRING: &str = "pairing.request";
    pub const OPERATION: &str = "bluetooth.operation";
    pub const SCAN: &str = "bluetooth.scan";
    pub const AUDIO: &str = "bluetooth.audio.changed";
    pub const OBEX: &str = "bluetooth.obex.transfer";
}

pub const METHODS: &[(&str, &str, &str, Option<&str>)] = &[
    (
        "bluetooth.snapshot",
        "{}",
        "snapshot",
        Some(stream::CHANGED),
    ),
    (
        "bluetooth.setPowered",
        r#"{"adapter_key":null,"powered":true}"#,
        "snapshot",
        Some(stream::CHANGED),
    ),
    (
        "bluetooth.scan",
        r#"{"adapter_key":"adapter-opaque","enabled":true,"timeout_ms":15000}"#,
        "scan",
        Some(stream::SCAN),
    ),
    (
        "bluetooth.adapter.operation",
        r#"{"key":"adapter-opaque","operation":"set-discoverable","discoverable":true}"#,
        "snapshot",
        Some(stream::CHANGED),
    ),
    (
        "bluetooth.management.update",
        r#"{"launch_state":"remember","reconnect_on_resume":true,"trust_after_pair":true,"preferred_adapter_key":"adapter-opaque","show_blocked_devices":false,"show_recent_devices":false}"#,
        "snapshot",
        Some(stream::CHANGED),
    ),
    ("bluetooth.obex.snapshot", "{}", "obex", None),
    (
        "bluetooth.obex.send",
        r#"{"device_key":"device-opaque","path":"/selected/file"}"#,
        "transfer",
        Some(stream::OBEX),
    ),
    (
        "bluetooth.obex.respond",
        r#"{"request_id":"obex-incoming-1","accept":true}"#,
        "authorization",
        Some(stream::OBEX),
    ),
    ("bluetooth.audio.snapshot", "{}", "audio_devices", None),
    (
        "bluetooth.audio.setProfile",
        r#"{"device_key":"device-opaque","profile_key":"audio-profile-opaque"}"#,
        "audio_devices",
        None,
    ),
    (
        "bluetooth.audio.setDefault",
        r#"{"device_key":"device-opaque","endpoint_key":"audio-endpoint-opaque"}"#,
        "audio_devices",
        Some(stream::AUDIO),
    ),
    ("bluetooth.requests.snapshot", "{}", "requests", None),
    (
        "bluetooth.device.operation",
        r#"{"key":"device-opaque","operation":"connect","power_on":true,"trust":false,"wait_for_services":true}"#,
        "operation",
        Some(stream::OPERATION),
    ),
    (
        "bluetooth.pairing.respond",
        r#"{"request_id":"pairing-1","accept":true,"value":null}"#,
        "result",
        Some(stream::PAIRING),
    ),
];

pub const STREAMS: &[(&str, &[&str])] = &[
    (stream::CHANGED, &["subscribed", "changed", "unavailable"]),
    (
        stream::PAIRING,
        &["requested", "display", "cancelled", "lagged"],
    ),
    (
        stream::OPERATION,
        &[
            "started",
            "progress",
            "completed",
            "failed",
            "cancelled",
            "lagged",
        ],
    ),
    (
        stream::SCAN,
        &["started", "completed", "failed", "cancelled", "lagged"],
    ),
    (
        stream::OBEX,
        &[
            "authorization-requested",
            "queued",
            "progress",
            "completed",
            "failed",
            "cancelled",
            "lagged",
        ],
    ),
    (stream::AUDIO, &["subscribed", "changed", "unavailable"]),
];

pub fn registry() -> Value {
    json!({
        "protocol": NAME,
        "version": VERSION,
        "methods": METHODS.iter().map(|(name, params, response_key, stream)| json!({
            "name": name,
            "params_example": serde_json::from_str::<Value>(params).expect("valid protocol fixture"),
            "response_key": response_key,
            "stream": stream,
        })).collect::<Vec<_>>(),
        "streams": STREAMS.iter().map(|(name, events)| json!({
            "name": name,
            "events": events,
        })).collect::<Vec<_>>(),
    })
}

/// Return the checked-in contract used by consumers and compatibility tests.
pub fn contract_fixture() -> Value {
    match serde_json::from_str(include_str!("../test_support/bt-api-v1.json")) {
        Ok(fixture) => fixture,
        Err(error) => json!({ "fixture_error": error.to_string() }),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{METHODS, STREAMS, VERSION, contract_fixture, registry};

    #[test]
    fn registry_names_are_unique_and_fixture_matches_registry() {
        let mut names = HashSet::new();
        assert!(METHODS.iter().all(|method| names.insert(method.0)));
        names.clear();
        assert!(STREAMS.iter().all(|stream| names.insert(stream.0)));

        let fixture = contract_fixture();
        assert_eq!(fixture["version"], VERSION);
        assert_eq!(fixture["registry"], registry());
    }
}
