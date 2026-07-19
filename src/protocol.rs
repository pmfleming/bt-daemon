use serde_json::{Value, json};

pub const METHODS: &[(&str, &str, &str, Option<&str>)] = &[
    (
        "bluetooth.snapshot",
        "{}",
        "snapshot",
        Some("bluetooth.changed"),
    ),
    (
        "bluetooth.setPowered",
        r#"{"adapter_key":null,"powered":true}"#,
        "snapshot",
        Some("bluetooth.changed"),
    ),
    (
        "bluetooth.scan",
        r#"{"adapter_key":"adapter-opaque","enabled":true,"timeout_ms":15000}"#,
        "scan",
        Some("bluetooth.scan"),
    ),
    (
        "bluetooth.adapter.operation",
        r#"{"key":"adapter-opaque","operation":"set-discoverable","discoverable":true}"#,
        "snapshot",
        Some("bluetooth.changed"),
    ),
    ("bluetooth.obex.snapshot", "{}", "obex", None),
    (
        "bluetooth.obex.send",
        r#"{"device_key":"device-opaque","path":"/selected/file"}"#,
        "transfer",
        Some("bluetooth.obex.transfer"),
    ),
    (
        "bluetooth.obex.respond",
        r#"{"request_id":"obex-incoming-1","accept":true}"#,
        "authorization",
        Some("bluetooth.obex.transfer"),
    ),
    ("bluetooth.audio.snapshot", "{}", "audio_devices", None),
    (
        "bluetooth.audio.setProfile",
        r#"{"device_key":"device-opaque","profile_key":"audio-profile-opaque"}"#,
        "audio_devices",
        None,
    ),
    (
        "bluetooth.device.operation",
        r#"{"key":"device-opaque","operation":"connect"}"#,
        "operation",
        Some("bluetooth.operation"),
    ),
    (
        "bluetooth.pairing.respond",
        r#"{"request_id":"pairing-1","accept":true,"value":null}"#,
        "result",
        Some("pairing.request"),
    ),
];

pub const STREAMS: &[(&str, &[&str])] = &[
    (
        "bluetooth.changed",
        &["subscribed", "changed", "unavailable"],
    ),
    ("pairing.request", &["requested", "display", "cancelled"]),
    (
        "bluetooth.operation",
        &["started", "completed", "failed", "cancelled"],
    ),
    (
        "bluetooth.scan",
        &["started", "completed", "failed", "cancelled"],
    ),
    (
        "bluetooth.obex.transfer",
        &[
            "authorization-requested",
            "queued",
            "progress",
            "completed",
            "failed",
            "cancelled",
        ],
    ),
    (
        "bluetooth.audio.changed",
        &["subscribed", "changed", "unavailable"],
    ),
];

pub fn registry() -> Value {
    json!({
        "protocol": "bt-api",
        "version": 1,
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

pub fn contract_fixture() -> Value {
    json!({
        "protocol": "bt-api",
        "version": 1,
        "registry": registry(),
        "snapshot": {
            "protocol": "bt-api",
            "version": 1,
            "ok": true,
            "data": {
                "snapshot": {
                    "adapters": [{
                        "key": "adapter-opaque",
                        "name": "hci0",
                        "alias": "computer",
                        "address": "00:11:22:33:44:55",
                        "address_type": "public",
                        "powered": true,
                        "discovering": false,
                        "discoverable": false,
                        "pairable": true,
                        "discoverable_timeout": 180,
                        "pairable_timeout": 0,
                        "modalias": null
                    }],
                    "devices": [{
                        "key": "device-opaque",
                        "adapter_key": "adapter-opaque",
                        "name": "Headphones",
                        "alias": "Headphones",
                        "address": "AA:BB:CC:DD:EE:FF",
                        "address_type": "public",
                        "icon": "audio-headset",
                        "paired": true,
                        "bonded": true,
                        "connected": false,
                        "services_resolved": true,
                        "trusted": true,
                        "blocked": false,
                        "wake_allowed": null,
                        "legacy_pairing": false,
                        "modalias": null,
                        "uuids": ["0000110b-0000-1000-8000-00805f9b34fb"],
                        "services": [{
                            "uuid": "0000110b-0000-1000-8000-00805f9b34fb",
                            "label": "Audio Sink"
                        }],
                        "battery": [{
                            "id": "aggregate", "label": "Battery", "component": "main",
                            "percentage": 80, "source": "bluez", "confidence": "standard"
                        }],
                        "rssi": null,
                        "signal_strength": null,
                        "present": false,
                        "last_seen_ms": null,
                        "capabilities": {
                            "can_pair": false,
                            "can_connect": true,
                            "can_disconnect": false,
                            "can_remove": true,
                            "can_trust": true,
                            "can_block": true,
                            "can_wake": false,
                            "can_rename": true,
                            "can_send_file": true,
                            "unsupported_reasons": {
                                "pair": "Device is already paired",
                                "disconnect": "Device is not connected",
                                "wake": "BlueZ does not expose wake control for this device"
                            }
                        }
                    }]
                }
            }
        },
        "scan_event": {
            "protocol": "bt-api",
            "version": 1,
            "stream": "bluetooth.scan",
            "event": "started",
            "subscription_id": "subscription-1",
            "data": {
                "event": "started",
                "request_id": "scan-1",
                "adapter_key": "adapter-opaque",
                "state": "running",
                "timeout_ms": 15000
            }
        },
        "obex_snapshot": {
            "protocol": "bt-api",
            "version": 1,
            "ok": true,
            "data": {
                "obex": {
                    "available": true,
                    "outgoing_object_push": true,
                    "incoming_authorization": true,
                    "transfer_progress": true,
                    "cancellation": true
                }
            }
        },
        "obex_event": {
            "protocol": "bt-api",
            "version": 1,
            "stream": "bluetooth.obex.transfer",
            "event": "progress",
            "subscription_id": "subscription-1",
            "data": {
                "event": "progress",
                "request_id": "obex-transfer-1",
                "direction": "outgoing",
                "device_key": "device-opaque",
                "file_name": "document.pdf",
                "status": "active",
                "transferred": 512,
                "size": 1024
            }
        },
        "obex_authorization_event": {
            "protocol": "bt-api",
            "version": 1,
            "stream": "bluetooth.obex.transfer",
            "event": "authorization-requested",
            "subscription_id": "subscription-1",
            "data": {
                "event": "authorization-requested",
                "request_id": "obex-incoming-1",
                "direction": "incoming",
                "device_key": "device-opaque",
                "device_name": "Phone",
                "file_name": "photo.jpg",
                "media_type": "image/jpeg",
                "status": "awaiting-authorization",
                "transferred": 0,
                "size": 2048,
                "timeout_ms": 60000
            }
        },
        "audio_snapshot": {
            "protocol": "bt-api",
            "version": 1,
            "ok": true,
            "data": {
                "audio_devices": [{
                    "device_key": "device-opaque",
                    "active_profile_key": "audio-profile-active",
                    "sink": { "ready": true, "state": "idle", "is_default": true },
                    "source": null,
                    "profiles": [{
                        "key": "audio-profile-active",
                        "label": "High Fidelity Playback (codec AAC)",
                        "mode": "high-fidelity",
                        "codec": "AAC",
                        "available": true,
                        "priority": 100
                    }]
                }]
            }
        },
        "operation_event": {
            "protocol": "bt-api",
            "version": 1,
            "stream": "bluetooth.operation",
            "event": "completed",
            "subscription_id": "subscription-1",
            "data": {
                "event": "completed",
                "request_id": "operation-1",
                "device_key": "device-opaque",
                "operation": "connect",
                "state": "completed"
            }
        },
        "pairing_event": {
            "protocol": "bt-api",
            "version": 1,
            "stream": "pairing.request",
            "event": "requested",
            "subscription_id": "subscription-1",
            "data": {
                "event": "requested",
                "request_id": "pairing-1",
                "kind": "confirmation",
                "device_key": "device-opaque",
                "response_required": true,
                "value": "123456",
                "timeout_ms": 60000
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{METHODS, STREAMS, contract_fixture};

    #[test]
    fn registry_names_are_unique_and_fixtures_are_valid() {
        let mut names = HashSet::new();
        assert!(METHODS.iter().all(|method| names.insert(method.0)));
        names.clear();
        assert!(STREAMS.iter().all(|stream| names.insert(stream.0)));
        assert_eq!(contract_fixture()["version"], 1);
        let checked_in: serde_json::Value =
            serde_json::from_str(include_str!("../test_support/bt-api-v1.json")).unwrap();
        assert_eq!(checked_in, contract_fixture());
    }
}
