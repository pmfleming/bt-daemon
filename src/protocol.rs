use serde_json::{Map, Value, json};

use crate::backend::{AdapterOperation, BackendErrorKind, DeviceOperation};

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

macro_rules! method_registry {
    ($($name:literal, $params:literal, $response:literal, $stream:expr;)+) => {
        &[$(($name, $params, $response, $stream)),+]
    };
}

pub const METHODS: &[(&str, &str, &str, Option<&str>)] = method_registry! {
    "bluetooth.protocol.describe", "{}", "registry", None;
    "bluetooth.snapshot", "{}", "snapshot", Some(stream::CHANGED);
    "bluetooth.setPowered", r#"{"adapter_key":null,"powered":true}"#, "snapshot", Some(stream::CHANGED);
    "bluetooth.scan", r#"{"adapter_key":"adapter-opaque","enabled":true,"timeout_ms":15000}"#, "scan", Some(stream::SCAN);
    "bluetooth.adapter.operation", r#"{"key":"adapter-opaque","operation":"set-discoverable","discoverable":true}"#, "snapshot", Some(stream::CHANGED);
    "bluetooth.management.update", r#"{"launch_state":"remember","reconnect_on_resume":true,"trust_after_pair":true,"preferred_adapter_key":"adapter-opaque","show_blocked_devices":false,"show_recent_devices":false}"#, "snapshot", Some(stream::CHANGED);
    "bluetooth.device.policy.update", r#"{"key":"device-opaque","reconnect_on_resume":true,"trust_after_pair":true,"power_on_connect":true,"wait_for_services":true,"audio_route_on_connect":"keep","preferred_audio_profile_key":null}"#, "snapshot", Some(stream::CHANGED);
    "bluetooth.obex.snapshot", "{}", "obex", None;
    "bluetooth.obex.send", r#"{"device_key":"device-opaque","path":"/selected/file"}"#, "transfer", Some(stream::OBEX);
    "bluetooth.obex.respond", r#"{"request_id":"obex-incoming-1","accept":true}"#, "authorization", Some(stream::OBEX);
    "bluetooth.audio.snapshot", "{}", "audio_devices", None;
    "bluetooth.audio.setProfile", r#"{"device_key":"device-opaque","profile_key":"audio-profile-opaque"}"#, "audio_devices", None;
    "bluetooth.audio.setDefault", r#"{"device_key":"device-opaque","endpoint_key":"audio-endpoint-opaque"}"#, "audio_devices", Some(stream::AUDIO);
    "bluetooth.requests.snapshot", "{}", "requests", None;
    "bluetooth.device.operation", r#"{"key":"device-opaque","operation":"connect","power_on":true,"trust":false,"wait_for_services":true}"#, "operation", Some(stream::OPERATION);
    "bluetooth.pairing.respond", r#"{"request_id":"pairing-1","accept":true,"value":null}"#, "result", Some(stream::PAIRING);
};

macro_rules! stream_registry {
    ($($name:expr => [$($event:literal),+];)+) => {
        &[$(($name, &[$($event),+])),+]
    };
}

pub const STREAMS: &[(&str, &[&str])] = stream_registry! {
    stream::CHANGED => ["subscribed", "changed", "unavailable"];
    stream::PAIRING => ["requested", "display", "cancelled", "lagged"];
    stream::OPERATION => ["started", "progress", "completed", "failed", "cancelled", "lagged"];
    stream::SCAN => ["started", "completed", "failed", "cancelled", "lagged"];
    stream::OBEX => ["authorization-requested", "queued", "progress", "completed", "failed", "cancelled", "lagged"];
    stream::AUDIO => ["subscribed", "changed", "unavailable"];
};

fn method_description(name: &str) -> &'static str {
    match name {
        "bluetooth.protocol.describe" => {
            "Return machine-readable bt-api methods, schemas, streams, operations, and errors."
        }
        "bluetooth.snapshot" => "Return the current radio, adapter, device, and policy snapshot.",
        "bluetooth.setPowered" => "Set global or adapter Bluetooth power.",
        "bluetooth.scan" => "Acquire or release a bounded caller-owned discovery lease.",
        "bluetooth.adapter.operation" => "Mutate one adapter setting.",
        "bluetooth.management.update" => "Update persistent global Bluetooth policy.",
        "bluetooth.device.policy.update" => {
            "Update persistent policy for one device; null clears an override."
        }
        "bluetooth.obex.snapshot" => "Return object-push capabilities.",
        "bluetooth.obex.send" => "Start an outgoing object-push transfer.",
        "bluetooth.obex.respond" => "Answer an incoming object-push authorization request.",
        "bluetooth.audio.snapshot" => "Return Bluetooth PipeWire cards, profiles, and endpoints.",
        "bluetooth.audio.setProfile" => "Activate an available Bluetooth audio profile.",
        "bluetooth.audio.setDefault" => "Make a Bluetooth sink or source the PipeWire default.",
        "bluetooth.requests.snapshot" => {
            "Recover active and recent request state after reconnect or event loss."
        }
        "bluetooth.device.operation" => "Start a cancellable staged device operation.",
        "bluetooth.pairing.respond" => "Answer an active pairing-agent prompt.",
        _ => "Bluetooth API method.",
    }
}

fn required_capability(name: &str) -> Option<&'static str> {
    match name {
        "bluetooth.obex.send" => Some("can_send_file"),
        "bluetooth.audio.setProfile" | "bluetooth.audio.setDefault" => Some("audio_device"),
        _ => None,
    }
}

fn params_schema(name: &str, encoded: &str) -> Value {
    let example = serde_json::from_str::<Value>(encoded).expect("valid protocol fixture");
    let mut properties = Map::new();
    let mut required = Vec::new();
    if let Some(object) = example.as_object() {
        for (key, value) in object {
            let mut schema = inferred_schema(value);
            apply_constraints(name, key, &mut schema);
            properties.insert(key.clone(), schema);
            if required_parameters(name).contains(&key.as_str()) {
                required.push(Value::String(key.clone()));
            }
        }
    }
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": true,
    })
}

fn required_parameters(name: &str) -> &'static [&'static str] {
    match name {
        "bluetooth.setPowered" => &["powered"],
        "bluetooth.adapter.operation" => &["key", "operation"],
        "bluetooth.device.policy.update" => &["key"],
        "bluetooth.obex.send" => &["device_key", "path"],
        "bluetooth.obex.respond" => &["request_id", "accept"],
        "bluetooth.audio.setProfile" => &["device_key", "profile_key"],
        "bluetooth.audio.setDefault" => &["device_key", "endpoint_key"],
        "bluetooth.device.operation" => &["key", "operation"],
        "bluetooth.pairing.respond" => &["request_id", "accept"],
        _ => &[],
    }
}

fn inferred_schema(value: &Value) -> Value {
    match value {
        Value::Null => json!({ "type": ["string", "null"] }),
        Value::Bool(_) => json!({ "type": "boolean" }),
        Value::Number(value) if value.is_u64() => json!({ "type": "integer", "minimum": 0 }),
        Value::Number(_) => json!({ "type": "number" }),
        Value::String(_) => json!({ "type": "string", "minLength": 1 }),
        Value::Array(_) => json!({ "type": "array" }),
        Value::Object(_) => json!({ "type": "object" }),
    }
}

fn apply_constraints(method: &str, property: &str, schema: &mut Value) {
    let enum_values = match (method, property) {
        ("bluetooth.adapter.operation", "operation") => Some(AdapterOperation::VALUES),
        ("bluetooth.device.operation", "operation") => Some(DeviceOperation::VALUES),
        ("bluetooth.management.update", "launch_state") => {
            Some(&["remember", "enable", "disable"] as &[&str])
        }
        ("bluetooth.device.policy.update", "audio_route_on_connect") => {
            Some(&["keep", "switch"] as &[&str])
        }
        _ => None,
    };
    if let Some(values) = enum_values {
        schema["enum"] = json!(values);
    }
    if method == "bluetooth.scan" && property == "timeout_ms" {
        schema["minimum"] = json!(1_000);
        schema["maximum"] = json!(60_000);
    }
}

fn operation_parameters() -> Value {
    json!({
        "adapter": {
            "set-alias": { "required": ["alias"], "properties": { "alias": { "type": "string", "minLength": 1 } } },
            "set-discoverable": { "required": ["discoverable"], "properties": { "discoverable": { "type": "boolean" } } },
            "set-pairable": { "required": ["pairable"], "properties": { "pairable": { "type": "boolean" } } },
            "set-discoverable-timeout": { "required": ["timeout"], "properties": { "timeout": { "type": "integer", "minimum": 0, "maximum": u32::MAX } } },
            "set-pairable-timeout": { "required": ["timeout"], "properties": { "timeout": { "type": "integer", "minimum": 0, "maximum": u32::MAX } } },
        },
        "device": {
            "pair": { "properties": {
                "power_on": { "type": "boolean" },
                "trust_after_pair": { "type": "boolean" },
                "wait_for_services": { "type": "boolean" },
                "fast_pair_anti_spoofing_public_key": { "type": "string" }
            } },
            "connect": { "properties": {
                "power_on": { "type": "boolean" },
                "trust": { "type": "boolean" },
                "wait_for_services": { "type": "boolean" }
            } },
            "disconnect": { "properties": {} },
            "remove": { "properties": {} },
            "set-trusted": { "required": ["trusted"], "properties": { "trusted": { "type": "boolean" } } },
            "set-blocked": { "required": ["blocked"], "properties": { "blocked": { "type": "boolean" } } },
            "set-wake-allowed": { "required": ["wake_allowed"], "properties": { "wake_allowed": { "type": "boolean" } } },
            "set-alias": { "required": ["alias"], "properties": { "alias": { "type": "string", "minLength": 1 } } },
            "reset-alias": { "properties": {} },
            "provision-fast-pair": { "required": ["anti_spoofing_public_key"], "properties": { "anti_spoofing_public_key": { "type": "string" } } },
            "set-multipoint": { "required": ["enabled"], "properties": { "enabled": { "type": "boolean" } } },
            "set-noise-control": { "required": ["mode"], "properties": { "mode": { "enum": ["transparent", "adaptive", "off", "noise-cancelling"] } } },
        }
    })
}

fn error_registry() -> Value {
    let backend = [
        BackendErrorKind::Timeout,
        BackendErrorKind::DeviceUnavailable,
        BackendErrorKind::Rejected,
        BackendErrorKind::Unavailable,
        BackendErrorKind::InvalidInput,
        BackendErrorKind::OperationFailed,
    ];
    let mut errors = backend
        .into_iter()
        .map(|kind| json!({ "code": kind.code(), "retryable": kind.retryable() }))
        .collect::<Vec<_>>();
    errors.extend([
        json!({ "code": "daemon-unavailable", "retryable": true }),
        json!({ "code": "device-busy", "retryable": true }),
        json!({ "code": "request-not-found", "retryable": false }),
        json!({ "code": "unsupported-method", "retryable": false }),
        json!({ "code": "unsupported-stream", "retryable": false }),
        json!({ "code": "audio-unavailable", "retryable": true }),
        json!({ "code": "audio-endpoint-unavailable", "retryable": true }),
    ]);
    Value::Array(errors)
}

pub fn registry() -> Value {
    json!({
        "protocol": NAME,
        "version": VERSION,
        "methods": METHODS.iter().map(|(name, params, response_key, stream)| {
            let example = serde_json::from_str::<Value>(params).expect("valid protocol fixture");
            json!({
                "name": name,
                "description": method_description(name),
                "params_example": example,
                "params_schema": params_schema(name, params),
                "response_key": response_key,
                "stream": stream,
                "cancellable": matches!(*name, "bluetooth.scan" | "bluetooth.device.operation" | "bluetooth.obex.send"),
                "required_capability": required_capability(name),
            })
        }).collect::<Vec<_>>(),
        "streams": STREAMS.iter().map(|(name, events)| json!({
            "name": name,
            "events": events,
        })).collect::<Vec<_>>(),
        "operations": {
            "adapter": {
                "names": AdapterOperation::VALUES,
                "parameters": operation_parameters()["adapter"].clone(),
            },
            "device": {
                "names": DeviceOperation::VALUES,
                "parameters": operation_parameters()["device"].clone(),
            },
        },
        "errors": error_registry(),
        "compatibility": {
            "request_unknown_fields": "method-dependent",
            "response_unknown_fields": "ignore",
            "event_recovery_method": "bluetooth.requests.snapshot",
        },
    })
}

/// Return the checked-in contract used by consumers and compatibility tests.
pub fn contract_fixture() -> Value {
    match shelllist_daemon_core::load_fixture(include_str!("../test_support/bt-api-v1.json")) {
        Ok(fixture) => fixture,
        Err(error) => json!({ "fixture_error": error.to_string() }),
    }
}

#[cfg(test)]
mod tests {
    use super::{METHODS, STREAMS, VERSION, contract_fixture, registry};

    #[test]
    fn registry_names_are_unique_and_fixture_matches_registry() {
        shelllist_daemon_core::validate_unique_names(
            &METHODS.iter().map(|method| method.0).collect::<Vec<_>>(),
        )
        .unwrap();
        shelllist_daemon_core::validate_unique_names(
            &STREAMS.iter().map(|stream| stream.0).collect::<Vec<_>>(),
        )
        .unwrap();

        let fixture = contract_fixture();
        assert_eq!(fixture["version"], VERSION);
        let registry = registry();
        assert_eq!(fixture["registry"], registry);
        assert!(
            registry["methods"]
                .as_array()
                .unwrap()
                .iter()
                .all(|method| method["params_schema"]["type"] == "object"
                    && method["description"].as_str().is_some())
        );
        assert_eq!(
            registry["operations"]["device"]["parameters"]["connect"]["properties"]["power_on"]["type"],
            "boolean"
        );
        assert!(
            registry["errors"]
                .as_array()
                .unwrap()
                .iter()
                .any(|error| error["code"] == "timeout" && error["retryable"] == true)
        );
    }
}
