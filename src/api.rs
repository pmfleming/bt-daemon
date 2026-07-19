use std::sync::Arc;

use anyhow::Error;
use serde_json::{Value, json};

use crate::backend::BluetoothBackend;

pub const PROTOCOL: &str = "bt-api";
pub const VERSION: u8 = 1;

pub async fn dispatch(backend: Arc<dyn BluetoothBackend>, method: &str, params: Value) -> Value {
    let result = match method {
        "bluetooth.snapshot" => backend.snapshot().await,
        "bluetooth.setPowered" => {
            backend
                .set_powered(
                    params.get("adapter_key").and_then(Value::as_str),
                    params
                        .get("powered")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                )
                .await
        }
        "bluetooth.scan" => {
            backend
                .set_scanning(
                    params
                        .get("enabled")
                        .and_then(Value::as_bool)
                        .unwrap_or(true),
                )
                .await
        }
        "bluetooth.device.operation" => {
            let key = params
                .get("key")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let operation = params
                .get("operation")
                .and_then(Value::as_str)
                .unwrap_or_default();
            backend.device_operation(key, operation, &params).await
        }
        _ => {
            return error(
                "unsupported-method",
                format!("Unsupported bt-api method: {method}"),
            );
        }
    };
    match result {
        Ok(snapshot) => success(json!({ "snapshot": snapshot })),
        Err(cause) => {
            let details = error_value(&cause);
            json!({ "protocol": PROTOCOL, "version": VERSION, "ok": false, "error": details })
        }
    }
}

pub fn success(data: Value) -> Value {
    json!({ "protocol": PROTOCOL, "version": VERSION, "ok": true, "data": data })
}

pub fn error_value(error: &Error) -> Value {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    let code = if lower.contains("timed out") {
        "timeout"
    } else if lower.contains("not found")
        || lower.contains("no bluetooth device")
        || lower.contains("no longer available")
    {
        "device-unavailable"
    } else if lower.contains("rejected") || lower.contains("authentication") {
        "rejected"
    } else if lower.contains("not ready") || lower.contains("unavailable") {
        "bluez-unavailable"
    } else {
        "operation-failed"
    };
    json!({ "code": code, "message": message, "retryable": matches!(code, "timeout" | "device-unavailable" | "bluez-unavailable") })
}

pub fn error(code: &str, message: String) -> Value {
    json!({ "protocol": PROTOCOL, "version": VERSION, "ok": false, "error": { "code": code, "message": message } })
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use super::error_value;

    #[test]
    fn classifies_operation_errors() {
        assert_eq!(
            error_value(&anyhow!("connect timed out"))["code"],
            "timeout"
        );
        assert_eq!(
            error_value(&anyhow!("Bluetooth device not found"))["code"],
            "device-unavailable"
        );
        assert_eq!(
            error_value(&anyhow!("Authentication rejected"))["code"],
            "rejected"
        );
    }
}
