use std::sync::Arc;

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
        "device.operation" => {
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
        Err(cause) => error("operation-failed", format!("{cause:#}")),
    }
}

pub fn success(data: Value) -> Value {
    json!({ "protocol": PROTOCOL, "version": VERSION, "ok": true, "data": data })
}

pub fn error(code: &str, message: String) -> Value {
    json!({ "protocol": PROTOCOL, "version": VERSION, "ok": false, "error": { "code": code, "message": message } })
}
