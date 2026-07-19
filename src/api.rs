use std::sync::Arc;

use anyhow::Error;
use serde_json::{Value, json};

use crate::{
    backend::{BackendError, BackendErrorKind, BluetoothBackend},
    params::Params,
};

pub use crate::protocol::{NAME as PROTOCOL, VERSION};

pub async fn dispatch(backend: Arc<dyn BluetoothBackend>, method: &str, params: Value) -> Value {
    let result = match method {
        "bluetooth.snapshot" => backend.snapshot().await,
        "bluetooth.setPowered" => {
            let powered = match params.require_bool("powered") {
                Ok(powered) => powered,
                Err(error) => return validation_error(error),
            };
            backend
                .set_powered(params.get("adapter_key").and_then(Value::as_str), powered)
                .await
        }
        "bluetooth.scan" => {
            let enabled = match params.require_bool("enabled") {
                Ok(enabled) => enabled,
                Err(error) => return validation_error(error),
            };
            backend
                .set_scanning(params.get("adapter_key").and_then(Value::as_str), enabled)
                .await
        }
        "bluetooth.adapter.operation" => {
            let (key, operation) = match params.require_strings("key", "operation") {
                Ok(params) => params,
                Err(error) => return validation_error(error),
            };
            backend.adapter_operation(key, operation, &params).await
        }
        "bluetooth.device.operation" => {
            let (key, operation) = match params.require_strings("key", "operation") {
                Ok(params) => params,
                Err(error) => return validation_error(error),
            };
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

fn validation_error(error: Error) -> Value {
    self::error("validation-error", error.to_string())
}

pub fn success(data: Value) -> Value {
    json!({ "protocol": PROTOCOL, "version": VERSION, "ok": true, "data": data })
}

pub fn error_value(error: &Error) -> Value {
    let kind = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<BackendError>())
        .map_or(BackendErrorKind::OperationFailed, |error| error.kind);
    json!({
        "code": kind.code(),
        "message": format!("{error:#}"),
        "retryable": kind.retryable(),
    })
}

pub fn error(code: &str, message: String) -> Value {
    json!({ "protocol": PROTOCOL, "version": VERSION, "ok": false, "error": { "code": code, "message": message } })
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use crate::backend::{BackendError, BackendErrorKind};

    use super::error_value;

    #[test]
    fn classifies_typed_errors_through_context() {
        let error = anyhow!(BackendError::new(
            BackendErrorKind::Timeout,
            "connect timed out",
        ))
        .context("connect headset");
        let value = error_value(&error);
        assert_eq!(value["code"], "timeout");
        assert_eq!(value["retryable"], true);
    }

    #[test]
    fn untyped_errors_are_not_classified_by_message() {
        let value = error_value(&anyhow!("authentication rejected after timeout"));
        assert_eq!(value["code"], "operation-failed");
        assert_eq!(value["retryable"], false);
    }
}
