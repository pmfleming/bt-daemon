use std::sync::Arc;

use anyhow::Error;
use serde_json::{Value, json};

use crate::{
    backend::{
        AdapterOperation, BackendError, BackendErrorKind, BluetoothBackend, DeviceOperation,
    },
    params::Params,
};

pub use crate::protocol::{NAME as PROTOCOL, VERSION};

enum BackendRequest<'a> {
    Snapshot,
    SetPowered {
        adapter_key: Option<&'a str>,
        powered: bool,
    },
    SetScanning {
        adapter_key: Option<&'a str>,
        enabled: bool,
    },
    AdapterOperation {
        key: &'a str,
        operation: AdapterOperation,
    },
    DeviceOperation {
        key: &'a str,
        operation: DeviceOperation,
    },
}

impl BackendRequest<'_> {
    async fn execute(
        self,
        backend: &Arc<dyn BluetoothBackend>,
        params: &Value,
    ) -> anyhow::Result<crate::model::Snapshot> {
        match self {
            Self::Snapshot => backend.snapshot().await,
            Self::SetPowered {
                adapter_key,
                powered,
            } => backend.set_powered(adapter_key, powered).await,
            Self::SetScanning {
                adapter_key,
                enabled,
            } => backend.set_scanning(adapter_key, enabled).await,
            Self::AdapterOperation { key, operation } => {
                backend.adapter_operation(key, operation, params).await
            }
            Self::DeviceOperation { key, operation } => {
                backend.device_operation(key, operation, params).await
            }
        }
    }
}

pub async fn dispatch(backend: Arc<dyn BluetoothBackend>, method: &str, params: Value) -> Value {
    tracing::debug!(%method, "backend API request started");
    let request = match parse_backend_request(method, &params) {
        Ok(request) => request,
        Err(response) => {
            log_response(method, &response);
            return response;
        }
    };
    let response = match request.execute(&backend, &params).await {
        Ok(snapshot) => success(json!({ "snapshot": snapshot })),
        Err(cause) => {
            tracing::warn!(%method, error = %cause, error_chain = %format!("{cause:#}"), "backend API request failed");
            let details = error_value(&cause);
            json!({ "protocol": PROTOCOL, "version": VERSION, "ok": false, "error": details })
        }
    };
    log_response(method, &response);
    response
}

fn parse_backend_request<'a>(method: &str, params: &'a Value) -> Result<BackendRequest<'a>, Value> {
    let adapter_key = || {
        params
            .optional_string("adapter_key")
            .map_err(validation_error)
    };
    match method {
        "bluetooth.snapshot" => Ok(BackendRequest::Snapshot),
        "bluetooth.setPowered" => Ok(BackendRequest::SetPowered {
            adapter_key: adapter_key()?,
            powered: params.require_bool("powered").map_err(validation_error)?,
        }),
        "bluetooth.scan" => Ok(BackendRequest::SetScanning {
            adapter_key: adapter_key()?,
            enabled: params.require_bool("enabled").map_err(validation_error)?,
        }),
        "bluetooth.adapter.operation" => {
            let (key, operation) = typed_operation(params).map_err(validation_error)?;
            Ok(BackendRequest::AdapterOperation { key, operation })
        }
        "bluetooth.device.operation" => {
            let (key, operation) = typed_operation(params).map_err(validation_error)?;
            Ok(BackendRequest::DeviceOperation { key, operation })
        }
        _ => Err(error(
            "unsupported-method",
            format!("Unsupported bt-api method: {method}"),
        )),
    }
}

pub fn log_response(action: &str, response: &Value) {
    if response["ok"].as_bool() == Some(true) {
        tracing::info!(%action, "request completed");
    } else {
        tracing::warn!(
            %action,
            code = response["error"]["code"].as_str().unwrap_or("unknown"),
            message = response["error"]["message"].as_str().unwrap_or("unspecified error"),
            "request returned an error"
        );
    }
}

fn typed_operation<T>(params: &Value) -> anyhow::Result<(&str, T)>
where
    for<'a> T: TryFrom<&'a str, Error = BackendError>,
{
    let (key, operation) = params.require_strings("key", "operation")?;
    Ok((key, T::try_from(operation).map_err(Error::new)?))
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

    use super::{error_value, parse_backend_request};

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

    #[test]
    fn invalid_optional_adapter_key_is_not_treated_as_all_adapters() {
        let params = serde_json::json!({ "adapter_key": 42, "powered": false });
        let Err(error) = parse_backend_request("bluetooth.setPowered", &params) else {
            panic!("invalid adapter key was accepted");
        };
        assert_eq!(error["error"]["code"], "validation-error");
    }
}
