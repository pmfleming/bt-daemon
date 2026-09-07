use std::sync::Arc;

use anyhow::Error;
use serde_json::{Value, json};
use shelllist_daemon_core::{ApiError as EnvelopeError, ApiIdentity};

use crate::backend::{AdapterOperation, BackendError, BackendErrorKind, BluetoothBackend, Params};

pub use crate::protocol::{NAME as PROTOCOL, VERSION};
const API: ApiIdentity = ApiIdentity::new(PROTOCOL, VERSION as u32);

enum BackendRequest<'a> {
    Snapshot,
    SetPowered {
        adapter_key: Option<&'a str>,
        powered: bool,
    },
    AdapterOperation {
        key: &'a str,
        operation: AdapterOperation,
    },
    UpdateManagement,
    UpdateDevicePolicy {
        key: &'a str,
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
            Self::AdapterOperation { key, operation } => {
                backend.adapter_operation(key, operation, params).await
            }
            Self::UpdateManagement => backend.update_management(params).await,
            Self::UpdateDevicePolicy { key } => {
                backend
                    .update_device_policy(key, &policy_values(params))
                    .await
            }
        }
    }
}

fn policy_values(params: &Value) -> Value {
    let mut values = params.clone();
    if let Some(object) = values.as_object_mut() {
        object.remove("key");
    }
    values
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
            backend_error(&cause)
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
        "bluetooth.adapter.operation" => {
            let (key, operation) = typed_operation(params).map_err(validation_error)?;
            Ok(BackendRequest::AdapterOperation { key, operation })
        }
        "bluetooth.management.update" => Ok(BackendRequest::UpdateManagement),
        "bluetooth.device.policy.update" => Ok(BackendRequest::UpdateDevicePolicy {
            key: params.require_string("key").map_err(validation_error)?,
        }),
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
    shelllist_daemon_core::success(API, data)
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

fn backend_error(cause: &Error) -> Value {
    let details = error_value(cause);
    shelllist_daemon_core::error(
        API,
        EnvelopeError::new(
            details["code"].as_str().unwrap_or("operation-failed"),
            details["message"]
                .as_str()
                .unwrap_or("Bluetooth operation failed"),
        )
        .with_retryable(details["retryable"].as_bool().unwrap_or(false)),
    )
}

pub fn error(code: &str, message: String) -> Value {
    shelllist_daemon_core::error(API, EnvelopeError::new(code, message))
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use crate::backend::{BackendError, BackendErrorKind};

    use super::{error_value, parse_backend_request};

    #[test]
    fn policy_routing_key_is_not_treated_as_a_setting() {
        let params = serde_json::json!({"key": "device-1", "reconnect_on_resume": false});
        let settings = super::policy_values(&params);
        let store = crate::management::ManagementStore::in_memory();
        assert!(
            !store
                .update_device_policy("device-1", &settings)
                .unwrap()
                .reconnect_on_resume
        );
        assert!(params.get("key").is_some());
        assert!(
            store
                .update_device_policy(
                    "device-1",
                    &super::policy_values(&serde_json::json!({"key":"device-1", "unknown": true}))
                )
                .is_err()
        );
    }

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
    fn invalid_optional_adapter_key_is_not_treated_as_all_adapters() {
        let params = serde_json::json!({ "adapter_key": 42, "powered": false });
        let Err(error) = parse_backend_request("bluetooth.setPowered", &params) else {
            panic!("invalid adapter key was accepted");
        };
        assert_eq!(error["error"]["code"], "validation-error");
    }
}
