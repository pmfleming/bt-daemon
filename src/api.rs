use std::sync::Arc;

use anyhow::Error;
use serde_json::{Value, json};
use shelllist_daemon_core::{ApiError as EnvelopeError, ApiIdentity};

use crate::backend::{AdapterOperation, BackendError, BackendErrorKind, BluetoothBackend, Params};

/// Protocol identity shared by every response envelope.
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

/// Validate and dispatch a backend request, returning a versioned success or error envelope.
/// Validation errors never invoke a backend operation.
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

/// Log an action's outcome without dumping the request or snapshot payload.
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

pub(crate) fn typed_operation<T>(params: &Value) -> anyhow::Result<(&str, T)>
where
    for<'a> T: TryFrom<&'a str, Error = BackendError>,
{
    let (key, operation) = params.require_strings("key", "operation")?;
    Ok((key, T::try_from(operation).map_err(Error::new)?))
}

fn validation_error(error: Error) -> Value {
    self::error("validation-error", error.to_string())
}

/// Wrap response data in the bt-api success envelope.
pub fn success(data: Value) -> Value {
    shelllist_daemon_core::success(API, data)
}

/// Preserve typed backend error codes and retryability through an anyhow context chain.
pub fn error_value(error: &Error) -> Value {
    error_details(error).into_value()
}

fn error_details(error: &Error) -> EnvelopeError {
    let kind = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<BackendError>())
        .map_or(BackendErrorKind::OperationFailed, |error| error.kind);
    EnvelopeError::new(kind.code(), format!("{error:#}")).with_retryable(kind.retryable())
}

pub(crate) fn backend_error(cause: &Error) -> Value {
    shelllist_daemon_core::error(API, error_details(cause))
}

/// Construct a versioned error response with the supplied code and human-readable message.
pub fn error(code: &str, message: String) -> Value {
    shelllist_daemon_core::error(API, EnvelopeError::new(code, message))
}

#[cfg(test)]
mod tests {
    #[test]
    fn backend_error_envelope_preserves_details_and_context() {
        use crate::backend::{BackendError, BackendErrorKind};
        for error in [
            anyhow::anyhow!("untyped failure"),
            anyhow::Error::new(BackendError::new(BackendErrorKind::Timeout, "timed out")),
            anyhow::Error::new(BackendError::new(BackendErrorKind::Rejected, "denied")),
        ] {
            let error = error.context("operation context");
            let response = super::backend_error(&error);
            assert_eq!(response["error"], super::error_value(&error));
            assert_eq!(response["ok"], false);
            assert!(
                response["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("operation context")
            );
        }
    }

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
}
