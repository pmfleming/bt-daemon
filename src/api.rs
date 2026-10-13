use std::sync::Arc;

use anyhow::Error;
use serde_json::{Value, json};
use shelllist_daemon_core::{ApiError as EnvelopeError, ApiIdentity};

use crate::backend::{AdapterOperation, BackendError, BackendErrorKind, BluetoothBackend, Params};

/// Protocol identity shared by every response envelope.
pub use crate::protocol::{NAME as PROTOCOL, VERSION};
const API: ApiIdentity = ApiIdentity::new(PROTOCOL, VERSION as u32);

mod adapter;

enum BackendRequest {
    Snapshot,
    SetPowered {
        adapter_key: Option<String>,
        powered: bool,
    },
    AdapterOperation {
        key: String,
        operation: AdapterOperation,
        params: Value,
    },
    UpdateManagement(Value),
    UpdateDevicePolicy {
        key: String,
        params: Value,
    },
}

impl BackendRequest {
    async fn execute(
        self,
        backend: &Arc<dyn BluetoothBackend>,
    ) -> anyhow::Result<crate::model::Snapshot> {
        match self {
            Self::Snapshot => backend.snapshot().await,
            Self::SetPowered {
                adapter_key,
                powered,
            } => backend.set_powered(adapter_key.as_deref(), powered).await,
            Self::AdapterOperation {
                key,
                operation,
                params,
            } => adapter::single(Arc::clone(backend), key, operation, params).await,
            Self::UpdateManagement(params) => backend.update_management(&params).await,
            Self::UpdateDevicePolicy { key, params } => {
                backend.update_device_policy(&key, &params).await
            }
        }
    }
}

fn policy_values(mut values: Value) -> Value {
    if let Some(object) = values.as_object_mut() {
        object.remove("key");
    }
    values
}

/// Validate and dispatch a backend request, returning a versioned success or error envelope.
/// Validation errors never invoke a backend operation.
pub async fn dispatch(backend: Arc<dyn BluetoothBackend>, method: &str, params: Value) -> Value {
    tracing::debug!(%method, "backend API request started");
    if method == "bluetooth.adapter.update" {
        let response = adapter::submit(backend, params).await;
        log_response(method, &response);
        return response;
    }
    let request = match parse_backend_request(method, params) {
        Ok(request) => request,
        Err(response) => {
            log_response(method, &response);
            return response;
        }
    };
    let response = match request.execute(&backend).await {
        Ok(snapshot) => success(json!({ "snapshot": snapshot })),
        Err(cause) => {
            tracing::warn!(%method, error = %cause, error_chain = %format!("{cause:#}"), "backend API request failed");
            backend_error(&cause)
        }
    };
    log_response(method, &response);
    response
}

fn parse_backend_request(method: &str, params: Value) -> Result<BackendRequest, Value> {
    match method {
        "bluetooth.snapshot" => Ok(BackendRequest::Snapshot),
        "bluetooth.setPowered" => Ok(BackendRequest::SetPowered {
            adapter_key: params
                .optional_string("adapter_key")
                .map_err(validation_error)?
                .map(str::to_owned),
            powered: params.require_bool("powered").map_err(validation_error)?,
        }),
        "bluetooth.adapter.operation" => {
            let (key, operation) = typed_operation(&params).map_err(validation_error)?;
            Ok(BackendRequest::AdapterOperation {
                key: key.to_owned(),
                operation,
                params,
            })
        }
        "bluetooth.management.update" => Ok(BackendRequest::UpdateManagement(params)),
        "bluetooth.device.policy.update" => Ok(BackendRequest::UpdateDevicePolicy {
            key: params
                .require_string("key")
                .map_err(validation_error)?
                .to_owned(),
            params: policy_values(params),
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
