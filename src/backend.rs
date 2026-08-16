use std::{error::Error, fmt, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::broadcast;

use crate::model::Snapshot;

pub type OperationProgress = Arc<dyn Fn(&'static str) + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendErrorKind {
    Timeout,
    DeviceUnavailable,
    Rejected,
    Unavailable,
    InvalidInput,
    OperationFailed,
}

impl BackendErrorKind {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::DeviceUnavailable => "device-unavailable",
            Self::Rejected => "rejected",
            Self::Unavailable => "bluez-unavailable",
            Self::InvalidInput => "validation-error",
            Self::OperationFailed => "operation-failed",
        }
    }

    pub const fn retryable(self) -> bool {
        matches!(
            self,
            Self::Timeout | Self::DeviceUnavailable | Self::Unavailable
        )
    }
}

#[derive(Debug)]
pub struct BackendError {
    pub kind: BackendErrorKind,
    message: String,
}

impl BackendError {
    pub fn new(kind: BackendErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for BackendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for BackendError {}

pub(crate) trait Params {
    fn optional_bool(&self, name: &str) -> Result<Option<bool>>;
    fn optional_string(&self, name: &str) -> Result<Option<&str>>;
    fn require_bool(&self, name: &str) -> Result<bool>;
    fn require_string(&self, name: &str) -> Result<&str>;
    fn require_u32(&self, name: &str) -> Result<u32>;

    fn require_strings(&self, first: &str, second: &str) -> Result<(&str, &str)> {
        Ok((self.require_string(first)?, self.require_string(second)?))
    }
}

fn invalid_parameter(name: &str, expected: &str, optional: bool) -> anyhow::Error {
    let qualifier = if optional {
        "invalid optional"
    } else {
        "missing or invalid"
    };
    BackendError::new(
        BackendErrorKind::InvalidInput,
        format!("{qualifier} {expected} parameter '{name}'"),
    )
    .into()
}

impl Params for Value {
    fn optional_bool(&self, name: &str) -> Result<Option<bool>> {
        match self.get(name) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Bool(value)) => Ok(Some(*value)),
            _ => Err(invalid_parameter(name, "boolean", true)),
        }
    }

    fn optional_string(&self, name: &str) -> Result<Option<&str>> {
        match self.get(name) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) if !value.is_empty() => Ok(Some(value)),
            _ => Err(invalid_parameter(name, "string", true)),
        }
    }

    fn require_bool(&self, name: &str) -> Result<bool> {
        self.get(name)
            .and_then(Value::as_bool)
            .ok_or_else(|| invalid_parameter(name, "boolean", false))
    }

    fn require_string(&self, name: &str) -> Result<&str> {
        self.get(name)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid_parameter(name, "string", false))
    }

    fn require_u32(&self, name: &str) -> Result<u32> {
        self.get(name)
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| invalid_parameter(name, "unsigned integer", false))
    }
}

macro_rules! operation_enum {
    ($name:ident, $target:literal, {$($variant:ident => $value:literal),+ $(,)?}) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub const VALUES: &'static [&'static str] = &[$($value),+];

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl TryFrom<&str> for $name {
            type Error = BackendError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    _ => Err(BackendError::new(
                        BackendErrorKind::InvalidInput,
                        format!("unsupported Bluetooth {} operation: {value}", $target),
                    )),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

operation_enum!(AdapterOperation, "adapter", {
    SetAlias => "set-alias",
    SetDiscoverable => "set-discoverable",
    SetPairable => "set-pairable",
    SetDiscoverableTimeout => "set-discoverable-timeout",
    SetPairableTimeout => "set-pairable-timeout",
});

operation_enum!(DeviceOperation, "device", {
    Pair => "pair",
    Connect => "connect",
    Disconnect => "disconnect",
    Remove => "remove",
    SetTrusted => "set-trusted",
    SetBlocked => "set-blocked",
    SetWakeAllowed => "set-wake-allowed",
    SetAlias => "set-alias",
    ResetAlias => "reset-alias",
    ProvisionFastPair => "provision-fast-pair",
    SetMultipoint => "set-multipoint",
    SetNoiseControl => "set-noise-control",
});

#[derive(Debug, Clone)]
pub struct ObexTarget {
    pub source: String,
    pub destination: String,
}

#[derive(Debug, Clone)]
pub struct ObexRemote {
    pub device_key: String,
    pub name: String,
}

#[async_trait]
pub trait BluetoothBackend: Send + Sync {
    fn subscribe_changes(&self) -> broadcast::Receiver<()>;
    async fn snapshot(&self) -> Result<Snapshot>;
    async fn set_powered(&self, adapter_key: Option<&str>, powered: bool) -> Result<Snapshot>;
    async fn set_scanning(&self, adapter_key: Option<&str>, enabled: bool) -> Result<Snapshot>;
    async fn adapter_operation(
        &self,
        adapter_key: &str,
        operation: AdapterOperation,
        params: &Value,
    ) -> Result<Snapshot>;
    async fn update_management(&self, params: &Value) -> Result<Snapshot>;
    async fn update_device_policy(&self, device_key: &str, params: &Value) -> Result<Snapshot>;
    async fn obex_target(&self, device_key: &str) -> Result<ObexTarget>;
    async fn obex_remote(&self, source: &str, destination: &str) -> Result<ObexRemote>;
    async fn device_operation(
        &self,
        device_key: &str,
        operation: DeviceOperation,
        params: &Value,
        progress: OperationProgress,
    ) -> Result<Snapshot>;
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::Params;

    #[test]
    fn required_parameters_reject_missing_empty_and_out_of_range_values() {
        let params = json!({ "name": "", "count": u64::from(u32::MAX) + 1, "adapter_key": 42 });
        assert!(params.require_string("name").is_err());
        assert!(params.optional_string("adapter_key").is_err());
        assert_eq!(
            json!({ "adapter_key": null })
                .optional_string("adapter_key")
                .unwrap(),
            None
        );
        assert!(params.require_bool("enabled").is_err());
        assert_eq!(params.optional_bool("enabled").unwrap(), None);
        assert!(
            json!({ "enabled": "yes" })
                .optional_bool("enabled")
                .is_err()
        );
        assert_eq!(
            json!({ "enabled": null }).optional_bool("enabled").unwrap(),
            None
        );
        assert_eq!(
            json!({ "enabled": false })
                .optional_bool("enabled")
                .unwrap(),
            Some(false)
        );
        assert!(params.require_u32("count").is_err());
    }
}
