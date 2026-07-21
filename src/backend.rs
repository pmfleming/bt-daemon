use std::{error::Error, fmt};

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::broadcast;

use crate::model::Snapshot;

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

macro_rules! operation_enum {
    ($name:ident, $target:literal, {$($variant:ident => $value:literal),+ $(,)?}) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name {
            $($variant),+
        }

        impl $name {
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
    async fn obex_target(&self, device_key: &str) -> Result<ObexTarget>;
    async fn obex_remote(&self, source: &str, destination: &str) -> Result<ObexRemote>;
    async fn device_operation(
        &self,
        device_key: &str,
        operation: DeviceOperation,
        params: &Value,
    ) -> Result<Snapshot>;
}
