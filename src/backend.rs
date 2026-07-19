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
        operation: &str,
        params: &Value,
    ) -> Result<Snapshot>;
    async fn obex_target(&self, device_key: &str) -> Result<ObexTarget>;
    async fn obex_remote(&self, source: &str, destination: &str) -> Result<ObexRemote>;
    async fn device_operation(
        &self,
        device_key: &str,
        operation: &str,
        params: &Value,
    ) -> Result<Snapshot>;
}
