use anyhow::Result;
use serde_json::Value;
use shelllist_daemon_core::DaemonEndpoint;
use shelllist_daemon_tokio::{CorrelationPolicy, JsonlClientConfig, run_jsonl_client};

use crate::daemon::{BUS_NAME, INTERFACE, OBJECT_PATH};

const ENDPOINT: DaemonEndpoint = DaemonEndpoint::new("bt-daemon", BUS_NAME, OBJECT_PATH, INTERFACE);

#[derive(Debug, Clone, Copy)]
pub(crate) struct BluetoothCorrelation;

impl CorrelationPolicy for BluetoothCorrelation {
    fn operation_id<'a>(&self, response: &'a Value) -> Option<&'a str> {
        [
            "/data/operation/request_id",
            "/data/scan/request_id",
            "/data/transfer/request_id",
        ]
        .into_iter()
        .find_map(|path| response.pointer(path)?.as_str())
    }

    fn event_id(&self, stream: &str, event: &Value) -> Option<String> {
        let value = if matches!(
            stream,
            crate::daemon::OPERATION_STREAM
                | crate::daemon::SCAN_STREAM
                | crate::daemon::OBEX_STREAM
        ) {
            event.pointer("/data/request_id")
        } else {
            event.get("subscription_id")
        };
        value.and_then(Value::as_str).map(str::to_owned)
    }

    fn is_terminal(&self, stream: &str, event: &Value) -> bool {
        matches!(
            stream,
            crate::daemon::OPERATION_STREAM
                | crate::daemon::SCAN_STREAM
                | crate::daemon::OBEX_STREAM
        ) && matches!(
            event.get("event").and_then(Value::as_str),
            Some("completed" | "failed" | "cancelled")
        )
    }
}

/// Bridge JSON Lines on standard input/output to the session D-Bus daemon.
/// Owns subscriptions for this frontend session and releases them on disconnect.
pub async fn run() -> Result<()> {
    tracing::info!("JSON-lines client started");
    run_jsonl_client(JsonlClientConfig {
        endpoint: ENDPOINT,
        correlation: BluetoothCorrelation,
        pending_event_limit: 32,
        max_in_flight_requests: 64,
        shutdown_timeout: Some(std::time::Duration::from_secs(5)),
    })
    .await
}
