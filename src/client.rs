use anyhow::Result;
use serde_json::Value;
use shelllist_daemon_core::DaemonEndpoint;
use shelllist_daemon_tokio::{
    CallFailure, CancelMode, CorrelationPolicy, JsonlClientConfig, TrackedId, TrackedKind,
    run_jsonl_client,
};

use crate::{
    api,
    daemon::{BUS_NAME, INTERFACE, OBJECT_PATH},
};

const ENDPOINT: DaemonEndpoint = DaemonEndpoint::new("bt-daemon", BUS_NAME, OBJECT_PATH, INTERFACE);

#[derive(Debug, Clone, Copy)]
struct BluetoothCorrelation;

impl CorrelationPolicy for BluetoothCorrelation {
    fn response_id(&self, response: &Value) -> Option<TrackedId> {
        [
            "/data/operation/request_id",
            "/data/scan/request_id",
            "/data/transfer/request_id",
        ]
        .into_iter()
        .find_map(|path| tracked(response.pointer(path), TrackedKind::Operation))
        .or_else(|| {
            tracked(
                response.pointer("/data/subscription/id"),
                TrackedKind::Subscription,
            )
        })
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

fn tracked(value: Option<&Value>, kind: TrackedKind) -> Option<TrackedId> {
    value.and_then(Value::as_str).map(|id| TrackedId {
        id: id.to_owned(),
        kind,
    })
}

fn call_failure(method: &str, error: &anyhow::Error) -> CallFailure {
    tracing::warn!(%method, error = %error, error_chain = %format!("{error:#}"), "client call to daemon failed");
    CallFailure::Api(api::error(
        "daemon-unavailable",
        "bt-daemon session service is unavailable".to_string(),
    ))
}

pub async fn run() -> Result<()> {
    tracing::info!("JSON-lines client started");
    run_jsonl_client(JsonlClientConfig {
        endpoint: ENDPOINT,
        correlation: BluetoothCorrelation,
        cancel_mode: CancelMode::Json,
        call_failure,
        pending_event_limit: 32,
        max_in_flight_requests: 64,
        shutdown_timeout: Some(std::time::Duration::from_secs(5)),
    })
    .await
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use shelllist_daemon_tokio::{CorrelationPolicy, TrackedKind};

    use super::BluetoothCorrelation;
    use crate::daemon::{OBEX_STREAM, OPERATION_STREAM, SCAN_STREAM};

    #[test]
    fn correlates_bluetooth_operation_families() {
        let policy = BluetoothCorrelation;
        for (field, stream) in [
            ("operation", OPERATION_STREAM),
            ("scan", SCAN_STREAM),
            ("transfer", OBEX_STREAM),
        ] {
            let response = json!({ "data": { field: { "request_id": "request-1" } } });
            let tracked = policy.response_id(&response).unwrap();
            assert_eq!(tracked.id, "request-1");
            assert_eq!(tracked.kind, TrackedKind::Operation);
            assert_eq!(
                policy.event_id(stream, &json!({ "data": { "request_id": "request-1" } })),
                Some("request-1".into())
            );
            assert!(policy.is_terminal(stream, &json!({ "event": "completed" })));
        }
    }
}
