use anyhow::Result;
use shelllist_daemon_core::DaemonEndpoint;
use shelllist_daemon_tokio::{
    BasicCorrelation, CallFailure, CancelMode, JsonlClientConfig, run_jsonl_client,
};

use crate::{
    api,
    daemon::{BUS_NAME, INTERFACE, OBJECT_PATH},
};

const ENDPOINT: DaemonEndpoint = DaemonEndpoint::new("bt-daemon", BUS_NAME, OBJECT_PATH, INTERFACE);

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
        correlation: BasicCorrelation,
        cancel_mode: CancelMode::Json,
        call_failure,
        pending_event_limit: 32,
        max_in_flight_requests: 64,
        shutdown_timeout: Some(std::time::Duration::from_secs(5)),
    })
    .await
}
