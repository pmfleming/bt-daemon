use anyhow::Result;
use serde_json::Value;
use shelllist_daemon_core::DaemonEndpoint;
use shelllist_daemon_tokio::{BasicCorrelation, CancelMode, JsonlClientConfig, run_jsonl_client};

use crate::{
    api,
    daemon::{BUS_NAME, INTERFACE, OBJECT_PATH},
};

const ENDPOINT: DaemonEndpoint = DaemonEndpoint::new("bt-daemon", BUS_NAME, OBJECT_PATH, INTERFACE);

fn call_failure(method: &str, error: &anyhow::Error) -> Value {
    tracing::warn!(%method, error = %error, error_chain = %format!("{error:#}"), "client call to daemon failed");
    api::error(
        "daemon-unavailable",
        "bt-daemon session service is unavailable".to_string(),
    )
}

pub async fn run() -> Result<()> {
    tracing::info!("JSON-lines client started");
    run_jsonl_client(JsonlClientConfig {
        endpoint: ENDPOINT,
        correlation: BasicCorrelation,
        cancel_mode: CancelMode::Json,
        call_failure,
    })
    .await
}
