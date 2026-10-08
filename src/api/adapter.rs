//! Submitted adapter patches: deterministic, stop-on-error, never atomic or replayed.
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};

use crate::{
    backend::{AdapterOperation, BluetoothBackend, Params},
    model::Snapshot,
};

const FIELDS: &[(&str, AdapterOperation)] = &[
    ("alias", AdapterOperation::SetAlias),
    (
        "discoverable_timeout",
        AdapterOperation::SetDiscoverableTimeout,
    ),
    ("pairable_timeout", AdapterOperation::SetPairableTimeout),
];

struct Setting {
    field: &'static str,
    operation: AdapterOperation,
    value: Value,
    params: Value,
}

fn parse(params: &Value) -> Result<(String, Vec<Setting>)> {
    let key = params.require_string("key")?.to_owned();
    let object = params
        .as_object()
        .context("expected adapter update object")?;
    ensure!(
        object
            .keys()
            .all(|key| matches!(key.as_str(), "key" | "changes")),
        "unknown adapter update parameter"
    );
    let changes = params.get("changes").context("missing adapter changes")?;
    let fields = changes
        .as_object()
        .context("expected adapter changes object")?;
    ensure!(!fields.is_empty(), "adapter changes must not be empty");
    ensure!(
        fields
            .keys()
            .all(|key| FIELDS.iter().any(|(field, _)| field == key)),
        "unknown adapter setting"
    );
    let mut settings = Vec::new();
    for &(field, operation) in FIELDS {
        if fields.contains_key(field) {
            let (value, params) = if operation == AdapterOperation::SetAlias {
                let value = changes.require_string(field)?;
                (json!(value), json!({"alias": value}))
            } else {
                let value = changes.require_u32(field)?;
                (json!(value), json!({"timeout": value}))
            };
            settings.push(Setting {
                field,
                operation,
                value,
                params,
            });
        }
    }
    Ok((key, settings))
}

pub(super) async fn submit(backend: Arc<dyn BluetoothBackend>, params: Value) -> Value {
    // Validate the entire patch before admitting any effects, including later fields.
    let (key, settings) = match parse(&params) {
        Ok(parsed) => parsed,
        Err(error) => return super::validation_error(error),
    };
    // Dropping a transport waiter must not interrupt the sequence or release its
    // gate while an already-dispatched BlueZ setter may still be taking effect.
    match tokio::spawn(async move {
        let gate = crate::task::adapter_gate(&key);
        let _exclusive = gate.lock().await;
        let mut stopped = false;
        let mut outcomes = Vec::new();
        for setting in settings {
            let mut outcome =
                json!({"field": setting.field, "value": setting.value, "state": "not-attempted"});
            if !stopped {
                match backend
                    .adapter_operation(&key, setting.operation, &setting.params)
                    .await
                {
                    Ok(()) => outcome["state"] = json!("applied"),
                    Err(error) => {
                        tracing::warn!(adapter_key = %key, field = setting.field, %error, "adapter batch stopped with an uncertain write outcome");
                        // A timeout/disconnection can happen after dispatch. Even
                        // an early backend failure is conservatively uncertain;
                        // we cannot infer that a setter had no effect from Err.
                        outcome["state"] = json!("unknown");
                        outcome["error"] = super::error_value(&error);
                        stopped = true;
                    }
                }
            }
            outcomes.push(outcome);
        }
        let mut data = json!({"adapter_batch": {"key": key, "outcomes": outcomes}});
        match backend.snapshot().await {
            Ok(snapshot) => data["snapshot"] = json!(snapshot),
            Err(error) => data["adapter_batch"]["snapshot_error"] = super::error_value(&error),
        }
        super::success(data)
    })
    .await
    {
        Ok(response) => response,
        Err(error) => super::error(
            "adapter-outcome-unknown",
            format!(
                "Adapter worker ended without an outcome; inspect settings before retrying: {error}"
            ),
        ),
    }
}

/// Keep the legacy one-setting response shape, but serialize it against patches
/// and retain ownership of its gate even if the caller stops awaiting the reply.
pub(super) async fn single(
    backend: Arc<dyn BluetoothBackend>,
    key: String,
    operation: AdapterOperation,
    params: Value,
) -> Result<Snapshot> {
    tokio::spawn(async move {
        let gate = crate::task::adapter_gate(&key);
        let _exclusive = gate.lock().await;
        backend.adapter_operation(&key, operation, &params).await?;
        backend.snapshot().await
    })
    .await
    .context("adapter operation outcome unknown")?
}
