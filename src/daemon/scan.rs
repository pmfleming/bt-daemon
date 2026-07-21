use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use serde::Serialize;
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, broadcast},
    task::JoinHandle,
};

use crate::{api, backend::BluetoothBackend, params::Params};

#[derive(Clone, Serialize)]
pub(super) struct ScanEvent {
    pub(super) event: String,
    pub(super) request_id: String,
    adapter_key: Option<String>,
    #[serde(skip)]
    adapter_keys: Vec<String>,
    pub(super) state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot: Option<crate::model::Snapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<Value>,
}

struct ScanTask {
    handle: JoinHandle<()>,
    event: ScanEvent,
}

pub(super) struct ScanCoordinator {
    backend: Arc<dyn BluetoothBackend>,
    sequence: AtomicU64,
    tasks: Arc<Mutex<HashMap<String, ScanTask>>>,
    transition: Arc<Mutex<()>>,
    events: broadcast::Sender<ScanEvent>,
}

impl ScanCoordinator {
    pub(super) fn new(backend: Arc<dyn BluetoothBackend>) -> Self {
        let (events, _) = broadcast::channel(32);
        Self {
            backend,
            sequence: AtomicU64::new(1),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            transition: Arc::new(Mutex::new(())),
            events,
        }
    }

    pub(super) fn subscribe(&self) -> broadcast::Receiver<ScanEvent> {
        self.events.subscribe()
    }

    pub(super) async fn contains(&self, request_id: &str) -> bool {
        self.tasks.lock().await.contains_key(request_id)
    }

    pub(super) async fn start(&self, params: &Value) -> Value {
        let enabled = match params.get("enabled") {
            None => true,
            Some(Value::Bool(enabled)) => *enabled,
            Some(_) => {
                return api::error(
                    "validation-error",
                    "invalid optional boolean parameter 'enabled'".to_string(),
                );
            }
        };
        if !enabled {
            return self
                .stop(
                    params.get("request_id").and_then(Value::as_str),
                    "cancelled",
                )
                .await;
        }
        let adapter_key = match params.optional_string("adapter_key") {
            Ok(key) => key.map(str::to_string),
            Err(error) => return api::error("validation-error", error.to_string()),
        };
        let timeout_ms = match params.get("timeout_ms") {
            None => 15_000,
            Some(Value::Number(value)) if value.as_u64().is_some() => {
                value.as_u64().unwrap_or_default().clamp(1_000, 60_000)
            }
            Some(_) => {
                return api::error(
                    "validation-error",
                    "invalid optional unsigned integer parameter 'timeout_ms'".to_string(),
                );
            }
        };
        tracing::info!(?adapter_key, timeout_ms, "Bluetooth scan requested");
        let _transition = self.transition.lock().await;
        let snapshot = match self
            .backend
            .set_scanning(adapter_key.as_deref(), true)
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::warn!(?adapter_key, error = %error, error_chain = %format!("{error:#}"), "Bluetooth scan failed to start");
                return api::error("scan-start-failed", format!("{error:#}"));
            }
        };
        let adapter_keys = adapter_key.clone().map_or_else(
            || {
                snapshot
                    .adapters
                    .iter()
                    .map(|adapter| adapter.key.clone())
                    .collect()
            },
            |key| vec![key],
        );
        let request_id = format!("scan-{}", self.sequence.fetch_add(1, Ordering::Relaxed));
        tracing::info!(%request_id, ?adapter_key, timeout_ms, "Bluetooth scan started");
        let running = ScanEvent {
            event: "started".into(),
            request_id: request_id.clone(),
            adapter_key: adapter_key.clone(),
            adapter_keys,
            state: "running".into(),
            timeout_ms: Some(timeout_ms),
            snapshot: Some(snapshot.clone()),
            error: None,
        };
        let tasks = Arc::clone(&self.tasks);
        let backend = Arc::clone(&self.backend);
        let transition = Arc::clone(&self.transition);
        let events = self.events.clone();
        let task_id = request_id.clone();
        let task_adapter = adapter_key.clone();
        let handle = crate::task::spawn("scan-timeout", async move {
            tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
            let _transition = transition.lock().await;
            let adapters_to_stop = {
                let mut active = tasks.lock().await;
                let finished = active.remove(&task_id);
                finished.map_or_else(Vec::new, |finished| {
                    finished
                        .event
                        .adapter_keys
                        .into_iter()
                        .filter(|adapter| {
                            !active
                                .values()
                                .any(|task| task.event.adapter_keys.contains(adapter))
                        })
                        .collect()
                })
            };
            let result = crate::task::catch(
                "Bluetooth scan completion",
                stop_adapters(&backend, &adapters_to_stop),
            )
            .await
            .and_then(|result| result);
            let (snapshot, error, state) = match result {
                Ok(snapshot) => {
                    tracing::info!(request_id = %task_id, "Bluetooth scan completed");
                    (Some(snapshot), None, "completed")
                }
                Err(error) => {
                    tracing::warn!(request_id = %task_id, error = %error, error_chain = %format!("{error:#}"), "Bluetooth scan completion failed");
                    (None, Some(api::error_value(&error)), "failed")
                }
            };
            let _ = events.send(ScanEvent {
                event: state.into(),
                request_id: task_id,
                adapter_key: task_adapter,
                adapter_keys: Vec::new(),
                state: state.into(),
                timeout_ms: None,
                snapshot,
                error,
            });
        });
        self.tasks.lock().await.insert(
            request_id,
            ScanTask {
                handle,
                event: running.clone(),
            },
        );
        let _ = self.events.send(running.clone());
        api::success(json!({ "scan": running, "snapshot": snapshot }))
    }

    pub(super) async fn stop(&self, request_id: Option<&str>, event: &str) -> Value {
        let _transition = self.transition.lock().await;
        let removed = {
            let mut tasks = self.tasks.lock().await;
            if let Some(request_id) = request_id {
                tasks.remove(request_id).into_iter().collect::<Vec<_>>()
            } else {
                tasks.drain().map(|(_, task)| task).collect::<Vec<_>>()
            }
        };
        if removed.is_empty() {
            tracing::warn!(
                ?request_id,
                "Bluetooth scan stop did not match an active scan"
            );
            return api::error(
                "request-not-found",
                "No matching scan is active".to_string(),
            );
        }
        tracing::info!(?request_id, count = removed.len(), %event, "stopping Bluetooth scan sessions");
        let active_adapters = {
            let tasks = self.tasks.lock().await;
            tasks
                .values()
                .flat_map(|task| task.event.adapter_keys.iter().cloned())
                .collect::<std::collections::HashSet<_>>()
        };
        let adapters_to_stop = removed
            .iter()
            .flat_map(|task| task.event.adapter_keys.iter().cloned())
            .filter(|adapter| !active_adapters.contains(adapter))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        for task in &removed {
            task.handle.abort();
        }
        let last_snapshot = match stop_adapters(&self.backend, &adapters_to_stop).await {
            Ok(snapshot) => Some(snapshot),
            Err(error) => {
                tracing::warn!(%error, "could not stop Bluetooth discovery");
                None
            }
        };
        for task in removed {
            let mut terminal = task.event;
            terminal.event = event.into();
            terminal.state = event.into();
            terminal.timeout_ms = None;
            terminal.snapshot = last_snapshot.clone();
            let _ = self.events.send(terminal);
        }
        api::success(json!({ "stopped": request_id, "snapshot": last_snapshot }))
    }

    #[cfg(test)]
    pub(super) async fn is_empty(&self) -> bool {
        self.tasks.lock().await.is_empty()
    }
}

async fn stop_adapters(
    backend: &Arc<dyn BluetoothBackend>,
    adapter_keys: &[String],
) -> anyhow::Result<crate::model::Snapshot> {
    let mut snapshot = None;
    let mut first_error = None;
    for adapter_key in adapter_keys {
        match backend.set_scanning(Some(adapter_key), false).await {
            Ok(current) => snapshot = Some(current),
            Err(error) if first_error.is_none() => first_error = Some(error),
            Err(_) => {}
        }
    }
    if let Some(error) = first_error {
        Err(error)
    } else {
        match snapshot {
            Some(snapshot) => Ok(snapshot),
            None => backend.snapshot().await,
        }
    }
}
