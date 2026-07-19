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

use crate::{api, backend::BluetoothBackend};

#[derive(Clone, Serialize)]
pub(super) struct ScanEvent {
    pub(super) event: String,
    pub(super) request_id: String,
    adapter_key: Option<String>,
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
    events: broadcast::Sender<ScanEvent>,
}

impl ScanCoordinator {
    pub(super) fn new(backend: Arc<dyn BluetoothBackend>) -> Self {
        let (events, _) = broadcast::channel(32);
        Self {
            backend,
            sequence: AtomicU64::new(1),
            tasks: Arc::new(Mutex::new(HashMap::new())),
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
        let enabled = params
            .get("enabled")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        if !enabled {
            return self
                .stop(
                    params.get("request_id").and_then(Value::as_str),
                    "cancelled",
                )
                .await;
        }
        let adapter_key = params
            .get("adapter_key")
            .and_then(Value::as_str)
            .map(str::to_string);
        let timeout_ms = params
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(15_000)
            .clamp(1_000, 60_000);
        let snapshot = match self
            .backend
            .set_scanning(adapter_key.as_deref(), true)
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => return api::error("scan-start-failed", format!("{error:#}")),
        };
        let request_id = format!("scan-{}", self.sequence.fetch_add(1, Ordering::Relaxed));
        let running = ScanEvent {
            event: "started".into(),
            request_id: request_id.clone(),
            adapter_key: adapter_key.clone(),
            state: "running".into(),
            timeout_ms: Some(timeout_ms),
            snapshot: Some(snapshot.clone()),
            error: None,
        };
        let tasks = Arc::clone(&self.tasks);
        let backend = Arc::clone(&self.backend);
        let events = self.events.clone();
        let task_id = request_id.clone();
        let task_adapter = adapter_key.clone();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
            let should_stop = {
                let mut active = tasks.lock().await;
                active.remove(&task_id);
                !active.values().any(|task| {
                    task.event.adapter_key.is_none()
                        || task_adapter.is_none()
                        || task.event.adapter_key == task_adapter
                })
            };
            let result = if should_stop {
                backend.set_scanning(task_adapter.as_deref(), false).await
            } else {
                backend.snapshot().await
            };
            let (snapshot, error, state) = match result {
                Ok(snapshot) => (Some(snapshot), None, "completed"),
                Err(error) => (None, Some(api::error_value(&error)), "failed"),
            };
            let _ = events.send(ScanEvent {
                event: state.into(),
                request_id: task_id,
                adapter_key: task_adapter,
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
        let removed = {
            let mut tasks = self.tasks.lock().await;
            if let Some(request_id) = request_id {
                tasks.remove(request_id).into_iter().collect::<Vec<_>>()
            } else {
                tasks.drain().map(|(_, task)| task).collect::<Vec<_>>()
            }
        };
        if removed.is_empty() {
            return api::error(
                "request-not-found",
                "No matching scan is active".to_string(),
            );
        }
        let mut last_snapshot = None;
        for task in removed {
            task.handle.abort();
            let adapter_key = task.event.adapter_key.clone();
            let should_stop = {
                let tasks = self.tasks.lock().await;
                !tasks.values().any(|active| {
                    active.event.adapter_key.is_none()
                        || adapter_key.is_none()
                        || active.event.adapter_key == adapter_key
                })
            };
            if should_stop {
                match self
                    .backend
                    .set_scanning(adapter_key.as_deref(), false)
                    .await
                {
                    Ok(snapshot) => last_snapshot = Some(snapshot),
                    Err(error) => tracing::warn!(%error, "could not stop Bluetooth discovery"),
                }
            }
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
