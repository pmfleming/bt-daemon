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
    snapshot: Option<Arc<crate::model::Snapshot>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<Value>,
}

struct ScanTask {
    handle: JoinHandle<()>,
    event: ScanEvent,
}

enum ScanRequest {
    Start {
        adapter_key: Option<String>,
        timeout_ms: u64,
    },
    Stop(Option<String>),
}

impl ScanRequest {
    fn parse(params: &Value) -> anyhow::Result<Self> {
        let enabled = match params.get("enabled") {
            None => true,
            Some(Value::Bool(enabled)) => *enabled,
            Some(_) => anyhow::bail!("invalid optional boolean parameter 'enabled'"),
        };
        if !enabled {
            return Ok(Self::Stop(
                params.optional_string("request_id")?.map(str::to_string),
            ));
        }
        let adapter_key = params.optional_string("adapter_key")?.map(str::to_string);
        let timeout_ms = match params.get("timeout_ms") {
            None => 15_000,
            Some(Value::Number(value)) if value.as_u64().is_some() => {
                value.as_u64().unwrap_or_default().clamp(1_000, 60_000)
            }
            Some(_) => anyhow::bail!("invalid optional unsigned integer parameter 'timeout_ms'"),
        };
        Ok(Self::Start {
            adapter_key,
            timeout_ms,
        })
    }
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
        let (adapter_key, timeout_ms) = match ScanRequest::parse(params) {
            Ok(ScanRequest::Start {
                adapter_key,
                timeout_ms,
            }) => (adapter_key, timeout_ms),
            Ok(ScanRequest::Stop(request_id)) => {
                return self.stop(request_id.as_deref(), "cancelled").await;
            }
            Err(error) => return api::error("validation-error", error.to_string()),
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
        let snapshot = Arc::new(snapshot);
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
            snapshot: Some(Arc::clone(&snapshot)),
            error: None,
        };
        let handle = self.spawn_timeout(request_id.clone(), adapter_key, timeout_ms);
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

    fn spawn_timeout(
        &self,
        task_id: String,
        task_adapter: Option<String>,
        timeout_ms: u64,
    ) -> JoinHandle<()> {
        let tasks = Arc::clone(&self.tasks);
        let backend = Arc::clone(&self.backend);
        let transition = Arc::clone(&self.transition);
        let events = self.events.clone();
        crate::task::spawn("scan-timeout", async move {
            tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
            let _transition = transition.lock().await;
            let adapters = removable_adapters(&tasks, &task_id).await;
            let result = crate::task::catch(
                "Bluetooth scan completion",
                stop_adapters(&backend, &adapters),
            )
            .await
            .and_then(|result| result);
            let terminal = terminal_event(task_id, task_adapter, result);
            let _ = events.send(terminal);
        })
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
            Ok(snapshot) => Some(Arc::new(snapshot)),
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
            terminal.snapshot = last_snapshot.as_ref().map(Arc::clone);
            let _ = self.events.send(terminal);
        }
        api::success(json!({ "stopped": request_id, "snapshot": last_snapshot }))
    }

    #[cfg(test)]
    pub(super) async fn is_empty(&self) -> bool {
        self.tasks.lock().await.is_empty()
    }
}

async fn removable_adapters(
    tasks: &Mutex<HashMap<String, ScanTask>>,
    request_id: &str,
) -> Vec<String> {
    let mut active = tasks.lock().await;
    active.remove(request_id).map_or_else(Vec::new, |finished| {
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
}

fn terminal_event(
    request_id: String,
    adapter_key: Option<String>,
    result: anyhow::Result<crate::model::Snapshot>,
) -> ScanEvent {
    let (snapshot, error, state) = match result {
        Ok(snapshot) => {
            tracing::info!(%request_id, "Bluetooth scan completed");
            (Some(Arc::new(snapshot)), None, "completed")
        }
        Err(error) => {
            tracing::warn!(%request_id, error = %error, error_chain = %format!("{error:#}"), "Bluetooth scan completion failed");
            (None, Some(api::error_value(&error)), "failed")
        }
    };
    ScanEvent {
        event: state.into(),
        request_id,
        adapter_key,
        adapter_keys: Vec::new(),
        state: state.into(),
        timeout_ms: None,
        snapshot,
        error,
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
