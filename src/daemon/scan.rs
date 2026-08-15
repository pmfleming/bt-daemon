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

use crate::backend::Params;
use crate::{api, backend::BluetoothBackend};

#[derive(Clone, Serialize)]
pub(super) struct ScanEvent {
    pub(super) event: String,
    pub(super) request_id: String,
    adapter_key: Option<String>,
    #[serde(skip)]
    adapter_keys: Vec<String>,
    #[serde(skip)]
    owner: String,
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

#[derive(Clone)]
pub(super) struct ScanCoordinator {
    backend: Arc<dyn BluetoothBackend>,
    sequence: Arc<AtomicU64>,
    tasks: Arc<Mutex<HashMap<String, ScanTask>>>,
    transition: Arc<Mutex<()>>,
    events: broadcast::Sender<ScanEvent>,
}

impl ScanCoordinator {
    pub(super) fn new(backend: Arc<dyn BluetoothBackend>) -> Self {
        let (events, _) = broadcast::channel(32);
        Self {
            backend,
            sequence: Arc::new(AtomicU64::new(1)),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            transition: Arc::new(Mutex::new(())),
            events,
        }
    }

    pub(super) fn subscribe(&self) -> broadcast::Receiver<ScanEvent> {
        self.events.subscribe()
    }

    pub(super) async fn snapshot(&self) -> Value {
        let active = self
            .tasks
            .lock()
            .await
            .values()
            .map(|task| task.event.clone())
            .collect::<Vec<_>>();
        json!({ "active": active })
    }

    pub(super) async fn contains(&self, request_id: &str) -> bool {
        self.tasks.lock().await.contains_key(request_id)
    }

    pub(super) async fn start(&self, params: &Value, owner: &str) -> Value {
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
        let adapter_keys = powered_adapter_keys(&snapshot, adapter_key.as_deref());
        if adapter_keys.is_empty() {
            tracing::warn!(
                ?adapter_key,
                "Bluetooth scan did not enter discovery on a powered adapter"
            );
            if let Err(error) = self
                .backend
                .set_scanning(adapter_key.as_deref(), false)
                .await
            {
                tracing::warn!(%error, "could not roll back an unconfirmed Bluetooth scan start");
            }
            return api::error(
                "scan-start-failed",
                "Bluetooth discovery did not start on a powered adapter".to_string(),
            );
        }
        let request_id = format!("scan-{}", self.sequence.fetch_add(1, Ordering::Relaxed));
        tracing::info!(%request_id, ?adapter_key, timeout_ms, "Bluetooth scan started");
        let running = ScanEvent {
            event: "started".into(),
            request_id: request_id.clone(),
            adapter_key: adapter_key.clone(),
            adapter_keys,
            owner: owner.to_string(),
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
            let mut failure_emitted = false;
            loop {
                let _transition = transition.lock().await;
                let Some(adapters) = removable_adapters(&tasks, &task_id).await else {
                    return;
                };
                let result = crate::task::catch(
                    "Bluetooth scan completion",
                    stop_adapters(&backend, &adapters),
                )
                .await
                .and_then(|result| result);
                match result {
                    Ok(snapshot) => {
                        tasks.lock().await.remove(&task_id);
                        if failure_emitted {
                            tracing::info!(%task_id, "Bluetooth scan cleanup eventually succeeded");
                        } else {
                            let terminal = terminal_event(task_id, task_adapter, Ok(snapshot));
                            let _ = events.send(terminal);
                        }
                        return;
                    }
                    Err(error) => {
                        if !failure_emitted {
                            let terminal =
                                terminal_event(task_id.clone(), task_adapter.clone(), Err(error));
                            let _ = events.send(terminal);
                            failure_emitted = true;
                        } else {
                            tracing::warn!(%task_id, "Bluetooth scan cleanup is retrying");
                        }
                    }
                }
                drop(_transition);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        })
    }

    pub(super) async fn stop_owner(&self, owner: &str) {
        let request_ids = self
            .tasks
            .lock()
            .await
            .iter()
            .filter(|(_, task)| task.event.owner == owner)
            .map(|(request_id, _)| request_id.clone())
            .collect::<Vec<_>>();
        for request_id in request_ids {
            let response = self.stop(Some(&request_id), "cancelled").await;
            if response["ok"].as_bool() != Some(true) {
                tracing::warn!(%owner, %request_id, "could not release scan after its D-Bus owner disappeared");
            }
        }
    }

    pub(super) async fn stop(&self, request_id: Option<&str>, event: &str) -> Value {
        let _transition = self.transition.lock().await;
        let (selected_ids, adapters_to_stop) = {
            let tasks = self.tasks.lock().await;
            let selected_ids = match request_id {
                Some(request_id) if tasks.contains_key(request_id) => vec![request_id.to_string()],
                Some(_) => Vec::new(),
                None => tasks.keys().cloned().collect::<Vec<_>>(),
            };
            let selected = selected_ids
                .iter()
                .cloned()
                .collect::<std::collections::HashSet<_>>();
            let active_adapters = tasks
                .iter()
                .filter(|(id, _)| !selected.contains(*id))
                .flat_map(|(_, task)| task.event.adapter_keys.iter().cloned())
                .collect::<std::collections::HashSet<_>>();
            let adapters = selected_ids
                .iter()
                .filter_map(|id| tasks.get(id))
                .flat_map(|task| task.event.adapter_keys.iter().cloned())
                .filter(|adapter| !active_adapters.contains(adapter))
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            (selected_ids, adapters)
        };
        if selected_ids.is_empty() {
            tracing::warn!(
                ?request_id,
                "Bluetooth scan stop did not match an active scan"
            );
            return api::error(
                "request-not-found",
                "No matching scan is active".to_string(),
            );
        }
        tracing::info!(?request_id, count = selected_ids.len(), %event, "stopping Bluetooth scan sessions");
        let last_snapshot = match stop_adapters(&self.backend, &adapters_to_stop).await {
            Ok(snapshot) => Arc::new(snapshot),
            Err(error) => {
                tracing::warn!(%error, error_chain = %format!("{error:#}"), "could not stop Bluetooth discovery");
                return api::error("scan-stop-failed", format!("{error:#}"));
            }
        };
        let removed = {
            let mut tasks = self.tasks.lock().await;
            selected_ids
                .iter()
                .filter_map(|id| tasks.remove(id))
                .collect::<Vec<_>>()
        };
        for task in removed {
            task.handle.abort();
            let mut terminal = task.event;
            terminal.event = event.into();
            terminal.state = event.into();
            terminal.timeout_ms = None;
            terminal.snapshot = Some(Arc::clone(&last_snapshot));
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
) -> Option<Vec<String>> {
    let active = tasks.lock().await;
    let finished = active.get(request_id)?;
    Some(
        finished
            .event
            .adapter_keys
            .iter()
            .filter(|adapter| {
                !active
                    .iter()
                    .any(|(id, task)| id != request_id && task.event.adapter_keys.contains(adapter))
            })
            .cloned()
            .collect(),
    )
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
        owner: String::new(),
        state: state.into(),
        timeout_ms: None,
        snapshot,
        error,
    }
}

fn powered_adapter_keys(snapshot: &crate::model::Snapshot, requested: Option<&str>) -> Vec<String> {
    snapshot
        .adapters
        .iter()
        .filter(|adapter| {
            adapter.powered && adapter.discovering && requested.is_none_or(|key| adapter.key == key)
        })
        .map(|adapter| adapter.key.clone())
        .collect()
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

#[cfg(test)]
mod tests {
    use crate::model::{Adapter, Snapshot};

    use super::powered_adapter_keys;

    fn adapter(key: &str, powered: bool, discovering: bool) -> Adapter {
        Adapter {
            key: key.into(),
            powered,
            discovering,
            ..Adapter::default()
        }
    }

    #[test]
    fn scans_require_a_matching_powered_adapter() {
        let snapshot = Snapshot {
            adapters: vec![
                adapter("adapter-off", false, false),
                adapter("adapter-idle", true, false),
                adapter("adapter-on", true, true),
            ],
            ..Snapshot::default()
        };
        assert_eq!(powered_adapter_keys(&snapshot, None), vec!["adapter-on"]);
        assert!(powered_adapter_keys(&snapshot, Some("adapter-off")).is_empty());
        assert!(powered_adapter_keys(&snapshot, Some("adapter-idle")).is_empty());
        assert!(powered_adapter_keys(&snapshot, Some("adapter-missing")).is_empty());
        assert_eq!(
            powered_adapter_keys(&snapshot, Some("adapter-on")),
            vec!["adapter-on"]
        );
        assert!(powered_adapter_keys(&Snapshot::default(), None).is_empty());
    }
}
