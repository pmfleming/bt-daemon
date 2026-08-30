use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use serde::Serialize;
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, broadcast, oneshot},
    task::JoinHandle,
};

use crate::{
    api,
    backend::{BluetoothBackend, DeviceOperation, Params},
};

#[derive(Clone, Serialize)]
pub(super) struct OperationEvent {
    pub(super) event: String,
    pub(super) request_id: String,
    device_key: String,
    operation: String,
    state: String,
    pub(super) stage: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot: Option<crate::model::Snapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<Value>,
}

impl OperationEvent {
    fn queued(request_id: String, device_key: String, operation: String) -> Self {
        Self {
            event: "queued".into(),
            request_id,
            device_key,
            operation,
            state: "queued".into(),
            stage: "queued".into(),
            snapshot: None,
            error: None,
        }
    }

    fn with_state(&self, event: &str, state: &str) -> Self {
        Self {
            event: event.into(),
            state: state.into(),
            stage: state.into(),
            ..self.clone()
        }
    }

    fn progress(&self, stage: &str) -> Self {
        Self {
            event: "progress".into(),
            state: "running".into(),
            stage: stage.into(),
            ..self.clone()
        }
    }

    fn finished(mut self, result: anyhow::Result<crate::model::Snapshot>) -> Self {
        match result {
            Ok(snapshot) => self.snapshot = Some(snapshot),
            Err(error) => self.error = Some(api::error_value(&error)),
        }
        self.event = if self.error.is_some() {
            "failed"
        } else {
            "completed"
        }
        .into();
        self.state.clone_from(&self.event);
        self.stage.clone_from(&self.event);
        self
    }
}

struct OperationTask {
    handle: JoinHandle<()>,
    event: OperationEvent,
    owner: Option<String>,
}

#[derive(Default)]
struct OperationState {
    tasks: HashMap<String, OperationTask>,
    active_devices: HashMap<String, String>,
}

pub(super) struct OperationCoordinator {
    backend: Arc<dyn BluetoothBackend>,
    sequence: AtomicU64,
    state: Arc<Mutex<OperationState>>,
    events: broadcast::Sender<OperationEvent>,
    recent: Arc<Mutex<VecDeque<OperationEvent>>>,
}

impl OperationCoordinator {
    pub(super) fn new(backend: Arc<dyn BluetoothBackend>) -> Self {
        let (events, _) = broadcast::channel(32);
        Self {
            backend,
            sequence: AtomicU64::new(1),
            state: Arc::new(Mutex::new(OperationState::default())),
            events,
            recent: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub(super) fn subscribe(&self) -> broadcast::Receiver<OperationEvent> {
        self.events.subscribe()
    }

    pub(super) async fn snapshot(&self) -> Value {
        let active = self
            .state
            .lock()
            .await
            .tasks
            .values()
            .map(|task| task.event.clone())
            .collect::<Vec<_>>();
        let recent = self.recent.lock().await.iter().cloned().collect::<Vec<_>>();
        json!({ "active": active, "recent": recent })
    }

    #[cfg(test)]
    pub(super) async fn start(&self, params: Value) -> Value {
        self.start_owned(params, None).await
    }

    pub(super) async fn start_owned(&self, params: Value, owner: Option<String>) -> Value {
        let (device_key, operation) = match operation_request(&params) {
            Ok(request) => request,
            Err(error) => return error,
        };

        let mut state = self.state.lock().await;
        if let Some(request_id) = state.active_devices.get(&device_key) {
            tracing::warn!(%device_key, %request_id, "Bluetooth device operation rejected because the device is busy");
            return api::error(
                "device-busy",
                format!("Device already has active operation {request_id}"),
            );
        }
        let request_id = format!(
            "operation-{}",
            self.sequence.fetch_add(1, Ordering::Relaxed)
        );
        let queued = OperationEvent::queued(
            request_id.clone(),
            device_key.clone(),
            operation.to_string(),
        );
        tracing::info!(%request_id, %device_key, %operation, "Bluetooth device operation queued");
        let execution = OperationExecution {
            backend: Arc::clone(&self.backend),
            state: Arc::clone(&self.state),
            events: self.events.clone(),
            recent: Arc::clone(&self.recent),
        };
        let task_event = queued.clone();
        let (start_sender, start_receiver) = oneshot::channel();
        let handle = crate::task::spawn("device-operation", async move {
            execution
                .run(task_event, operation, params, start_receiver)
                .await;
        });
        state.tasks.insert(
            request_id.clone(),
            OperationTask {
                handle,
                event: queued.clone(),
                owner,
            },
        );
        state.active_devices.insert(device_key, request_id);
        drop(state);
        let _ = start_sender.send(());
        api::success(json!({ "operation": queued }))
    }

    pub(super) async fn cancel_owned(&self, request_id: &str, owner: Option<&str>) -> bool {
        let task = {
            let mut state = self.state.lock().await;
            if state
                .tasks
                .get(request_id)
                .is_none_or(|task| task.owner.as_deref() != owner)
            {
                return false;
            }
            let task = state.tasks.remove(request_id);
            if let Some(task) = &task
                && state
                    .active_devices
                    .get(&task.event.device_key)
                    .is_some_and(|active| active == request_id)
            {
                state.active_devices.remove(&task.event.device_key);
            }
            task
        };
        let Some(task) = task else {
            return false;
        };
        task.handle.abort();
        tracing::info!(%request_id, "Bluetooth device operation cancelled");
        let terminal = task.event.with_state("cancelled", "cancelled");
        retain_terminal(&self.recent, terminal.clone()).await;
        let _ = self.events.send(terminal);
        true
    }

    #[cfg(test)]
    pub(super) async fn is_empty(&self) -> bool {
        self.state.lock().await.tasks.is_empty()
    }
}

fn operation_request(params: &Value) -> Result<(String, DeviceOperation), Value> {
    let (key, operation) = params
        .require_strings("key", "operation")
        .map_err(|error| api::error("validation-error", error.to_string()))?;
    let operation = DeviceOperation::try_from(operation)
        .map_err(|error| api::error("validation-error", error.to_string()))?;
    Ok((key.to_string(), operation))
}

struct OperationExecution {
    backend: Arc<dyn BluetoothBackend>,
    state: Arc<Mutex<OperationState>>,
    events: broadcast::Sender<OperationEvent>,
    recent: Arc<Mutex<VecDeque<OperationEvent>>>,
}

impl OperationExecution {
    async fn run(
        self,
        event: OperationEvent,
        operation: DeviceOperation,
        params: Value,
        start: oneshot::Receiver<()>,
    ) {
        if start.await.is_err() {
            return;
        }
        tracing::info!(request_id = %event.request_id, device_key = %event.device_key, %operation, "Bluetooth device operation started");
        self.publish_started(&event).await;
        let progress = self.progress_reporter(&event);
        let result = crate::task::catch(
            "Bluetooth backend device operation",
            self.backend
                .device_operation(&event.device_key, operation, &params, progress),
        )
        .await;
        log_operation_result(&event.request_id, &result);
        let terminal = event.clone().finished(result);
        self.remove_active(&event).await;
        retain_terminal(&self.recent, terminal.clone()).await;
        let _ = self.events.send(terminal);
    }

    async fn publish_started(&self, event: &OperationEvent) {
        let started = event.with_state("started", "running");
        if let Some(task) = self.state.lock().await.tasks.get_mut(&event.request_id) {
            task.event = started.clone();
        }
        let _ = self.events.send(started);
    }

    fn progress_reporter(&self, event: &OperationEvent) -> Arc<dyn Fn(&'static str) + Send + Sync> {
        let events = self.events.clone();
        let event = event.clone();
        let state = Arc::clone(&self.state);
        Arc::new(move |stage| {
            let update = event.progress(stage);
            if let Ok(mut state) = state.try_lock()
                && let Some(task) = state.tasks.get_mut(&event.request_id)
            {
                task.event = update.clone();
            }
            let _ = events.send(update);
        })
    }

    async fn remove_active(&self, event: &OperationEvent) {
        let mut state = self.state.lock().await;
        state.tasks.remove(&event.request_id);
        if state.active_devices.get(&event.device_key) == Some(&event.request_id) {
            state.active_devices.remove(&event.device_key);
        }
    }
}

fn log_operation_result(request_id: &str, result: &anyhow::Result<crate::model::Snapshot>) {
    match result {
        Ok(_) => tracing::info!(%request_id, "Bluetooth device operation completed"),
        Err(error) => {
            tracing::warn!(%request_id, %error, error_chain = %format!("{error:#}"), "Bluetooth device operation failed")
        }
    }
}

async fn retain_terminal(recent: &Mutex<VecDeque<OperationEvent>>, event: OperationEvent) {
    let mut recent = recent.lock().await;
    recent.push_back(event);
    while recent.len() > 64 {
        recent.pop_front();
    }
}
