use std::{
    collections::HashMap,
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
            snapshot: None,
            error: None,
        }
    }

    fn with_state(&self, event: &str, state: &str) -> Self {
        Self {
            event: event.into(),
            state: state.into(),
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
        self
    }
}

struct OperationTask {
    handle: JoinHandle<()>,
    event: OperationEvent,
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
}

impl OperationCoordinator {
    pub(super) fn new(backend: Arc<dyn BluetoothBackend>) -> Self {
        let (events, _) = broadcast::channel(32);
        Self {
            backend,
            sequence: AtomicU64::new(1),
            state: Arc::new(Mutex::new(OperationState::default())),
            events,
        }
    }

    pub(super) fn subscribe(&self) -> broadcast::Receiver<OperationEvent> {
        self.events.subscribe()
    }

    pub(super) async fn start(&self, params: Value) -> Value {
        let (device_key, operation) = match params.require_strings("key", "operation") {
            Ok((key, operation)) => match DeviceOperation::try_from(operation) {
                Ok(operation) => (key.to_string(), operation),
                Err(error) => return api::error("validation-error", error.to_string()),
            },
            Err(error) => return api::error("validation-error", error.to_string()),
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
        let backend = Arc::clone(&self.backend);
        let operation_state = Arc::clone(&self.state);
        let events = self.events.clone();
        let task_event = queued.clone();
        let task_operation = operation;
        let (start_sender, start_receiver) = oneshot::channel();
        let handle = crate::task::spawn("device-operation", async move {
            if start_receiver.await.is_err() {
                return;
            }
            tracing::info!(request_id = %task_event.request_id, device_key = %task_event.device_key, operation = %task_operation, "Bluetooth device operation started");
            let _ = events.send(task_event.with_state("started", "running"));
            let result = crate::task::catch(
                "Bluetooth backend device operation",
                backend.device_operation(&task_event.device_key, task_operation, &params),
            )
            .await
            .and_then(|result| result);
            match &result {
                Ok(_) => {
                    tracing::info!(request_id = %task_event.request_id, "Bluetooth device operation completed")
                }
                Err(error) => {
                    tracing::warn!(request_id = %task_event.request_id, error = %error, error_chain = %format!("{error:#}"), "Bluetooth device operation failed")
                }
            }
            let mut state = operation_state.lock().await;
            state.tasks.remove(&task_event.request_id);
            if state.active_devices.get(&task_event.device_key) == Some(&task_event.request_id) {
                state.active_devices.remove(&task_event.device_key);
            }
            drop(state);
            let _ = events.send(task_event.finished(result));
        });
        state.tasks.insert(
            request_id.clone(),
            OperationTask {
                handle,
                event: queued.clone(),
            },
        );
        state.active_devices.insert(device_key, request_id);
        drop(state);
        let _ = start_sender.send(());
        api::success(json!({ "operation": queued }))
    }

    pub(super) async fn cancel(&self, request_id: &str) -> bool {
        let task = {
            let mut state = self.state.lock().await;
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
        let _ = self
            .events
            .send(task.event.with_state("cancelled", "cancelled"));
        true
    }

    #[cfg(test)]
    pub(super) async fn is_empty(&self) -> bool {
        self.state.lock().await.tasks.is_empty()
    }
}
