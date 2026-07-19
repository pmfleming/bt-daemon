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

use crate::{api, backend::BluetoothBackend, params::Params};

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
            Ok((key, operation)) => (key.to_string(), operation.to_string()),
            Err(error) => return api::error("validation-error", error.to_string()),
        };
        if !matches!(
            operation.as_str(),
            "pair"
                | "connect"
                | "disconnect"
                | "remove"
                | "set-trusted"
                | "set-blocked"
                | "set-wake-allowed"
                | "set-alias"
        ) {
            return api::error(
                "validation-error",
                format!("unsupported Bluetooth device operation: {operation}"),
            );
        }

        let mut state = self.state.lock().await;
        if let Some(request_id) = state.active_devices.get(&device_key) {
            return api::error(
                "device-busy",
                format!("Device already has active operation {request_id}"),
            );
        }
        let request_id = format!(
            "operation-{}",
            self.sequence.fetch_add(1, Ordering::Relaxed)
        );
        let queued = OperationEvent {
            event: "queued".into(),
            request_id: request_id.clone(),
            device_key: device_key.clone(),
            operation: operation.clone(),
            state: "queued".into(),
            snapshot: None,
            error: None,
        };
        let backend = Arc::clone(&self.backend);
        let operation_state = Arc::clone(&self.state);
        let events = self.events.clone();
        let task_id = request_id.clone();
        let task_key = device_key.clone();
        let task_operation = operation.clone();
        let (start_sender, start_receiver) = oneshot::channel();
        let handle = tokio::spawn(async move {
            if start_receiver.await.is_err() {
                return;
            }
            let _ = events.send(OperationEvent {
                event: "started".into(),
                request_id: task_id.clone(),
                device_key: task_key.clone(),
                operation: task_operation.clone(),
                state: "running".into(),
                snapshot: None,
                error: None,
            });
            let result = backend
                .device_operation(&task_key, &task_operation, &params)
                .await;
            let mut state = operation_state.lock().await;
            state.tasks.remove(&task_id);
            if state.active_devices.get(&task_key) == Some(&task_id) {
                state.active_devices.remove(&task_key);
            }
            drop(state);
            let (event, snapshot, error) = match result {
                Ok(snapshot) => ("completed", Some(snapshot), None),
                Err(error) => ("failed", None, Some(api::error_value(&error))),
            };
            let _ = events.send(OperationEvent {
                event: event.into(),
                request_id: task_id,
                device_key: task_key,
                operation: task_operation,
                state: event.into(),
                snapshot,
                error,
            });
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
        let mut event = task.event;
        event.event = "cancelled".into();
        event.state = "cancelled".into();
        let _ = self.events.send(event);
        true
    }

    #[cfg(test)]
    pub(super) async fn is_empty(&self) -> bool {
        self.state.lock().await.tasks.is_empty()
    }
}
