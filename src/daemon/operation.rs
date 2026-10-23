use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use serde::Serialize;
use serde_json::{Value, json};
use shelllist_daemon_core::{OperationLimits, OwnedOperations, RecentResults};
use tokio::{
    sync::{Mutex, broadcast, oneshot},
    task::JoinHandle,
};

use crate::{
    api,
    backend::{BluetoothBackend, DeviceOperation},
};

#[derive(Clone, Serialize)]
pub(super) struct OperationEvent {
    pub(super) event: &'static str,
    pub(super) request_id: String,
    device_key: String,
    operation: &'static str,
    state: &'static str,
    pub(super) stage: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot: Option<Arc<crate::model::Snapshot>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<Value>,
}

impl OperationEvent {
    fn queued(request_id: String, device_key: String, operation: &'static str) -> Self {
        Self {
            event: "queued",
            request_id,
            device_key,
            operation,
            state: "queued",
            stage: "queued",
            snapshot: None,
            error: None,
        }
    }

    fn with_state(mut self, event: &'static str, state: &'static str) -> Self {
        self.event = event;
        self.state = state;
        self.stage = state;
        self
    }

    fn progress(&self, stage: &'static str) -> Self {
        Self {
            event: "progress",
            state: "running",
            stage,
            ..self.clone()
        }
    }

    fn finished(mut self, result: anyhow::Result<crate::model::Snapshot>) -> Self {
        match result {
            Ok(snapshot) => self.snapshot = Some(Arc::new(snapshot)),
            Err(error) => self.error = Some(api::error_value(&error)),
        }
        self.event = if self.error.is_some() {
            "failed"
        } else {
            "completed"
        };
        self.state = self.event;
        self.stage = self.event;
        self
    }
}

struct OperationTask {
    handle: JoinHandle<()>,
    event: OperationEvent,
}

pub(super) struct OperationCoordinator {
    backend: Arc<dyn BluetoothBackend>,
    sequence: AtomicU64,
    state: Mutex<OwnedOperations<OperationTask>>,
    events: broadcast::Sender<OperationEvent>,
    recent: Mutex<RecentResults<OperationEvent>>,
}

impl OperationCoordinator {
    pub(super) fn new(backend: Arc<dyn BluetoothBackend>) -> Arc<Self> {
        let (events, _) = broadcast::channel(32);
        Arc::new(Self {
            backend,
            sequence: AtomicU64::new(1),
            state: Mutex::new(OwnedOperations::new(OperationLimits {
                total: 256,
                per_owner: 64,
            })),
            events,
            recent: Mutex::new(RecentResults::new(64, None)),
        })
    }

    pub(super) fn subscribe(&self) -> broadcast::Receiver<OperationEvent> {
        self.events.subscribe()
    }

    pub(super) async fn snapshot(&self) -> Value {
        let active = json!(
            self.state
                .lock()
                .await
                .iter()
                .map(|(_, task)| &task.value.event)
                .collect::<Vec<_>>()
        );
        let recent = json!(self.recent.lock().await.values().collect::<Vec<_>>());
        json!({ "active": active, "recent": recent })
    }

    pub(super) async fn start_owned(
        self: &Arc<Self>,
        params: Value,
        owner: Option<String>,
    ) -> Value {
        let (device_key, operation) = match api::typed_operation::<DeviceOperation>(&params) {
            Ok((key, operation)) => (key.to_owned(), operation),
            Err(error) => return api::error("validation-error", error.to_string()),
        };

        let mut state = self.state.lock().await;
        if let Err(error) = state.admit(owner.as_deref()) {
            return api::error("operation-unavailable", error.to_string());
        }
        // Admission caps this scan at 256 tasks; avoid a second device-to-task index.
        if let Some((request_id, _)) = state
            .iter()
            .find(|(_, task)| task.value.event.device_key == device_key)
        {
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
        tracing::info!(%request_id, %device_key, %operation, "Bluetooth device operation queued");
        let queued = OperationEvent::queued(request_id.clone(), device_key, operation.as_str());
        let execution = Arc::clone(self);
        let response = api::success(json!({ "operation": queued }));
        let task_event = queued.clone();
        let (start_sender, start_receiver) = oneshot::channel();
        let handle = crate::task::spawn("device-operation", async move {
            execution
                .run(task_event, operation, params, start_receiver)
                .await;
        });
        let abort = handle.abort_handle();
        if let Err(error) = state.insert(
            request_id,
            owner,
            OperationTask {
                handle,
                event: queued,
            },
        ) {
            abort.abort();
            return api::error("operation-unavailable", error.to_string());
        }
        drop(state);
        let _ = start_sender.send(());
        response
    }

    pub(super) async fn cancel_owned(&self, request_id: &str, owner: Option<&str>) -> bool {
        let Some(task) = self
            .state
            .lock()
            .await
            .claim_owned(request_id, owner)
            .map(|entry| entry.value)
        else {
            return false;
        };
        task.handle.abort();
        tracing::info!(%request_id, "Bluetooth device operation cancelled");
        let terminal = task.event.with_state("cancelled", "cancelled");
        retain_terminal(&self.recent, terminal.clone()).await;
        let _ = self.events.send(terminal);
        true
    }

    async fn run(
        self: Arc<Self>,
        event: OperationEvent,
        operation: DeviceOperation,
        params: Value,
        start: oneshot::Receiver<()>,
    ) {
        if start.await.is_err() {
            return;
        }
        tracing::info!(request_id = %event.request_id, device_key = %event.device_key, %operation, "Bluetooth device operation started");
        if !self.publish_started(&event).await {
            return;
        }
        let progress = self.progress_reporter(&event);
        let result = crate::task::catch(
            "Bluetooth backend device operation",
            self.backend
                .device_operation(&event.device_key, operation, &params, progress),
        )
        .await;
        log_operation_result(&event.request_id, &result);
        let completed = self.state.lock().await.claim(&event.request_id).is_some();
        if completed {
            let terminal = event.finished(result);
            retain_terminal(&self.recent, terminal.clone()).await;
            let _ = self.events.send(terminal);
        }
    }

    async fn publish_started(&self, event: &OperationEvent) -> bool {
        let started = event.clone().with_state("started", "running");
        let mut state = self.state.lock().await;
        let Some(task) = state.get_mut(&event.request_id) else {
            return false;
        };
        task.event = started.clone();
        let _ = self.events.send(started);
        true
    }

    fn progress_reporter(
        self: &Arc<Self>,
        event: &OperationEvent,
    ) -> Arc<dyn Fn(&'static str) + Send + Sync> {
        let event = event.clone();
        let coordinator = Arc::clone(self);
        Arc::new(move |stage| {
            let update = event.progress(stage);
            if let Ok(mut state) = coordinator.state.try_lock()
                && let Some(task) = state.get_mut(&event.request_id)
            {
                task.event = update.clone();
                let _ = coordinator.events.send(update);
            }
        })
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

async fn retain_terminal(recent: &Mutex<RecentResults<OperationEvent>>, event: OperationEvent) {
    // The existing requests snapshot is an aggregate API, not owner-private history.
    recent
        .lock()
        .await
        .record(event.request_id.clone(), None, event);
}
