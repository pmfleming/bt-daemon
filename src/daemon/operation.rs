use std::{sync::Arc, time::Duration};

use serde::Serialize;
use serde_json::{Value, json};
use shelllist_daemon_core::{OperationLimits, OwnedOperations, RecentResults};
use tokio::{
    sync::{Mutex, broadcast, oneshot},
    task::JoinHandle,
};

use crate::{
    api,
    backend::{BluetoothBackend, DeviceOperation, Params},
};

#[cfg(test)]
mod tests;

const TERMINAL_LIMIT: usize = 64;
const TERMINAL_RETENTION: Duration = Duration::from_secs(300);
const MAX_REVISION: u64 = (1_u64 << 53) - 1;

#[derive(Clone, Serialize)]
pub(super) struct OperationEvent {
    pub(super) event: &'static str,
    pub(super) request_id: String,
    // Never-reused IDs and exact per-operation revisions, not delivery counters.
    revision: u64,
    #[serde(skip)]
    owner: Option<String>,
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
    fn queued(
        request_id: String,
        device_key: String,
        operation: &'static str,
        owner: Option<String>,
    ) -> Self {
        Self {
            event: "queued",
            request_id,
            revision: 1,
            owner,
            device_key,
            operation,
            state: "queued",
            stage: "queued",
            snapshot: None,
            error: None,
        }
    }

    pub(super) fn visible_to(&self, owner: Option<&str>) -> bool {
        self.owner.as_deref() == owner
    }

    fn with_state(mut self, event: &'static str, state: &'static str) -> Self {
        self.revision += 1;
        self.event = event;
        self.state = state;
        self.stage = state;
        self
    }

    fn progress(&self, stage: &'static str) -> Self {
        let mut update = self.clone().with_state("progress", "running");
        update.stage = stage;
        update
    }

    fn finished(mut self, result: anyhow::Result<crate::model::Snapshot>) -> Self {
        match result {
            Ok(snapshot) => self.snapshot = Some(Arc::new(snapshot)),
            Err(error) => self.error = Some(api::error_value(&error)),
        }
        let terminal = if self.error.is_some() {
            "failed"
        } else {
            "completed"
        };
        self.with_state(terminal, terminal)
    }
}

struct OperationTask {
    handle: Option<JoinHandle<()>>,
    event: OperationEvent,
}

struct OperationState {
    active: OwnedOperations<OperationTask>,
    recent: RecentResults<OperationEvent>,
}

pub(super) struct OperationCoordinator {
    backend: Arc<dyn BluetoothBackend>,
    // Active -> retained terminal and publication are one critical section.
    state: Mutex<OperationState>,
    events: broadcast::Sender<OperationEvent>,
}

impl OperationCoordinator {
    pub(super) fn new(backend: Arc<dyn BluetoothBackend>) -> Arc<Self> {
        let (events, _) = broadcast::channel(32);
        Arc::new(Self {
            backend,
            state: Mutex::new(OperationState {
                active: OwnedOperations::new(OperationLimits {
                    total: 256,
                    per_owner: 64,
                }),
                recent: RecentResults::new(TERMINAL_LIMIT, Some(TERMINAL_RETENTION)),
            }),
            events,
        })
    }

    pub(super) fn subscribe(&self) -> broadcast::Receiver<OperationEvent> {
        self.events.subscribe()
    }

    #[cfg(test)]
    pub(super) async fn counts(&self) -> (usize, usize) {
        let mut state = self.state.lock().await;
        (state.active.iter().count(), state.recent.values().count())
    }

    pub(super) async fn snapshot(&self, owner: Option<&str>) -> Value {
        let mut state = self.state.lock().await;
        let active = json!(
            state
                .active
                .iter()
                .filter(|(_, task)| task.owner.as_deref() == owner)
                .map(|(_, task)| &task.value.event)
                .collect::<Vec<_>>()
        );
        let recent = json!(
            state
                .recent
                .values()
                .filter(|event| event.visible_to(owner))
                .collect::<Vec<_>>()
        );
        json!({ "active": active, "recent": recent })
    }

    pub(super) async fn status(&self, params: &Value, owner: Option<&str>) -> Value {
        let request_id = match params.require_string("request_id") {
            Ok(id) => id,
            Err(error) => return api::error("validation-error", error.to_string()),
        };
        let mut state = self.state.lock().await;
        let event = state
            .active
            .get_owned(request_id, owner)
            .map(|task| &task.event)
            .cloned()
            .or_else(|| state.recent.get_owned(request_id, owner).cloned());
        match event {
            // Deliberately distinct from an admission receipt: status reads must
            // not register/rebind accepted-work routes in the retained client.
            Some(event) => api::success(json!({"operation_status": event})),
            None => api::error(
                "request-not-found",
                "No retained operation for this caller".into(),
            ),
        }
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
        if let Err(error) = state.active.admit(owner.as_deref()) {
            return api::error("operation-unavailable", error.to_string());
        }
        if state
            .active
            .iter()
            .any(|(_, task)| task.value.event.device_key == device_key)
        {
            return api::error(
                "device-busy",
                "Device already has an active operation".into(),
            );
        }
        let request_id = format!("operation-{}", uuid::Uuid::new_v4());
        tracing::info!(%request_id, %device_key, %operation, "Bluetooth device operation queued");
        let queued = OperationEvent::queued(
            request_id.clone(),
            device_key,
            operation.as_str(),
            owner.clone(),
        );
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
        if let Err(error) = state.active.insert(
            request_id,
            owner,
            OperationTask {
                handle: Some(handle),
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

    pub(super) async fn cancel_owned(
        self: &Arc<Self>,
        request_id: &str,
        owner: Option<&str>,
    ) -> bool {
        let mut state = self.state.lock().await;
        if state.active.get_owned(request_id, owner).is_none() {
            return false;
        }
        let task = state
            .active
            .get_mut(request_id)
            .expect("checked owned task");
        let Some(handle) = task.handle.take() else {
            return true; // Cancellation already owns the worker's terminal claim.
        };
        handle.abort();
        // Keep the active entry/admission until the worker actually stops. The
        // reaper is independent of the Cancel RPC future: losing that reply or
        // dropping its waiter cannot create a missing-outcome/admission hole.
        let coordinator = Arc::clone(self);
        let id = request_id.to_owned();
        let (finished, stopped) = oneshot::channel();
        crate::task::spawn("device-operation-cancel", async move {
            let _ = handle.await;
            let mut state = coordinator.state.lock().await;
            if let Some(task) = state.active.claim(&id) {
                tracing::info!(request_id = %id, "Bluetooth device operation cancelled; effects are not rolled back");
                coordinator.retain_terminal(
                    &mut state,
                    task.value.event.with_state("cancelled", "cancelled"),
                );
            }
            let _ = finished.send(());
        });
        drop(state);
        let _ = stopped.await;
        true
    }

    pub(super) async fn cancel_owner(self: &Arc<Self>, owner: &str) {
        let ids = self
            .state
            .lock()
            .await
            .active
            .iter()
            .filter(|(_, entry)| entry.owner.as_deref() == Some(owner))
            .map(|(id, _)| id.to_owned())
            .collect::<Vec<_>>();
        for id in ids {
            self.cancel_owned(&id, Some(owner)).await;
        }
    }

    fn retain_terminal(&self, state: &mut OperationState, event: OperationEvent) {
        state
            .recent
            .record(event.request_id.clone(), event.owner.clone(), event.clone());
        let _ = self.events.send(event);
    }

    async fn run(
        self: Arc<Self>,
        event: OperationEvent,
        operation: DeviceOperation,
        params: Value,
        start: oneshot::Receiver<()>,
    ) {
        if start.await.is_err() || !self.publish_started(&event).await {
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
        let mut state = self.state.lock().await;
        if let Some(task) = state.active.claim(&event.request_id) {
            self.retain_terminal(&mut state, task.value.event.finished(result));
        }
    }

    async fn publish_started(&self, event: &OperationEvent) -> bool {
        let mut state = self.state.lock().await;
        let Some(task) = state.active.get_mut(&event.request_id) else {
            return false;
        };
        task.event = task.event.clone().with_state("started", "running");
        let _ = self.events.send(task.event.clone());
        true
    }

    fn progress_reporter(
        self: &Arc<Self>,
        event: &OperationEvent,
    ) -> Arc<dyn Fn(&'static str) + Send + Sync> {
        let id = event.request_id.clone();
        let coordinator = Arc::clone(self);
        Arc::new(move |stage| {
            if let Ok(mut state) = coordinator.state.try_lock()
                && let Some(task) = state.active.get_mut(&id)
                // Reserve the final exact revision for a terminal transition.
                && task.handle.is_some()
                && task.event.revision < MAX_REVISION - 1
            {
                task.event = task.event.progress(stage);
                let _ = coordinator.events.send(task.event.clone());
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

// Same constructors as receipts/status/events; fixed opaque ID only for offline evidence.
pub(super) fn ordering_fixture() -> Value {
    let queued = OperationEvent::queued(
        "fixture-operation".into(),
        "device-opaque".into(),
        "connect",
        Some("fixture-owner".into()),
    );
    let started = queued.clone().with_state("started", "running");
    let progress = started.progress("connecting");
    let completed = progress
        .clone()
        .finished(Ok(crate::model::Snapshot::default()));
    let failed = progress
        .clone()
        .finished(Err(anyhow::anyhow!("fixture failure")));
    let cancelled = progress.clone().with_state("cancelled", "cancelled");
    json!({"queued": queued, "started": started, "progress": progress, "completed": completed,
        "failed": failed, "cancelled": cancelled, "status": api::success(json!({"operation_status": completed}))})
}
