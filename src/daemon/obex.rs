use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast, oneshot};

use crate::{api, backend::BluetoothBackend, obex, params::Params};

pub(super) struct OutgoingTransfers {
    backend: Arc<dyn BluetoothBackend>,
    sequence: AtomicU64,
    events: broadcast::Sender<obex::ObexEvent>,
    cancellations: Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>,
}

impl OutgoingTransfers {
    pub(super) fn new(
        backend: Arc<dyn BluetoothBackend>,
        events: broadcast::Sender<obex::ObexEvent>,
    ) -> Self {
        Self {
            backend,
            sequence: AtomicU64::new(1),
            events,
            cancellations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(super) async fn start(&self, params: &Value) -> Value {
        let (device_key, path) = match params.require_strings("device_key", "path") {
            Ok(params) => params,
            Err(error) => return api::error("validation-error", error.to_string()),
        };
        let target = match self.backend.obex_target(device_key).await {
            Ok(target) => target,
            Err(error) => return api::error("device-unavailable", format!("{error:#}")),
        };
        let transfer = match obex::start_file(&target.source, &target.destination, path).await {
            Ok(transfer) => transfer,
            Err(error) => return api::error("obex-start-failed", format!("{error:#}")),
        };
        let request_id = format!(
            "obex-transfer-{}",
            self.sequence.fetch_add(1, Ordering::Relaxed)
        );
        let file_name = transfer.file_name.clone();
        let size = transfer.size;
        let queued = obex::ObexEvent {
            event: "queued".into(),
            request_id: request_id.clone(),
            direction: "outgoing".into(),
            device_key: device_key.into(),
            device_name: None,
            file_name: file_name.clone(),
            media_type: None,
            status: "queued".into(),
            transferred: 0,
            size,
            timeout_ms: None,
            error: None,
        };
        let (cancel_sender, cancel_receiver) = oneshot::channel();
        self.cancellations
            .lock()
            .await
            .insert(request_id.clone(), cancel_sender);
        let events = self.events.clone();
        let cancellations = Arc::clone(&self.cancellations);
        let task_id = request_id;
        let task_device = device_key.to_string();
        tokio::spawn(async move {
            let update_events = events.clone();
            let update_id = task_id.clone();
            let update_device = task_device.clone();
            let update_name = file_name.clone();
            let result = transfer
                .run(cancel_receiver, move |update| {
                    let event = obex::lifecycle_event(&update.status);
                    let _ = update_events.send(obex::ObexEvent {
                        event: event.into(),
                        request_id: update_id.clone(),
                        direction: "outgoing".into(),
                        device_key: update_device.clone(),
                        device_name: None,
                        file_name: update_name.clone(),
                        media_type: None,
                        status: update.status,
                        transferred: update.transferred,
                        size: update.size,
                        timeout_ms: None,
                        error: None,
                    });
                })
                .await;
            cancellations.lock().await.remove(&task_id);
            if let Err(error) = result {
                let _ = events.send(obex::ObexEvent {
                    event: "failed".into(),
                    request_id: task_id,
                    direction: "outgoing".into(),
                    device_key: task_device,
                    device_name: None,
                    file_name,
                    media_type: None,
                    status: "error".into(),
                    transferred: 0,
                    size,
                    timeout_ms: None,
                    error: Some(api::error_value(&error)),
                });
            }
        });
        api::success(json!({ "transfer": queued }))
    }

    pub(super) async fn cancel(&self, request_id: &str) -> bool {
        let Some(cancel) = self.cancellations.lock().await.remove(request_id) else {
            return false;
        };
        let _ = cancel.send(());
        true
    }
}
