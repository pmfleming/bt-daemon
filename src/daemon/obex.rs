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

pub(super) async fn respond(incoming: &obex::IncomingBroker, params: &Value) -> Value {
    let request = params
        .require_string("request_id")
        .and_then(|id| params.require_bool("accept").map(|accept| (id, accept)));
    let (request_id, accept) = match request {
        Ok(request) => request,
        Err(error) => return api::error("validation-error", error.to_string()),
    };
    match incoming.respond(request_id, accept).await {
        Ok(()) => api::success(json!({
            "authorization": { "request_id": request_id, "accepted": accept }
        })),
        Err(error) => api::error("obex-response-rejected", format!("{error:#}")),
    }
}

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
        let queued = obex::ObexEvent::outgoing(&request_id, device_key, &file_name, size);
        let (cancel_sender, cancel_receiver) = oneshot::channel();
        self.cancellations
            .lock()
            .await
            .insert(request_id.clone(), cancel_sender);
        let events = self.events.clone();
        let cancellations = Arc::clone(&self.cancellations);
        let task_id = request_id;
        let task_event = queued.clone();
        tokio::spawn(async move {
            let update_events = events.clone();
            let update_event = task_event.clone();
            let result = transfer
                .run(cancel_receiver, move |update| {
                    let _ = update_events.send(update_event.updated(update));
                })
                .await;
            cancellations.lock().await.remove(&task_id);
            if let Err(error) = result {
                let _ = events.send(task_event.failed(api::error_value(&error)));
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
