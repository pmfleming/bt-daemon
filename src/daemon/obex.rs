use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast, oneshot};

use crate::backend::Params;
use crate::{api, backend::BluetoothBackend, obex};

pub(super) async fn respond(incoming: &obex::IncomingBroker, params: &Value) -> Value {
    let request = params
        .require_string("request_id")
        .and_then(|id| params.require_bool("accept").map(|accept| (id, accept)));
    let (request_id, accept) = match request {
        Ok(request) => request,
        Err(error) => return api::error("validation-error", error.to_string()),
    };
    tracing::info!(%request_id, accept, "incoming OBEX authorization response received");
    match incoming.respond(request_id, accept).await {
        Ok(()) => api::success(json!({
            "authorization": { "request_id": request_id, "accepted": accept }
        })),
        Err(error) => {
            tracing::warn!(%request_id, accept, error = %error, error_chain = %format!("{error:#}"), "incoming OBEX authorization response failed");
            api::error("obex-response-rejected", format!("{error:#}"))
        }
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
        tracing::info!(%device_key, "outgoing OBEX transfer requested");
        let target = match self.backend.obex_target(device_key).await {
            Ok(target) => target,
            Err(error) => {
                tracing::warn!(%device_key, error = %error, error_chain = %format!("{error:#}"), "could not resolve outgoing OBEX target");
                let details = api::error_value(&error);
                return json!({
                    "protocol": api::PROTOCOL,
                    "version": api::VERSION,
                    "ok": false,
                    "error": details,
                });
            }
        };
        let transfer = match obex::start_file(&target.source, &target.destination, path).await {
            Ok(transfer) => transfer,
            Err(error) => {
                tracing::warn!(%device_key, error = %error, error_chain = %format!("{error:#}"), "could not start outgoing OBEX transfer");
                return api::error("obex-start-failed", format!("{error:#}"));
            }
        };
        let request_id = format!(
            "obex-transfer-{}",
            self.sequence.fetch_add(1, Ordering::Relaxed)
        );
        let file_name = transfer.file_name.clone();
        let size = transfer.size;
        let queued = obex::ObexEvent::outgoing(&request_id, device_key, &file_name, size);
        tracing::info!(%request_id, %device_key, %file_name, size, "outgoing OBEX transfer queued");
        let (cancel_sender, cancel_receiver) = oneshot::channel();
        self.cancellations
            .lock()
            .await
            .insert(request_id.clone(), cancel_sender);
        let events = self.events.clone();
        let cancellations = Arc::clone(&self.cancellations);
        let task_id = request_id;
        let task_event = queued.clone();
        crate::task::spawn("outgoing-obex-transfer", async move {
            let update_events = events.clone();
            let update_event = task_event.clone();
            let result = crate::task::catch(
                "outgoing OBEX transfer",
                transfer.run(cancel_receiver, move |update| {
                    let _ = update_events.send(update_event.updated(update));
                }),
            )
            .await
            .and_then(|result| result);
            cancellations.lock().await.remove(&task_id);
            if let Err(error) = result {
                tracing::warn!(request_id = %task_id, error = %error, error_chain = %format!("{error:#}"), "outgoing OBEX transfer failed");
                let _ = events.send(task_event.failed(api::error_value(&error)));
            } else {
                tracing::info!(request_id = %task_id, "outgoing OBEX transfer completed");
            }
        });
        api::success(json!({ "transfer": queued }))
    }

    pub(super) async fn cancel(&self, request_id: &str) -> bool {
        let Some(cancel) = self.cancellations.lock().await.remove(request_id) else {
            return false;
        };
        let _ = cancel.send(());
        tracing::info!(%request_id, "outgoing OBEX transfer cancellation sent");
        true
    }
}
