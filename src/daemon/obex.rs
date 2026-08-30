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

pub(super) const AGENT_PATH: &str = obex::AGENT_PATH;
pub(super) use crate::obex::ObexEvent;
pub(super) struct ObexCoordinator {
    pub(super) outgoing: OutgoingTransfers,
    pub(super) incoming: Arc<obex::IncomingBroker>,
}

impl ObexCoordinator {
    pub(super) fn new(backend: Arc<dyn BluetoothBackend>) -> Arc<Self> {
        let (events, _) = broadcast::channel(32);
        Arc::new(Self {
            outgoing: OutgoingTransfers::new(Arc::clone(&backend), events.clone()),
            incoming: obex::IncomingBroker::new(backend, events.clone()),
        })
    }

    pub(super) fn subscribe(&self) -> broadcast::Receiver<ObexEvent> {
        self.outgoing.events.subscribe()
    }

    pub(super) fn agent(&self) -> obex::ObexAgent {
        obex::ObexAgent::new(Arc::clone(&self.incoming))
    }

    pub(super) async fn activate(&self, connection: zbus::Connection) {
        self.incoming.set_connection(connection.clone());
        if let Err(error) = obex::register_agent(&connection, &self.incoming).await {
            tracing::warn!(error = %error, error_chain = %format!("{error:#}"), "incoming OBEX authorization is unavailable");
        }
        obex::monitor_agent_owner(connection, Arc::clone(&self.incoming));
    }

    pub(super) async fn snapshot(&self) -> Value {
        match obex::probe(self.incoming.is_available()).await {
            Ok(capabilities) => api::success(json!({ "obex": capabilities })),
            Err(error) => api::success(json!({ "obex": {
                "available": false,
                "outgoing_object_push": false,
                "incoming_authorization": false,
                "transfer_progress": false,
                "cancellation": false,
                "reason": format!("{error:#}"),
            }})),
        }
    }

    pub(super) async fn cancel(&self, request_id: &str) -> Option<&'static str> {
        (self.outgoing.cancel(request_id).await || self.incoming.cancel_transfer(request_id).await)
            .then_some("obex-transfer")
    }
}

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

struct OutgoingCancellation {
    owner: Option<String>,
    sender: oneshot::Sender<()>,
}

pub(super) struct OutgoingTransfers {
    backend: Arc<dyn BluetoothBackend>,
    sequence: AtomicU64,
    events: broadcast::Sender<obex::ObexEvent>,
    cancellations: Arc<Mutex<HashMap<String, OutgoingCancellation>>>,
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

    pub(super) async fn start_owned(&self, params: &Value, owner: Option<String>) -> Value {
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
        self.cancellations.lock().await.insert(
            request_id.clone(),
            OutgoingCancellation {
                owner,
                sender: cancel_sender,
            },
        );
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
            .await;
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
        self.cancel_owned(request_id, None).await
    }

    pub(super) async fn cancel_owned(&self, request_id: &str, owner: Option<&str>) -> bool {
        let mut cancellations = self.cancellations.lock().await;
        if cancellations
            .get(request_id)
            .is_none_or(|cancellation| cancellation.owner.as_deref() != owner)
        {
            return false;
        }
        let Some(cancel) = cancellations.remove(request_id) else {
            return false;
        };
        drop(cancellations);
        let _ = cancel.sender.send(());
        tracing::info!(%request_id, "outgoing OBEX transfer cancellation sent");
        true
    }
}
