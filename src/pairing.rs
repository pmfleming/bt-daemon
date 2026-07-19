use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use bluer::{
    Address,
    agent::{
        Agent, AuthorizeService, DisplayPasskey, DisplayPinCode, ReqError, ReqResult,
        RequestAuthorization, RequestConfirmation, RequestPasskey, RequestPinCode,
    },
};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{Mutex, broadcast, oneshot};

use crate::identity::DeviceIdentityRegistry;

const PROMPT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Serialize)]
pub struct PairingEvent {
    pub event: String,
    pub request_id: String,
    pub kind: String,
    pub device_key: String,
    pub response_required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entered: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub timeout_ms: u64,
}

enum PendingResponse {
    Pin(oneshot::Sender<ReqResult<String>>),
    Passkey(oneshot::Sender<ReqResult<u32>>),
    Unit(oneshot::Sender<ReqResult<()>>),
}

pub struct PairingBroker {
    sequence: AtomicU64,
    pending: Mutex<HashMap<String, PendingResponse>>,
    events: broadcast::Sender<PairingEvent>,
    identities: Arc<DeviceIdentityRegistry>,
}

impl PairingBroker {
    pub fn new(identities: Arc<DeviceIdentityRegistry>) -> Arc<Self> {
        let (events, _) = broadcast::channel(32);
        Arc::new(Self {
            sequence: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            events,
            identities,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PairingEvent> {
        self.events.subscribe()
    }

    pub fn device_key(&self, adapter: &str, address: Address) -> String {
        self.identities.device_key(adapter, address)
    }

    pub fn agent(self: &Arc<Self>) -> Agent {
        Agent {
            request_default: false,
            request_pin_code: Some(callback_pin(Arc::clone(self))),
            display_pin_code: Some(callback_display_pin(Arc::clone(self))),
            request_passkey: Some(callback_passkey(Arc::clone(self))),
            display_passkey: Some(callback_display_passkey(Arc::clone(self))),
            request_confirmation: Some(callback_confirmation(Arc::clone(self))),
            request_authorization: Some(callback_authorization(Arc::clone(self))),
            authorize_service: Some(callback_service(Arc::clone(self))),
            ..Default::default()
        }
    }

    pub async fn respond(&self, params: &Value) -> Result<()> {
        let request_id = params
            .get("request_id")
            .and_then(Value::as_str)
            .context("missing pairing request_id")?;
        let accept = params
            .get("accept")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let pending = self
            .pending
            .lock()
            .await
            .remove(request_id)
            .context("pairing request is no longer pending")?;
        if !accept {
            reject(pending);
            return Ok(());
        }
        match pending {
            PendingResponse::Pin(sender) => {
                let value = params
                    .get("value")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if value.is_empty() || value.chars().count() > 16 {
                    self.pending
                        .lock()
                        .await
                        .insert(request_id.to_string(), PendingResponse::Pin(sender));
                    bail!("PIN must contain 1 to 16 characters");
                }
                let _ = sender.send(Ok(value.to_string()));
            }
            PendingResponse::Passkey(sender) => {
                let value = params
                    .get("value")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if value.len() > 6
                    || value.is_empty()
                    || !value.chars().all(|char| char.is_ascii_digit())
                {
                    self.pending
                        .lock()
                        .await
                        .insert(request_id.to_string(), PendingResponse::Passkey(sender));
                    bail!("passkey must contain 1 to 6 digits");
                }
                let parsed = value.parse::<u32>().context("parse pairing passkey")?;
                let _ = sender.send(Ok(parsed));
            }
            PendingResponse::Unit(sender) => {
                let _ = sender.send(Ok(()));
            }
        }
        Ok(())
    }

    async fn request_pin(&self, adapter: String, device: Address) -> ReqResult<String> {
        let (sender, receiver) = oneshot::channel();
        let id = self
            .insert_request(
                "pin-code",
                &adapter,
                device,
                PendingResponse::Pin(sender),
                None,
                None,
            )
            .await;
        wait_for_response(self, id, receiver).await
    }

    async fn request_passkey(&self, adapter: String, device: Address) -> ReqResult<u32> {
        let (sender, receiver) = oneshot::channel();
        let id = self
            .insert_request(
                "passkey",
                &adapter,
                device,
                PendingResponse::Passkey(sender),
                None,
                None,
            )
            .await;
        wait_for_response(self, id, receiver).await
    }

    async fn request_unit(
        &self,
        kind: &str,
        adapter: String,
        device: Address,
        value: Option<String>,
        service: Option<String>,
    ) -> ReqResult<()> {
        let (sender, receiver) = oneshot::channel();
        let id = self
            .insert_request(
                kind,
                &adapter,
                device,
                PendingResponse::Unit(sender),
                value,
                service,
            )
            .await;
        wait_for_response(self, id, receiver).await
    }

    async fn insert_request(
        &self,
        kind: &str,
        adapter: &str,
        device: Address,
        pending: PendingResponse,
        value: Option<String>,
        service: Option<String>,
    ) -> String {
        let id = format!("pairing-{}", self.sequence.fetch_add(1, Ordering::Relaxed));
        self.pending.lock().await.insert(id.clone(), pending);
        self.emit(PairingEvent {
            event: "requested".to_string(),
            request_id: id.clone(),
            kind: kind.to_string(),
            device_key: self.identities.device_key(adapter, device),
            response_required: true,
            value,
            entered: None,
            service,
            reason: None,
            timeout_ms: PROMPT_TIMEOUT.as_millis() as u64,
        });
        id
    }

    fn display(
        &self,
        kind: &str,
        adapter: &str,
        device: Address,
        value: String,
        entered: Option<u16>,
    ) -> String {
        let id = format!(
            "pairing-display-{}",
            self.sequence.fetch_add(1, Ordering::Relaxed)
        );
        self.emit(PairingEvent {
            event: "display".to_string(),
            request_id: id.clone(),
            kind: kind.to_string(),
            device_key: self.identities.device_key(adapter, device),
            response_required: false,
            value: Some(value),
            entered,
            service: None,
            reason: None,
            timeout_ms: 0,
        });
        id
    }

    fn cancelled(
        &self,
        request_id: String,
        kind: &str,
        adapter: &str,
        device: Address,
        reason: &str,
    ) {
        self.emit(PairingEvent {
            event: "cancelled".to_string(),
            request_id,
            kind: kind.to_string(),
            device_key: self.identities.device_key(adapter, device),
            response_required: false,
            value: None,
            entered: None,
            service: None,
            reason: Some(reason.to_string()),
            timeout_ms: 0,
        });
    }

    fn emit(&self, event: PairingEvent) {
        let _ = self.events.send(event);
    }
}

async fn wait_for_response<T>(
    broker: &PairingBroker,
    id: String,
    receiver: oneshot::Receiver<ReqResult<T>>,
) -> ReqResult<T> {
    match tokio::time::timeout(PROMPT_TIMEOUT, receiver).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(ReqError::Canceled),
        Err(_) => {
            broker.pending.lock().await.remove(&id);
            Err(ReqError::Canceled)
        }
    }
}

fn reject(pending: PendingResponse) {
    match pending {
        PendingResponse::Pin(sender) => {
            let _ = sender.send(Err(ReqError::Rejected));
        }
        PendingResponse::Passkey(sender) => {
            let _ = sender.send(Err(ReqError::Rejected));
        }
        PendingResponse::Unit(sender) => {
            let _ = sender.send(Err(ReqError::Rejected));
        }
    }
}

fn callback_pin(broker: Arc<PairingBroker>) -> bluer::agent::RequestPinCodeFn {
    Box::new(move |request: RequestPinCode| {
        let broker = Arc::clone(&broker);
        Box::pin(async move { broker.request_pin(request.adapter, request.device).await })
    })
}

fn callback_passkey(broker: Arc<PairingBroker>) -> bluer::agent::RequestPasskeyFn {
    Box::new(move |request: RequestPasskey| {
        let broker = Arc::clone(&broker);
        Box::pin(async move {
            broker
                .request_passkey(request.adapter, request.device)
                .await
        })
    })
}

fn callback_confirmation(broker: Arc<PairingBroker>) -> bluer::agent::RequestConfirmationFn {
    Box::new(move |request: RequestConfirmation| {
        let broker = Arc::clone(&broker);
        Box::pin(async move {
            broker
                .request_unit(
                    "confirmation",
                    request.adapter,
                    request.device,
                    Some(format!("{:06}", request.passkey)),
                    None,
                )
                .await
        })
    })
}

fn callback_authorization(broker: Arc<PairingBroker>) -> bluer::agent::RequestAuthorizationFn {
    Box::new(move |request: RequestAuthorization| {
        let broker = Arc::clone(&broker);
        Box::pin(async move {
            broker
                .request_unit("authorization", request.adapter, request.device, None, None)
                .await
        })
    })
}

fn callback_service(broker: Arc<PairingBroker>) -> bluer::agent::AuthorizeServiceFn {
    Box::new(move |request: AuthorizeService| {
        let broker = Arc::clone(&broker);
        Box::pin(async move {
            broker
                .request_unit(
                    "service-authorization",
                    request.adapter,
                    request.device,
                    None,
                    Some(request.service.to_string()),
                )
                .await
        })
    })
}

fn callback_display_pin(broker: Arc<PairingBroker>) -> bluer::agent::DisplayPinCodeFn {
    Box::new(move |request: DisplayPinCode| {
        let broker = Arc::clone(&broker);
        Box::pin(async move {
            let adapter = request.adapter;
            let device = request.device;
            let id = broker.display("display-pin", &adapter, device, request.pincode, None);
            let watcher = Arc::clone(&broker);
            tokio::spawn(async move {
                let _ = request.cancel.await;
                watcher.cancelled(id, "display-pin", &adapter, device, "cancelled");
            });
            Ok(())
        })
    })
}

fn callback_display_passkey(broker: Arc<PairingBroker>) -> bluer::agent::DisplayPasskeyFn {
    Box::new(move |request: DisplayPasskey| {
        let broker = Arc::clone(&broker);
        Box::pin(async move {
            let adapter = request.adapter;
            let device = request.device;
            let id = broker.display(
                "display-passkey",
                &adapter,
                device,
                format!("{:06}", request.passkey),
                Some(request.entered),
            );
            let watcher = Arc::clone(&broker);
            tokio::spawn(async move {
                let _ = request.cancel.await;
                watcher.cancelled(id, "display-passkey", &adapter, device, "cancelled");
            });
            Ok(())
        })
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::identity::DeviceIdentityRegistry;

    use super::{PairingBroker, PendingResponse};

    #[tokio::test]
    async fn rejects_invalid_passkeys_without_answering_bluez() {
        let broker = PairingBroker::new(DeviceIdentityRegistry::in_memory());
        let (sender, mut receiver) = tokio::sync::oneshot::channel();
        broker
            .pending
            .lock()
            .await
            .insert("request-1".into(), PendingResponse::Passkey(sender));
        let result = broker
            .respond(&json!({ "request_id": "request-1", "accept": true, "value": "12x" }))
            .await;
        assert!(result.is_err());
        assert!(receiver.try_recv().is_err());
        assert!(broker.pending.lock().await.contains_key("request-1"));
    }

    #[tokio::test]
    async fn prompt_event_uses_opaque_identity_and_accepts_response() {
        let identities = DeviceIdentityRegistry::in_memory();
        let address = "AA:BB:CC:DD:EE:FF".parse().unwrap();
        let expected_key = identities.device_key("hci0", address);
        let broker = PairingBroker::new(identities);
        let mut events = broker.subscribe();
        let request_broker = broker.clone();
        let request = tokio::spawn(async move {
            request_broker
                .request_unit(
                    "confirmation",
                    "hci0".into(),
                    address,
                    Some("123456".into()),
                    None,
                )
                .await
        });
        let event = events.recv().await.unwrap();
        assert_eq!(event.kind, "confirmation");
        assert_eq!(event.value.as_deref(), Some("123456"));
        assert_eq!(event.device_key, expected_key);
        assert!(!event.device_key.contains("AA:BB"));
        broker
            .respond(&json!({ "request_id": event.request_id, "accept": true }))
            .await
            .unwrap();
        assert_eq!(request.await.unwrap(), Ok(()));
    }

    #[tokio::test]
    async fn confirmation_response_is_forwarded() {
        let broker = PairingBroker::new(DeviceIdentityRegistry::in_memory());
        let (sender, receiver) = tokio::sync::oneshot::channel();
        broker
            .pending
            .lock()
            .await
            .insert("request-2".into(), PendingResponse::Unit(sender));
        broker
            .respond(&json!({ "request_id": "request-2", "accept": true }))
            .await
            .unwrap();
        assert_eq!(receiver.await.unwrap(), Ok(()));
    }
}
