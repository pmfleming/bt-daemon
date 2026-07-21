use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
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
use tokio::sync::{broadcast, oneshot};

use crate::{identity::DeviceIdentityRegistry, params::Params};

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

impl PairingEvent {
    fn cancelled(mut self, reason: &str) -> Self {
        self.event = "cancelled".into();
        self.response_required = false;
        self.value = None;
        self.service = None;
        self.reason = Some(reason.into());
        self.timeout_ms = 0;
        self
    }
}

enum PendingResponse {
    Pin(oneshot::Sender<ReqResult<String>>),
    Passkey(oneshot::Sender<ReqResult<u32>>),
    Unit(oneshot::Sender<ReqResult<()>>),
}

impl PendingResponse {
    fn validate(&self, params: &Value) -> Result<()> {
        match self {
            Self::Pin(_) => pin_value(params).map(drop),
            Self::Passkey(_) => passkey_value(params).map(drop),
            Self::Unit(_) => Ok(()),
        }
    }

    fn accept(self, params: &Value) -> Result<()> {
        match self {
            Self::Pin(sender) => send_response(sender, Ok(pin_value(params)?)),
            Self::Passkey(sender) => send_response(sender, Ok(passkey_value(params)?)),
            Self::Unit(sender) => send_response(sender, Ok(())),
        }
    }
}

fn send_response<T>(sender: oneshot::Sender<ReqResult<T>>, response: ReqResult<T>) -> Result<()> {
    sender
        .send(response)
        .map_err(|_| anyhow::anyhow!("pairing request was cancelled"))
}

fn pin_value(params: &Value) -> Result<String> {
    let value = params
        .get("value")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if value.is_empty() || value.chars().count() > 16 {
        bail!("PIN must contain 1 to 16 characters");
    }
    Ok(value.to_string())
}

fn passkey_value(params: &Value) -> Result<u32> {
    let value = params
        .get("value")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if value.is_empty() || value.len() > 6 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("passkey must contain 1 to 6 digits");
    }
    value.parse().context("parse pairing passkey")
}

struct PendingRequest {
    response: PendingResponse,
    event: PairingEvent,
}

struct RequestGuard {
    broker: Arc<PairingBroker>,
    request_id: Option<String>,
}

impl RequestGuard {
    fn new(broker: Arc<PairingBroker>, request_id: String) -> Self {
        Self {
            broker,
            request_id: Some(request_id),
        }
    }

    fn disarm(&mut self) {
        self.request_id = None;
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        // BlueR reports Agent1.Cancel by dropping the callback future.
        if let Some(request_id) = self.request_id.take() {
            self.broker.cancel_pending(&request_id, "bluez-cancelled");
        }
    }
}

pub struct PairingBroker {
    sequence: AtomicU64,
    // Synchronous so a dropped callback can remove its prompt in Drop without a race.
    pending: Mutex<HashMap<String, PendingRequest>>,
    events: broadcast::Sender<PairingEvent>,
    identities: Arc<DeviceIdentityRegistry>,
    prompt_timeout: Duration,
}

impl PairingBroker {
    pub fn new(identities: Arc<DeviceIdentityRegistry>) -> Arc<Self> {
        Self::with_timeout(identities, PROMPT_TIMEOUT)
    }

    fn with_timeout(
        identities: Arc<DeviceIdentityRegistry>,
        prompt_timeout: Duration,
    ) -> Arc<Self> {
        let (events, _) = broadcast::channel(32);
        Arc::new(Self {
            sequence: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            events,
            identities,
            prompt_timeout,
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

    pub async fn respond(&self, params: &Value) -> Result<bool> {
        let request_id = params.require_string("request_id")?;
        let accept = params.require_bool("accept")?;
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let request = pending
            .get(request_id)
            .context("pairing request is no longer pending")?;
        if accept {
            request.response.validate(params)?;
        }
        tracing::info!(%request_id, accept, kind = %request.event.kind, device_key = %request.event.device_key, "pairing response received");
        let response = pending
            .remove(request_id)
            .context("pairing request is no longer pending")?
            .response;
        drop(pending);
        if accept {
            response.accept(params)?;
        } else {
            reject(response);
        }
        Ok(accept)
    }

    async fn request_pin(self: &Arc<Self>, adapter: String, device: Address) -> ReqResult<String> {
        let (sender, receiver) = oneshot::channel();
        let id = self.insert_request(
            "pin-code",
            &adapter,
            device,
            PendingResponse::Pin(sender),
            None,
            None,
        );
        self.wait_for_response(id, receiver).await
    }

    async fn request_passkey(self: &Arc<Self>, adapter: String, device: Address) -> ReqResult<u32> {
        let (sender, receiver) = oneshot::channel();
        let id = self.insert_request(
            "passkey",
            &adapter,
            device,
            PendingResponse::Passkey(sender),
            None,
            None,
        );
        self.wait_for_response(id, receiver).await
    }

    async fn request_unit(
        self: &Arc<Self>,
        kind: &str,
        adapter: String,
        device: Address,
        value: Option<String>,
        service: Option<String>,
    ) -> ReqResult<()> {
        let (sender, receiver) = oneshot::channel();
        let id = self.insert_request(
            kind,
            &adapter,
            device,
            PendingResponse::Unit(sender),
            value,
            service,
        );
        self.wait_for_response(id, receiver).await
    }

    fn insert_request(
        self: &Arc<Self>,
        kind: &str,
        adapter: &str,
        device: Address,
        pending: PendingResponse,
        value: Option<String>,
        service: Option<String>,
    ) -> String {
        let id = format!("pairing-{}", self.sequence.fetch_add(1, Ordering::Relaxed));
        let event = PairingEvent {
            event: "requested".to_string(),
            request_id: id.clone(),
            kind: kind.to_string(),
            device_key: self.identities.device_key(adapter, device),
            response_required: true,
            value,
            entered: None,
            service,
            reason: None,
            timeout_ms: self.prompt_timeout.as_millis() as u64,
        };
        tracing::info!(request_id = %id, %kind, device_key = %event.device_key, timeout_ms = event.timeout_ms, "pairing request started");
        self.pending
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(
                id.clone(),
                PendingRequest {
                    response: pending,
                    event: event.clone(),
                },
            );
        self.emit(event);
        self.schedule_timeout(id.clone());
        id
    }

    async fn wait_for_response<T>(
        self: &Arc<Self>,
        request_id: String,
        receiver: oneshot::Receiver<ReqResult<T>>,
    ) -> ReqResult<T> {
        let mut guard = RequestGuard::new(Arc::clone(self), request_id);
        let result = match receiver.await {
            Ok(result) => result,
            Err(_) => Err(ReqError::Canceled),
        };
        guard.disarm();
        result
    }

    fn cancel_pending(&self, request_id: &str, reason: &str) {
        let request = self
            .pending
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(request_id);
        if let Some(request) = request {
            tracing::warn!(%request_id, kind = %request.event.kind, device_key = %request.event.device_key, %reason, "pairing request cancelled");
            cancel(request.response);
            self.emit(request.event.cancelled(reason));
        }
    }

    fn schedule_timeout(self: &Arc<Self>, request_id: String) {
        let broker = Arc::clone(self);
        crate::task::spawn("pairing-timeout", async move {
            tokio::time::sleep(broker.prompt_timeout).await;
            broker.cancel_pending(&request_id, "timeout");
        });
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
        let device_key = self.identities.device_key(adapter, device);
        tracing::info!(request_id = %id, %kind, %device_key, "pairing display started");
        self.emit(PairingEvent {
            event: "display".to_string(),
            request_id: id.clone(),
            kind: kind.to_string(),
            device_key,
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
        tracing::warn!(%request_id, %kind, %reason, "pairing interaction cancelled");
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

fn reject(pending: PendingResponse) {
    finish(pending, ReqError::Rejected);
}

fn cancel(pending: PendingResponse) {
    finish(pending, ReqError::Canceled);
}

fn finish(pending: PendingResponse, error: ReqError) {
    match pending {
        PendingResponse::Pin(sender) => drop(sender.send(Err(error))),
        PendingResponse::Passkey(sender) => drop(sender.send(Err(error))),
        PendingResponse::Unit(sender) => drop(sender.send(Err(error))),
    }
}

macro_rules! pairing_callbacks {
    ($($name:ident($request_type:ty) -> $callback_type:ty |$broker:ident, $request:ident| $body:expr;)*) => {
        $(fn $name($broker: Arc<PairingBroker>) -> $callback_type {
            Box::new(move |$request: $request_type| {
                let $broker = Arc::clone(&$broker);
                Box::pin(async move { $body })
            })
        })*
    };
}

pairing_callbacks! {
    callback_pin(RequestPinCode) -> bluer::agent::RequestPinCodeFn |broker, request|
        broker.request_pin(request.adapter, request.device).await;
    callback_passkey(RequestPasskey) -> bluer::agent::RequestPasskeyFn |broker, request|
        broker.request_passkey(request.adapter, request.device).await;
    callback_confirmation(RequestConfirmation) -> bluer::agent::RequestConfirmationFn |broker, request|
        broker.request_unit("confirmation", request.adapter, request.device,
            Some(format!("{:06}", request.passkey)), None).await;
    callback_authorization(RequestAuthorization) -> bluer::agent::RequestAuthorizationFn |broker, request|
        broker.request_unit("authorization", request.adapter, request.device, None, None).await;
    callback_service(AuthorizeService) -> bluer::agent::AuthorizeServiceFn |broker, request|
        broker.request_unit("service-authorization", request.adapter, request.device,
            None, Some(request.service.to_string())).await;
}

fn callback_display_pin(broker: Arc<PairingBroker>) -> bluer::agent::DisplayPinCodeFn {
    Box::new(move |request: DisplayPinCode| {
        let broker = Arc::clone(&broker);
        Box::pin(async move {
            let adapter = request.adapter;
            let device = request.device;
            let id = broker.display("display-pin", &adapter, device, request.pincode, None);
            let watcher = Arc::clone(&broker);
            crate::task::spawn("pairing-display-pin", async move {
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
            crate::task::spawn("pairing-display-passkey", async move {
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

    use std::{sync::Arc, time::Duration};

    use bluer::agent::{ReqError, ReqResult};
    use tokio::{sync::broadcast, task::JoinHandle};

    use super::{PairingBroker, PairingEvent};

    fn confirmation_request(
        broker: &Arc<PairingBroker>,
        value: Option<String>,
    ) -> (broadcast::Receiver<PairingEvent>, JoinHandle<ReqResult<()>>) {
        let events = broker.subscribe();
        let request_broker = Arc::clone(broker);
        let address = "AA:BB:CC:DD:EE:FF".parse().unwrap();
        let request = tokio::spawn(async move {
            request_broker
                .request_unit("confirmation", "hci0".into(), address, value, None)
                .await
        });
        (events, request)
    }

    #[tokio::test]
    async fn rejects_invalid_passkeys_without_answering_bluez() {
        let broker = PairingBroker::new(DeviceIdentityRegistry::in_memory());
        let mut events = broker.subscribe();
        let request_broker = broker.clone();
        let address = "AA:BB:CC:DD:EE:FF".parse().unwrap();
        let request =
            tokio::spawn(
                async move { request_broker.request_passkey("hci0".into(), address).await },
            );
        let event = events.recv().await.unwrap();
        let result = broker
            .respond(&json!({ "request_id": event.request_id, "accept": true, "value": "12x" }))
            .await;
        assert!(result.is_err());
        assert!(!request.is_finished());
        let pending = broker
            .pending
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        assert!(pending.contains_key(&event.request_id));
        request.abort();
    }

    #[tokio::test]
    async fn prompt_event_uses_opaque_identity_and_accepts_response() {
        let identities = DeviceIdentityRegistry::in_memory();
        let address = "AA:BB:CC:DD:EE:FF".parse().unwrap();
        let expected_key = identities.device_key("hci0", address);
        let broker = PairingBroker::new(identities);
        let (mut events, request) = confirmation_request(&broker, Some("123456".into()));
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
    async fn dropped_bluez_request_is_cancelled_immediately() {
        let broker = PairingBroker::new(DeviceIdentityRegistry::in_memory());
        let (mut events, request) = confirmation_request(&broker, None);
        let requested = events.recv().await.unwrap();
        request.abort();
        let cancelled = events.recv().await.unwrap();
        assert_eq!(cancelled.request_id, requested.request_id);
        assert_eq!(cancelled.reason.as_deref(), Some("bluez-cancelled"));
        assert!(
            broker
                .respond(&json!({ "request_id": requested.request_id, "accept": true }))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn timeout_cancels_request_and_emits_terminal_event() {
        let broker = PairingBroker::with_timeout(
            DeviceIdentityRegistry::in_memory(),
            Duration::from_millis(10),
        );
        let (mut events, request) = confirmation_request(&broker, None);
        let requested = events.recv().await.unwrap();
        let cancelled = events.recv().await.unwrap();
        assert_eq!(cancelled.event, "cancelled");
        assert_eq!(cancelled.request_id, requested.request_id);
        assert_eq!(cancelled.reason.as_deref(), Some("timeout"));
        assert!(!cancelled.response_required);
        assert_eq!(request.await.unwrap(), Err(ReqError::Canceled));
        let pending = broker
            .pending
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        assert!(pending.is_empty());
    }
}
