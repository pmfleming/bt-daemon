use super::{
    AuthorizationDecision, IncomingBroker, ObexEvent, PendingAuthorization, authorization::Prompt,
};
use crate::{
    backend::{
        AdapterOperation, BackendError, BackendErrorKind, BluetoothBackend, DeviceOperation,
        ObexRemote, ObexTarget, OperationProgress,
    },
    model::Snapshot,
};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::{broadcast, oneshot};

struct Offline;
macro_rules! offline_backend {
    ($(fn $name:ident($($arg:ident: $ty:ty),*) -> $result:ty;)+) => {
        #[async_trait::async_trait]
        impl BluetoothBackend for Offline {
            fn subscribe_changes(&self) -> broadcast::Receiver<()> { broadcast::channel(1).1 }
            $(async fn $name(&self, $($arg:$ty),*) -> anyhow::Result<$result> {
                let _ = ($($arg,)*);
                Err(BackendError::new(BackendErrorKind::Unavailable, "hardware is unavailable in lifecycle tests").into())
            })+
        }
    };
}
offline_backend! {
    fn snapshot() -> Snapshot;
    fn set_powered(adapter_key: Option<&str>, powered: bool) -> Snapshot;
    fn set_scanning(adapter_key: Option<&str>, enabled: bool) -> Snapshot;
    fn adapter_operation(adapter_key: &str, operation: AdapterOperation, params: &Value) -> Snapshot;
    fn update_management(params: &Value) -> Snapshot;
    fn update_device_policy(device_key: &str, params: &Value) -> Snapshot;
    fn obex_target(device_key: &str) -> ObexTarget;
    fn obex_remote(source: &str, destination: &str) -> ObexRemote;
    fn device_operation(device_key: &str, operation: DeviceOperation, params: &Value, progress: OperationProgress) -> Snapshot;
}

fn broker() -> (Arc<IncomingBroker>, broadcast::Receiver<ObexEvent>) {
    let (events, receiver) = broadcast::channel(16);
    (IncomingBroker::new(Arc::new(Offline), events), receiver)
}
fn pending(broker: &IncomingBroker, id: &str) -> oneshot::Receiver<AuthorizationDecision> {
    let (sender, receiver) = oneshot::channel();
    broker
        .pending
        .lock()
        .unwrap()
        .insert(id.into(), PendingAuthorization { sender });
    receiver
}

#[tokio::test]
async fn cancellation_is_request_scoped_and_late_authorization_is_rejected() {
    let (broker, _) = broker();
    let cancelled = pending(&broker, "cancel");
    let accepted = pending(&broker, "accept");
    assert!(broker.cancel_transfer("cancel").await);
    assert!(matches!(
        cancelled.await.unwrap(),
        AuthorizationDecision::Cancel
    ));
    assert!(broker.respond("cancel", true).await.is_err());
    assert!(!broker.cancel_transfer("cancel").await);
    broker.respond("accept", true).await.unwrap();
    assert!(matches!(
        accepted.await.unwrap(),
        AuthorizationDecision::Accept
    ));
}

#[tokio::test]
async fn active_transfer_cancel_does_not_cancel_another_transfer() {
    let (broker, _) = broker();
    let (cancel, cancelled) = oneshot::channel();
    let (other, mut other_receiver) = oneshot::channel();
    broker
        .cancellations
        .lock()
        .await
        .insert("active".into(), cancel);
    broker
        .cancellations
        .lock()
        .await
        .insert("other".into(), other);
    assert!(broker.cancel_transfer("active").await);
    cancelled.await.unwrap();
    assert!(matches!(
        other_receiver.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(!broker.cancel_transfer("missing").await);
}

#[tokio::test]
async fn obex_agent_loss_cancels_all_waiting_authorizations() {
    let (broker, _) = broker();
    let first = pending(&broker, "first");
    let second = pending(&broker, "second");
    broker.cancel_authorizations().await;
    assert!(matches!(
        first.await.unwrap(),
        AuthorizationDecision::Cancel
    ));
    assert!(matches!(
        second.await.unwrap(),
        AuthorizationDecision::Cancel
    ));
    assert!(broker.pending.lock().unwrap().is_empty());
}

#[tokio::test]
async fn dropping_authorization_prompt_removes_it_and_emits_one_terminal_event() {
    let (broker, mut events) = broker();
    let receiver = pending(&broker, "cancelled-callback");
    let mut event = ObexEvent::outgoing("cancelled-callback", "opaque-peer", "file.txt", 0);
    event.direction = "incoming".into();
    let prompt = Prompt {
        broker: &broker,
        event,
    };
    drop(prompt);
    assert!(receiver.await.is_err());
    assert!(broker.pending.lock().unwrap().is_empty());
    let terminal = events.recv().await.unwrap();
    assert_eq!(terminal.event, "cancelled");
    assert_eq!(terminal.status, "cancelled");
    assert_eq!(terminal.request_id, "cancelled-callback");
    assert!(broker.respond("cancelled-callback", true).await.is_err());
    assert!(events.try_recv().is_err());
}
