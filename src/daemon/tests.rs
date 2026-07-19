use std::sync::{Arc, atomic::AtomicU64};

use anyhow::Result;
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast};

use crate::{
    backend::{BluetoothBackend, ObexRemote, ObexTarget},
    identity::DeviceIdentityRegistry,
    model::Snapshot,
    pairing::PairingBroker,
};

use super::{
    BluetoothDaemon, OperationCoordinator, OutgoingTransfers, ScanCoordinator,
    operation::OperationEvent, scan::ScanEvent,
};

struct TestBackend {
    complete: bool,
}

#[async_trait]
impl BluetoothBackend for TestBackend {
    fn subscribe_changes(&self) -> broadcast::Receiver<()> {
        broadcast::channel(1).1
    }

    async fn snapshot(&self) -> Result<Snapshot> {
        Ok(Snapshot {
            adapters: vec![],
            devices: vec![],
        })
    }

    async fn set_powered(&self, _: Option<&str>, _: bool) -> Result<Snapshot> {
        self.snapshot().await
    }

    async fn set_scanning(&self, _: Option<&str>, _: bool) -> Result<Snapshot> {
        self.snapshot().await
    }

    async fn adapter_operation(&self, _: &str, _: &str, _: &Value) -> Result<Snapshot> {
        self.snapshot().await
    }

    async fn obex_target(&self, _: &str) -> Result<ObexTarget> {
        Ok(ObexTarget {
            source: "00:00:00:00:00:00".into(),
            destination: "11:11:11:11:11:11".into(),
        })
    }

    async fn obex_remote(&self, _: &str, _: &str) -> Result<ObexRemote> {
        Ok(ObexRemote {
            device_key: "device-opaque".into(),
            name: "Test device".into(),
        })
    }

    async fn device_operation(&self, _: &str, _: &str, _: &Value) -> Result<Snapshot> {
        if self.complete {
            self.snapshot().await
        } else {
            std::future::pending().await
        }
    }
}

fn daemon(
    complete: bool,
) -> (
    BluetoothDaemon,
    broadcast::Receiver<OperationEvent>,
    broadcast::Receiver<ScanEvent>,
) {
    let (audio_events, _) = broadcast::channel(8);
    let (obex_events, _) = broadcast::channel(8);
    let backend: Arc<dyn BluetoothBackend> = Arc::new(TestBackend { complete });
    let incoming_obex = crate::obex::IncomingBroker::new(Arc::clone(&backend), obex_events.clone());
    let operations = OperationCoordinator::new(Arc::clone(&backend));
    let receiver = operations.subscribe();
    let scans = ScanCoordinator::new(Arc::clone(&backend));
    let scan_receiver = scans.subscribe();
    let outgoing_obex = OutgoingTransfers::new(Arc::clone(&backend), obex_events.clone());
    (
        BluetoothDaemon {
            backend,
            pairing: PairingBroker::new(DeviceIdentityRegistry::in_memory()),
            sequence: AtomicU64::new(1),
            subscriptions: Arc::new(Mutex::new(Default::default())),
            operations,
            scans,
            audio_events,
            obex_events,
            outgoing_obex,
            incoming_obex,
        },
        receiver,
        scan_receiver,
    )
}

#[tokio::test]
async fn operation_emits_started_and_completed_events() {
    let (daemon, mut events, _) = daemon(true);
    let response = daemon
        .operations
        .start(json!({ "key": "device-opaque", "operation": "connect" }))
        .await;
    assert_eq!(response["data"]["operation"]["state"], "queued");
    assert_eq!(events.recv().await.unwrap().event, "started");
    assert_eq!(events.recv().await.unwrap().event, "completed");
    assert!(daemon.operations.is_empty().await);
}

#[tokio::test]
async fn active_operation_can_be_cancelled() {
    let (daemon, mut events, _) = daemon(false);
    let response = daemon
        .operations
        .start(json!({ "key": "device-opaque", "operation": "pair" }))
        .await;
    let request_id = response["data"]["operation"]["request_id"]
        .as_str()
        .unwrap();
    assert_eq!(events.recv().await.unwrap().event, "started");
    let response: Value = serde_json::from_str(&daemon.cancel(request_id).await).unwrap();
    assert_eq!(response["data"]["kind"], "operation");
    let cancelled = events.recv().await.unwrap();
    assert_eq!(cancelled.event, "cancelled");
    assert_eq!(cancelled.request_id, request_id);
}

#[tokio::test]
async fn rejects_concurrent_operations_for_one_device() {
    let (daemon, mut events, _) = daemon(false);
    let first = daemon
        .operations
        .start(json!({ "key": "device-opaque", "operation": "connect" }))
        .await;
    let request_id = first["data"]["operation"]["request_id"].as_str().unwrap();
    assert_eq!(events.recv().await.unwrap().event, "started");
    let second = daemon
        .operations
        .start(json!({ "key": "device-opaque", "operation": "remove" }))
        .await;
    assert_eq!(second["error"]["code"], "device-busy");
    let _ = daemon.cancel(request_id).await;
}

#[tokio::test]
async fn scan_sessions_are_bounded_and_cancellable() {
    let (daemon, _, mut events) = daemon(true);
    let response = daemon
        .scans
        .start(&json!({
            "adapter_key": "adapter-opaque",
            "enabled": true,
            "timeout_ms": 1000
        }))
        .await;
    let request_id = response["data"]["scan"]["request_id"].as_str().unwrap();
    assert_eq!(events.recv().await.unwrap().state, "running");
    let response: Value = serde_json::from_str(&daemon.cancel(request_id).await).unwrap();
    assert_eq!(response["data"]["stopped"], request_id);
    assert_eq!(events.recv().await.unwrap().state, "cancelled");
    assert!(daemon.scans.is_empty().await);
}
