use std::sync::{Arc, Mutex as StdMutex, atomic::AtomicU64};

use anyhow::Result;
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast};

use crate::{
    backend::{
        AdapterOperation, BluetoothBackend, DeviceOperation, ObexRemote, ObexTarget,
        OperationProgress,
    },
    identity::DeviceIdentityRegistry,
    model::{Adapter, Snapshot},
    pairing::PairingBroker,
};

use super::{
    BluetoothDaemon, ObexCoordinator, OperationCoordinator, ScanCoordinator,
    operation::OperationEvent, scan::ScanEvent,
};

type ScanningCalls = Arc<StdMutex<Vec<(Option<String>, bool)>>>;

struct TestBackend {
    complete: bool,
    fail_scan_stop: bool,
    scanning: ScanningCalls,
}

#[async_trait]
impl BluetoothBackend for TestBackend {
    fn subscribe_changes(&self) -> broadcast::Receiver<()> {
        broadcast::channel(1).1
    }

    async fn snapshot(&self) -> Result<Snapshot> {
        Ok(Snapshot {
            adapters: vec![test_adapter("adapter-1"), test_adapter("adapter-2")],
            devices: vec![],
            ..Snapshot::default()
        })
    }

    async fn set_powered(&self, _: Option<&str>, _: bool) -> Result<Snapshot> {
        self.snapshot().await
    }

    async fn set_scanning(&self, adapter: Option<&str>, enabled: bool) -> Result<Snapshot> {
        self.scanning
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((adapter.map(str::to_string), enabled));
        if !enabled && self.fail_scan_stop {
            anyhow::bail!("simulated scan stop failure");
        }
        self.snapshot().await
    }

    async fn adapter_operation(&self, _: &str, _: AdapterOperation, _: &Value) -> Result<Snapshot> {
        self.snapshot().await
    }

    async fn update_management(&self, _: &Value) -> Result<Snapshot> {
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

    async fn device_operation(
        &self,
        _: &str,
        _: DeviceOperation,
        _: &Value,
        progress: OperationProgress,
    ) -> Result<Snapshot> {
        progress("connecting");
        if self.complete {
            self.snapshot().await
        } else {
            std::future::pending().await
        }
    }
}

fn test_adapter(key: &str) -> Adapter {
    Adapter {
        key: key.into(),
        name: key.into(),
        alias: key.into(),
        address_type: "public".into(),
        powered: true,
        discovering: true,
        pairable: true,
        ..Adapter::default()
    }
}

fn test_backend(
    complete: bool,
    fail_scan_stop: bool,
    scanning: ScanningCalls,
) -> Arc<dyn BluetoothBackend> {
    Arc::new(TestBackend {
        complete,
        fail_scan_stop,
        scanning,
    })
}

fn daemon(
    complete: bool,
) -> (
    BluetoothDaemon,
    broadcast::Receiver<OperationEvent>,
    broadcast::Receiver<ScanEvent>,
) {
    let (audio_events, _) = broadcast::channel(8);
    let backend = test_backend(complete, false, Arc::new(StdMutex::new(Vec::new())));
    let operations = OperationCoordinator::new(Arc::clone(&backend));
    let receiver = operations.subscribe();
    let scans = ScanCoordinator::new(Arc::clone(&backend));
    let scan_receiver = scans.subscribe();
    let obex = ObexCoordinator::new(Arc::clone(&backend));
    (
        BluetoothDaemon {
            backend,
            pairing: PairingBroker::new(DeviceIdentityRegistry::in_memory()),
            sequence: AtomicU64::new(1),
            subscriptions: Arc::new(Mutex::new(Default::default())),
            scan_owner_watches: Arc::new(Mutex::new(Default::default())),
            operations,
            scans,
            audio_events,
            obex,
        },
        receiver,
        scan_receiver,
    )
}

async fn start_operation(daemon: &BluetoothDaemon, operation: &str) -> Value {
    daemon
        .operations
        .start(json!({ "key": "device-opaque", "operation": operation }))
        .await
}

async fn start_scan(scans: &ScanCoordinator, adapter: &str, timeout_ms: u64) -> Value {
    scans
        .start(
            &json!({
                "adapter_key": adapter,
                "enabled": true,
                "timeout_ms": timeout_ms
            }),
            ":test-owner",
        )
        .await
}

fn stopped_calls(scanning: &ScanningCalls) -> Vec<(Option<String>, bool)> {
    scanning
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .iter()
        .filter(|(_, enabled)| !enabled)
        .cloned()
        .collect()
}

#[tokio::test]
async fn operation_emits_started_and_completed_events() {
    let (daemon, mut events, _) = daemon(true);
    let response = start_operation(&daemon, "connect").await;
    assert_eq!(response["data"]["operation"]["state"], "queued");
    assert_eq!(events.recv().await.unwrap().event, "started");
    let progress = events.recv().await.unwrap();
    assert_eq!(progress.event, "progress");
    assert_eq!(progress.stage, "connecting");
    assert_eq!(events.recv().await.unwrap().event, "completed");
    assert!(daemon.operations.is_empty().await);
    let recovered = daemon.operations.snapshot().await;
    assert_eq!(recovered["active"].as_array().unwrap().len(), 0);
    assert_eq!(recovered["recent"][0]["event"], "completed");
}

#[tokio::test]
async fn active_operation_can_be_cancelled() {
    let (daemon, mut events, _) = daemon(false);
    let response = start_operation(&daemon, "pair").await;
    let request_id = response["data"]["operation"]["request_id"]
        .as_str()
        .unwrap();
    assert_eq!(events.recv().await.unwrap().event, "started");
    let response: Value = serde_json::from_str(&daemon.cancel(request_id).await).unwrap();
    assert_eq!(response["data"]["kind"], "operation");
    let mut cancelled = events.recv().await.unwrap();
    while cancelled.event != "cancelled" {
        cancelled = events.recv().await.unwrap();
    }
    assert_eq!(cancelled.request_id, request_id);
}

#[tokio::test]
async fn rejects_concurrent_operations_for_one_device() {
    let (daemon, mut events, _) = daemon(false);
    let first = start_operation(&daemon, "connect").await;
    let request_id = first["data"]["operation"]["request_id"].as_str().unwrap();
    assert_eq!(events.recv().await.unwrap().event, "started");
    let second = start_operation(&daemon, "remove").await;
    assert_eq!(second["error"]["code"], "device-busy");
    let _ = daemon.cancel(request_id).await;
}

#[tokio::test]
async fn scan_rejects_malformed_optional_parameters() {
    let (daemon, _, _) = daemon(true);
    let response = daemon
        .scans
        .start(
            &json!({ "adapter_key": 42, "enabled": true }),
            ":test-owner",
        )
        .await;
    assert_eq!(response["error"]["code"], "validation-error");
    let response = daemon
        .scans
        .start(&json!({ "enabled": "yes" }), ":test-owner")
        .await;
    assert_eq!(response["error"]["code"], "validation-error");
    assert_eq!(daemon.obex.cancel("missing-transfer").await, None);
}

#[tokio::test]
async fn overlapping_global_scan_stops_only_uncovered_adapters() {
    let scanning = Arc::new(StdMutex::new(Vec::new()));
    let backend = test_backend(true, false, Arc::clone(&scanning));
    let scans = ScanCoordinator::new(backend);
    let global = scans
        .start(
            &json!({ "enabled": true, "timeout_ms": 60_000 }),
            ":global-owner",
        )
        .await;
    let global_id = global["data"]["scan"]["request_id"].as_str().unwrap();
    let targeted = start_scan(&scans, "adapter-1", 60_000).await;
    let targeted_id = targeted["data"]["scan"]["request_id"].as_str().unwrap();

    scans.stop(Some(global_id), "cancelled").await;
    assert_eq!(
        stopped_calls(&scanning),
        vec![(Some("adapter-2".into()), false)]
    );

    scans.stop(Some(targeted_id), "cancelled").await;
    let stopped = stopped_calls(&scanning);
    assert_eq!(
        stopped,
        vec![
            (Some("adapter-2".into()), false),
            (Some("adapter-1".into()), false)
        ]
    );
}

#[tokio::test]
async fn scan_sessions_are_bounded_and_cancellable() {
    let (daemon, _, mut events) = daemon(true);
    let response = start_scan(&daemon.scans, "adapter-1", 1000).await;
    let request_id = response["data"]["scan"]["request_id"].as_str().unwrap();
    assert_eq!(events.recv().await.unwrap().state, "running");
    let response: Value = serde_json::from_str(&daemon.cancel(request_id).await).unwrap();
    assert_eq!(response["data"]["stopped"], request_id);
    assert_eq!(events.recv().await.unwrap().state, "cancelled");
    assert!(daemon.scans.is_empty().await);
}

#[tokio::test]
async fn scan_owner_loss_releases_only_that_owners_leases() {
    let scanning = Arc::new(StdMutex::new(Vec::new()));
    let backend = test_backend(true, false, Arc::clone(&scanning));
    let scans = ScanCoordinator::new(backend);
    let first = scans
        .start(
            &json!({ "adapter_key": "adapter-1", "timeout_ms": 60_000 }),
            ":owner-one",
        )
        .await;
    let second = scans
        .start(
            &json!({ "adapter_key": "adapter-1", "timeout_ms": 60_000 }),
            ":owner-two",
        )
        .await;
    assert_eq!(first["ok"], true);
    assert_eq!(second["ok"], true);

    scans.stop_owner(":owner-one").await;
    assert!(stopped_calls(&scanning).is_empty());
    scans.stop_owner(":owner-two").await;
    assert_eq!(
        stopped_calls(&scanning),
        vec![(Some("adapter-1".into()), false)]
    );
}

#[tokio::test]
async fn failed_scan_stop_is_reported_and_remains_retryable() {
    let scanning = Arc::new(StdMutex::new(Vec::new()));
    let backend = test_backend(true, true, scanning);
    let scans = ScanCoordinator::new(backend);
    let response = start_scan(&scans, "adapter-1", 60_000).await;
    let request_id = response["data"]["scan"]["request_id"].as_str().unwrap();

    let stopped = scans.stop(Some(request_id), "cancelled").await;
    assert_eq!(stopped["error"]["code"], "scan-stop-failed");
    assert!(scans.contains(request_id).await);
}
