use std::sync::{Arc, Mutex as StdMutex};

use anyhow::Result;
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast, watch};

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
    BluetoothDaemon, ObexCoordinator, OperationCoordinator, ScanCoordinator, SharedSnapshot,
    operation::OperationEvent, scan::ScanEvent,
};

mod adapter;
mod routing;

type ScanningCalls = Arc<StdMutex<Vec<(Option<String>, bool)>>>;

struct TestBackend {
    complete: bool,
    fail_scan_stop: bool,
    scanning: ScanningCalls,
    adapter: Option<Arc<adapter::Effects>>,
}

#[async_trait]
impl BluetoothBackend for TestBackend {
    fn subscribe_changes(&self) -> broadcast::Receiver<()> {
        broadcast::channel(1).1
    }

    async fn snapshot(&self) -> Result<Snapshot> {
        if self
            .adapter
            .as_ref()
            .is_some_and(|effects| effects.fail_snapshot)
        {
            anyhow::bail!("snapshot unavailable after write");
        }
        Ok(Snapshot {
            adapters: vec![
                test_adapter("adapter-1"),
                test_adapter("adapter-2"),
                Adapter {
                    powered: false,
                    ..test_adapter("adapter-off")
                },
                Adapter {
                    discovering: false,
                    ..test_adapter("adapter-idle")
                },
            ],
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

    async fn adapter_operation(
        &self,
        key: &str,
        operation: AdapterOperation,
        params: &Value,
    ) -> Result<()> {
        if let Some(effects) = &self.adapter {
            effects.apply(key, operation, params).await?;
        }
        Ok(())
    }

    async fn update_management(&self, _: &Value) -> Result<Snapshot> {
        self.snapshot().await
    }

    async fn update_device_policy(&self, _: &str, _: &Value) -> Result<Snapshot> {
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
        adapter: None,
    })
}

fn daemon(
    complete: bool,
) -> (
    BluetoothDaemon,
    broadcast::Receiver<OperationEvent>,
    broadcast::Receiver<ScanEvent>,
) {
    let backend = test_backend(complete, false, Arc::new(StdMutex::new(Vec::new())));
    let operations = OperationCoordinator::new(Arc::clone(&backend));
    let receiver = operations.subscribe();
    let scans = ScanCoordinator::new(Arc::clone(&backend));
    let scan_receiver = scans.subscribe();
    let obex = ObexCoordinator::new(Arc::clone(&backend));
    let (snapshots, _) = watch::channel(SharedSnapshot::Available(Arc::new(Snapshot::default())));
    let (audio_snapshots, _) =
        watch::channel(json!({ "ok": true, "data": { "audio_devices": [] } }));
    (
        BluetoothDaemon {
            backend,
            pairing: PairingBroker::new(DeviceIdentityRegistry::in_memory()),
            subscriptions: Arc::new(shelllist_daemon_tokio::OwnedTaskRegistry::default()),
            scan_owner_watches: Arc::new(Mutex::new(Default::default())),
            tasks: Arc::new(shelllist_daemon_tokio::TaskGroup::default()),
            operations,
            scans,
            snapshots,
            audio_snapshots,
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

async fn operation_request_id(
    response: &Value,
    events: &mut broadcast::Receiver<OperationEvent>,
) -> String {
    let id = response["data"]["operation"]["request_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let started = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(started.event, "started");
    assert_eq!(started.request_id, id);
    id
}

#[tokio::test]
async fn operation_completion_and_cancellation_publish_one_recoverable_terminal_result() {
    for cancel_first in [false, true] {
        let (daemon, mut events, _) = daemon(true);
        let response = start_operation(&daemon, "connect").await;
        let id = response["data"]["operation"]["request_id"]
            .as_str()
            .unwrap();
        assert_eq!(response["data"]["operation"]["state"], "queued");
        let cancelled = cancel_first && daemon.operations.cancel_owned(id, None).await;
        let expected = if cancelled { "cancelled" } else { "completed" };
        let mut sequence = Vec::new();
        loop {
            let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(event.request_id, id);
            assert!(["started", "progress", expected].contains(&event.event.as_str()));
            let terminal = event.event == expected;
            sequence.push(event);
            if terminal {
                break;
            }
        }
        if !cancelled {
            assert_eq!(
                sequence
                    .iter()
                    .map(|event| event.event.as_str())
                    .collect::<Vec<_>>(),
                ["started", "progress", "completed"]
            );
            assert_eq!(sequence[1].stage, "connecting");
        }
        tokio::task::yield_now().await;
        assert!(
            events.try_recv().is_err(),
            "no progress or duplicate terminal event after {}",
            expected
        );
        assert!(!daemon.operations.cancel_owned(id, None).await);
        let recovered = daemon.operations.snapshot().await;
        assert_eq!(recovered["active"], json!([]));
        assert_eq!(recovered["recent"].as_array().unwrap().len(), 1);
        assert_eq!(recovered["recent"][0]["request_id"], id);
        assert_eq!(recovered["recent"][0]["event"], expected);
        assert_eq!(
            recovered["recent"][0],
            serde_json::to_value(sequence.last().unwrap()).unwrap()
        );
    }
}

#[tokio::test]
async fn owner_loss_releases_global_scan_without_stopping_another_owners_lease() {
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

    scans.stop_owner(":global-owner").await;
    assert!(!scans.contains(global_id).await);
    assert!(scans.contains(targeted_id).await);
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
