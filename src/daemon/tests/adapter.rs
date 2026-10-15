use super::TestBackend;
use crate::backend::{AdapterOperation, BluetoothBackend};
use anyhow::Result;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

#[derive(Default)]
pub(super) struct Effects {
    calls: StdMutex<Vec<(String, AdapterOperation, Value)>>,
    fail: Option<AdapterOperation>,
    pub(super) fail_snapshot: bool,
    block_once: AtomicBool,
    started: Notify,
    release: Notify,
}

impl Effects {
    pub(super) async fn apply(
        &self,
        key: &str,
        operation: AdapterOperation,
        params: &Value,
    ) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push((key.into(), operation, params.clone()));
        if self.block_once.swap(false, Ordering::SeqCst) {
            self.started.notify_one();
            self.release.notified().await;
        }
        if self.fail == Some(operation) {
            anyhow::bail!("write acknowledgement lost");
        }
        Ok(())
    }
}

fn backend(effects: &Arc<Effects>) -> Arc<dyn BluetoothBackend> {
    Arc::new(TestBackend {
        complete: true,
        fail_scan_stop: false,
        scanning: Arc::new(StdMutex::new(Vec::new())),
        adapter: Some(Arc::clone(effects)),
    })
}

fn patch(key: &str) -> Value {
    json!({"key": key, "changes": {"pairable_timeout": 0, "alias": "Computer", "discoverable_timeout": 120}})
}

#[tokio::test]
async fn whole_patch_is_validated_before_any_write() {
    let effects = Arc::new(Effects::default());
    for params in [
        json!({"key": "", "changes": {"alias": "ok"}}),
        json!({"key": "a", "changes": {}}),
        json!({"key": "a", "changes": []}),
        json!({"key": "a", "changes": {"alias": ""}}),
        json!({"key": "a", "changes": {"alias": null}}),
        json!({"key": "a", "changes": {"alias": "ok", "pairable_timeout": -1}}),
        json!({"key": "a", "changes": {"alias": "ok", "pairable_timeout": 1.5}}),
        json!({"key": "a", "changes": {"alias": "ok", "pairable_timeout": 4294967296_u64}}),
        json!({"key": "a", "changes": {"alias": "ok", "pairable_timeout": "120"}}),
        json!({"key": "a", "changes": {"alias": "ok", "pairable_timeout": null}}),
        json!({"key": "a", "changes": {"alias": "ok", "unknown": true}}),
        json!({"key": "a", "changes": {"alias": "ok"}, "unknown": true}),
    ] {
        let response =
            crate::api::dispatch(backend(&effects), "bluetooth.adapter.update", params).await;
        assert_eq!(response["error"]["code"], "validation-error");
        assert!(effects.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn outcomes_preserve_applied_unknown_and_unattempted_fields() {
    for (fail, states, attempted) in [
        (None, ["applied"; 3], 3),
        (
            Some(AdapterOperation::SetAlias),
            ["unknown", "not-attempted", "not-attempted"],
            1,
        ),
        (
            Some(AdapterOperation::SetDiscoverableTimeout),
            ["applied", "unknown", "not-attempted"],
            2,
        ),
        (
            Some(AdapterOperation::SetPairableTimeout),
            ["applied", "applied", "unknown"],
            3,
        ),
    ] {
        for fail_snapshot in [false, true] {
            let effects = Arc::new(Effects {
                fail,
                fail_snapshot,
                ..Default::default()
            });
            let response = crate::api::dispatch(
                backend(&effects),
                "bluetooth.adapter.update",
                patch("adapter-opaque"),
            )
            .await;
            assert_eq!(response["ok"], true); // terminal result, NOT atomic success
            let batch = &response["data"]["adapter_batch"];
            assert_eq!(batch["key"], "adapter-opaque");
            assert_eq!(batch["snapshot_error"].is_object(), fail_snapshot);
            assert_eq!(response["data"]["snapshot"].is_object(), !fail_snapshot);
            let expected = [
                ("alias", json!("Computer")),
                ("discoverable_timeout", json!(120)),
                ("pairable_timeout", json!(0)),
            ];
            assert_eq!(batch["outcomes"].as_array().unwrap().len(), expected.len());
            for (i, (field, value)) in expected.iter().enumerate() {
                let outcome = &batch["outcomes"][i];
                assert_eq!(outcome["field"], *field);
                assert_eq!(&outcome["value"], value);
                assert_eq!(outcome["state"], states[i]);
                assert_eq!(outcome["error"].is_object(), states[i] == "unknown");
            }
            assert_eq!(effects.calls.lock().unwrap().len(), attempted);
            if fail == Some(AdapterOperation::SetDiscoverableTimeout) && fail_snapshot {
                assert_batch_fixture(&response);
            }
        }
    }
    // A one-field save stays one field; the daemon never invents other changes.
    let effects = Arc::new(Effects::default());
    let response = crate::api::dispatch(
        backend(&effects),
        "bluetooth.adapter.update",
        json!({"key":"one", "changes":{"pairable_timeout":u32::MAX}}),
    )
    .await;
    assert_eq!(
        response["data"]["adapter_batch"]["outcomes"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(effects.calls.lock().unwrap()[0].2["timeout"], u32::MAX);
}

fn assert_batch_fixture(response: &Value) {
    let mut fixture = crate::protocol::contract_fixture();
    if std::env::var_os("BT_DAEMON_UPDATE_CONTRACT_FIXTURE").is_some() {
        fixture["adapter_update"] = response.clone();
        fixture["registry"] = crate::protocol::registry();
        std::fs::write(
            concat!(env!("CARGO_MANIFEST_DIR"), "/test_support/bt-api-v1.json"),
            serde_json::to_string_pretty(&fixture).unwrap() + "\n",
        )
        .unwrap();
        return;
    }
    assert_eq!(&fixture["adapter_update"], response);
}

#[tokio::test]
async fn submitted_batch_survives_waiter_abort_and_serializes_legacy_settings() {
    let effects = Arc::new(Effects {
        block_once: AtomicBool::new(true),
        ..Default::default()
    });
    let waiter = tokio::spawn(crate::api::dispatch(
        backend(&effects),
        "bluetooth.adapter.update",
        patch("captured"),
    ));
    tokio::time::timeout(Duration::from_secs(2), effects.started.notified())
        .await
        .unwrap();
    assert!(crate::task::device_gate("captured").try_lock().is_ok());
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    let legacy = tokio::spawn(crate::api::dispatch(
        backend(&effects),
        "bluetooth.adapter.operation",
        json!({"key":"captured", "operation":"set-pairable", "pairable":true}),
    ));
    // A different adapter can complete while the first batch owns its gate.
    let other = tokio::time::timeout(
        Duration::from_secs(2),
        crate::api::dispatch(
            backend(&effects),
            "bluetooth.adapter.update",
            patch("other"),
        ),
    )
    .await
    .unwrap();
    assert_eq!(other["ok"], true);
    assert!(!legacy.is_finished());
    assert_eq!(
        effects
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _, _)| key == "captured")
            .count(),
        1
    );
    effects.release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(2), legacy)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result["ok"], true);
    let calls = effects.calls.lock().unwrap();
    assert_eq!(
        calls
            .iter()
            .filter(|(key, _, _)| key == "captured")
            .map(|(_, op, _)| *op)
            .collect::<Vec<_>>(),
        [
            AdapterOperation::SetAlias,
            AdapterOperation::SetDiscoverableTimeout,
            AdapterOperation::SetPairableTimeout,
            AdapterOperation::SetPairable
        ]
    );
}
