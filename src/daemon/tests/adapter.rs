use super::*;
use std::{
    sync::atomic::{AtomicBool, Ordering},
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
    for fail in [
        None,
        Some(AdapterOperation::SetAlias),
        Some(AdapterOperation::SetDiscoverableTimeout),
        Some(AdapterOperation::SetPairableTimeout),
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
                patch("outcomes"),
            )
            .await;
            assert_eq!(response["ok"], true); // terminal result, NOT atomic success
            let batch = &response["data"]["adapter_batch"];
            assert_eq!(batch["key"], "outcomes");
            assert_eq!(batch["snapshot_error"].is_object(), fail_snapshot);
            assert_eq!(response["data"]["snapshot"].is_object(), !fail_snapshot);
            let mut stopped = false;
            let expected = [
                ("alias", AdapterOperation::SetAlias, json!("Computer")),
                (
                    "discoverable_timeout",
                    AdapterOperation::SetDiscoverableTimeout,
                    json!(120),
                ),
                (
                    "pairable_timeout",
                    AdapterOperation::SetPairableTimeout,
                    json!(0),
                ),
            ];
            let mut attempted = 0;
            for (i, (field, operation, value)) in expected.iter().enumerate() {
                let outcome = &batch["outcomes"][i];
                assert_eq!(outcome["field"], *field);
                assert_eq!(&outcome["value"], value);
                let state = if stopped {
                    "not-attempted"
                } else {
                    attempted += 1;
                    if fail == Some(*operation) {
                        stopped = true;
                        "unknown"
                    } else {
                        "applied"
                    }
                };
                assert_eq!(outcome["state"], state);
                assert_eq!(outcome["error"].is_object(), state == "unknown");
            }
            assert_eq!(effects.calls.lock().unwrap().len(), attempted);
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

#[tokio::test]
async fn adapter_batch_fixture_is_current() {
    let effects = Arc::new(Effects {
        fail: Some(AdapterOperation::SetDiscoverableTimeout),
        fail_snapshot: true,
        ..Default::default()
    });
    let response = crate::api::dispatch(
        backend(&effects),
        "bluetooth.adapter.update",
        patch("adapter-opaque"),
    )
    .await;
    let mut fixture = crate::protocol::contract_fixture();
    if std::env::var_os("BT_DAEMON_UPDATE_CONTRACT_FIXTURE").is_some() {
        fixture["adapter_update"] = response;
        fixture["registry"] = crate::protocol::registry();
        std::fs::write(
            concat!(env!("CARGO_MANIFEST_DIR"), "/test_support/bt-api-v1.json"),
            serde_json::to_string_pretty(&fixture).unwrap() + "\n",
        )
        .unwrap();
        return;
    }
    assert_eq!(fixture["adapter_update"], response);
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
