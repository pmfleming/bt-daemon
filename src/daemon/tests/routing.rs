use super::daemon;
use crate::daemon::{SharedSnapshot, load_snapshot, receive_refresh, send_changed};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::broadcast;

#[tokio::test]
async fn dispatch_routes_cached_snapshots_registry_and_request_recovery() {
    let (daemon, _, _) = daemon(true);
    let registry = daemon
        .dispatch_call("bluetooth.protocol.describe", json!({}), None, None)
        .await;
    assert_eq!(registry["ok"], true);
    assert!(registry["data"]["registry"].is_object());
    let snapshot = daemon
        .dispatch_call("bluetooth.snapshot", json!({}), None, None)
        .await;
    assert!(snapshot["data"]["snapshot"]["devices"].is_array());
    let audio = daemon
        .dispatch_call("bluetooth.audio.snapshot", json!({}), None, None)
        .await;
    assert_eq!(audio["data"]["audio_devices"], json!([]));
    let requests = daemon
        .dispatch_call("bluetooth.requests.snapshot", json!({}), None, None)
        .await;
    assert_eq!(requests["data"]["requests"]["pairing"]["active"], json!([]));
    assert_eq!(
        requests["data"]["requests"]["operations"]["active"],
        json!([])
    );
    assert_eq!(requests["data"]["requests"]["scans"]["active"], json!([]));
    let mut watched = daemon.snapshots.subscribe();
    for (state, code) in [
        (SharedSnapshot::Loading, "snapshot-loading"),
        (
            SharedSnapshot::Unavailable(Arc::new("offline".into())),
            "bluez-unavailable",
        ),
    ] {
        send_changed(&daemon.snapshots, state.clone());
        assert!(watched.has_changed().unwrap());
        watched.borrow_and_update();
        send_changed(&daemon.snapshots, state);
        assert!(!watched.has_changed().unwrap());
        let response = daemon
            .dispatch_call("bluetooth.snapshot", json!({}), None, None)
            .await;
        assert_eq!(response["error"]["code"], code);
    }
    assert!(matches!(
        load_snapshot(&daemon.backend).await,
        SharedSnapshot::Available(_)
    ));
    let (sender, mut receiver) = broadcast::channel(1);
    sender.send(()).unwrap();
    sender.send(()).unwrap();
    assert!(receive_refresh(&mut receiver, Duration::ZERO).await);
    assert!(matches!(
        receiver.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    ));
    drop(sender);
    assert!(!receive_refresh(&mut receiver, Duration::ZERO).await);
}

#[tokio::test]
async fn dispatch_validates_commands_and_routes_supported_backend_mutations() {
    let (daemon, _, _) = daemon(true);
    for method in [
        "bluetooth.scan",
        "bluetooth.obex.send",
        "bluetooth.obex.respond",
        "bluetooth.audio.setProfile",
        "bluetooth.audio.setDefault",
        "bluetooth.device.operation",
        "bluetooth.setPowered",
        "bluetooth.adapter.operation",
        "bluetooth.device.policy.update",
    ] {
        let response = daemon
            .dispatch_call(method, json!({"enabled": "invalid"}), None, None)
            .await;
        assert_eq!(
            response["error"]["code"], "validation-error",
            "{method}: {response}"
        );
    }
    for (method, params) in [
        (
            "bluetooth.setPowered",
            json!({"adapter_key": 42, "powered": false}),
        ),
        (
            "bluetooth.scan",
            json!({"adapter_key": 42, "enabled": true}),
        ),
    ] {
        let response = daemon.dispatch_call(method, params, None, None).await;
        assert_eq!(response["error"]["code"], "validation-error");
    }
    let pairing = daemon
        .dispatch_call("bluetooth.pairing.respond", json!({}), None, None)
        .await;
    assert_eq!(pairing["error"]["code"], "pairing-response-rejected");
    let unsupported = daemon
        .dispatch_call("bluetooth.unknown", json!({}), None, None)
        .await;
    assert_eq!(unsupported["error"]["code"], "unsupported-method");
    for (method, params) in [
        ("bluetooth.setPowered", json!({"powered": true})),
        (
            "bluetooth.adapter.operation",
            json!({"key": "adapter-1", "operation": "set-discoverable", "discoverable": true}),
        ),
        (
            "bluetooth.management.update",
            json!({"reconnect_on_resume": false}),
        ),
        (
            "bluetooth.device.policy.update",
            json!({"key": "device", "trust_after_pair": false}),
        ),
    ] {
        let response = daemon.dispatch_call(method, params, None, None).await;
        assert_eq!(response["ok"], true, "{method}: {response}");
        assert_eq!(
            response["data"]["snapshot"]["adapters"]
                .as_array()
                .unwrap()
                .len(),
            4
        );
    }
}

#[tokio::test]
async fn routed_operations_and_scans_keep_cancellation_scoped_to_the_caller_and_request() {
    let (daemon, mut events, mut scan_events) = daemon(false);
    let operation = daemon
        .dispatch_call(
            "bluetooth.device.operation",
            json!({"key": "device-opaque", "operation": "connect"}),
            Some(":owner"),
            None,
        )
        .await;
    let id = super::operation_request_id(&operation, &mut events).await;
    let conflicting = super::start_operation(&daemon, "remove").await;
    assert_eq!(conflicting["error"]["code"], "device-busy");
    for key in ["adapter-off", "adapter-idle", "adapter-missing"] {
        let response = daemon
            .dispatch_call(
                "bluetooth.scan",
                json!({"adapter_key": key}),
                Some(":owner"),
                None,
            )
            .await;
        assert_eq!(response["error"]["code"], "scan-start-failed");
        assert_eq!(daemon.scans.snapshot().await["active"], json!([]));
    }
    let scan = daemon
        .dispatch_call(
            "bluetooth.scan",
            json!({"adapter_key": "adapter-1", "timeout_ms": 1000}),
            Some(":owner"),
            None,
        )
        .await;
    let scan_id = scan["data"]["scan"]["request_id"].as_str().unwrap();
    assert_eq!(scan_events.recv().await.unwrap().state, "running");
    let cancelled = assert_owned_cancellation(&daemon, &id).await;
    assert_eq!(cancelled["data"]["kind"], "operation");
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap()
            .unwrap();
        if event.event == "cancelled" {
            assert_eq!(event.request_id, id);
            break;
        }
    }
    assert_eq!(
        daemon.scans.snapshot().await["active"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let cancelled = assert_owned_cancellation(&daemon, scan_id).await;
    assert_eq!(cancelled["data"]["stopped"], scan_id);
    assert_eq!(scan_events.recv().await.unwrap().state, "cancelled");
    assert_eq!(daemon.scans.snapshot().await["active"], json!([]));
}

async fn assert_owned_cancellation(daemon: &crate::daemon::BluetoothDaemon, id: &str) -> Value {
    let denied: Value =
        serde_json::from_str(&daemon.cancel_owned(id, Some(":other")).await).unwrap();
    assert_eq!(denied["error"]["code"], "request-not-found");
    let cancelled: Value =
        serde_json::from_str(&daemon.cancel_owned(id, Some(":owner")).await).unwrap();
    assert_eq!(cancelled["ok"], true);
    cancelled
}
