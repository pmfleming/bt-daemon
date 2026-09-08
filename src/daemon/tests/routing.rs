use super::daemon;
use crate::daemon::{SharedSnapshot, load_snapshot, receive_refresh, snapshot_response};
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
}

#[tokio::test]
async fn dispatch_validates_hardware_commands_before_any_io() {
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
    let pairing = daemon
        .dispatch_call("bluetooth.pairing.respond", json!({}), None, None)
        .await;
    assert_eq!(pairing["error"]["code"], "pairing-response-rejected");
    let unsupported = daemon
        .dispatch_call("bluetooth.unknown", json!({}), None, None)
        .await;
    assert_eq!(unsupported["error"]["code"], "unsupported-method");
}

#[tokio::test]
async fn dispatch_delegates_backend_mutations_to_the_injected_backend() {
    let (daemon, _, _) = daemon(true);
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
            2
        );
    }
}

#[tokio::test]
async fn routed_operations_preserve_caller_ownership() {
    let (daemon, mut events, _) = daemon(false);
    let operation = daemon
        .dispatch_call(
            "bluetooth.device.operation",
            json!({"key": "device-opaque", "operation": "connect"}),
            Some(":owner"),
            None,
        )
        .await;
    let id = super::operation_request_id(&operation, &mut events).await;
    let cancelled = assert_owned_cancellation(&daemon, &id).await;
    assert_eq!(cancelled["data"]["kind"], "operation");
}

#[tokio::test]
async fn routed_scans_preserve_caller_ownership_without_a_bus_connection() {
    let (daemon, _, _) = daemon(true);
    let scan = daemon
        .dispatch_call(
            "bluetooth.scan",
            json!({"adapter_key": "adapter-1", "timeout_ms": 1000}),
            Some(":owner"),
            None,
        )
        .await;
    let id = scan["data"]["scan"]["request_id"].as_str().unwrap();
    assert_owned_cancellation(&daemon, id).await;
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

#[tokio::test]
async fn snapshot_states_and_refresh_channel_lifecycle_are_explicit() {
    assert_eq!(
        snapshot_response(&SharedSnapshot::Loading)["error"]["code"],
        "snapshot-loading"
    );
    let unavailable = snapshot_response(&SharedSnapshot::Unavailable(Arc::new("offline".into())));
    assert_eq!(unavailable["error"]["code"], "bluez-unavailable");
    assert_eq!(unavailable["error"]["message"], "offline");
    let (daemon, _, _) = daemon(true);
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
