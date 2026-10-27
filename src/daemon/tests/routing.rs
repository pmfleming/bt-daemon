use super::daemon;
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn dispatch_validates_commands_and_routes_supported_methods() {
    let (daemon, _, _) = daemon(true);
    for (method, field) in [
        ("bluetooth.protocol.describe", "registry"),
        ("bluetooth.snapshot", "snapshot"),
        ("bluetooth.audio.snapshot", "audio_devices"),
        ("bluetooth.requests.snapshot", "requests"),
    ] {
        let response = daemon.dispatch_call(method, json!({}), None, None).await;
        assert_eq!(response["ok"], true, "{method}: {response}");
        assert!(
            response["data"].get(field).is_some(),
            "{method}: {response}"
        );
    }
    crate::daemon::Published::update(&daemon.snapshots, crate::daemon::SharedSnapshot::Loading);
    let loading = daemon
        .dispatch_call("bluetooth.snapshot", json!({}), None, None)
        .await;
    assert_eq!(loading["error"]["code"], "snapshot-loading");
    for method in [
        "bluetooth.scan",
        "bluetooth.obex.send",
        "bluetooth.obex.respond",
        "bluetooth.audio.setProfile",
        "bluetooth.audio.setDefault",
        "bluetooth.device.operation",
        "bluetooth.operation.status",
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
        (
            "bluetooth.audio.setProfile",
            json!({"device_key": "peer", "profile_key": "selection", "remember": "true"}),
        ),
        (
            "bluetooth.audio.setDefault",
            json!({"device_key": "peer", "endpoint_key": "selection", "remember": true}),
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
async fn scan_disable_only_cancels_callers_leases() {
    let (mut daemon, _, _) = daemon(true);
    let scanning = Arc::new(std::sync::Mutex::new(Vec::new()));
    daemon.scans = super::ScanCoordinator::new(super::test_backend(true, false, scanning.clone()));
    let mut events = daemon.scans.subscribe();
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
    scanning.lock().unwrap().clear();
    let mut ids = Vec::new();
    for owner in [":other", ":owner", ":owner", ":owner"] {
        let response = daemon
            .dispatch_call(
                "bluetooth.scan",
                json!({"timeout_ms": 60_000}),
                Some(owner),
                None,
            )
            .await;
        assert_eq!(response["ok"], true);
        ids.push(
            response["data"]["scan"]["request_id"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
        assert_eq!(events.recv().await.unwrap().state, "running");
    }
    for (owner, params) in [
        (":owner", json!({"enabled": false, "request_id": ids[0]})),
        (":stranger", json!({"enabled": false})),
    ] {
        let response = daemon
            .dispatch_call("bluetooth.scan", params, Some(owner), None)
            .await;
        assert_eq!(response["error"]["code"], "request-not-found");
        assert!(events.try_recv().is_err());
        assert_eq!(
            daemon.scans.snapshot().await["active"]
                .as_array()
                .unwrap()
                .len(),
            4
        );
    }
    let denied: serde_json::Value =
        serde_json::from_str(&daemon.cancel_owned(&ids[1], Some(":stranger")).await).unwrap();
    assert_eq!(denied["error"]["code"], "request-not-found");
    let response: serde_json::Value =
        serde_json::from_str(&daemon.cancel_owned(&ids[1], Some(":owner")).await).unwrap();
    assert_eq!(response["ok"], true);
    assert_eq!(events.recv().await.unwrap().request_id, ids[1]);
    assert!(daemon.scans.contains(&ids[2]).await);
    assert!(daemon.scans.contains(&ids[3]).await);

    let response = daemon
        .dispatch_call(
            "bluetooth.scan",
            json!({"enabled": false}),
            Some(":owner"),
            None,
        )
        .await;
    assert_eq!(response["ok"], true);
    for _ in 0..2 {
        let event = events.recv().await.unwrap();
        assert_eq!(event.state, "cancelled");
        assert!(ids[2..].contains(&event.request_id));
    }
    assert_eq!(
        daemon.scans.snapshot().await["active"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(daemon.scans.contains(&ids[0]).await);
    assert!(super::stopped_calls(&scanning).is_empty());

    let response = daemon
        .dispatch_call(
            "bluetooth.scan",
            json!({"enabled": false}),
            Some(":other"),
            None,
        )
        .await;
    assert_eq!(response["ok"], true);
    assert_eq!(
        super::stopped_calls(&scanning),
        vec![
            (Some("adapter-1".into()), false),
            (Some("adapter-2".into()), false),
        ]
    );
    assert_eq!(daemon.scans.snapshot().await["active"], json!([]));
}
