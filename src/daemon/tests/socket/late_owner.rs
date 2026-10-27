use super::*;

#[tokio::test]
async fn late_admission_during_dbus_owner_cleanup_is_cancelled_without_touching_peer_work() {
    let mut rig = Rig::new().await;
    let departed = rig.bus.connection().await;
    let owner = departed.unique_name().unwrap().to_string();
    departed.close().await.unwrap();
    let interface = rig
        .connection
        .object_server()
        .interface::<_, api_daemon::BluetoothDaemon>(api_daemon::OBJECT_PATH)
        .await
        .unwrap();
    let daemon = interface.get().await;
    // Deterministically hold the watch-set window after cleanup captured its
    // IDs and before its watch entry is removed. This represents an already
    // admitted call finishing registration, not a call from a disconnected bus.
    daemon.scan_owner_watches.lock().await.insert(owner.clone());
    let peer = daemon
        .dispatch_call(
            "bluetooth.device.operation",
            json!({"key":"peer-device", "operation":"connect"}),
            Some("fixture-peer"),
            None,
        )
        .await;
    let peer_id = peer["data"]["operation"]["request_id"].as_str().unwrap();
    let late = daemon
        .dispatch_call(
            "bluetooth.device.operation",
            json!({"key":"late-device", "operation":"connect"}),
            Some(&owner),
            Some(&rig.connection),
        )
        .await;
    let late_id = late["data"]["operation"]["request_id"].as_str().unwrap();
    let status = daemon
        .dispatch_call(
            "bluetooth.operation.status",
            json!({"request_id": late_id}),
            Some(&owner),
            None,
        )
        .await;
    assert_eq!(status["data"]["operation_status"]["state"], "cancelled");
    assert_eq!(
        daemon.operations.snapshot(Some(&owner)).await["active"],
        json!([])
    );
    assert_eq!(
        daemon
            .dispatch_call(
                "bluetooth.operation.status",
                json!({"request_id":peer_id}),
                Some("fixture-peer"),
                None
            )
            .await["ok"],
        true
    );
    daemon.operations.cancel_owner("fixture-peer").await;
    drop(daemon);
    drop(interface);
    rig.stop().await;
}
