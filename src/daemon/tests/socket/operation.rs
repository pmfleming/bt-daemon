use super::*;

#[tokio::test]
async fn operation_status_and_streams_are_owned_on_socket_and_bus() -> anyhow::Result<()> {
    let mut rig = Rig::new().await;
    let proxy = rig.proxy().await?;
    let mut owner = rig.client().await;
    let mut foreign = rig.client().await;
    for peer in [&mut owner, &mut foreign] {
        send(
            peer,
            json!({"op":"subscribe", "id":"sub", "streams":[api_daemon::OPERATION_STREAM]}),
        )
        .await?;
        assert_eq!(next(peer).await?["response"]["ok"], true);
    }
    send(
        &mut owner,
        json!({"op":"call", "id":"start", "method":"bluetooth.device.operation",
        "params":{"key":"owned-device", "operation":"connect"}}),
    )
    .await?;
    let mut receipt = None;
    let mut progress = None;
    while receipt.is_none() || progress.is_none() {
        let record = next(&mut owner).await?;
        if record["kind"] == "response" {
            receipt = Some(record["response"]["data"]["operation"].clone());
        } else if record["event"]["data"]["stage"] == "connecting" {
            progress = Some(record["event"]["data"].clone());
        }
    }
    let receipt = receipt.unwrap();
    let progress = progress.unwrap();
    let id = receipt["request_id"].as_str().unwrap();
    let params = json!({"request_id":id});
    assert_eq!(receipt["revision"], 1);
    assert_eq!(progress["revision"], 3);
    assert!(progress.get("owner").is_none());
    assert_eq!(
        call(&mut owner, "bluetooth.operation.status", params.clone()).await?["data"]["operation_status"],
        progress
    );
    assert_eq!(
        call(&mut foreign, "bluetooth.operation.status", params.clone()).await?["error"]["code"],
        "request-not-found"
    );
    assert_eq!(
        call(&mut foreign, "bluetooth.requests.snapshot", json!({})).await?["data"]["requests"]["operations"],
        json!({"active":[], "recent":[]})
    );
    let denied = bus_call(&proxy, "bluetooth.operation.status", &params).await?;
    assert_eq!(denied["error"]["code"], "request-not-found");
    send(
        &mut owner,
        json!({"op":"cancel", "id":"stop", "request_id":id}),
    )
    .await?;
    let mut terminal = None;
    let mut cancelled = false;
    while !cancelled {
        let record = next(&mut owner).await?;
        if record["kind"] == "response" {
            assert_eq!(record["response"]["ok"], true);
            cancelled = true;
        } else if record["event"]["event"] == "cancelled" {
            terminal = Some(record["event"]["data"].clone());
        }
    }
    // Explicit transport cancellation may retire the operation route before
    // terminal delivery. Its authoritative status must still survive in silence.
    let status = call(&mut owner, "bluetooth.operation.status", params.clone()).await?;
    let retained = &status["data"]["operation_status"];
    assert_eq!(retained["state"], "cancelled");
    assert_eq!(retained["revision"], 4);
    if let Some(terminal) = terminal {
        assert_eq!(retained, &terminal);
    }
    // A read is not new accepted work and must never rebind an operation route.
    use shelllist_daemon_tokio::CorrelationPolicy;
    assert!(
        crate::client::BluetoothCorrelation
            .operation_id(&status)
            .is_none()
    );
    // Subscription emits neither another owner's progress nor terminal data.
    let mut line = String::new();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), foreign.read_line(&mut line))
            .await
            .is_err()
    );
    // The retained bridge/D-Bus caller gets the same private status dispatch,
    // but its own identity (never socket authority).
    let started = bus_call(
        &proxy,
        "bluetooth.device.operation",
        &json!({"key":"bus-device","operation":"connect"}),
    )
    .await?;
    let bus_id = started["data"]["operation"]["request_id"].as_str().unwrap();
    let bus_params = json!({"request_id":bus_id});
    let status = bus_call(&proxy, "bluetooth.operation.status", &bus_params).await?;
    assert_eq!(status["ok"], true);
    assert_eq!(
        call(
            &mut foreign,
            "bluetooth.operation.status",
            bus_params.clone()
        )
        .await?["error"]["code"],
        "request-not-found"
    );
    assert_eq!(bus_rpc(&proxy, "Cancel", &(bus_id,)).await?["ok"], true);
    let status = bus_call(&proxy, "bluetooth.operation.status", &bus_params).await?;
    assert_eq!(status["data"]["operation_status"]["state"], "cancelled");
    rig.stop().await;
    Ok(())
}

#[tokio::test]
async fn dbus_owner_loss_cancels_only_its_admitted_operation() -> anyhow::Result<()> {
    let mut rig = Rig::new().await;
    let mut peer = rig.client().await;
    let proxy = rig.proxy().await?;
    let started = bus_call(
        &proxy,
        "bluetooth.device.operation",
        &json!({"key":"bus-device","operation":"connect"}),
    )
    .await?;
    assert_eq!(started["ok"], true);
    let retained = call(
        &mut peer,
        "bluetooth.device.operation",
        json!({"key":"peer-device", "operation":"connect"}),
    )
    .await?;
    let peer_id = retained["data"]["operation"]["request_id"]
        .as_str()
        .unwrap();
    proxy.connection().clone().close().await?;
    drop(proxy);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let response = call(
                &mut peer,
                "bluetooth.device.operation",
                json!({"key":"bus-device", "operation":"connect"}),
            )
            .await?;
            if response["ok"] == true {
                break Ok::<(), anyhow::Error>(());
            }
            assert_eq!(response["error"]["code"], "device-busy");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    assert_eq!(
        call(
            &mut peer,
            "bluetooth.operation.status",
            json!({"request_id":peer_id})
        )
        .await?["ok"],
        true
    );
    rig.stop().await;
    Ok(())
}
