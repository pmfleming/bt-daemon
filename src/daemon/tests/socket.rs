use super::daemon;
use crate::{
    daemon::{self as api_daemon, socket::Service},
    test_bus::Bus,
};
use serde_json::{Value, json};
use shelllist_daemon_tokio::{JsonlListener, JsonlSocketConfig};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::oneshot,
};

struct Rig {
    bus: Bus,
    directory: tempfile::TempDir,
    connection: zbus::Connection,
    stop: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
}
impl Rig {
    async fn new() -> Self {
        let bus = Bus::new();
        let (daemon, _, _) = daemon(false);
        let connection = bus
            .builder()
            .name(api_daemon::BUS_NAME)
            .unwrap()
            .serve_at(api_daemon::OBJECT_PATH, daemon)
            .unwrap()
            .build()
            .await
            .unwrap();
        let mut rig = Self {
            bus,
            directory: tempfile::tempdir().unwrap(),
            connection,
            stop: None,
            task: None,
        };
        rig.start().await;
        rig
    }
    fn path(&self) -> std::path::PathBuf {
        self.directory.path().join("bt-daemon.sock")
    }
    async fn start(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            self.directory.path(),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        let listener = JsonlListener::bind(self.directory.path(), "bt-daemon")
            .await
            .unwrap();
        let service = Arc::new(Service {
            connection: self.connection.clone(),
        });
        let (stop, stopped) = oneshot::channel();
        self.task = Some(tokio::spawn(listener.serve(
            service,
            crate::client::BluetoothCorrelation,
            JsonlSocketConfig::default(),
            async {
                let _ = stopped.await;
            },
        )));
        self.stop = Some(stop);
    }
    async fn stop(&mut self) {
        let _ = self.stop.take().unwrap().send(());
        tokio::time::timeout(Duration::from_secs(5), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    async fn client(&self) -> BufReader<UnixStream> {
        BufReader::new(UnixStream::connect(self.path()).await.unwrap())
    }
}
async fn send(client: &mut BufReader<UnixStream>, value: Value) {
    client
        .get_mut()
        .write_all(format!("{value}\n").as_bytes())
        .await
        .unwrap();
}
async fn next(client: &mut BufReader<UnixStream>) -> Value {
    let mut line = String::new();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), client.read_line(&mut line))
            .await
            .unwrap()
            .unwrap()
            > 0
    );
    serde_json::from_str(&line).unwrap()
}
async fn call(client: &mut BufReader<UnixStream>, method: &str, params: Value) -> Value {
    send(
        client,
        json!({"op":"call","id":"call","method":method,"params":params}),
    )
    .await;
    next(client).await["response"].clone()
}

#[tokio::test]
async fn socket_and_bus_share_dispatch_and_disconnect_cancels_only_socket_owned_work() {
    let mut rig = Rig::new().await;
    let bus_client = rig.bus.connection().await;
    let proxy = zbus::Proxy::new(
        &bus_client,
        api_daemon::BUS_NAME,
        api_daemon::OBJECT_PATH,
        api_daemon::INTERFACE,
    )
    .await
    .unwrap();
    let mut a = rig.client().await;
    let mut b = rig.client().await;
    let bus_snapshot: String = proxy
        .call("Call", &("bluetooth.snapshot", "{}"))
        .await
        .unwrap();
    assert_eq!(
        call(&mut a, "bluetooth.snapshot", json!({})).await,
        serde_json::from_str::<Value>(&bus_snapshot).unwrap()
    );
    let operation = call(
        &mut a,
        "bluetooth.device.operation",
        json!({"key":"device-socket","operation":"connect"}),
    )
    .await;
    let id = operation["data"]["operation"]["request_id"]
        .as_str()
        .unwrap();
    let cancelled: String = proxy.call("Cancel", &(id,)).await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&cancelled).unwrap()["error"]["code"],
        "request-not-found"
    );
    send(
        &mut b,
        json!({"op":"cancel","id":"foreign","request_id":id}),
    )
    .await;
    assert_eq!(
        next(&mut b).await["response"]["error"]["code"],
        "request-not-found"
    );
    assert_eq!(
        call(
            &mut a,
            "bluetooth.scan",
            json!({"enabled":true,"timeout_ms":60000})
        )
        .await["ok"],
        true
    );
    let bus_scan: String = proxy
        .call(
            "Call",
            &(
                "bluetooth.scan",
                json!({"enabled":true,"timeout_ms":60000}).to_string(),
            ),
        )
        .await
        .unwrap();
    let bus_scan: Value = serde_json::from_str(&bus_scan).unwrap();
    let bus_id = bus_scan["data"]["scan"]["request_id"].as_str().unwrap();
    let route = json!({"consumerId":"view","localId":"sub","generation":1,"kind":"subscription"});
    send(
        &mut a,
        json!({"op":"subscribe","id":"sub","streams":["bluetooth.changed"],"route":route}),
    )
    .await;
    assert_eq!(next(&mut a).await["route"], route);
    let event = next(&mut a).await;
    assert_eq!(event["event"]["event"], "subscribed");
    assert_eq!(event["route"], route);
    drop(a);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let state = call(&mut b, "bluetooth.requests.snapshot", json!({})).await;
            if state["data"]["requests"]["operations"]["active"] == json!([])
                && state["data"]["requests"]["scans"]["active"]
                    .as_array()
                    .unwrap()
                    .len()
                    == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let cancelled: String = proxy.call("Cancel", &(bus_id,)).await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&cancelled).unwrap()["ok"],
        true
    );
    rig.stop().await;
    let mut end = String::new();
    assert_eq!(b.read_line(&mut end).await.unwrap(), 0);
    rig.start().await;
    let mut reopened = rig.client().await;
    send(
        &mut reopened,
        json!({"op":"subscribe","id":"fresh","streams":["bluetooth.changed"]}),
    )
    .await;
    next(&mut reopened).await;
    assert_eq!(next(&mut reopened).await["event"]["event"], "subscribed");
    drop(reopened);
    rig.stop().await;
}
