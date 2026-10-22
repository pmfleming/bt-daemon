//! End-to-end regressions on a private bus, with no host BlueZ or PipeWire access.
use std::{
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::StreamExt;
use serde_json::{Value, json};
use zbus::{Connection, Proxy, fdo, zvariant::ObjectPath};

#[path = "../src/test_bus.rs"]
mod test_bus;
use test_bus::{Bus, Process};

mod profile_recovery;

struct AgentManager {
    allowed: Arc<AtomicBool>,
    attempts: Arc<AtomicUsize>,
}

#[zbus::interface(name = "org.bluez.AgentManager1")]
impl AgentManager {
    fn register_agent(&self, _path: ObjectPath<'_>, _capability: &str) -> fdo::Result<()> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.allowed.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(fdo::Error::Failed("agent not ready".into()))
        }
    }

    fn unregister_agent(&self, _path: ObjectPath<'_>) {}
}

struct ObexAgentManager(Arc<AtomicUsize>);

#[zbus::interface(name = "org.bluez.obex.AgentManager1")]
impl ObexAgentManager {
    fn register_agent(&self, _path: ObjectPath<'_>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

async fn bluez(bus: &Bus, allowed: Arc<AtomicBool>, attempts: Arc<AtomicUsize>) -> Connection {
    bus.builder()
        .serve_at("/", fdo::ObjectManager)
        .unwrap()
        .serve_at("/org/bluez", AgentManager { allowed, attempts })
        .unwrap()
        .name("org.bluez")
        .unwrap()
        .build()
        .await
        .unwrap()
}

async fn call(proxy: &Proxy<'_>, method: &str, params: Value) -> zbus::Result<Value> {
    let response: String = proxy.call("Call", &(method, params.to_string())).await?;
    Ok(serde_json::from_str(&response).unwrap())
}

async fn snapshot_when(proxy: &Proxy<'_>, predicate: impl Fn(&Value) -> bool) -> Value {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(snapshot) = call(proxy, "bluetooth.snapshot", json!({})).await
                && predicate(&snapshot)
            {
                return snapshot;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("daemon snapshot did not reach the expected state")
}

#[tokio::test]
async fn startup_recovery_and_policy_changes_reach_existing_clients() {
    for initially_present in [false, true] {
        let state = tempfile::tempdir().unwrap();
        let bus = Bus::new();
        let address = bus.address.as_str();
        let incoming_registrations = Arc::new(AtomicUsize::new(0));
        let _obex = bus
            .builder()
            .name("org.bluez.obex")
            .unwrap()
            .serve_at(
                "/org/bluez/obex",
                ObexAgentManager(Arc::clone(&incoming_registrations)),
            )
            .unwrap()
            .build()
            .await
            .unwrap();
        let allowed = Arc::new(AtomicBool::new(false));
        let attempts = Arc::new(AtomicUsize::new(0));
        let mut service = if initially_present {
            Some(bluez(&bus, allowed.clone(), attempts.clone()).await)
        } else {
            None
        };
        let mut daemon = Process(
            Command::new(env!("CARGO_BIN_EXE_bt-daemon"))
                .arg("daemon")
                .env("DBUS_SESSION_BUS_ADDRESS", address)
                .env("DBUS_SYSTEM_BUS_ADDRESS", address)
                .env("BT_DAEMON_STATE_DIR", state.path())
                .env_remove("BT_DAEMON_OBEX_INCOMING")
                .env("XDG_RUNTIME_DIR", state.path())
                .env("PIPEWIRE_REMOTE", "bt-daemon-test-no-server")
                .env("RUST_LOG", "error")
                .stdout(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let client = bus.connection().await;
        let proxy = Proxy::new(
            &client,
            "org.laufan.BluetoothDaemon",
            "/org/laufan/BluetoothDaemon",
            bt_daemon::daemon::INTERFACE,
        )
        .await
        .unwrap();
        snapshot_when(&proxy, |value| {
            value["error"]["code"] == "bluez-unavailable"
        })
        .await;
        assert!(daemon.0.try_wait().unwrap().is_none());
        let owner_proxy = zbus::fdo::DBusProxy::new(&client).await.unwrap();
        let name = "org.laufan.BluetoothDaemon".try_into().unwrap();
        let original_owner = owner_proxy.get_name_owner(name).await.unwrap();
        let mut events = proxy.receive_signal("Event").await.unwrap();
        let subscription: String = proxy
            .call("Subscribe", &(vec!["bluetooth.changed"],))
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&subscription).unwrap()["ok"],
            true
        );

        if service.is_none() {
            service = Some(bluez(&bus, allowed.clone(), attempts.clone()).await);
        }
        // Agent registration failures must keep the API available and retry even
        // if BlueZ keeps the same owner throughout startup.
        tokio::time::timeout(Duration::from_secs(10), async {
            while attempts.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        snapshot_when(&proxy, |value| {
            value["error"]["code"] == "bluez-unavailable"
        })
        .await;
        allowed.store(true, Ordering::SeqCst);
        snapshot_when(&proxy, |value| value["ok"] == true).await;
        assert!(attempts.load(Ordering::SeqCst) >= 2);

        let updated = call(
            &proxy,
            "bluetooth.management.update",
            json!({"trust_after_pair": false}),
        )
        .await
        .unwrap();
        assert_eq!(
            updated["data"]["snapshot"]["management"]["trust_after_pair"],
            false
        );
        // No adapter/device signals are emitted by this mock. The policy update
        // itself must trigger both the cache refresh and the subscriber event.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = events.next().await.unwrap();
                let (stream, payload): (String, String) = event.body().deserialize().unwrap();
                let payload: Value = serde_json::from_str(&payload).unwrap();
                if stream == "bluetooth.changed"
                    && payload["data"]["snapshot"]["management"]["trust_after_pair"] == false
                {
                    break;
                }
            }
        })
        .await
        .expect("policy change was not delivered to the subscriber");
        snapshot_when(&proxy, |value| {
            value["data"]["snapshot"]["management"]["trust_after_pair"] == false
        })
        .await;

        service
            .take()
            .unwrap()
            .release_name("org.bluez")
            .await
            .unwrap();
        snapshot_when(&proxy, |value| {
            value["error"]["code"] == "bluez-unavailable"
        })
        .await;
        let _replacement = bluez(&bus, allowed.clone(), attempts.clone()).await;
        snapshot_when(&proxy, |value| {
            value["data"]["snapshot"]["management"]["trust_after_pair"] == false
        })
        .await;
        let name = "org.laufan.BluetoothDaemon".try_into().unwrap();
        assert_eq!(
            owner_proxy.get_name_owner(name).await.unwrap(),
            original_owner
        );
        // Even with obexd present, default startup/recovery must not claim incoming transfers.
        assert_eq!(incoming_registrations.load(Ordering::SeqCst), 0);
    }
}
