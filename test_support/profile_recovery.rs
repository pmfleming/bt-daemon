//! Actual provider reconcilers against private mock BlueZ; no host radio access.
use super::{AgentManager, Bus, Process};
use std::{
    collections::HashMap,
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use zbus::{
    fdo,
    zvariant::{ObjectPath, OwnedObjectPath, OwnedValue},
};

const FAST_PAIR: &str = "df21fe2c-2515-4fdb-8886-f12c4d67927c";
const SONY: &str = "956c7b26-d49a-4ba8-b03f-b17d393cb6e2";
const BLE: &str = "0000fe2c-0000-1000-8000-00805f9b34fb";
const ADAPTER: &str = "/org/bluez/hci0";
const BLE_DEVICE: &str = "/org/bluez/hci0/dev_AA_BB_CC_DD_EE_04";
const GATT_SERVICE: &str = "/org/bluez/hci0/dev_AA_BB_CC_DD_EE_04/service0001";

type Calls = Arc<Mutex<Vec<(u8, String)>>>;
struct Profiles;
#[zbus::interface(name = "org.bluez.ProfileManager1")]
impl Profiles {
    fn register_profile(
        &self,
        _profile: ObjectPath<'_>,
        _uuid: &str,
        _options: HashMap<String, OwnedValue>,
    ) {
    }
    fn unregister_profile(&self, _profile: ObjectPath<'_>) {}
}
struct Adapter;
#[zbus::interface(name = "org.bluez.Adapter1")]
impl Adapter {
    #[zbus(property)]
    fn address(&self) -> &str {
        "AA:BB:CC:DD:00:00"
    }
    #[zbus(property)]
    fn powered(&self) -> bool {
        true
    }
}
struct Device {
    id: u8,
    connected: bool,
    resolved: Arc<AtomicBool>,
    uuids: Vec<String>,
    calls: Calls,
}
#[zbus::interface(name = "org.bluez.Device1")]
impl Device {
    #[zbus(property)]
    fn connected(&self) -> bool {
        self.connected
    }
    #[zbus(property)]
    fn services_resolved(&self) -> bool {
        self.resolved.load(Ordering::SeqCst)
    }
    #[zbus(property, name = "UUIDs")]
    fn uuids(&self) -> Vec<String> {
        self.uuids.clone()
    }
    #[zbus(property)]
    fn adapter(&self) -> OwnedObjectPath {
        ADAPTER.try_into().unwrap()
    }
    #[zbus(property)]
    fn paired(&self) -> bool {
        true
    }
    #[zbus(property)]
    fn blocked(&self) -> bool {
        false
    }
    fn connect_profile(&self, uuid: &str) {
        self.calls.lock().unwrap().push((self.id, uuid.into()));
        // A successful method reply deliberately does not deliver a stream.
    }
}
struct Service;
#[zbus::interface(name = "org.bluez.GattService1")]
impl Service {
    #[zbus(property, name = "UUID")]
    fn uuid(&self) -> &str {
        BLE
    }
    #[zbus(property)]
    fn device(&self) -> OwnedObjectPath {
        BLE_DEVICE.try_into().unwrap()
    }
    #[zbus(property)]
    fn primary(&self) -> bool {
        true
    }
}
struct Psm(Arc<AtomicUsize>);
#[zbus::interface(name = "org.bluez.GattCharacteristic1")]
impl Psm {
    #[zbus(property, name = "UUID")]
    fn uuid(&self) -> &str {
        "fe2c1239-8366-4814-8eb0-01de32100bea"
    }
    #[zbus(property)]
    fn service(&self) -> OwnedObjectPath {
        GATT_SERVICE.try_into().unwrap()
    }
    fn read_value(&self, _options: HashMap<String, OwnedValue>) -> Vec<u8> {
        self.0.fetch_add(1, Ordering::SeqCst);
        vec![2, 0, 0]
    }
}

#[tokio::test]
async fn cached_rfcomm_profiles_connect_without_service_resolution_but_ble_waits() {
    let state = tempfile::tempdir().unwrap();
    let bus = Bus::new();
    let calls = Calls::default();
    let psm_reads = Arc::new(AtomicUsize::new(0));
    let resolved = Arc::new(AtomicBool::new(false));
    let mut builder = bus
        .builder()
        .name("org.bluez")
        .unwrap()
        .serve_at("/", fdo::ObjectManager)
        .unwrap()
        .serve_at(
            "/org/bluez",
            AgentManager {
                allowed: Arc::new(AtomicBool::new(true)),
                attempts: Arc::new(AtomicUsize::new(0)),
            },
        )
        .unwrap()
        .serve_at("/org/bluez", Profiles)
        .unwrap()
        .serve_at(ADAPTER, Adapter)
        .unwrap()
        .serve_at(GATT_SERVICE, Service)
        .unwrap()
        .serve_at(format!("{GATT_SERVICE}/char0002"), Psm(psm_reads.clone()))
        .unwrap();
    for (id, connected, uuids) in [
        (1, true, vec![FAST_PAIR, SONY]),
        (2, false, vec![FAST_PAIR, SONY]),
        (3, true, vec![]),
        (4, true, vec![BLE]),
    ] {
        builder = builder
            .serve_at(
                format!("{ADAPTER}/dev_AA_BB_CC_DD_EE_{id:02X}"),
                Device {
                    id,
                    connected,
                    resolved: resolved.clone(),
                    uuids: uuids.into_iter().map(String::from).collect(),
                    calls: calls.clone(),
                },
            )
            .unwrap();
    }
    let bluez = builder.build().await.unwrap();
    let mut daemon = Process(
        Command::new(env!("CARGO_BIN_EXE_bt-daemon"))
            .arg("daemon")
            .env("DBUS_SESSION_BUS_ADDRESS", &bus.address)
            .env("DBUS_SYSTEM_BUS_ADDRESS", &bus.address)
            .env("BT_DAEMON_STATE_DIR", state.path())
            .env_remove("BT_DAEMON_OBEX_INCOMING")
            .env("XDG_RUNTIME_DIR", state.path())
            .env("PIPEWIRE_REMOTE", "bt-daemon-test-no-server")
            .env("RUST_LOG", "error")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let seen = calls.lock().unwrap().clone();
            if [FAST_PAIR, SONY]
                .iter()
                .all(|uuid| seen.contains(&(1, uuid.to_string())))
            {
                break;
            }
            assert!(daemon.0.try_wait().unwrap().is_none(), "test daemon exited");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("cached RFCOMM profiles were blocked by unresolved services");
    // Let both adapter scans finish, including peers that must not be contacted.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let seen = calls.lock().unwrap().clone();
    assert_eq!(
        seen.len(),
        2,
        "no duplicate/absent-profile/disconnected attempts: {seen:?}"
    );
    assert!(seen.iter().all(|(id, _)| *id == 1));
    assert_eq!(
        psm_reads.load(Ordering::SeqCst),
        0,
        "unresolved GATT must not be queried"
    );
    // Prove the GATT mock is reachable once resolution finishes, not merely
    // absent/broken in a way that would make the negative assertion vacuous.
    resolved.store(true, Ordering::SeqCst);
    let interface = bluez
        .object_server()
        .interface::<_, Device>(BLE_DEVICE)
        .await
        .unwrap();
    interface
        .get()
        .await
        .services_resolved_changed(interface.signal_emitter())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(12), async {
        while psm_reads.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("resolved GATT was not queried");
}
