use super::*;
use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use zbus::connection::Builder;

struct Bus {
    child: Child,
    address: String,
    _dir: tempfile::TempDir,
}
impl Bus {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("bus.conf");
        std::fs::write(&config, r#"<busconfig><type>session</type><listen>unix:tmpdir=/tmp</listen><auth>EXTERNAL</auth><policy context="default"><allow send_destination="*"/><allow receive_sender="*"/><allow own="*"/></policy></busconfig>"#).unwrap();
        let mut child = Command::new("dbus-daemon")
            .arg("--config-file")
            .arg(config)
            .args(["--nofork", "--print-address=1"])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut address = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut address)
            .unwrap();
        Self {
            child,
            address: address.trim().into(),
            _dir: dir,
        }
    }
    async fn connection(&self) -> Connection {
        Builder::address(self.address.as_str())
            .unwrap()
            .build()
            .await
            .unwrap()
    }
}
impl Drop for Bus {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Default)]
struct Fake {
    allowed: AtomicBool,
    connected: AtomicBool,
    stall: AtomicBool,
    reads: AtomicUsize,
    caller: Mutex<String>,
}
#[async_trait]
impl Backend for Fake {
    async fn authorize(&self, _: &Connection, sender: &str) -> fdo::Result<()> {
        *self.caller.lock().await = sender.into();
        if self.allowed.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(fdo::Error::AccessDenied("denied".into()))
        }
    }
    async fn connected(&self, _: &Connection, _: u16, _: Address) -> fdo::Result<Option<bool>> {
        Ok(self.connected.load(Ordering::SeqCst).then_some(false))
    }
    async fn read(&self, _: u16, _: Address, random: bool) -> io::Result<(Reply, bool)> {
        assert!(!random);
        self.reads.fetch_add(1, Ordering::SeqCst);
        if self.stall.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        Ok((Reply::Reading(0), true))
    }
}

async fn serve(bus: &Bus, backend: Arc<dyn Backend>) -> Connection {
    Builder::address(bus.address.as_str())
        .unwrap()
        .name(BUS)
        .unwrap()
        .serve_at(PATH, Helper::new(backend))
        .unwrap()
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn private_bus_checks_identity_authorization_targets_limits_and_helper_restart() {
    let bus = Bus::new();
    let caller = bus.connection().await;
    let address: Address = "01:23:45:67:89:AB".parse().unwrap();
    assert!(
        client::read(&caller, 0, address).await.is_err(),
        "missing helper must not open HCI directly"
    );
    let fake = Arc::new(Fake::default());
    let server = serve(&bus, fake.clone()).await;
    assert!(client::read(&caller, 0, address).await.is_err());
    assert_eq!(fake.reads.load(Ordering::SeqCst), 0);
    assert_eq!(
        *fake.caller.lock().await,
        caller.unique_name().unwrap().as_str()
    );
    fake.allowed.store(true, Ordering::SeqCst);
    fake.connected.store(true, Ordering::SeqCst);
    assert_eq!(
        client::read(&caller, 0, address).await.unwrap(),
        (Reply::Reading(0), true)
    );
    assert!(
        client::read(&caller, 0, address).await.is_err(),
        "repeated hardware target must be throttled"
    );
    assert_eq!(fake.reads.load(Ordering::SeqCst), 1);
    let proxy = zbus::Proxy::new(&caller, BUS, PATH, INTERFACE)
        .await
        .unwrap();
    for (index, address) in [
        (u16::MAX, "01:23:45:67:89:AB"),
        (0, "../dev_bad"),
        (0, "ZZ:23:45:67:89:AB"),
    ] {
        assert!(
            proxy
                .call::<_, _, Sample>("Read", &(index, address))
                .await
                .is_err()
        );
    }
    assert!(
        proxy
            .call::<_, _, Sample>("Write", &(0_u16, "01:23:45:67:89:AB"))
            .await
            .is_err()
    );
    fake.connected.store(false, Ordering::SeqCst);
    assert_eq!(
        client::read(&caller, 1, address).await.unwrap().0,
        Reply::NotConnected
    );
    assert_eq!(fake.reads.load(Ordering::SeqCst), 1);
    server.close().await.unwrap();
    let replacement = Arc::new(Fake::default());
    replacement.allowed.store(true, Ordering::SeqCst);
    replacement.connected.store(true, Ordering::SeqCst);
    let _replacement = serve(&bus, replacement).await;
    assert_eq!(
        client::read(&caller, 0, address).await.unwrap(),
        (Reply::Reading(0), true)
    );
}

#[tokio::test(start_paused = true)]
async fn limits_are_bounded_and_recover_without_persistent_state() {
    let mut limits = Limits::default();
    for _ in 0..16 {
        limits.admit().unwrap();
    }
    assert!(limits.admit().is_err());
    assert_eq!(limits.arrivals.len(), 16);
    tokio::time::advance(Duration::from_secs(1)).await;
    limits.admit().unwrap();
    let address = Address::default();
    for index in 0..64 {
        limits.target(index, address).unwrap();
    }
    assert!(limits.target(0, address).is_err());
    assert!(limits.target(64, address).is_err());
    tokio::time::advance(TARGET_INTERVAL).await;
    limits.target(0, address).unwrap();
    assert_eq!(limits.targets.len(), 1);
}

#[tokio::test]
async fn admission_never_waits_and_stalled_requests_release_their_permit() {
    let bus = Bus::new();
    let connection = bus.connection().await;
    let fake = Arc::new(Fake::default());
    fake.allowed.store(true, Ordering::SeqCst);
    fake.connected.store(true, Ordering::SeqCst);
    fake.stall.store(true, Ordering::SeqCst);
    let helper = Helper::new(fake.clone());
    let permits = helper.permits.acquire_many(4).await.unwrap();
    assert!(matches!(
        helper
            .execute(&connection, ":1.1", 0, "01:23:45:67:89:AB")
            .await,
        Err(fdo::Error::LimitsExceeded(_))
    ));
    assert_eq!(fake.reads.load(Ordering::SeqCst), 0);
    drop(permits);
    assert!(matches!(
        helper
            .execute(&connection, ":1.1", 0, "01:23:45:67:89:AB")
            .await,
        Err(fdo::Error::Timeout(_))
    ));
    assert_eq!(helper.permits.available_permits(), 4);
    assert_eq!(fake.reads.load(Ordering::SeqCst), 1);
}

struct Authority {
    allowed: Arc<AtomicBool>,
    expected: String,
}
#[zbus::interface(name = "org.freedesktop.PolicyKit1.Authority")]
impl Authority {
    async fn check_authorization(
        &self,
        subject: (String, HashMap<String, zvariant::OwnedValue>),
        action: &str,
        details: HashMap<String, String>,
        flags: u32,
        cancellation_id: &str,
    ) -> (bool, bool, HashMap<String, String>) {
        assert_eq!(subject.0, "system-bus-name");
        assert_eq!(<&str>::try_from(&subject.1["name"]).unwrap(), self.expected);
        assert_eq!(subject.1.len(), 1);
        assert_eq!(action, ACTION);
        assert_eq!(flags, 0, "polling must never prompt for authentication");
        assert!(details.is_empty() && cancellation_id.is_empty());
        (self.allowed.load(Ordering::SeqCst), false, HashMap::new())
    }
}
struct Device {
    connected: Arc<AtomicBool>,
}
#[zbus::interface(name = "org.bluez.Device1")]
impl Device {
    #[zbus(property)]
    fn connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
    #[zbus(property)]
    fn address_type(&self) -> &str {
        "random"
    }
}

#[tokio::test]
async fn system_backend_uses_polkit_bus_identity_and_bluez_connection_not_client_claims() {
    let bus = Bus::new();
    let connection = bus.connection().await;
    let sender = connection.unique_name().unwrap().as_str();
    let allowed = Arc::new(AtomicBool::new(false));
    let connected = Arc::new(AtomicBool::new(false));
    let _authority = Builder::address(bus.address.as_str())
        .unwrap()
        .name("org.freedesktop.PolicyKit1")
        .unwrap()
        .serve_at(
            "/org/freedesktop/PolicyKit1/Authority",
            Authority {
                allowed: allowed.clone(),
                expected: sender.into(),
            },
        )
        .unwrap()
        .build()
        .await
        .unwrap();
    let _bluez = Builder::address(bus.address.as_str())
        .unwrap()
        .name("org.bluez")
        .unwrap()
        .serve_at(
            "/org/bluez/hci2/dev_01_23_45_67_89_AB",
            Device {
                connected: connected.clone(),
            },
        )
        .unwrap()
        .build()
        .await
        .unwrap();
    assert!(SystemBackend.authorize(&connection, sender).await.is_err());
    allowed.store(true, Ordering::SeqCst);
    SystemBackend.authorize(&connection, sender).await.unwrap();
    let address = "01:23:45:67:89:AB".parse().unwrap();
    assert_eq!(
        SystemBackend
            .connected(&connection, 2, address)
            .await
            .unwrap(),
        None
    );
    connected.store(true, Ordering::SeqCst);
    assert_eq!(
        SystemBackend
            .connected(&connection, 2, address)
            .await
            .unwrap(),
        Some(true)
    );
    assert!(
        SystemBackend
            .connected(&connection, 3, address)
            .await
            .is_err()
    );
    _authority.close().await.unwrap();
    assert!(
        SystemBackend.authorize(&connection, sender).await.is_err(),
        "missing polkit fails closed"
    );
}
