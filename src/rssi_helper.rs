//! Narrow system-bus broker for connected Bluetooth RSSI. Only this executable
//! needs CAP_NET_ADMIN; the session daemon never opens a management socket.
use std::{
    collections::{HashMap, VecDeque},
    io,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use bluer::Address;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex, Semaphore},
    time::Instant,
};
use zbus::{Connection, fdo, message::Header};
use zvariant::{Type, Value};

pub(crate) mod client;
mod mgmt;

const BUS: &str = "org.laufan.BluetoothRssiHelper";
const PATH: &str = "/org/laufan/BluetoothRssiHelper";
const INTERFACE: &str = "org.laufan.BluetoothRssiHelper1";
const ACTION: &str = "org.laufan.bt-daemon.read-connected-rssi";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(4);
const TARGET_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, PartialEq)]
pub(crate) enum Reply {
    Reading(i16),
    NotConnected,
    Unavailable,
    Denied,
}

// No cached sample is served by the privileged broker. Freshness/fallback stays
// with bt-daemon. Non-ok replies carry no meaningful numeric reading.
#[derive(Debug, Serialize, Deserialize, Type)]
struct Sample {
    status: String,
    rssi: i16,
    relative: bool,
}

impl Sample {
    fn status(status: &str) -> Self {
        Self {
            status: status.into(),
            rssi: 0,
            relative: false,
        }
    }
    fn from_read(result: io::Result<(Reply, bool)>) -> Self {
        match result {
            Ok((Reply::Reading(rssi), relative)) => Self {
                status: "ok".into(),
                rssi,
                relative,
            },
            Ok((Reply::NotConnected, _)) => Self::status("not-connected"),
            Ok((Reply::Denied, _)) => Self::status("permission-denied"),
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                Self::status("permission-denied")
            }
            Err(error) if error.kind() == io::ErrorKind::TimedOut => Self::status("timeout"),
            _ => Self::status("unavailable"),
        }
    }
}

#[async_trait]
trait Backend: Send + Sync {
    async fn authorize(&self, connection: &Connection, sender: &str) -> fdo::Result<()>;
    async fn connected(
        &self,
        connection: &Connection,
        index: u16,
        address: Address,
    ) -> fdo::Result<Option<bool>>;
    async fn read(&self, index: u16, address: Address, random: bool) -> io::Result<(Reply, bool)>;
}

struct SystemBackend;

#[async_trait]
impl Backend for SystemBackend {
    async fn authorize(&self, connection: &Connection, sender: &str) -> fdo::Result<()> {
        let proxy = zbus::Proxy::new(
            connection,
            "org.freedesktop.PolicyKit1",
            "/org/freedesktop/PolicyKit1/Authority",
            "org.freedesktop.PolicyKit1.Authority",
        )
        .await
        .map_err(failed)?;
        let subject = (
            "system-bus-name",
            HashMap::from([("name", Value::from(sender))]),
        );
        let details = HashMap::<&str, &str>::new();
        // Never open an authentication prompt from telemetry polling. Polkit
        // derives uid/session from the unique bus sender, not a supplied PID/uid.
        let (authorized, _, _): (bool, bool, HashMap<String, String>) = proxy
            .call("CheckAuthorization", &(subject, ACTION, details, 0_u32, ""))
            .await
            .map_err(failed)?;
        if authorized {
            Ok(())
        } else {
            Err(fdo::Error::AccessDenied(
                "connected RSSI read not authorized".into(),
            ))
        }
    }

    async fn connected(
        &self,
        connection: &Connection,
        index: u16,
        address: Address,
    ) -> fdo::Result<Option<bool>> {
        let path = format!(
            "/org/bluez/hci{index}/dev_{}",
            address.to_string().replace(':', "_")
        );
        let proxy = zbus::Proxy::new(connection, "org.bluez", path, "org.bluez.Device1")
            .await
            .map_err(failed)?;
        if !proxy
            .get_property::<bool>("Connected")
            .await
            .map_err(failed)?
        {
            return Ok(None);
        }
        match proxy
            .get_property::<String>("AddressType")
            .await
            .map_err(failed)?
            .as_str()
        {
            "public" | "br/edr" => Ok(Some(false)),
            "random" => Ok(Some(true)),
            _ => Err(fdo::Error::NotSupported(
                "unknown Bluetooth address type".into(),
            )),
        }
    }

    async fn read(&self, index: u16, address: Address, random: bool) -> io::Result<(Reply, bool)> {
        mgmt::read(index, address, random).await
    }
}

fn failed(error: impl std::fmt::Display) -> fdo::Error {
    fdo::Error::Failed(error.to_string())
}
fn limited() -> fdo::Error {
    fdo::Error::LimitsExceeded("RSSI helper request limit reached".into())
}

#[derive(Default)]
struct Limits {
    arrivals: VecDeque<Instant>,
    targets: HashMap<(u16, Address), Instant>,
}

impl Limits {
    fn admit(&mut self) -> fdo::Result<()> {
        let now = Instant::now();
        self.arrivals
            .retain(|time| now.duration_since(*time) < Duration::from_secs(1));
        if self.arrivals.len() >= 16 {
            return Err(limited());
        }
        self.arrivals.push_back(now);
        Ok(())
    }
    fn target(&mut self, index: u16, address: Address) -> fdo::Result<()> {
        let now = Instant::now();
        self.targets
            .retain(|_, time| now.duration_since(*time) < TARGET_INTERVAL);
        if self.targets.len() >= 64 || self.targets.contains_key(&(index, address)) {
            return Err(limited());
        }
        self.targets.insert((index, address), now);
        Ok(())
    }
}

struct Helper {
    backend: Arc<dyn Backend>,
    permits: Semaphore,
    limits: Mutex<Limits>,
}

impl Helper {
    fn new(backend: Arc<dyn Backend>) -> Self {
        Self {
            backend,
            permits: Semaphore::new(4),
            limits: Mutex::new(Limits::default()),
        }
    }

    async fn execute(
        &self,
        connection: &Connection,
        sender: &str,
        index: u16,
        address: &str,
    ) -> fdo::Result<Sample> {
        // No waiting queues and no caller-selected operations, paths or packets.
        let _permit = self.permits.try_acquire().map_err(|_| limited())?;
        self.limits.lock().await.admit()?;
        let address = parse_target(index, address)?;
        tokio::time::timeout(REQUEST_TIMEOUT, async {
            self.backend.authorize(connection, sender).await?;
            self.limits.lock().await.target(index, address)?;
            let Some(random) = self.backend.connected(connection, index, address).await? else {
                return Ok(Sample::status("not-connected"));
            };
            let sample = Sample::from_read(self.backend.read(index, address, random).await);
            if sample.status == "permission-denied" {
                tracing::warn!("RSSI helper lacks required CAP_NET_ADMIN");
            }
            Ok(sample)
        })
        .await
        .map_err(|_| fdo::Error::Timeout("RSSI helper request timed out".into()))?
    }
}

fn parse_target(index: u16, address: &str) -> fdo::Result<Address> {
    if index == u16::MAX || address.len() != 17 {
        return Err(fdo::Error::InvalidArgs("invalid Bluetooth target".into()));
    }
    address
        .parse()
        .map_err(|_| fdo::Error::InvalidArgs("invalid Bluetooth address".into()))
}

#[zbus::interface(name = "org.laufan.BluetoothRssiHelper1")]
impl Helper {
    async fn read(
        &self,
        index: u16,
        address: &str,
        #[zbus(header)] header: Header<'_>,
        #[zbus(connection)] connection: &Connection,
    ) -> fdo::Result<Sample> {
        let sender = header
            .sender()
            .ok_or_else(|| fdo::Error::AccessDenied("missing D-Bus sender".into()))?;
        self.execute(connection, sender.as_str(), index, address)
            .await
    }
}

/// Run the system service. Deployment supplies its capabilities and bus policy.
pub async fn run() -> anyhow::Result<()> {
    let connection = zbus::connection::Builder::system()?
        .name(BUS)?
        .serve_at(PATH, Helper::new(Arc::new(SystemBackend)))?
        .build()
        .await?;
    tracing::info!("read-only Bluetooth RSSI helper ready");
    shelllist_daemon_tokio::wait_for_shutdown().await?;
    connection.close().await?;
    Ok(())
}

#[cfg(test)]
mod tests;
