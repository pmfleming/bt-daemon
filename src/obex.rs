use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use serde::Serialize;
use serde_json::Value as JsonValue;
use tokio::sync::{Mutex, broadcast, oneshot};
use zbus::{DBusError, fdo::PropertiesProxy};
use zvariant::{OwnedObjectPath, OwnedValue, Value};

use crate::backend::{BluetoothBackend, ObexRemote};

const BUS_NAME: &str = "org.bluez.obex";
const OBJECT_PATH: &str = "/org/bluez/obex";
const CLIENT_INTERFACE: &str = "org.bluez.obex.Client1";
const AGENT_MANAGER_INTERFACE: &str = "org.bluez.obex.AgentManager1";
const PUSH_INTERFACE: &str = "org.bluez.obex.ObjectPush1";
const TRANSFER_INTERFACE: &str = "org.bluez.obex.Transfer1";
const SESSION_INTERFACE: &str = "org.bluez.obex.Session1";
pub const AGENT_PATH: &str = "/org/laufan/BluetoothDaemon/ObexAgent";
const AUTHORIZATION_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Serialize)]
pub struct ObexCapabilities {
    pub available: bool,
    pub outgoing_object_push: bool,
    pub incoming_authorization: bool,
    pub transfer_progress: bool,
    pub cancellation: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ObexEvent {
    pub event: String,
    pub request_id: String,
    pub direction: String,
    pub device_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_name: Option<String>,
    pub file_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    pub status: String,
    pub transferred: u64,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonValue>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransferUpdate {
    pub status: String,
    pub transferred: u64,
    pub size: u64,
}

pub struct ActiveTransfer {
    connection: zbus::Connection,
    session_path: OwnedObjectPath,
    transfer_path: OwnedObjectPath,
    pub file_name: String,
    pub size: u64,
    initial_status: String,
    initial_transferred: u64,
}

#[derive(Clone, Copy)]
enum AuthorizationDecision {
    Accept,
    Reject,
    Cancel,
}

struct PendingAuthorization {
    sender: oneshot::Sender<AuthorizationDecision>,
}

struct IncomingAuthorization {
    connection: zbus::Connection,
    transfer_path: OwnedObjectPath,
    request_id: String,
    remote: ObexRemote,
    details: IncomingDetails,
    file_name: String,
    destination: PathBuf,
}

impl IncomingAuthorization {
    fn event(&self, event: &str, status: &str, timeout_ms: Option<u64>) -> ObexEvent {
        incoming_event(
            &self.request_id,
            &self.remote,
            &self.details,
            &self.file_name,
            event,
            status,
            timeout_ms,
        )
    }
}

fn incoming_event(
    request_id: &str,
    remote: &ObexRemote,
    details: &IncomingDetails,
    file_name: &str,
    event: &str,
    status: &str,
    timeout_ms: Option<u64>,
) -> ObexEvent {
    ObexEvent {
        event: event.into(),
        request_id: request_id.into(),
        direction: "incoming".into(),
        device_key: remote.device_key.clone(),
        device_name: Some(remote.name.clone()),
        file_name: file_name.into(),
        media_type: details.media_type.clone(),
        status: status.into(),
        transferred: 0,
        size: details.size,
        timeout_ms,
        error: None,
    }
}

pub struct IncomingBroker {
    backend: Arc<dyn BluetoothBackend>,
    events: broadcast::Sender<ObexEvent>,
    sequence: AtomicU64,
    available: AtomicBool,
    connection: OnceLock<zbus::Connection>,
    pending: Mutex<HashMap<String, PendingAuthorization>>,
    cancellations: Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>,
}

impl IncomingBroker {
    pub fn new(
        backend: Arc<dyn BluetoothBackend>,
        events: broadcast::Sender<ObexEvent>,
    ) -> Arc<Self> {
        Arc::new(Self {
            backend,
            events,
            sequence: AtomicU64::new(1),
            available: AtomicBool::new(false),
            connection: OnceLock::new(),
            pending: Mutex::new(HashMap::new()),
            cancellations: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn set_connection(&self, connection: zbus::Connection) {
        let _ = self.connection.set(connection);
    }

    pub fn is_available(&self) -> bool {
        self.available.load(Ordering::Relaxed)
    }

    pub async fn respond(&self, request_id: &str, accept: bool) -> Result<()> {
        let pending = self
            .pending
            .lock()
            .await
            .remove(request_id)
            .context("incoming transfer authorization is no longer pending")?;
        let decision = if accept {
            AuthorizationDecision::Accept
        } else {
            AuthorizationDecision::Reject
        };
        let _ = pending.sender.send(decision);
        Ok(())
    }

    pub async fn cancel_transfer(&self, request_id: &str) -> bool {
        if let Some(pending) = self.pending.lock().await.remove(request_id) {
            let _ = pending.sender.send(AuthorizationDecision::Cancel);
            return true;
        }
        if let Some(cancel) = self.cancellations.lock().await.remove(request_id) {
            let _ = cancel.send(());
            return true;
        }
        false
    }

    async fn cancel_authorizations(&self) {
        for (_, pending) in self.pending.lock().await.drain() {
            let _ = pending.sender.send(AuthorizationDecision::Cancel);
        }
    }

    async fn authorize(&self, transfer_path: OwnedObjectPath) -> Result<String, ObexAgentError> {
        let authorization = self.prepare_authorization(transfer_path).await?;
        match self.request_decision(&authorization).await? {
            AuthorizationDecision::Accept => Ok(self.start_incoming(authorization).await),
            AuthorizationDecision::Reject => Err(self.reject_authorization(
                authorization,
                ObexAgentError::Rejected("incoming transfer was rejected".into()),
            )),
            AuthorizationDecision::Cancel => Err(self.reject_authorization(
                authorization,
                ObexAgentError::Canceled("incoming transfer was cancelled".into()),
            )),
        }
    }

    async fn prepare_authorization(
        &self,
        transfer_path: OwnedObjectPath,
    ) -> Result<IncomingAuthorization, ObexAgentError> {
        let connection = self.connection.get().cloned().ok_or_else(|| {
            ObexAgentError::Canceled("OBEX agent connection is unavailable".into())
        })?;
        let details = incoming_details(&connection, &transfer_path)
            .await
            .map_err(rejected)?;
        let remote = self
            .backend
            .obex_remote(&details.source, &details.destination)
            .await
            .map_err(rejected)?;
        let file_name = safe_file_name(&details.name);
        let destination = incoming_destination(&file_name).map_err(rejected)?;
        let request_id = format!(
            "obex-incoming-{}",
            self.sequence.fetch_add(1, Ordering::Relaxed)
        );
        Ok(IncomingAuthorization {
            connection,
            transfer_path,
            request_id,
            remote,
            details,
            file_name,
            destination,
        })
    }

    async fn request_decision(
        &self,
        authorization: &IncomingAuthorization,
    ) -> Result<AuthorizationDecision, ObexAgentError> {
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(
            authorization.request_id.clone(),
            PendingAuthorization { sender },
        );
        let requested = authorization.event(
            "authorization-requested",
            "awaiting-authorization",
            Some(AUTHORIZATION_TIMEOUT.as_millis() as u64),
        );
        let _ = self.events.send(requested);
        match tokio::time::timeout(AUTHORIZATION_TIMEOUT, receiver).await {
            Ok(Ok(decision)) => Ok(decision),
            Ok(Err(_)) => Ok(AuthorizationDecision::Cancel),
            Err(_) => {
                self.pending.lock().await.remove(&authorization.request_id);
                let _ = self
                    .events
                    .send(authorization.event("cancelled", "cancelled", None));
                Err(ObexAgentError::Canceled(
                    "incoming transfer authorization timed out".into(),
                ))
            }
        }
    }

    fn reject_authorization(
        &self,
        authorization: IncomingAuthorization,
        error: ObexAgentError,
    ) -> ObexAgentError {
        let _ = self
            .events
            .send(authorization.event("cancelled", "cancelled", None));
        error
    }

    async fn start_incoming(&self, authorization: IncomingAuthorization) -> String {
        let (cancel_sender, cancel_receiver) = oneshot::channel();
        self.cancellations
            .lock()
            .await
            .insert(authorization.request_id.clone(), cancel_sender);
        let events = self.events.clone();
        let cancellations = Arc::clone(&self.cancellations);
        let task_id = authorization.request_id.clone();
        let event = authorization.event("queued", "queued", None);
        let destination = authorization.destination.to_string_lossy().into_owned();
        tokio::spawn(async move {
            let result = monitor_incoming(
                &authorization.connection,
                authorization.transfer_path,
                cancel_receiver,
                event.clone(),
                &events,
            )
            .await;
            cancellations.lock().await.remove(&task_id);
            if let Err(error) = result {
                let mut failed = event;
                failed.event = "failed".into();
                failed.status = "error".into();
                failed.error = Some(serde_json::json!({
                    "code": "obex-transfer-failed",
                    "message": format!("{error:#}"),
                }));
                let _ = events.send(failed);
            }
        });
        destination
    }
}

fn rejected(error: anyhow::Error) -> ObexAgentError {
    ObexAgentError::Rejected(format!("{error:#}"))
}
#[derive(Clone)]
pub struct ObexAgent {
    broker: Arc<IncomingBroker>,
}

impl ObexAgent {
    pub fn new(broker: Arc<IncomingBroker>) -> Self {
        Self { broker }
    }
}

#[derive(Debug, DBusError)]
#[zbus(prefix = "org.bluez.obex.Error")]
pub enum ObexAgentError {
    Rejected(String),
    Canceled(String),
}

#[zbus::interface(name = "org.bluez.obex.Agent1")]
impl ObexAgent {
    async fn release(&self) {
        // NameOwnerChanged is authoritative for registration state. During an
        // obexd replacement, the old owner can deliver Release after the new
        // owner has already accepted this same agent again.
        self.broker.cancel_authorizations().await;
    }

    async fn authorize_push(
        &self,
        transfer: OwnedObjectPath,
    ) -> std::result::Result<String, ObexAgentError> {
        self.broker.authorize(transfer).await
    }

    async fn cancel(&self) {
        self.broker.cancel_authorizations().await;
    }
}

pub async fn register_agent(
    connection: &zbus::Connection,
    broker: &Arc<IncomingBroker>,
) -> Result<()> {
    let manager = zbus::Proxy::new(connection, BUS_NAME, OBJECT_PATH, AGENT_MANAGER_INTERFACE)
        .await
        .context("create OBEX agent-manager proxy")?;
    let path = OwnedObjectPath::try_from(AGENT_PATH).context("create OBEX agent path")?;
    manager
        .call::<_, _, ()>("RegisterAgent", &(path,))
        .await
        .context("register incoming OBEX authorization agent")?;
    broker.available.store(true, Ordering::Relaxed);
    Ok(())
}

pub fn monitor_agent_owner(connection: zbus::Connection, broker: Arc<IncomingBroker>) {
    tokio::spawn(async move {
        loop {
            let result = watch_agent_owner(&connection, &broker).await;
            broker.available.store(false, Ordering::Relaxed);
            if let Err(error) = result {
                tracing::warn!(%error, "OBEX agent owner monitor is retrying");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Err(error) = register_agent(&connection, &broker).await {
                tracing::warn!(%error, "could not restore incoming OBEX agent");
            }
        }
    });
}

async fn watch_agent_owner(
    connection: &zbus::Connection,
    broker: &Arc<IncomingBroker>,
) -> Result<()> {
    let proxy = zbus::Proxy::new(
        connection,
        "org.freedesktop.DBus",
        "/org/freedesktop/DBus",
        "org.freedesktop.DBus",
    )
    .await?;
    let mut changes = proxy.receive_signal("NameOwnerChanged").await?;
    let mut retry = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            message = changes.next() => {
                let message = message.context("OBEX owner watch ended")?;
                let (name, old_owner, new_owner): (String, String, String) =
                    message.body().deserialize()?;
                if name == BUS_NAME && old_owner != new_owner {
                    broker.available.store(false, Ordering::Relaxed);
                    broker.cancel_authorizations().await;
                    if !new_owner.is_empty()
                        && let Err(error) = register_agent(connection, broker).await
                    {
                        tracing::debug!(%error, "incoming OBEX agent is waiting for ownership");
                    }
                }
            }
            _ = retry.tick(), if !broker.is_available() => {
                if let Err(error) = register_agent(connection, broker).await {
                    tracing::debug!(%error, "incoming OBEX agent is waiting for ownership");
                }
            }
        }
    }
}

pub async fn probe(incoming_authorization: bool) -> Result<ObexCapabilities> {
    let connection = zbus::Connection::session()
        .await
        .context("connect to session D-Bus for OBEX")?;
    let peer = zbus::Proxy::new(
        &connection,
        BUS_NAME,
        OBJECT_PATH,
        "org.freedesktop.DBus.Peer",
    )
    .await
    .context("create obexd peer proxy")?;
    peer.call_method("Ping", &())
        .await
        .context("activate and ping obexd")?;
    Ok(ObexCapabilities {
        available: true,
        outgoing_object_push: true,
        incoming_authorization,
        transfer_progress: true,
        cancellation: true,
    })
}

pub async fn start_file(
    source: &str,
    destination: &str,
    selected_path: &str,
) -> Result<ActiveTransfer> {
    let path = validate_outgoing_path(selected_path)?;
    let metadata = std::fs::metadata(&path).context("read outgoing file metadata")?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("outgoing filename is not valid UTF-8")?
        .to_string();
    let path = path.to_str().context("outgoing path is not valid UTF-8")?;
    let connection = zbus::Connection::session()
        .await
        .context("connect to obexd")?;
    let client = zbus::Proxy::new(&connection, BUS_NAME, OBJECT_PATH, CLIENT_INTERFACE)
        .await
        .context("create obexd client proxy")?;
    let mut args = HashMap::<&str, Value<'_>>::new();
    args.insert("Target", Value::from("opp"));
    args.insert("Source", Value::from(source));
    let session_path: OwnedObjectPath = client
        .call("CreateSession", &(destination, args))
        .await
        .context("create OBEX object-push session")?;
    let transfer_result = async {
        let push = zbus::Proxy::new(&connection, BUS_NAME, session_path.as_str(), PUSH_INTERFACE)
            .await
            .context("create OBEX object-push proxy")?;
        push.call::<_, _, (OwnedObjectPath, HashMap<String, OwnedValue>)>("SendFile", &(path,))
            .await
            .context("start OBEX file transfer")
    }
    .await;
    let (transfer_path, properties) = match transfer_result {
        Ok(result) => result,
        Err(error) => {
            let _ = client
                .call::<_, _, ()>("RemoveSession", &(session_path.clone(),))
                .await;
            return Err(error);
        }
    };
    Ok(ActiveTransfer {
        connection,
        session_path,
        transfer_path,
        file_name,
        size: property_u64(&properties, "Size").unwrap_or(metadata.len()),
        initial_status: property_string(&properties, "Status").unwrap_or_else(|| "queued".into()),
        initial_transferred: property_u64(&properties, "Transferred").unwrap_or(0),
    })
}

impl ActiveTransfer {
    pub async fn run(
        self,
        mut cancel: oneshot::Receiver<()>,
        mut update: impl FnMut(TransferUpdate),
    ) -> Result<()> {
        let result = async {
            let mut current = TransferUpdate {
                status: self.initial_status.clone(),
                transferred: self.initial_transferred,
                size: self.size,
            };
            update(current.clone());
            if !matches!(current.status.as_str(), "complete" | "error") {
                let transfer = zbus::Proxy::new(
                    &self.connection,
                    BUS_NAME,
                    self.transfer_path.as_str(),
                    TRANSFER_INTERFACE,
                )
                .await
                .context("create OBEX transfer proxy")?;
                let properties = PropertiesProxy::builder(&self.connection)
                    .destination(BUS_NAME)?
                    .path(self.transfer_path.clone())?
                    .build()
                    .await?;
                let mut changes = properties.receive_properties_changed().await?;
                while !matches!(current.status.as_str(), "complete" | "error") {
                    tokio::select! {
                        _ = &mut cancel => {
                            transfer.call::<_, _, ()>("Cancel", &()).await.context("cancel OBEX transfer")?;
                            current.status = "cancelled".into();
                            update(current.clone());
                            break;
                        }
                        signal = changes.next() => {
                            let signal = signal.context("OBEX property stream ended")?;
                            let args = signal.args()?;
                            if args.interface_name() != TRANSFER_INTERFACE { continue; }
                            if let Some(status) = args.changed_properties().get("Status").and_then(borrowed_string) {
                                current.status = status;
                            }
                            if let Some(value) = args.changed_properties().get("Transferred").and_then(borrowed_u64) {
                                current.transferred = value;
                            }
                            update(current.clone());
                        }
                    }
                }
            }
            if current.status == "error" {
                bail!("OBEX transfer failed");
            }
            Ok(())
        }
        .await;
        if let Ok(client) =
            zbus::Proxy::new(&self.connection, BUS_NAME, OBJECT_PATH, CLIENT_INTERFACE).await
        {
            let _ = client
                .call::<_, _, ()>("RemoveSession", &(self.session_path,))
                .await;
        }
        result
    }
}

struct IncomingDetails {
    source: String,
    destination: String,
    name: String,
    media_type: Option<String>,
    size: u64,
}

async fn incoming_details(
    connection: &zbus::Connection,
    transfer_path: &OwnedObjectPath,
) -> Result<IncomingDetails> {
    let transfer = PropertiesProxy::builder(connection)
        .destination(BUS_NAME)?
        .path(transfer_path.clone())?
        .build()
        .await?;
    let values = transfer
        .get_all(TRANSFER_INTERFACE.try_into()?)
        .await
        .context("read incoming OBEX transfer")?;
    let session_path = values
        .get("Session")
        .and_then(|value| value.try_clone().ok())
        .and_then(|value| OwnedObjectPath::try_from(value).ok())
        .context("incoming OBEX transfer has no session")?;
    let session = PropertiesProxy::builder(connection)
        .destination(BUS_NAME)?
        .path(session_path)?
        .build()
        .await?;
    let session_values = session
        .get_all(SESSION_INTERFACE.try_into()?)
        .await
        .context("read incoming OBEX session")?;
    let name = property_string(&values, "Name")
        .or_else(|| {
            property_string(&values, "Filename").and_then(|value| {
                Path::new(&value)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(str::to_string)
            })
        })
        .unwrap_or_else(|| "bluetooth-transfer".into());
    Ok(IncomingDetails {
        source: property_string(&session_values, "Source")
            .context("incoming OBEX session has no source")?,
        destination: property_string(&session_values, "Destination")
            .context("incoming OBEX session has no destination")?,
        name,
        media_type: property_string(&values, "Type"),
        size: property_u64(&values, "Size").unwrap_or(0),
    })
}

async fn monitor_incoming(
    connection: &zbus::Connection,
    transfer_path: OwnedObjectPath,
    mut cancel: oneshot::Receiver<()>,
    mut event: ObexEvent,
    events: &broadcast::Sender<ObexEvent>,
) -> Result<()> {
    let transfer = zbus::Proxy::new(
        connection,
        BUS_NAME,
        transfer_path.as_str(),
        TRANSFER_INTERFACE,
    )
    .await
    .context("create incoming OBEX transfer proxy")?;
    let properties = PropertiesProxy::builder(connection)
        .destination(BUS_NAME)?
        .path(transfer_path.clone())?
        .build()
        .await?;
    let mut changes = properties.receive_properties_changed().await?;
    let initial = properties.get_all(TRANSFER_INTERFACE.try_into()?).await?;
    event.status = property_string(&initial, "Status").unwrap_or_else(|| "queued".into());
    event.transferred = property_u64(&initial, "Transferred").unwrap_or(0);
    event.size = property_u64(&initial, "Size").unwrap_or(event.size);
    event.event = lifecycle_event(&event.status).into();
    let _ = events.send(event.clone());
    while !matches!(event.status.as_str(), "complete" | "error" | "cancelled") {
        tokio::select! {
            _ = &mut cancel => {
                transfer.call::<_, _, ()>("Cancel", &()).await.context("cancel incoming OBEX transfer")?;
                event.event = "cancelled".into();
                event.status = "cancelled".into();
                let _ = events.send(event.clone());
                return Ok(());
            }
            signal = changes.next() => {
                let signal = signal.context("incoming OBEX property stream ended")?;
                let args = signal.args()?;
                if args.interface_name() != TRANSFER_INTERFACE { continue; }
                if let Some(status) = args.changed_properties().get("Status").and_then(borrowed_string) {
                    event.status = status;
                }
                if let Some(value) = args.changed_properties().get("Transferred").and_then(borrowed_u64) {
                    event.transferred = value;
                }
                event.event = lifecycle_event(&event.status).into();
                let _ = events.send(event.clone());
            }
        }
    }
    if event.status == "error" {
        bail!("incoming OBEX transfer failed");
    }
    Ok(())
}

pub(crate) fn lifecycle_event(status: &str) -> &'static str {
    match status {
        "complete" => "completed",
        "cancelled" => "cancelled",
        "error" => "failed",
        "queued" => "queued",
        _ => "progress",
    }
}

fn incoming_destination(file_name: &str) -> Result<PathBuf> {
    let directory = std::env::var_os("BT_DAEMON_DOWNLOAD_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("XDG_DOWNLOAD_DIR").map(PathBuf::from))
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join("Downloads")))
        .context("no incoming Bluetooth download directory is configured")?;
    incoming_destination_in(&directory, file_name)
}

fn incoming_destination_in(directory: &Path, file_name: &str) -> Result<PathBuf> {
    std::fs::create_dir_all(directory).with_context(|| {
        format!(
            "create Bluetooth download directory {}",
            directory.display()
        )
    })?;
    let directory = directory
        .canonicalize()
        .context("resolve Bluetooth download directory")?;
    let candidate = directory.join(file_name);
    if !candidate.exists() {
        return Ok(candidate);
    }
    let path = Path::new(file_name);
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("bluetooth-transfer");
    let extension = path.extension().and_then(|value| value.to_str());
    for suffix in 1..10_000 {
        let name = match extension {
            Some(extension) => format!("{stem} ({suffix}).{extension}"),
            None => format!("{stem} ({suffix})"),
        };
        let candidate = directory.join(name);
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    bail!("could not allocate a unique incoming Bluetooth filename")
}

fn safe_file_name(value: &str) -> String {
    let basename = Path::new(value)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("bluetooth-transfer");
    let sanitized = basename
        .chars()
        .filter(|character| !character.is_control())
        .take(180)
        .collect::<String>();
    if sanitized.is_empty() || matches!(sanitized.as_str(), "." | "..") {
        "bluetooth-transfer".into()
    } else {
        sanitized
    }
}

fn validate_outgoing_path(value: &str) -> Result<PathBuf> {
    if value.is_empty() {
        bail!("outgoing file path is required");
    }
    let path = Path::new(value)
        .canonicalize()
        .context("resolve outgoing file path")?;
    let metadata = std::fs::metadata(&path).context("read outgoing file metadata")?;
    if !metadata.is_file() {
        bail!("outgoing path is not a regular file");
    }
    Ok(path)
}

fn property_string(values: &HashMap<String, OwnedValue>, key: &str) -> Option<String> {
    values.get(key).and_then(value_string)
}
fn property_u64(values: &HashMap<String, OwnedValue>, key: &str) -> Option<u64> {
    values.get(key).and_then(value_u64)
}
fn value_string(value: &OwnedValue) -> Option<String> {
    <&str>::try_from(value).ok().map(str::to_string)
}
fn value_u64(value: &OwnedValue) -> Option<u64> {
    u64::try_from(value).ok()
}
fn borrowed_string(value: &Value<'_>) -> Option<String> {
    value.downcast_ref::<&str>().ok().map(str::to_string)
}
fn borrowed_u64(value: &Value<'_>) -> Option<u64> {
    value.downcast_ref::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::backend::ObexRemote;

    use super::{
        IncomingDetails, incoming_destination_in, incoming_event, lifecycle_event, safe_file_name,
        validate_outgoing_path,
    };

    #[test]
    fn incoming_names_are_confined_to_the_download_directory() {
        assert_eq!(safe_file_name("../../secret.txt"), "secret.txt");
        assert_eq!(safe_file_name(".."), "bluetooth-transfer");
        assert_eq!(safe_file_name("bad\nname.txt"), "badname.txt");
    }

    #[test]
    fn incoming_names_do_not_overwrite_existing_files() {
        let directory = std::env::temp_dir().join(format!("bt-obex-in-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join("example.txt"), b"existing").unwrap();
        assert_eq!(
            incoming_destination_in(&directory, "example.txt").unwrap(),
            directory.join("example (1).txt")
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn incoming_event_lifecycle_is_built_without_dbus() {
        let details = IncomingDetails {
            source: "00:11:22:33:44:55".into(),
            destination: "AA:BB:CC:DD:EE:FF".into(),
            name: "photo.jpg".into(),
            media_type: Some("image/jpeg".into()),
            size: 2048,
        };
        let event = incoming_event(
            "request-1",
            &ObexRemote {
                device_key: "device-1".into(),
                name: "Phone".into(),
            },
            &details,
            "photo.jpg",
            "authorization-requested",
            "awaiting-authorization",
            Some(60_000),
        );
        assert_eq!(event.device_key, "device-1");
        assert_eq!(event.size, 2048);
        assert_eq!(event.timeout_ms, Some(60_000));
        assert_eq!(lifecycle_event("complete"), "completed");
        assert_eq!(lifecycle_event("active"), "progress");
    }

    #[test]
    fn outgoing_paths_must_be_regular_files() {
        let directory = std::env::temp_dir().join(format!("bt-obex-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&directory).unwrap();
        assert!(validate_outgoing_path(directory.to_str().unwrap()).is_err());
        let file = directory.join("example.txt");
        fs::write(&file, b"hello").unwrap();
        assert_eq!(
            validate_outgoing_path(file.to_str().unwrap()).unwrap(),
            file
        );
        fs::remove_dir_all(directory).unwrap();
    }
}
