use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex, OnceLock,
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

mod authorization;
#[cfg(test)]
mod lifecycle_tests;

const BUS_NAME: &str = "org.bluez.obex";
const OBJECT_PATH: &str = "/org/bluez/obex";
const CLIENT_INTERFACE: &str = "org.bluez.obex.Client1";
const AGENT_MANAGER_INTERFACE: &str = "org.bluez.obex.AgentManager1";
const PUSH_INTERFACE: &str = "org.bluez.obex.ObjectPush1";
const TRANSFER_INTERFACE: &str = "org.bluez.obex.Transfer1";
const SESSION_INTERFACE: &str = "org.bluez.obex.Session1";
/// Session-bus object path for the optional incoming-transfer authorization agent.
pub const AGENT_PATH: &str = "/org/laufan/BluetoothDaemon/ObexAgent";
const AUTHORIZATION_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Serialize)]
/// Transfer features currently available from obexd and the local authorization agent.
pub struct ObexCapabilities {
    pub available: bool,
    pub outgoing_object_push: bool,
    pub incoming_authorization: bool,
    pub transfer_progress: bool,
    pub cancellation: bool,
}

#[derive(Debug, Clone, Serialize)]
/// Transfer progress or authorization event correlated by request and opaque device IDs.
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

impl ObexEvent {
    pub(crate) fn outgoing(request_id: &str, device_key: &str, file_name: &str, size: u64) -> Self {
        Self {
            event: "queued".into(),
            request_id: request_id.into(),
            direction: "outgoing".into(),
            device_key: device_key.into(),
            device_name: None,
            file_name: file_name.into(),
            media_type: None,
            status: "queued".into(),
            transferred: 0,
            size,
            timeout_ms: None,
            error: None,
        }
    }

    pub(crate) fn updated(&self, update: &TransferUpdate) -> Self {
        Self {
            event: lifecycle_event(&update.status).into(),
            status: update.status.clone(),
            transferred: update.transferred,
            size: update.size,
            ..self.clone()
        }
    }

    pub(crate) fn failed(mut self, error: JsonValue) -> Self {
        self.event = "failed".into();
        self.status = "error".into();
        self.error = Some(error);
        self
    }
}

#[derive(Debug, Clone, Serialize)]
/// Latest obexd transfer state; byte counts accumulate across property updates.
pub struct TransferUpdate {
    pub status: String,
    pub transferred: u64,
    pub size: u64,
}

impl TransferUpdate {
    fn apply(&mut self, properties: &HashMap<String, OwnedValue>) {
        self.apply_values(
            properties.get("Status").and_then(value_string),
            properties.get("Transferred").and_then(value_u64),
            properties.get("Size").and_then(value_u64),
        );
    }

    fn apply_changed(&mut self, properties: &HashMap<&str, Value<'_>>) {
        self.apply_values(
            properties.get("Status").and_then(borrowed_string),
            properties.get("Transferred").and_then(borrowed_u64),
            None,
        );
    }

    fn apply_values(
        &mut self,
        status: Option<String>,
        transferred: Option<u64>,
        size: Option<u64>,
    ) {
        if let Some(status) = status {
            self.status = status;
        }
        if let Some(transferred) = transferred {
            self.transferred = transferred;
        }
        if let Some(size) = size {
            self.size = size;
        }
    }

    fn terminal(&self) -> bool {
        matches!(self.status.as_str(), "complete" | "error" | "cancelled")
    }
}

/// Owns the D-Bus session and transfer paths needed to monitor/cancel an outgoing push.
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
    destination: authorization::Destination,
}

impl IncomingAuthorization {
    fn event(&self, event: &str, status: &str, timeout_ms: Option<u64>) -> ObexEvent {
        ObexEvent {
            event: event.into(),
            request_id: self.request_id.clone(),
            direction: "incoming".into(),
            device_key: self.remote.device_key.clone(),
            device_name: Some(self.remote.name.clone()),
            file_name: self.file_name.clone(),
            media_type: self.details.media_type.clone(),
            status: status.into(),
            transferred: 0,
            size: self.details.size,
            timeout_ms,
            error: None,
        }
    }
}

/// Tracks incoming authorizations and transfers, with bounded approval deadlines.
pub struct IncomingBroker {
    backend: Arc<dyn BluetoothBackend>,
    events: broadcast::Sender<ObexEvent>,
    sequence: AtomicU64,
    available: AtomicBool,
    connection: OnceLock<zbus::Connection>,
    pending: StdMutex<HashMap<String, PendingAuthorization>>,
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
            pending: StdMutex::new(HashMap::new()),
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
        tracing::info!(%request_id, accept, "answering incoming OBEX authorization");
        let pending = self
            .pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
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
        if let Some(pending) = self
            .pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(request_id)
        {
            tracing::info!(%request_id, "cancelling pending incoming OBEX authorization");
            let _ = pending.sender.send(AuthorizationDecision::Cancel);
            return true;
        }
        if let Some(cancel) = self.cancellations.lock().await.remove(request_id) {
            tracing::info!(%request_id, "cancelling active incoming OBEX transfer");
            let _ = cancel.send(());
            return true;
        }
        false
    }

    async fn cancel_authorizations(&self) {
        for (_, pending) in self
            .pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .drain()
        {
            let _ = pending.sender.send(AuthorizationDecision::Cancel);
        }
    }

    async fn authorize(&self, transfer_path: OwnedObjectPath) -> Result<String, ObexAgentError> {
        let authorization = self.prepare_authorization(transfer_path).await?;
        let decision = match self.request_decision(&authorization).await {
            Ok(decision) => decision,
            Err(error) => {
                return Err(error);
            }
        };
        match decision {
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
        let destination = reserve_incoming_destination(&file_name).map_err(rejected)?;
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
            destination: authorization::Destination {
                path: destination,
                accepted: false,
            },
        })
    }

    async fn request_decision(
        &self,
        authorization: &IncomingAuthorization,
    ) -> Result<AuthorizationDecision, ObexAgentError> {
        let (sender, receiver) = oneshot::channel();
        self.pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                authorization.request_id.clone(),
                PendingAuthorization { sender },
            );
        tracing::info!(request_id = %authorization.request_id, device_key = %authorization.remote.device_key, file_name = %authorization.file_name, size = authorization.details.size, "incoming OBEX authorization requested");
        let requested = authorization.event(
            "authorization-requested",
            "awaiting-authorization",
            Some(AUTHORIZATION_TIMEOUT.as_millis() as u64),
        );
        let _prompt = authorization::Prompt {
            broker: self,
            event: requested.clone(),
        };
        let _ = self.events.send(requested);
        match tokio::time::timeout(AUTHORIZATION_TIMEOUT, receiver).await {
            Ok(Ok(decision)) => {
                tracing::info!(request_id = %authorization.request_id, "incoming OBEX authorization answered");
                Ok(decision)
            }
            Ok(Err(_)) => {
                tracing::warn!(request_id = %authorization.request_id, "incoming OBEX authorization responder disappeared");
                Ok(AuthorizationDecision::Cancel)
            }
            Err(_) => {
                tracing::warn!(request_id = %authorization.request_id, "incoming OBEX authorization timed out");
                self.pending
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(&authorization.request_id);
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

    async fn start_incoming(&self, mut authorization: IncomingAuthorization) -> String {
        let (cancel_sender, cancel_receiver) = oneshot::channel();
        self.cancellations
            .lock()
            .await
            .insert(authorization.request_id.clone(), cancel_sender);
        let events = self.events.clone();
        let cancellations = Arc::clone(&self.cancellations);
        let task_id = authorization.request_id.clone();
        let event = authorization.event("queued", "queued", None);
        let destination = authorization
            .destination
            .path
            .to_string_lossy()
            .into_owned();
        authorization.destination.accepted = true;
        tracing::info!(request_id = %authorization.request_id, device_key = %authorization.remote.device_key, file_name = %authorization.file_name, "incoming OBEX transfer queued");
        crate::task::spawn("incoming-obex-transfer", async move {
            let result = crate::task::catch(
                "incoming OBEX transfer",
                monitor_incoming(
                    &authorization.connection,
                    authorization.transfer_path,
                    cancel_receiver,
                    event.clone(),
                    &events,
                ),
            )
            .await;
            cancellations.lock().await.remove(&task_id);
            if let Err(error) = result {
                let error = serde_json::json!({
                    "code": "obex-transfer-failed",
                    "message": format!("{error:#}"),
                });
                let _ = events.send(event.failed(error));
            }
        });
        destination
    }
}

fn rejected(error: anyhow::Error) -> ObexAgentError {
    ObexAgentError::Rejected(format!("{error:#}"))
}
#[derive(Clone)]
/// BlueZ OBEX Agent1 implementation delegating authorization to the incoming broker.
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
/// D-Bus errors distinguishing user rejection from cancelled authorization.
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

/// Register the already-exported incoming agent with obexd.
/// Call only when incoming transfers have been explicitly enabled by the operator.
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
    tracing::info!(path = AGENT_PATH, "incoming OBEX agent registered");
    Ok(())
}

/// Monitor obexd ownership and restore an explicitly enabled agent after restarts.
pub fn monitor_agent_owner(connection: zbus::Connection, broker: Arc<IncomingBroker>) {
    crate::task::spawn("obex-agent-owner", async move {
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

/// Activate/ping obexd and report transfer support; may activate the session service.
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

/// Validate a selected regular file and start an OBEX push between transport addresses.
/// Returns ownership of the active transfer for progress monitoring and cancellation.
pub async fn start_file(
    source: &str,
    destination: &str,
    selected_path: &str,
) -> Result<ActiveTransfer> {
    tracing::debug!("creating outgoing OBEX session");
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
            if let Err(cleanup_error) = client
                .call::<_, _, ()>("RemoveSession", &(&session_path,))
                .await
            {
                tracing::warn!(%cleanup_error, path = %session_path, "could not clean up failed OBEX session");
            }
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
        cancel: oneshot::Receiver<()>,
        mut update: impl FnMut(&TransferUpdate),
    ) -> Result<()> {
        let current = TransferUpdate {
            status: self.initial_status,
            transferred: self.initial_transferred,
            size: self.size,
        };
        update(&current);
        let result = monitor_transfer(
            &self.connection,
            &self.transfer_path,
            current,
            cancel,
            update,
        )
        .await;
        cleanup_session(&self.connection, self.session_path).await;
        result
    }
}

async fn cleanup_session(connection: &zbus::Connection, session_path: OwnedObjectPath) {
    match zbus::Proxy::new(connection, BUS_NAME, OBJECT_PATH, CLIENT_INTERFACE).await {
        Ok(client) => {
            if let Err(error) = client
                .call::<_, _, ()>("RemoveSession", &(session_path,))
                .await
            {
                tracing::warn!(%error, "could not remove completed OBEX session");
            }
        }
        Err(error) => tracing::warn!(%error, "could not create OBEX client for session cleanup"),
    }
}

struct IncomingDetails {
    source: String,
    destination: String,
    name: String,
    media_type: Option<String>,
    size: u64,
}

async fn read_properties(
    connection: &zbus::Connection,
    path: &OwnedObjectPath,
    interface: &'static str,
    description: &'static str,
) -> Result<HashMap<String, OwnedValue>> {
    PropertiesProxy::builder(connection)
        .destination(BUS_NAME)?
        .path(path.as_str())?
        .build()
        .await?
        .get_all(interface.try_into()?)
        .await
        .with_context(|| format!("read {description}"))
}

fn property_path(values: &HashMap<String, OwnedValue>, name: &str) -> Option<OwnedObjectPath> {
    values
        .get(name)?
        .try_clone()
        .ok()
        .and_then(|value| OwnedObjectPath::try_from(value).ok())
}

fn incoming_name(values: &HashMap<String, OwnedValue>) -> String {
    property_string(values, "Name")
        .or_else(|| {
            let filename = property_string(values, "Filename")?;
            Path::new(&filename)
                .file_name()?
                .to_str()
                .map(str::to_string)
        })
        .unwrap_or_else(|| "bluetooth-transfer".into())
}

async fn incoming_details(
    connection: &zbus::Connection,
    transfer_path: &OwnedObjectPath,
) -> Result<IncomingDetails> {
    let values = read_properties(
        connection,
        transfer_path,
        TRANSFER_INTERFACE,
        "incoming OBEX transfer",
    )
    .await?;
    let session_path =
        property_path(&values, "Session").context("incoming OBEX transfer has no session")?;
    let session_values = read_properties(
        connection,
        &session_path,
        SESSION_INTERFACE,
        "incoming OBEX session",
    )
    .await?;
    Ok(IncomingDetails {
        source: property_string(&session_values, "Source")
            .context("incoming OBEX session has no source")?,
        destination: property_string(&session_values, "Destination")
            .context("incoming OBEX session has no destination")?,
        name: incoming_name(&values),
        media_type: property_string(&values, "Type"),
        size: property_u64(&values, "Size").unwrap_or(0),
    })
}

async fn monitor_incoming(
    connection: &zbus::Connection,
    transfer_path: OwnedObjectPath,
    cancel: oneshot::Receiver<()>,
    mut event: ObexEvent,
    events: &broadcast::Sender<ObexEvent>,
) -> Result<()> {
    let current = TransferUpdate {
        status: "queued".into(),
        transferred: 0,
        size: event.size,
    };
    monitor_transfer(connection, &transfer_path, current, cancel, |current| {
        publish_transfer_update(&mut event, current, events);
    })
    .await
    .context("monitor incoming OBEX transfer")
}

// Both directions subscribe before refreshing: completion can occur between the
// initial request/authorization and the start of monitoring. Cancellation must
// be acknowledged by obexd before publishing a terminal event.
async fn monitor_transfer(
    connection: &zbus::Connection,
    path: &OwnedObjectPath,
    mut current: TransferUpdate,
    mut cancel: oneshot::Receiver<()>,
    mut update: impl FnMut(&TransferUpdate),
) -> Result<()> {
    if !current.terminal() {
        let transfer = zbus::Proxy::new(connection, BUS_NAME, path.as_str(), TRANSFER_INTERFACE)
            .await
            .context("create OBEX transfer proxy")?;
        let properties = PropertiesProxy::builder(connection)
            .destination(BUS_NAME)?
            .path(path.as_str())?
            .build()
            .await?;
        let mut changes = properties.receive_properties_changed().await?;
        current.apply(&properties.get_all(TRANSFER_INTERFACE.try_into()?).await?);
        update(&current);
        while !current.terminal() {
            tokio::select! {
                _ = &mut cancel => {
                    transfer.call::<_, _, ()>("Cancel", &()).await.context("cancel OBEX transfer")?;
                    current.status = "cancelled".into();
                }
                signal = changes.next() => {
                    let signal = signal.context("OBEX property stream ended")?;
                    let args = signal.args()?;
                    if args.interface_name() != TRANSFER_INTERFACE { continue; }
                    current.apply_changed(args.changed_properties());
                }
            }
            update(&current);
        }
    }
    anyhow::ensure!(current.status != "error", "OBEX transfer failed");
    Ok(())
}

fn publish_transfer_update(
    event: &mut ObexEvent,
    update: &TransferUpdate,
    events: &broadcast::Sender<ObexEvent>,
) {
    event.status.clone_from(&update.status);
    event.transferred = update.transferred;
    event.size = update.size;
    event.event = lifecycle_event(&event.status).into();
    tracing::debug!(request_id = %event.request_id, status = %event.status, transferred = event.transferred, size = event.size, "incoming OBEX transfer status changed");
    let _ = events.send(event.clone());
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

fn reserve_incoming_destination(file_name: &str) -> Result<PathBuf> {
    let directory = std::env::var_os("BT_DAEMON_DOWNLOAD_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("XDG_DOWNLOAD_DIR").map(PathBuf::from))
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join("Downloads")))
        .context("no incoming Bluetooth download directory is configured")?;
    reserve_incoming_destination_in(&directory, file_name)
}

fn reserve_incoming_destination_in(directory: &Path, file_name: &str) -> Result<PathBuf> {
    std::fs::create_dir_all(directory).with_context(|| {
        format!(
            "create Bluetooth download directory {}",
            directory.display()
        )
    })?;
    let directory = directory
        .canonicalize()
        .context("resolve Bluetooth download directory")?;
    let path = Path::new(file_name);
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("bluetooth-transfer");
    let extension = path.extension().and_then(|value| value.to_str());
    for suffix in 0..10_000 {
        let name = match (suffix, extension) {
            (0, _) => file_name.to_string(),
            (_, Some(extension)) => format!("{stem} ({suffix}).{extension}"),
            (_, None) => format!("{stem} ({suffix})"),
        };
        let candidate = directory.join(name);
        // obexd opens the authorized destination with O_TRUNC. Creating it
        // exclusively here reserves the name across concurrent authorizations.
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&candidate) {
            Ok(_) => return Ok(candidate),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("reserve incoming Bluetooth file {}", candidate.display())
                });
            }
        }
    }
    bail!("could not reserve a unique incoming Bluetooth filename")
}

fn remove_reservation(path: &Path) {
    if let Err(error) = std::fs::remove_file(path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(path = %path.display(), %error, "could not remove incoming Bluetooth reservation");
    }
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

    use super::{reserve_incoming_destination_in, safe_file_name, validate_outgoing_path};

    #[test]
    fn refreshed_and_incremental_transfer_states_publish_consistent_events() {
        use super::{
            HashMap, ObexEvent, OwnedValue, TransferUpdate, Value, broadcast,
            publish_transfer_update,
        };
        let mut current = TransferUpdate {
            status: "queued".into(),
            transferred: 0,
            size: 1,
        };
        current.apply(&HashMap::from([
            ("Status".into(), Value::from("active").try_into().unwrap()),
            ("Size".into(), OwnedValue::from(100_u64)),
        ]));
        let mut event = ObexEvent::outgoing("request", "peer", "file", 1);
        let (events, mut received) = broadcast::channel(1);
        for (status, expected, terminal) in [
            ("active", "progress", false),
            ("complete", "completed", true),
            ("cancelled", "cancelled", true),
            ("error", "failed", true),
        ] {
            current.apply_changed(&HashMap::from([
                ("Status", Value::from(status)),
                ("Transferred", Value::from(42_u64)),
            ]));
            assert_eq!(current.terminal(), terminal);
            publish_transfer_update(&mut event, &current, &events);
            let published = received.try_recv().unwrap();
            assert_eq!(
                (published.event.as_str(), published.status.as_str()),
                (expected, status)
            );
            assert_eq!((published.transferred, published.size), (42, 100));
            assert_eq!(
                serde_json::to_value(event.updated(&current)).unwrap(),
                serde_json::to_value(&published).unwrap()
            );
        }
    }

    #[test]
    fn transfer_paths_are_confined_non_overwriting_and_regular_files() {
        let directory = std::env::temp_dir().join(format!("bt-obex-in-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&directory).unwrap();
        assert!(validate_outgoing_path(directory.to_str().unwrap()).is_err());
        fs::write(directory.join("example.txt"), b"existing").unwrap();
        assert_eq!(
            validate_outgoing_path(directory.join("example.txt").to_str().unwrap()).unwrap(),
            directory.join("example.txt")
        );
        for (name, safe) in [
            ("../../secret.txt", "secret.txt"),
            ("..", "bluetooth-transfer"),
            ("bad\nname.txt", "badname.txt"),
        ] {
            let reserved =
                reserve_incoming_destination_in(&directory, &safe_file_name(name)).unwrap();
            assert_eq!(reserved, directory.join(safe));
            assert!(reserved.is_file());
        }
        assert_eq!(
            reserve_incoming_destination_in(&directory, "example.txt").unwrap(),
            directory.join("example (1).txt")
        );
        assert_eq!(
            reserve_incoming_destination_in(&directory, "example.txt").unwrap(),
            directory.join("example (2).txt")
        );
        assert_eq!(
            fs::read(directory.join("example.txt")).unwrap(),
            b"existing"
        );
        fs::remove_dir_all(directory).unwrap();
    }
}
