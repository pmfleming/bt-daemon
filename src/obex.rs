use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use serde::Serialize;
use tokio::sync::oneshot;
use zbus::fdo::PropertiesProxy;
use zvariant::{OwnedObjectPath, OwnedValue, Value};

const BUS_NAME: &str = "org.bluez.obex";
const OBJECT_PATH: &str = "/org/bluez/obex";
const CLIENT_INTERFACE: &str = "org.bluez.obex.Client1";
const PUSH_INTERFACE: &str = "org.bluez.obex.ObjectPush1";
const TRANSFER_INTERFACE: &str = "org.bluez.obex.Transfer1";

#[derive(Debug, Clone, Serialize)]
pub struct ObexCapabilities {
    pub available: bool,
    pub outgoing_object_push: bool,
    pub incoming_authorization: bool,
    pub transfer_progress: bool,
    pub cancellation: bool,
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

pub async fn probe() -> Result<ObexCapabilities> {
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
        incoming_authorization: false,
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

    use super::{ObexCapabilities, validate_outgoing_path};

    #[test]
    fn staged_capabilities_are_explicit() {
        let capabilities = ObexCapabilities {
            available: true,
            outgoing_object_push: true,
            incoming_authorization: false,
            transfer_progress: true,
            cancellation: true,
        };
        assert!(capabilities.outgoing_object_push);
        assert!(!capabilities.incoming_authorization);
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
