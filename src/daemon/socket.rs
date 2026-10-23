//! In-process adapter to the exact object exported on D-Bus. No proxy/bus call.
use super::{BluetoothDaemon, OBJECT_PATH, subscription};
use anyhow::Result;
use serde_json::Value;
use shelllist_daemon_tokio::{JsonlListener, JsonlSocketConfig, LocalJsonlService, SocketSession};
use std::sync::Arc;

pub(super) struct Service {
    pub connection: zbus::Connection,
}
impl LocalJsonlService for Service {
    async fn call(&self, session: &SocketSession, method: &str, params: Value) -> Result<Value> {
        let interface = self
            .connection
            .object_server()
            .interface::<_, BluetoothDaemon>(OBJECT_PATH)
            .await?;
        let daemon = interface.get().await;
        let admitted =
            match daemon
                .boundary
                .call(Some(session.owner()), method, &params.to_string())
            {
                Ok(admitted) => admitted,
                Err(response) => return decode(response),
            };
        let response = daemon
            .dispatch_call(method, params, Some(session.owner()), None)
            .await;
        crate::api::log_response(method, &response);
        decode(admitted.encode(&response))
    }
    async fn subscribe(&self, session: &SocketSession, streams: Vec<String>) -> Result<Value> {
        let interface = self
            .connection
            .object_server()
            .interface::<_, BluetoothDaemon>(OBJECT_PATH)
            .await?;
        let daemon = interface.get().await;
        let admitted = match daemon.boundary.subscribe(Some(session.owner()), &streams) {
            Ok(admitted) => admitted,
            Err(response) => return decode(response),
        };
        decode(admitted.encoded(subscription::start_socket(&daemon, streams, session).await))
    }
    async fn cancel(&self, session: &SocketSession, id: &str) -> Result<Value> {
        let interface = self
            .connection
            .object_server()
            .interface::<_, BluetoothDaemon>(OBJECT_PATH)
            .await?;
        let daemon = interface.get().await;
        let admitted = match daemon.boundary.cancel(Some(session.owner()), id) {
            Ok(admitted) => admitted,
            Err(response) => return decode(response),
        };
        decode(admitted.encoded(daemon.cancel_owned(id, Some(session.owner())).await))
    }
    async fn disconnected(&self, session: &SocketSession) {
        if let Ok(interface) = self
            .connection
            .object_server()
            .interface::<_, BluetoothDaemon>(OBJECT_PATH)
            .await
        {
            let daemon = interface.get().await;
            // Subscription tasks already observe session.closed(). No D-Bus
            // watcher and no global cancellation of somebody else's work.
            daemon.operations.cancel_owner(session.owner()).await;
            daemon.obex.outgoing.cancel_owner(session.owner()).await;
            daemon.scans.stop_owner(session.owner()).await;
        }
    }
}
fn decode(response: String) -> Result<Value> {
    Ok(serde_json::from_str(&response)?)
}

pub(super) async fn listen(
    connection: zbus::Connection,
    stop: impl std::future::Future<Output = ()>,
) -> Result<()> {
    let runtime = std::env::var_os("XDG_RUNTIME_DIR")
        .ok_or_else(|| anyhow::anyhow!("XDG_RUNTIME_DIR is required for the Bluetooth socket"))?;
    let listener = JsonlListener::bind(std::path::Path::new(&runtime), "bt-daemon").await?;
    listener
        .serve(
            Arc::new(Service { connection }),
            crate::client::BluetoothCorrelation,
            JsonlSocketConfig::default(),
            stop,
        )
        .await
}
