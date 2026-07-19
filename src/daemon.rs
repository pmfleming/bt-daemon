use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tokio::{sync::Mutex, task::JoinHandle};
use zbus::{connection, object_server::SignalEmitter};

use crate::{
    api,
    backend::BluetoothBackend,
    pairing::{PairingBroker, PairingEvent},
};

pub const BUS_NAME: &str = "org.laufan.BluetoothDaemon";
pub const OBJECT_PATH: &str = "/org/laufan/BluetoothDaemon";
pub const INTERFACE: &str = "org.laufan.BluetoothDaemon1";
pub const CHANGED_STREAM: &str = "bluetooth.changed";
pub const PAIRING_STREAM: &str = "pairing.request";

pub struct BluetoothDaemon {
    backend: Arc<dyn BluetoothBackend>,
    pairing: Arc<PairingBroker>,
    sequence: AtomicU64,
    subscriptions: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
}

impl BluetoothDaemon {
    fn next_id(&self, prefix: &str) -> String {
        format!("{prefix}-{}", self.sequence.fetch_add(1, Ordering::Relaxed))
    }
}

#[zbus::interface(name = "org.laufan.BluetoothDaemon1")]
impl BluetoothDaemon {
    async fn call(&self, method: &str, params_json: &str) -> String {
        let params = serde_json::from_str::<Value>(params_json).unwrap_or(Value::Null);
        if method == "bluetooth.pairing.respond" {
            return match self.pairing.respond(&params).await {
                Ok(()) => api::success(json!({ "result": { "accepted": true } })).to_string(),
                Err(error) => {
                    api::error("pairing-response-rejected", format!("{error:#}")).to_string()
                }
            };
        }
        api::dispatch(Arc::clone(&self.backend), method, params)
            .await
            .to_string()
    }

    async fn subscribe(
        &self,
        streams: Vec<String>,
        #[zbus(signal_emitter)] emitter: SignalEmitter<'_>,
    ) -> String {
        if streams.is_empty()
            || streams
                .iter()
                .any(|stream| stream != CHANGED_STREAM && stream != PAIRING_STREAM)
        {
            return api::error(
                "unsupported-stream",
                "Subscriptions require bluetooth.changed and/or pairing.request".to_string(),
            )
            .to_string();
        }
        let wants_changes = streams.iter().any(|stream| stream == CHANGED_STREAM);
        let wants_pairing = streams.iter().any(|stream| stream == PAIRING_STREAM);
        let id = self.next_id("subscription");
        let mut changes = self.backend.subscribe_changes();
        let mut pairing_events = self.pairing.subscribe();
        let backend = Arc::clone(&self.backend);
        let signal_emitter = emitter.to_owned();
        let subscription_id = id.clone();
        let task = tokio::spawn(async move {
            if wants_changes {
                emit_snapshot(&signal_emitter, &backend, &subscription_id, "subscribed").await;
            }
            loop {
                tokio::select! {
                    result = changes.recv(), if wants_changes => match result {
                        Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            tokio::time::sleep(std::time::Duration::from_millis(80)).await;
                            while changes.try_recv().is_ok() {}
                            emit_snapshot(&signal_emitter, &backend, &subscription_id, "changed").await;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    },
                    result = pairing_events.recv(), if wants_pairing => match result {
                        Ok(event) => emit_pairing(&signal_emitter, &subscription_id, event).await,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        });
        self.subscriptions.lock().await.insert(id.clone(), task);
        api::success(json!({ "subscription": { "id": id, "streams": streams } })).to_string()
    }

    async fn cancel(&self, request_id: &str) {
        if let Some(task) = self.subscriptions.lock().await.remove(request_id) {
            task.abort();
        }
    }

    #[zbus(signal)]
    async fn event(emitter: &SignalEmitter<'_>, stream: &str, event_json: &str)
    -> zbus::Result<()>;
}

async fn emit_snapshot(
    emitter: &SignalEmitter<'_>,
    backend: &Arc<dyn BluetoothBackend>,
    subscription_id: &str,
    event: &str,
) {
    let value = match backend.snapshot().await {
        Ok(snapshot) => json!({
            "protocol": api::PROTOCOL,
            "version": api::VERSION,
            "stream": CHANGED_STREAM,
            "event": event,
            "subscription_id": subscription_id,
            "data": { "snapshot": snapshot }
        }),
        Err(error) => json!({
            "protocol": api::PROTOCOL,
            "version": api::VERSION,
            "stream": CHANGED_STREAM,
            "event": "unavailable",
            "subscription_id": subscription_id,
            "error": { "code": "bluez-unavailable", "message": format!("{error:#}") }
        }),
    };
    if let Err(error) = BluetoothDaemon::event(emitter, CHANGED_STREAM, &value.to_string()).await {
        tracing::debug!(%error, "could not emit Bluetooth subscription event");
    }
}

async fn emit_pairing(emitter: &SignalEmitter<'_>, subscription_id: &str, event: PairingEvent) {
    let value = json!({
        "protocol": api::PROTOCOL,
        "version": api::VERSION,
        "stream": PAIRING_STREAM,
        "subscription_id": subscription_id,
        "event": event.event,
        "data": event,
    });
    if let Err(error) = BluetoothDaemon::event(emitter, PAIRING_STREAM, &value.to_string()).await {
        tracing::debug!(%error, "could not emit pairing event");
    }
}

pub async fn run(backend: Arc<dyn BluetoothBackend>, pairing: Arc<PairingBroker>) -> Result<()> {
    let _connection = connection::Builder::session()
        .context("connect to session D-Bus")?
        .name(BUS_NAME)
        .context("claim bt-daemon bus name")?
        .serve_at(
            OBJECT_PATH,
            BluetoothDaemon {
                backend,
                pairing,
                sequence: AtomicU64::new(1),
                subscriptions: Arc::new(Mutex::new(HashMap::new())),
            },
        )
        .context("export bt-daemon D-Bus interface")?
        .build()
        .await
        .context("start bt-daemon D-Bus service")?;
    tracing::info!(
        bus_name = BUS_NAME,
        object_path = OBJECT_PATH,
        "bt-daemon started"
    );
    std::future::pending::<()>().await;
    Ok(())
}
