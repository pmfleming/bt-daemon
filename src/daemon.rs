use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, broadcast, oneshot},
    task::JoinHandle,
};
use zbus::{connection, object_server::SignalEmitter};

use crate::{
    api, audio,
    backend::BluetoothBackend,
    obex,
    pairing::{PairingBroker, PairingEvent},
};

pub const BUS_NAME: &str = "org.laufan.BluetoothDaemon";
pub const OBJECT_PATH: &str = "/org/laufan/BluetoothDaemon";
pub const INTERFACE: &str = "org.laufan.BluetoothDaemon1";
pub const CHANGED_STREAM: &str = "bluetooth.changed";
pub const PAIRING_STREAM: &str = "pairing.request";
pub const OPERATION_STREAM: &str = "bluetooth.operation";
pub const AUDIO_STREAM: &str = "bluetooth.audio.changed";

#[derive(Clone, Serialize)]
struct OperationEvent {
    event: String,
    request_id: String,
    device_key: String,
    operation: String,
    state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot: Option<crate::model::Snapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<Value>,
}

struct OperationTask {
    handle: JoinHandle<()>,
    event: OperationEvent,
}

pub struct BluetoothDaemon {
    backend: Arc<dyn BluetoothBackend>,
    pairing: Arc<PairingBroker>,
    sequence: AtomicU64,
    subscriptions: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    operations: Arc<Mutex<HashMap<String, OperationTask>>>,
    operation_events: broadcast::Sender<OperationEvent>,
    audio_events: broadcast::Sender<()>,
}

impl BluetoothDaemon {
    fn next_id(&self, prefix: &str) -> String {
        format!("{prefix}-{}", self.sequence.fetch_add(1, Ordering::Relaxed))
    }

    async fn set_audio_profile(&self, params: &Value) -> Value {
        let device_key = params
            .get("device_key")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let profile_key = params
            .get("profile_key")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if device_key.is_empty() || profile_key.is_empty() {
            return api::error(
                "validation-error",
                "device_key and profile_key are required".to_string(),
            );
        }
        let devices = match tokio::task::spawn_blocking(audio::probe).await {
            Ok(Ok(devices)) => devices,
            Ok(Err(error)) => return api::error("audio-unavailable", format!("{error:#}")),
            Err(error) => return api::error("audio-unavailable", error.to_string()),
        };
        let selection = devices.into_iter().find_map(|device| {
            let address = device.address.parse().ok()?;
            if device.adapter.is_empty()
                || self.pairing.device_key(&device.adapter, address) != device_key
            {
                return None;
            }
            let profile = device.profiles.into_iter().find(|profile| {
                audio::profile_key(device_key, &profile.name) == profile_key && profile.available
            })?;
            Some((device.address, profile.index))
        });
        let Some((address, index)) = selection else {
            return api::error(
                "audio-profile-unavailable",
                "Bluetooth audio profile is not available".to_string(),
            );
        };
        match tokio::task::spawn_blocking(move || audio::set_profile(&address, index)).await {
            Ok(Ok(())) => Self::audio_snapshot_for(Arc::clone(&self.pairing)).await,
            Ok(Err(error)) => api::error("audio-operation-failed", format!("{error:#}")),
            Err(error) => api::error("audio-operation-failed", error.to_string()),
        }
    }

    async fn audio_snapshot_for(pairing: Arc<PairingBroker>) -> Value {
        let devices = match tokio::task::spawn_blocking(audio::probe).await {
            Ok(Ok(devices)) => devices,
            Ok(Err(error)) => return api::error("audio-unavailable", format!("{error:#}")),
            Err(error) => return api::error("audio-unavailable", error.to_string()),
        };
        let devices = devices
            .into_iter()
            .filter_map(|device| {
                let address = device.address.parse().ok()?;
                if device.adapter.is_empty() {
                    return None;
                }
                let device_key = pairing.device_key(&device.adapter, address);
                let active_profile_key = device
                    .active_profile
                    .and_then(|active| {
                        device
                            .profiles
                            .iter()
                            .find(|profile| profile.index == active)
                    })
                    .map(|profile| audio::profile_key(&device_key, &profile.name));
                let profiles = device
                    .profiles
                    .into_iter()
                    .map(|profile| {
                        json!({
                            "key": audio::profile_key(&device_key, &profile.name),
                            "label": profile.description,
                            "mode": profile.mode,
                            "codec": profile.codec,
                            "available": profile.available,
                            "priority": profile.priority,
                        })
                    })
                    .collect::<Vec<_>>();
                let endpoint = |value: Option<audio::AudioEndpoint>| {
                    value.map(|endpoint| {
                        json!({
                            "ready": !matches!(endpoint.state.as_str(), "creating" | "error"),
                            "state": endpoint.state,
                            "is_default": endpoint.is_default,
                        })
                    })
                };
                Some(json!({
                    "device_key": device_key,
                    "active_profile_key": active_profile_key,
                    "profiles": profiles,
                    "sink": endpoint(device.sink),
                    "source": endpoint(device.source),
                }))
            })
            .collect::<Vec<_>>();
        api::success(json!({ "audio_devices": devices }))
    }

    async fn start_operation(&self, params: Value) -> Value {
        let device_key = params
            .get("key")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let operation = params
            .get("operation")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if device_key.is_empty() {
            return api::error("validation-error", "device key is required".to_string());
        }
        if !matches!(
            operation,
            "pair"
                | "connect"
                | "disconnect"
                | "remove"
                | "set-trusted"
                | "set-blocked"
                | "set-wake-allowed"
                | "set-alias"
        ) {
            return api::error(
                "validation-error",
                format!("unsupported Bluetooth device operation: {operation}"),
            );
        }

        let request_id = self.next_id("operation");
        let queued = OperationEvent {
            event: "queued".to_string(),
            request_id: request_id.clone(),
            device_key: device_key.to_string(),
            operation: operation.to_string(),
            state: "queued".to_string(),
            snapshot: None,
            error: None,
        };
        let backend = Arc::clone(&self.backend);
        let operations = Arc::clone(&self.operations);
        let events = self.operation_events.clone();
        let task_id = request_id.clone();
        let task_key = device_key.to_string();
        let task_operation = operation.to_string();
        let (start_sender, start_receiver) = oneshot::channel();
        let handle = tokio::spawn(async move {
            if start_receiver.await.is_err() {
                return;
            }
            let _ = events.send(OperationEvent {
                event: "started".to_string(),
                request_id: task_id.clone(),
                device_key: task_key.clone(),
                operation: task_operation.clone(),
                state: "running".to_string(),
                snapshot: None,
                error: None,
            });
            let result = backend
                .device_operation(&task_key, &task_operation, &params)
                .await;
            operations.lock().await.remove(&task_id);
            let event = match result {
                Ok(snapshot) => OperationEvent {
                    event: "completed".to_string(),
                    request_id: task_id,
                    device_key: task_key,
                    operation: task_operation,
                    state: "completed".to_string(),
                    snapshot: Some(snapshot),
                    error: None,
                },
                Err(error) => OperationEvent {
                    event: "failed".to_string(),
                    request_id: task_id,
                    device_key: task_key,
                    operation: task_operation,
                    state: "failed".to_string(),
                    snapshot: None,
                    error: Some(api::error_value(&error)),
                },
            };
            let _ = events.send(event);
        });
        self.operations.lock().await.insert(
            request_id.clone(),
            OperationTask {
                handle,
                event: queued.clone(),
            },
        );
        let _ = start_sender.send(());
        api::success(json!({ "operation": queued }))
    }
}

#[zbus::interface(name = "org.laufan.BluetoothDaemon1")]
impl BluetoothDaemon {
    async fn call(&self, method: &str, params_json: &str) -> String {
        let params = serde_json::from_str::<Value>(params_json).unwrap_or(Value::Null);
        if method == "bluetooth.obex.snapshot" {
            return match obex::probe().await {
                Ok(capabilities) => api::success(json!({ "obex": capabilities })).to_string(),
                Err(error) => api::success(json!({
                    "obex": {
                        "available": false,
                        "outgoing_object_push": false,
                        "incoming_authorization": false,
                        "transfer_progress": false,
                        "cancellation": false,
                        "reason": format!("{error:#}"),
                    }
                }))
                .to_string(),
            };
        }
        if method == "bluetooth.audio.snapshot" {
            return Self::audio_snapshot_for(Arc::clone(&self.pairing))
                .await
                .to_string();
        }
        if method == "bluetooth.audio.setProfile" {
            return self.set_audio_profile(&params).await.to_string();
        }
        if method == "bluetooth.device.operation" {
            return self.start_operation(params).await.to_string();
        }
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
            || streams.iter().any(|stream| {
                stream != CHANGED_STREAM
                    && stream != PAIRING_STREAM
                    && stream != OPERATION_STREAM
                    && stream != AUDIO_STREAM
            })
        {
            return api::error(
                "unsupported-stream",
                "Subscriptions require bluetooth.changed, pairing.request, bluetooth.operation, and/or bluetooth.audio.changed"
                    .to_string(),
            )
            .to_string();
        }
        let wants_changes = streams.iter().any(|stream| stream == CHANGED_STREAM);
        let wants_pairing = streams.iter().any(|stream| stream == PAIRING_STREAM);
        let wants_operations = streams.iter().any(|stream| stream == OPERATION_STREAM);
        let wants_audio = streams.iter().any(|stream| stream == AUDIO_STREAM);
        let id = self.next_id("subscription");
        let mut changes = self.backend.subscribe_changes();
        let mut pairing_events = self.pairing.subscribe();
        let mut operation_events = self.operation_events.subscribe();
        let mut audio_events = self.audio_events.subscribe();
        let backend = Arc::clone(&self.backend);
        let pairing = Arc::clone(&self.pairing);
        let signal_emitter = emitter.to_owned();
        let subscription_id = id.clone();
        let task = tokio::spawn(async move {
            if wants_changes {
                emit_snapshot(&signal_emitter, &backend, &subscription_id, "subscribed").await;
            }
            if wants_audio {
                emit_audio(&signal_emitter, &pairing, &subscription_id, "subscribed").await;
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
                    },
                    result = operation_events.recv(), if wants_operations => match result {
                        Ok(event) => emit_operation(&signal_emitter, &subscription_id, event).await,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    },
                    result = audio_events.recv(), if wants_audio => match result {
                        Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                            while audio_events.try_recv().is_ok() {}
                            emit_audio(&signal_emitter, &pairing, &subscription_id, "changed").await;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        });
        self.subscriptions.lock().await.insert(id.clone(), task);
        api::success(json!({ "subscription": { "id": id, "streams": streams } })).to_string()
    }

    async fn cancel(&self, request_id: &str) -> String {
        if let Some(task) = self.subscriptions.lock().await.remove(request_id) {
            task.abort();
            return api::success(json!({ "cancelled": request_id, "kind": "subscription" }))
                .to_string();
        }
        if let Some(task) = self.operations.lock().await.remove(request_id) {
            task.handle.abort();
            let mut event = task.event;
            event.event = "cancelled".to_string();
            event.state = "cancelled".to_string();
            let _ = self.operation_events.send(event);
            return api::success(json!({ "cancelled": request_id, "kind": "operation" }))
                .to_string();
        }
        api::error(
            "request-not-found",
            format!("No active subscription or operation named {request_id}"),
        )
        .to_string()
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

async fn emit_audio(
    emitter: &SignalEmitter<'_>,
    pairing: &Arc<PairingBroker>,
    subscription_id: &str,
    event: &str,
) {
    let envelope = BluetoothDaemon::audio_snapshot_for(Arc::clone(pairing)).await;
    let value = if envelope["ok"].as_bool().unwrap_or(false) {
        json!({
            "protocol": api::PROTOCOL,
            "version": api::VERSION,
            "stream": AUDIO_STREAM,
            "event": event,
            "subscription_id": subscription_id,
            "data": { "audio_devices": envelope["data"]["audio_devices"].clone() }
        })
    } else {
        json!({
            "protocol": api::PROTOCOL,
            "version": api::VERSION,
            "stream": AUDIO_STREAM,
            "event": "unavailable",
            "subscription_id": subscription_id,
            "error": envelope["error"].clone()
        })
    };
    if let Err(error) = BluetoothDaemon::event(emitter, AUDIO_STREAM, &value.to_string()).await {
        tracing::debug!(%error, "could not emit Bluetooth audio event");
    }
}

async fn emit_operation(emitter: &SignalEmitter<'_>, subscription_id: &str, event: OperationEvent) {
    let value = json!({
        "protocol": api::PROTOCOL,
        "version": api::VERSION,
        "stream": OPERATION_STREAM,
        "subscription_id": subscription_id,
        "event": event.event,
        "data": event,
    });
    if let Err(error) = BluetoothDaemon::event(emitter, OPERATION_STREAM, &value.to_string()).await
    {
        tracing::debug!(%error, "could not emit Bluetooth operation event");
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
    let (operation_events, _) = broadcast::channel(32);
    let (audio_events, _) = broadcast::channel(32);
    let monitor_events = audio_events.clone();
    std::thread::spawn(move || {
        loop {
            let events = monitor_events.clone();
            let notify = Arc::new(move || {
                let _ = events.send(());
            });
            if let Err(error) = audio::monitor(notify) {
                tracing::warn!(%error, "PipeWire audio monitor is retrying");
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    });
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
                operations: Arc::new(Mutex::new(HashMap::new())),
                operation_events,
                audio_events,
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
    watch_bluez_owner().await
}

async fn watch_bluez_owner() -> Result<()> {
    let connection = zbus::Connection::system()
        .await
        .context("connect BlueZ owner watcher to system D-Bus")?;
    let proxy = zbus::Proxy::new(
        &connection,
        "org.freedesktop.DBus",
        "/org/freedesktop/DBus",
        "org.freedesktop.DBus",
    )
    .await
    .context("create system D-Bus owner proxy")?;
    let mut changes = proxy
        .receive_signal("NameOwnerChanged")
        .await
        .context("subscribe to system bus owner changes")?;
    while let Some(message) = changes.next().await {
        let (name, old_owner, new_owner): (String, String, String) =
            message
                .body()
                .deserialize()
                .context("decode system bus owner change")?;
        if name == "org.bluez" && old_owner != new_owner {
            bail!(
                "BlueZ owner changed; restarting bt-daemon to rebuild sessions and pairing agent"
            );
        }
    }
    bail!("BlueZ owner watch ended")
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::AtomicU64};

    use anyhow::Result;
    use async_trait::async_trait;
    use serde_json::{Value, json};
    use tokio::sync::{Mutex, broadcast};

    use crate::{
        backend::BluetoothBackend, identity::DeviceIdentityRegistry, model::Snapshot,
        pairing::PairingBroker,
    };

    use super::{BluetoothDaemon, OperationEvent};

    struct TestBackend {
        complete: bool,
    }

    #[async_trait]
    impl BluetoothBackend for TestBackend {
        fn subscribe_changes(&self) -> broadcast::Receiver<()> {
            broadcast::channel(1).1
        }

        async fn snapshot(&self) -> Result<Snapshot> {
            Ok(Snapshot {
                adapters: vec![],
                devices: vec![],
            })
        }

        async fn set_powered(&self, _: Option<&str>, _: bool) -> Result<Snapshot> {
            self.snapshot().await
        }

        async fn set_scanning(&self, _: bool) -> Result<Snapshot> {
            self.snapshot().await
        }

        async fn device_operation(&self, _: &str, _: &str, _: &Value) -> Result<Snapshot> {
            if self.complete {
                self.snapshot().await
            } else {
                std::future::pending().await
            }
        }
    }

    fn daemon(complete: bool) -> (BluetoothDaemon, broadcast::Receiver<OperationEvent>) {
        let (operation_events, receiver) = broadcast::channel(8);
        let (audio_events, _) = broadcast::channel(8);
        (
            BluetoothDaemon {
                backend: Arc::new(TestBackend { complete }),
                pairing: PairingBroker::new(DeviceIdentityRegistry::in_memory()),
                sequence: AtomicU64::new(1),
                subscriptions: Arc::new(Mutex::new(Default::default())),
                operations: Arc::new(Mutex::new(Default::default())),
                operation_events,
                audio_events,
            },
            receiver,
        )
    }

    #[tokio::test]
    async fn operation_emits_started_and_completed_events() {
        let (daemon, mut events) = daemon(true);
        let response = daemon
            .start_operation(json!({ "key": "device-opaque", "operation": "connect" }))
            .await;
        assert_eq!(response["data"]["operation"]["state"], "queued");
        assert_eq!(events.recv().await.unwrap().event, "started");
        assert_eq!(events.recv().await.unwrap().event, "completed");
        assert!(daemon.operations.lock().await.is_empty());
    }

    #[tokio::test]
    async fn active_operation_can_be_cancelled() {
        let (daemon, mut events) = daemon(false);
        let response = daemon
            .start_operation(json!({ "key": "device-opaque", "operation": "pair" }))
            .await;
        let request_id = response["data"]["operation"]["request_id"]
            .as_str()
            .unwrap();
        assert_eq!(events.recv().await.unwrap().event, "started");
        let response: Value = serde_json::from_str(&daemon.cancel(request_id).await).unwrap();
        assert_eq!(response["data"]["kind"], "operation");
        let cancelled = events.recv().await.unwrap();
        assert_eq!(cancelled.event, "cancelled");
        assert_eq!(cancelled.request_id, request_id);
    }
}
