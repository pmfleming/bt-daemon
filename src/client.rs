use std::sync::Arc;

use anyhow::{Context, Result};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::Mutex,
    task::JoinSet,
};

use crate::{
    api,
    backend::BluetoothBackend,
    daemon::{BUS_NAME, INTERFACE, OBJECT_PATH},
};

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case")]
enum Request {
    Call {
        id: String,
        method: String,
        #[serde(default)]
        params: Value,
    },
    Subscribe {
        id: String,
        #[serde(default)]
        streams: Vec<String>,
    },
    Cancel {
        id: String,
        request_id: String,
    },
    Shutdown {
        id: String,
    },
}

type Output = Arc<Mutex<tokio::io::Stdout>>;

pub async fn run(backend: Arc<dyn BluetoothBackend>) -> Result<()> {
    let dbus = zbus::Connection::session().await.ok();
    let output = Arc::new(Mutex::new(tokio::io::stdout()));
    if let Some(connection) = dbus.clone() {
        spawn_event_forwarder(connection.clone(), Arc::clone(&output));
        spawn_owner_watcher(connection, Arc::clone(&output));
    }

    let mut calls = JoinSet::new();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await.context("read client request")? {
        if line.trim().is_empty() {
            continue;
        }
        let request = match serde_json::from_str::<Request>(&line) {
            Ok(request) => request,
            Err(error) => {
                emit(
                    &output,
                    &json!({ "kind": "protocol-error", "error": error.to_string() }),
                )
                .await?;
                continue;
            }
        };
        match request {
            Request::Call { id, method, params } => {
                let connection = dbus.clone();
                let fallback = Arc::clone(&backend);
                let output = Arc::clone(&output);
                calls.spawn(async move {
                    let response = dispatch(&connection, fallback, &method, params).await;
                    let _ = emit(
                        &output,
                        &json!({ "kind": "response", "id": id, "ok": true, "response": response }),
                    )
                    .await;
                });
            }
            Request::Subscribe { id, streams } => {
                let response = call_subscribe(dbus.as_ref(), streams).await;
                emit_transport_response(&output, &id, response).await?;
            }
            Request::Cancel { id, request_id } => {
                let response = call_cancel(dbus.as_ref(), &request_id).await;
                emit_transport_response(&output, &id, response).await?;
            }
            Request::Shutdown { id } => {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    while calls.join_next().await.is_some() {}
                })
                .await;
                calls.abort_all();
                emit(
                    &output,
                    &json!({ "kind": "response", "id": id, "ok": true, "response": { "shutdown": true } }),
                )
                .await?;
                break;
            }
        }
    }
    Ok(())
}

async fn dispatch(
    connection: &Option<zbus::Connection>,
    fallback: Arc<dyn BluetoothBackend>,
    method: &str,
    params: Value,
) -> Value {
    if let Some(connection) = connection
        && let Ok(proxy) = zbus::Proxy::new(connection, BUS_NAME, OBJECT_PATH, INTERFACE).await
    {
        let params_json = params.to_string();
        let response: zbus::Result<String> =
            proxy.call("Call", &(method, params_json.as_str())).await;
        if let Ok(response) = response
            && let Ok(value) = serde_json::from_str(&response)
        {
            return value;
        }
    }
    api::dispatch(fallback, method, params).await
}

async fn call_subscribe(
    connection: Option<&zbus::Connection>,
    streams: Vec<String>,
) -> zbus::Result<Value> {
    let connection =
        connection.ok_or_else(|| zbus::Error::Failure("session D-Bus unavailable".into()))?;
    let proxy = zbus::Proxy::new(connection, BUS_NAME, OBJECT_PATH, INTERFACE).await?;
    let response: String = proxy.call("Subscribe", &(streams,)).await?;
    serde_json::from_str(&response).map_err(|error| zbus::Error::Failure(error.to_string()))
}

async fn call_cancel(
    connection: Option<&zbus::Connection>,
    request_id: &str,
) -> zbus::Result<Value> {
    let connection =
        connection.ok_or_else(|| zbus::Error::Failure("session D-Bus unavailable".into()))?;
    let proxy = zbus::Proxy::new(connection, BUS_NAME, OBJECT_PATH, INTERFACE).await?;
    let response: String = proxy.call("Cancel", &(request_id,)).await?;
    serde_json::from_str(&response).map_err(|error| zbus::Error::Failure(error.to_string()))
}

fn spawn_owner_watcher(connection: zbus::Connection, output: Output) {
    tokio::spawn(async move {
        let result = async {
            let proxy = zbus::Proxy::new(
                &connection,
                "org.freedesktop.DBus",
                "/org/freedesktop/DBus",
                "org.freedesktop.DBus",
            )
            .await?;
            let mut changes = proxy.receive_signal("NameOwnerChanged").await?;
            while let Some(message) = changes.next().await {
                let (name, old_owner, new_owner): (String, String, String) =
                    message.body().deserialize()?;
                if name == BUS_NAME && !old_owner.is_empty() && old_owner != new_owner {
                    emit(
                        &output,
                        &json!({ "kind": "transport-error", "error": "bt-daemon restarted; reconnecting" }),
                    )
                    .await?;
                    break;
                }
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(error) = result {
            let _ = emit(
                &output,
                &json!({ "kind": "transport-error", "error": error.to_string() }),
            )
            .await;
        }
    });
}

fn spawn_event_forwarder(connection: zbus::Connection, output: Output) {
    tokio::spawn(async move {
        let result = async {
            let proxy = zbus::Proxy::new(&connection, BUS_NAME, OBJECT_PATH, INTERFACE).await?;
            let mut signals = proxy.receive_signal("Event").await?;
            while let Some(message) = signals.next().await {
                let (stream, event_json): (String, String) = message.body().deserialize()?;
                let event = serde_json::from_str::<Value>(&event_json)
                    .unwrap_or_else(|_| json!({ "raw": event_json }));
                emit(
                    &output,
                    &json!({ "kind": "event", "stream": stream, "event": event }),
                )
                .await?;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(error) = result {
            let _ = emit(
                &output,
                &json!({ "kind": "transport-error", "error": error.to_string() }),
            )
            .await;
        }
    });
}

async fn emit_transport_response(
    output: &Output,
    id: &str,
    response: zbus::Result<Value>,
) -> Result<()> {
    let value = match response {
        Ok(response) => json!({ "kind": "response", "id": id, "ok": true, "response": response }),
        Err(error) => {
            json!({ "kind": "response", "id": id, "ok": false, "error": error.to_string() })
        }
    };
    emit(output, &value).await
}

async fn emit(output: &Output, value: &Value) -> Result<()> {
    let mut stdout = output.lock().await;
    let mut bytes = serde_json::to_vec(value).context("serialize client response")?;
    bytes.push(b'\n');
    stdout
        .write_all(&bytes)
        .await
        .context("write client response")?;
    stdout.flush().await.context("flush client response")
}
