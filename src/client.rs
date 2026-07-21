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

pub async fn run() -> Result<()> {
    tracing::info!("JSON-lines client started");
    let dbus = match zbus::Connection::session().await {
        Ok(connection) => Some(connection),
        Err(error) => {
            tracing::warn!(%error, "client could not connect to session D-Bus");
            None
        }
    };
    let output = Arc::new(Mutex::new(tokio::io::stdout()));
    if let Some(connection) = dbus.clone() {
        spawn_event_forwarder(connection.clone(), Arc::clone(&output));
        spawn_owner_watcher(connection, Arc::clone(&output));
    }

    let mut calls = JoinSet::new();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await.context("read client request")? {
        let request = match decode_request(&line) {
            Ok(Some(request)) => request,
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(%error, "client received invalid JSON request");
                emit(
                    &output,
                    &json!({ "kind": "protocol-error", "error": error.to_string() }),
                )
                .await?;
                continue;
            }
        };
        if !handle_request(request, &dbus, &output, &mut calls).await? {
            break;
        }
    }
    while calls.join_next().await.is_some() {}
    Ok(())
}

async fn handle_request(
    request: Request,
    dbus: &Option<zbus::Connection>,
    output: &Output,
    calls: &mut JoinSet<()>,
) -> Result<bool> {
    match request {
        Request::Call { id, method, params } => {
            let connection = dbus.clone();
            let output = Arc::clone(output);
            calls.spawn(async move {
                let response = dispatch(&connection, &method, params).await;
                if let Err(error) = emit(
                    &output,
                    &json!({ "kind": "response", "id": id, "ok": true, "response": response }),
                )
                .await
                {
                    tracing::error!(%error, "client could not emit call response");
                }
            });
        }
        Request::Subscribe { id, streams } => {
            let response = call_subscribe(dbus.as_ref(), streams).await;
            emit_transport_response(output, &id, response).await?;
        }
        Request::Cancel { id, request_id } => {
            let response = call_cancel(dbus.as_ref(), &request_id).await;
            emit_transport_response(output, &id, response).await?;
        }
        Request::Shutdown { id } => {
            if tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while calls.join_next().await.is_some() {}
            })
            .await
            .is_err()
            {
                tracing::warn!("client shutdown timed out while waiting for calls");
            }
            calls.abort_all();
            emit(
                output,
                &json!({ "kind": "response", "id": id, "ok": true, "response": { "shutdown": true } }),
            )
            .await?;
            return Ok(false);
        }
    }
    Ok(true)
}

async fn dispatch(connection: &Option<zbus::Connection>, method: &str, params: Value) -> Value {
    match call_daemon(connection, method, params).await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%method, error = %error, error_chain = %format!("{error:#}"), "client call to daemon failed");
            api::error(
                "daemon-unavailable",
                "bt-daemon session service is unavailable".to_string(),
            )
        }
    }
}

async fn call_daemon(
    connection: &Option<zbus::Connection>,
    method: &str,
    params: Value,
) -> Result<Value> {
    let connection = connection
        .as_ref()
        .context("session D-Bus connection is unavailable")?;
    let proxy = zbus::Proxy::new(connection, BUS_NAME, OBJECT_PATH, INTERFACE)
        .await
        .context("create bt-daemon client proxy")?;
    let params_json = params.to_string();
    let response: String = proxy
        .call("Call", &(method, params_json.as_str()))
        .await
        .context("call bt-daemon")?;
    serde_json::from_str(&response).context("decode bt-daemon response")
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
    let task_output = Arc::clone(&output);
    spawn_transport_task("client-owner-watch", output, async move {
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
                    &task_output,
                    &json!({ "kind": "transport-error", "error": "bt-daemon restarted; reconnecting" }),
                )
                .await?;
                break;
            }
        }
        Ok(())
    });
}

fn spawn_event_forwarder(connection: zbus::Connection, output: Output) {
    let task_output = Arc::clone(&output);
    spawn_transport_task("client-event-forwarder", output, async move {
        let proxy = zbus::Proxy::new(&connection, BUS_NAME, OBJECT_PATH, INTERFACE).await?;
        let mut signals = proxy.receive_signal("Event").await?;
        while let Some(message) = signals.next().await {
            let (stream, event_json): (String, String) = message.body().deserialize()?;
            let event = decode_event(&event_json);
            emit(
                &task_output,
                &json!({ "kind": "event", "stream": stream, "event": event }),
            )
            .await?;
        }
        Ok(())
    });
}

fn spawn_transport_task(
    name: &'static str,
    output: Output,
    task: impl std::future::Future<Output = Result<()>> + Send + 'static,
) {
    crate::task::spawn(name, async move {
        if let Err(error) = task.await {
            tracing::warn!(task = name, error = %error, error_chain = %format!("{error:#}"), "client transport task failed");
            if let Err(emit_error) = emit(
                &output,
                &json!({ "kind": "transport-error", "error": error.to_string() }),
            )
            .await
            {
                tracing::error!(task = name, %emit_error, "client could not report transport failure");
            }
        }
    });
}

async fn emit_transport_response(
    output: &Output,
    id: &str,
    response: zbus::Result<Value>,
) -> Result<()> {
    if let Err(error) = &response {
        tracing::warn!(%id, %error, "client transport request failed");
    }
    let value = transport_response(id, response.map_err(|error| error.to_string()));
    emit(output, &value).await
}

fn decode_request(line: &str) -> serde_json::Result<Option<Request>> {
    if line.trim().is_empty() {
        Ok(None)
    } else {
        serde_json::from_str(line).map(Some)
    }
}

fn decode_event(event_json: &str) -> Value {
    serde_json::from_str(event_json).unwrap_or_else(|_| json!({ "raw": event_json }))
}

fn transport_response(id: &str, response: std::result::Result<Value, String>) -> Value {
    match response {
        Ok(response) => json!({ "kind": "response", "id": id, "ok": true, "response": response }),
        Err(error) => json!({ "kind": "response", "id": id, "ok": false, "error": error }),
    }
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        Request, call_cancel, call_subscribe, decode_event, decode_request, dispatch,
        transport_response,
    };

    #[test]
    fn requests_decode_without_transport_setup() {
        assert!(decode_request("  ").unwrap().is_none());
        let request = decode_request(r#"{"op":"call","id":"1","method":"bluetooth.snapshot"}"#)
            .unwrap()
            .unwrap();
        assert!(matches!(
            request,
            Request::Call { id, method, params }
                if id == "1" && method == "bluetooth.snapshot" && params.is_null()
        ));
        assert!(decode_request("not-json").is_err());
    }

    #[test]
    fn event_and_transport_envelopes_preserve_failures() {
        assert_eq!(decode_event(r#"{"event":"changed"}"#)["event"], "changed");
        assert_eq!(decode_event("not-json")["raw"], "not-json");
        assert_eq!(
            transport_response("7", Err("session unavailable".into())),
            json!({
                "kind": "response",
                "id": "7",
                "ok": false,
                "error": "session unavailable"
            })
        );
    }

    #[tokio::test]
    async fn absent_dbus_connection_has_stable_errors() {
        assert_eq!(
            dispatch(&None, "bluetooth.snapshot", json!({})).await["error"]["code"],
            "daemon-unavailable"
        );
        assert!(call_subscribe(None, vec![]).await.is_err());
        assert!(call_cancel(None, "request-1").await.is_err());
    }
}
