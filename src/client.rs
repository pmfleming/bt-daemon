use std::sync::Arc;

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::{
    api,
    backend::BluetoothBackend,
    daemon::{BUS_NAME, OBJECT_PATH},
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
    Shutdown {
        id: String,
    },
}

pub async fn run(backend: Arc<dyn BluetoothBackend>) -> Result<()> {
    let dbus = zbus::Connection::session().await.ok();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    while let Some(line) = lines.next_line().await.context("read client request")? {
        if line.trim().is_empty() {
            continue;
        }
        let request = match serde_json::from_str::<Request>(&line) {
            Ok(request) => request,
            Err(error) => {
                emit(
                    &mut stdout,
                    &json!({ "kind": "protocol-error", "error": error.to_string() }),
                )
                .await?;
                continue;
            }
        };
        match request {
            Request::Call { id, method, params } => {
                let response = dispatch(&dbus, Arc::clone(&backend), &method, params).await;
                emit(
                    &mut stdout,
                    &json!({ "kind": "response", "id": id, "ok": true, "response": response }),
                )
                .await?;
            }
            Request::Shutdown { id } => {
                emit(&mut stdout, &json!({ "kind": "response", "id": id, "ok": true, "response": { "shutdown": true } })).await?;
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
        && let Ok(proxy) = zbus::Proxy::new(
            connection,
            BUS_NAME,
            OBJECT_PATH,
            "org.laufan.BluetoothDaemon1",
        )
        .await
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

async fn emit(stdout: &mut tokio::io::Stdout, value: &Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(value).context("serialize client response")?;
    bytes.push(b'\n');
    stdout
        .write_all(&bytes)
        .await
        .context("write client response")?;
    stdout.flush().await.context("flush client response")
}
