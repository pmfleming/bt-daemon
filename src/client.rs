use std::sync::Arc;

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::{api, backend::BluetoothBackend};

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
                let response = api::dispatch(Arc::clone(&backend), &method, params).await;
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

async fn emit(stdout: &mut tokio::io::Stdout, value: &Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(value).context("serialize client response")?;
    bytes.push(b'\n');
    stdout
        .write_all(&bytes)
        .await
        .context("write client response")?;
    stdout.flush().await.context("flush client response")
}
