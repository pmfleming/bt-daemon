use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::Value;
use zbus::connection;

use crate::{api, backend::BluetoothBackend};

pub const BUS_NAME: &str = "org.laufan.BluetoothDaemon";
pub const OBJECT_PATH: &str = "/org/laufan/BluetoothDaemon";

pub struct BluetoothDaemon {
    backend: Arc<dyn BluetoothBackend>,
}

#[zbus::interface(name = "org.laufan.BluetoothDaemon1")]
impl BluetoothDaemon {
    async fn call(&self, method: &str, params_json: &str) -> String {
        let params = serde_json::from_str::<Value>(params_json).unwrap_or(Value::Null);
        api::dispatch(Arc::clone(&self.backend), method, params)
            .await
            .to_string()
    }
}

pub async fn run(backend: Arc<dyn BluetoothBackend>) -> Result<()> {
    let _connection = connection::Builder::session()
        .context("connect to session D-Bus")?
        .name(BUS_NAME)
        .context("claim bt-daemon bus name")?
        .serve_at(OBJECT_PATH, BluetoothDaemon { backend })
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
