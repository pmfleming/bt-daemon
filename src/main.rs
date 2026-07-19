use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use bt_daemon::{api, backend::BluetoothBackend, bluez::BluezBackend, client, daemon};

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the session D-Bus service.
    Daemon,
    /// Run a frontend-owned JSON Lines session directly against BlueZ.
    Client,
    /// Verify BlueZ access and print the current bt-api snapshot.
    ProbeBluez,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("bt_daemon=info")),
        )
        .init();
    let cli = Cli::parse();
    let backend: Arc<dyn BluetoothBackend> = Arc::new(BluezBackend::new().await?);
    match cli.command {
        Command::Daemon => daemon::run(backend).await,
        Command::Client => client::run(backend).await,
        Command::ProbeBluez => {
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &api::dispatch(backend, "bluetooth.snapshot", serde_json::json!({})).await
                )?
            );
            Ok(())
        }
    }
}
