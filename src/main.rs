use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use bt_daemon::{
    api, backend::BluetoothBackend, bluez::BluezBackend, client, daemon, pairing::PairingBroker,
    protocol,
};

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
    /// Print stable protocol metadata and fixtures.
    Debug {
        #[command(subcommand)]
        command: DebugCommand,
    },
}

#[derive(Debug, Subcommand)]
enum DebugCommand {
    ProtocolRegistry,
    ContractFixture,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("bt_daemon=info")),
        )
        .init();
    let cli = Cli::parse();
    if let Command::Debug { command } = &cli.command {
        let value = match command {
            DebugCommand::ProtocolRegistry => protocol::registry(),
            DebugCommand::ContractFixture => protocol::contract_fixture(),
        };
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    let bluez = Arc::new(BluezBackend::new().await?);
    bluez.start_monitoring();
    let backend: Arc<dyn BluetoothBackend> = bluez.clone();
    match cli.command {
        Command::Daemon => {
            let pairing = PairingBroker::new(bluez.identity_registry());
            let _agent = bluez.register_agent(pairing.agent()).await?;
            daemon::run(backend, pairing).await
        }
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
        Command::Debug { .. } => unreachable!("debug commands return before BlueZ startup"),
    }
}
