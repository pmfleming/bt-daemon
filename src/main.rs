use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use bt_daemon::{
    api, audio, backend::BluetoothBackend, bluez::BluezBackend, client, daemon,
    pairing::PairingBroker, protocol,
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
    /// Bridge a frontend-owned JSON Lines session through the D-Bus daemon.
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
    AudioProbe,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("bt_daemon=info")),
        )
        .init();
    let cli = Cli::parse();
    if let Command::Debug { command } = &cli.command {
        let value = match command {
            DebugCommand::ProtocolRegistry => protocol::registry(),
            DebugCommand::ContractFixture => protocol::contract_fixture(),
            DebugCommand::AudioProbe => serde_json::to_value(audio::probe()?)?,
        };
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    match cli.command {
        Command::Daemon => {
            let bluez = Arc::new(BluezBackend::new().await?);
            bluez.start_monitoring();
            let backend: Arc<dyn BluetoothBackend> = bluez.clone();
            let pairing = PairingBroker::new(bluez.identity_registry());
            let _agent = bluez.register_agent(pairing.agent()).await?;
            daemon::run(backend, pairing).await
        }
        Command::Client => client::run().await,
        Command::ProbeBluez => {
            let bluez = Arc::new(BluezBackend::new().await?);
            bluez.start_monitoring();
            let backend: Arc<dyn BluetoothBackend> = bluez;
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
