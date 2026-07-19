use anyhow::Result;
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    /// Verify that a BlueZ session can be opened, then exit.
    #[arg(long)]
    probe_bluez: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("bt_daemon=info")),
        )
        .init();

    let cli = Cli::parse();
    if cli.probe_bluez {
        let session = bluer::Session::new().await?;
        let adapters = session.adapter_names().await?;
        println!(
            "{}",
            serde_json::to_string(&serde_json::json!({ "adapters": adapters }))?
        );
        return Ok(());
    }

    info!("bt-daemon development environment is ready");
    Ok(())
}
