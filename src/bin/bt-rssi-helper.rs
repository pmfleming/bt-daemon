//! Capability-scoped read-only Bluetooth RSSI service. No session-daemon startup,
//! pairing agent, discovery, audio, mutation API or CLI operation dispatcher.
#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "bt_daemon=info".into()),
        )
        .init();
    bt_daemon::rssi_helper::run().await
}
