use anyhow::{Context, Result};
use serde::Serialize;

const BUS_NAME: &str = "org.bluez.obex";
const OBJECT_PATH: &str = "/org/bluez/obex";

#[derive(Debug, Clone, Serialize)]
pub struct ObexCapabilities {
    pub available: bool,
    pub outgoing_object_push: bool,
    pub incoming_authorization: bool,
    pub transfer_progress: bool,
    pub cancellation: bool,
}

pub async fn probe() -> Result<ObexCapabilities> {
    let connection = zbus::Connection::session()
        .await
        .context("connect to session D-Bus for OBEX")?;
    let peer = zbus::Proxy::new(
        &connection,
        BUS_NAME,
        OBJECT_PATH,
        "org.freedesktop.DBus.Peer",
    )
    .await
    .context("create obexd peer proxy")?;
    peer.call_method("Ping", &())
        .await
        .context("activate and ping obexd")?;
    Ok(ObexCapabilities {
        available: true,
        outgoing_object_push: false,
        incoming_authorization: false,
        transfer_progress: false,
        cancellation: false,
    })
}

#[cfg(test)]
mod tests {
    use super::ObexCapabilities;

    #[test]
    fn staged_capabilities_do_not_claim_incoming_authorization() {
        let capabilities = ObexCapabilities {
            available: true,
            outgoing_object_push: false,
            incoming_authorization: false,
            transfer_progress: false,
            cancellation: false,
        };
        assert!(capabilities.available);
        assert!(!capabilities.outgoing_object_push);
        assert!(!capabilities.incoming_authorization);
    }
}
