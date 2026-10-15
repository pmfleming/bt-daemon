//! Unprivileged client: never falls back to opening a management socket.
pub(crate) use super::Reply;
use super::{BUS, INTERFACE, PATH, Sample};
use bluer::Address;
use std::{io, time::Duration};
use zbus::Connection;

pub(crate) async fn read(
    connection: &Connection,
    index: u16,
    address: Address,
) -> io::Result<(Reply, bool)> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let proxy = zbus::Proxy::new(connection, BUS, PATH, INTERFACE)
            .await
            .map_err(io::Error::other)?;
        let sample: Sample = proxy
            .call("Read", &(index, address.to_string()))
            .await
            .map_err(io::Error::other)?;
        decode(sample)
    })
    .await
    .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
}

fn decode(sample: Sample) -> io::Result<(Reply, bool)> {
    let reply = match sample.status.as_str() {
        "ok" if (-128..127).contains(&sample.rssi) => Reply::Reading(sample.rssi),
        "not-connected" => Reply::NotConnected,
        "permission-denied" => {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "RSSI helper lacks CAP_NET_ADMIN",
            ));
        }
        "timeout" => return Err(io::ErrorKind::TimedOut.into()),
        "unavailable" => Reply::Unavailable,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid RSSI helper response",
            ));
        }
    };
    Ok((reply, sample.relative))
}
