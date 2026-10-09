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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_samples_without_inventing_values_for_failures() {
        for (value, relative) in [(0, true), (-42, false)] {
            assert_eq!(
                decode(Sample {
                    status: "ok".into(),
                    rssi: value,
                    relative
                })
                .unwrap(),
                (Reply::Reading(value), relative)
            );
        }
        for value in [127, 128, i16::MIN, i16::MAX] {
            assert_eq!(
                decode(Sample {
                    status: "ok".into(),
                    rssi: value,
                    relative: false
                })
                .unwrap_err()
                .kind(),
                io::ErrorKind::InvalidData
            );
        }
        assert_eq!(
            decode(Sample::status("not-connected")).unwrap().0,
            Reply::NotConnected
        );
        assert_eq!(
            decode(Sample::status("unavailable")).unwrap().0,
            Reply::Unavailable
        );
        assert_eq!(
            decode(Sample::status("permission-denied"))
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            decode(Sample::status("timeout")).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(
            decode(Sample::status("future-status")).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
