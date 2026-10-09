//! Read-only Linux Bluetooth management Get Connection Information (0x0031).
//! Each request owns its socket: cancellation closes it and late replies cannot
//! satisfy another request. No adapter settings or discovery commands are sent.
use std::{
    io, mem,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    time::Duration,
};

use bluer::Address;
use tokio::io::unix::AsyncFd;

const GET_CONN_INFO: u16 = 0x0031;
const COMMAND_COMPLETE: u16 = 0x0001;
const COMMAND_STATUS: u16 = 0x0002;
const NOT_CONNECTED: u8 = 0x02;
const PERMISSION_DENIED: u8 = 0x14;

use super::Reply;

#[repr(C)]
struct SockAddrHci {
    family: libc::sa_family_t,
    dev: u16,
    channel: u16,
}

fn socket() -> io::Result<AsyncFd<OwnedFd>> {
    // SAFETY: constant socket arguments; the returned descriptor is owned below.
    let fd = unsafe {
        libc::socket(
            libc::AF_BLUETOOTH,
            libc::SOCK_RAW | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
            1,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: socket returned a new, valid, exclusively owned descriptor.
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let address = SockAddrHci {
        family: libc::AF_BLUETOOTH as _,
        dev: 0xffff, // HCI_DEV_NONE
        channel: 3,  // HCI_CHANNEL_CONTROL (not raw HCI / not user channel)
    };
    // SAFETY: repr(C) matches Linux sockaddr_hci; pointer and length cover address.
    let result = unsafe {
        libc::bind(
            fd.as_raw_fd(),
            (&address as *const SockAddrHci).cast(),
            mem::size_of_val(&address) as _,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    AsyncFd::new(fd)
}

fn peer(address: Address, kind: u8) -> [u8; 7] {
    let mut bytes = [0; 7];
    bytes[..6].copy_from_slice(&address.0);
    bytes[..6].reverse(); // bdaddr_t is little endian
    bytes[6] = kind;
    bytes
}

fn request(index: u16, peer: &[u8; 7]) -> [u8; 13] {
    let mut packet = [0; 13];
    packet[..2].copy_from_slice(&GET_CONN_INFO.to_le_bytes());
    packet[2..4].copy_from_slice(&index.to_le_bytes());
    packet[4..6].copy_from_slice(&7_u16.to_le_bytes());
    packet[6..].copy_from_slice(peer);
    packet
}

fn reply(packet: &[u8], index: u16, peer: &[u8; 7]) -> Option<Reply> {
    let header = packet.get(..9)?;
    let word = |i| u16::from_le_bytes([header[i], header[i + 1]]);
    let event = word(0);
    if word(2) != index
        || usize::from(word(4)) != packet.len() - 6
        || word(6) != GET_CONN_INFO
        || ![COMMAND_COMPLETE, COMMAND_STATUS].contains(&event)
    {
        return None;
    }
    let status = packet[8];
    if status != 0 {
        return Some(match status {
            NOT_CONNECTED => Reply::NotConnected,
            PERMISSION_DENIED => Reply::Denied,
            _ => Reply::Unavailable,
        });
    }
    if event != COMMAND_COMPLETE || packet.len() != 19 || &packet[9..16] != peer {
        return None;
    }
    Some(if packet[16] == 127 {
        Reply::Unavailable
    } else {
        Reply::Reading(i16::from(packet[16] as i8))
    })
}

fn sent_request(sent: isize, length: usize) -> io::Result<()> {
    if sent < 0 {
        Err(io::Error::last_os_error())
    } else if sent == 0 || sent as usize == length {
        // Linux's management dispatcher can return zero after queuing a Command
        // Status error (e.g. permission denied). The reply, not send's byte count,
        // is authoritative. Still reject a genuinely partial positive write.
        Ok(())
    } else {
        Err(io::ErrorKind::WriteZero.into())
    }
}

async fn exchange(socket: &AsyncFd<OwnedFd>, index: u16, peer: &[u8; 7]) -> io::Result<Reply> {
    let packet = request(index, peer);
    socket
        .async_io(tokio::io::Interest::WRITABLE, |fd| {
            // SAFETY: packet is readable for packet.len(); fd is alive throughout send.
            let sent = unsafe {
                libc::send(
                    fd.as_raw_fd(),
                    packet.as_ptr().cast(),
                    packet.len(),
                    libc::MSG_NOSIGNAL,
                )
            };
            sent_request(sent, packet.len())
        })
        .await?;
    let mut buffer = [0_u8; 1024];
    loop {
        let count = socket
            .async_io(tokio::io::Interest::READABLE, |fd| {
                // SAFETY: buffer is writable for its full size; fd remains alive.
                let count = unsafe {
                    libc::recv(
                        fd.as_raw_fd(),
                        buffer.as_mut_ptr().cast(),
                        buffer.len(),
                        libc::MSG_TRUNC,
                    )
                };
                usize::try_from(count).map_err(|_| io::Error::last_os_error())
            })
            .await?;
        if count == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        if let Some(reply) = buffer
            .get(..count)
            .and_then(|packet| reply(packet, index, peer))
        {
            return Ok(reply);
        }
    }
}

pub(super) async fn read(index: u16, address: Address, random: bool) -> io::Result<(Reply, bool)> {
    read_from(socket()?, index, address, random).await
}

async fn read_from(
    socket: AsyncFd<OwnedFd>,
    index: u16,
    address: Address,
    random: bool,
) -> io::Result<(Reply, bool)> {
    // Public addresses can have a BR/EDR or LE connection. Probe BR/EDR first,
    // and only try LE after an authoritative Not Connected response.
    tokio::time::timeout(Duration::from_secs(2), async {
        if !random {
            let reply = exchange(&socket, index, &peer(address, 0)).await?;
            if reply != Reply::NotConnected {
                return Ok((reply, true));
            }
        }
        exchange(&socket, index, &peer(address, if random { 2 } else { 1 }))
            .await
            .map(|reply| (reply, false))
    })
    .await
    .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
}

#[cfg(test)]
mod tests {
    use super::{
        NOT_CONNECTED, PERMISSION_DENIED, Reply, peer, read_from, reply, request, sent_request,
    };
    use bluer::Address;
    use std::{io, os::fd::OwnedFd, time::Duration};
    use tokio::io::unix::AsyncFd;

    fn complete(index: u16, peer: &[u8; 7], rssi: u8) -> Vec<u8> {
        let mut packet = vec![1, 0, 0, 0, 13, 0, 0x31, 0, 0];
        packet[2..4].copy_from_slice(&index.to_le_bytes());
        packet.extend(peer);
        packet.extend([rssi, 127, 127]);
        packet
    }

    #[test]
    fn wire_format_correlation_signed_values_and_unknown() {
        let peer = peer("01:23:45:67:89:AB".parse().unwrap(), 2);
        assert_eq!(
            request(0x1234, &peer),
            [
                0x31, 0, 0x34, 0x12, 7, 0, 0xab, 0x89, 0x67, 0x45, 0x23, 1, 2
            ]
        );
        let packet = complete(3, &peer, (-63_i8) as u8);
        assert_eq!(reply(&packet, 3, &peer), Some(Reply::Reading(-63)));
        for end in 0..packet.len() {
            assert_eq!(reply(&packet[..end], 3, &peer), None);
        }
        assert_eq!(reply(&packet, 2, &peer), None);
        assert_eq!(reply(&packet, 3, &[0; 7]), None);
        assert_eq!(
            reply(&complete(3, &peer, 0), 3, &peer),
            Some(Reply::Reading(0))
        );
        assert_eq!(
            reply(&complete(3, &peer, 127), 3, &peer),
            Some(Reply::Unavailable)
        );
        for (offset, value) in [(0, 3), (4, 12), (6, 0x32)] {
            let mut wrong = packet.clone();
            wrong[offset] = value;
            assert_eq!(reply(&wrong, 3, &peer), None);
        }
    }

    fn pair() -> (AsyncFd<OwnedFd>, tokio::net::UnixDatagram) {
        let (client, server) = std::os::unix::net::UnixDatagram::pair().unwrap();
        client.set_nonblocking(true).unwrap();
        server.set_nonblocking(true).unwrap();
        (
            AsyncFd::new(client.into()).unwrap(),
            tokio::net::UnixDatagram::from_std(server).unwrap(),
        )
    }

    #[tokio::test]
    async fn transport_reads_classic_and_both_le_address_types() {
        for (random, classic) in [(false, true), (false, false), (true, false)] {
            let (client, server) = pair();
            let address = Address::default();
            let worker = tokio::spawn(read_from(client, 4, address, random));
            let mut buffer = [0; 32];
            let count = server.recv(&mut buffer).await.unwrap();
            assert_eq!(
                &buffer[..count],
                &request(4, &peer(address, if random { 2 } else { 0 }))
            );
            if !random && !classic {
                server
                    .send(&[1, 0, 4, 0, 3, 0, 0x31, 0, NOT_CONNECTED])
                    .await
                    .unwrap();
                let count = server.recv(&mut buffer).await.unwrap();
                assert_eq!(&buffer[..count], &request(4, &peer(address, 1)));
            }
            let kind = if classic {
                0
            } else if random {
                2
            } else {
                1
            };
            // Oversized datagrams and unrelated events must not complete the request.
            server.send(&[0; 2048]).await.unwrap();
            server
                .send(&complete(5, &peer(address, kind), 20))
                .await
                .unwrap();
            server
                .send(&complete(4, &peer(address, kind), (-42_i8) as u8))
                .await
                .unwrap();
            assert_eq!(
                worker.await.unwrap().unwrap(),
                (Reply::Reading(-42), classic)
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn silent_transport_times_out_and_permission_failure_does_not_probe_le() {
        let (client, server) = pair();
        let worker = tokio::spawn(read_from(client, 0, Address::default(), false));
        let mut buffer = [0; 32];
        server.recv(&mut buffer).await.unwrap();
        tokio::time::advance(Duration::from_secs(3)).await;
        assert_eq!(
            worker.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        tokio::time::resume(); // Real socket readiness must not race auto-advanced timers.
        let (client, server) = pair();
        let worker = tokio::spawn(read_from(client, 0, Address::default(), false));
        server.recv(&mut buffer).await.unwrap();
        server
            .send(&[2, 0, 0, 0, 3, 0, 0x31, 0, PERMISSION_DENIED])
            .await
            .unwrap();
        assert_eq!(worker.await.unwrap().unwrap(), (Reply::Denied, true));
        assert_eq!(
            server.try_recv(&mut buffer).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn zero_byte_management_send_must_still_read_the_error_reply() {
        assert!(sent_request(0, 13).is_ok());
        assert!(sent_request(13, 13).is_ok());
        assert_eq!(
            sent_request(12, 13).unwrap_err().kind(),
            io::ErrorKind::WriteZero
        );
        assert_eq!(
            reply(&[2, 0, 0, 0, 3, 0, 0x31, 0, PERMISSION_DENIED], 0, &[0; 7]),
            Some(Reply::Denied)
        );
    }

    #[test]
    fn failures_never_decode_rssi() {
        for (status, expected) in [
            (2, Reply::NotConnected),
            (0x14, Reply::Denied),
            (0x0c, Reply::Unavailable),
            (1, Reply::Unavailable),
        ] {
            let packet = [2, 0, 0, 0, 3, 0, 0x31, 0, status];
            assert_eq!(reply(&packet, 0, &[0; 7]), Some(expected));
        }
        assert_eq!(reply(&[2, 0, 0, 0, 3, 0, 0x31, 0, 0], 0, &[0; 7]), None);
    }
}
