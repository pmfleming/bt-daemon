use super::{
    ConnectionState, Frame, LinkState, MAX_FRAME_PAYLOAD, MessageStreamTransport, read_transport,
    write_transport,
};
use bluer::Address;
use std::time::Duration;
use tokio::sync::mpsc;

#[test]
fn connection_lifecycle_preserves_transport_suppression_and_releases_writers() {
    let peer = Address::default();
    let mut state = ConnectionState::default();
    state.retry.psm_unavailable(peer);
    assert!(!state.begin(peer, MessageStreamTransport::L2cap));
    assert!(state.begin(peer, MessageStreamTransport::Rfcomm));
    assert!(!state.begin(peer, MessageStreamTransport::Rfcomm));
    state.retry.failed(peer);
    assert!(state.mark_connected(peer));
    assert!(state.retry.ready(peer));
    let since = state.connected_since[&peer];
    assert!(!state.mark_connected(peer));
    assert_eq!(state.connected_since[&peer], since);
    assert!(!state.begin(peer, MessageStreamTransport::Rfcomm));
    let (writer, mut receiver) = mpsc::channel(1);
    state.writers.insert(peer, writer);
    state.ended(peer);
    assert!(matches!(
        receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    ));
    assert!(!state.begin(peer, MessageStreamTransport::Rfcomm));
    assert!(!state.retry.l2cap_allowed(peer));
    state.retry.reset_session(peer);
    assert!(state.begin(peer, MessageStreamTransport::L2cap));
}

#[test]
fn failed_connect_only_changes_the_matching_connecting_peer() {
    let pending = Address::default();
    let established = "AA:BB:CC:DD:EE:FF".parse().unwrap();
    let mut state = ConnectionState::default();
    state.begin(pending, MessageStreamTransport::Rfcomm);
    state.mark_connected(established);
    state.connection_failed(pending);
    assert!(!state.links.contains_key(&pending));
    assert!(!state.retry.ready(pending));
    state.connection_failed(established);
    assert_eq!(state.links[&established], LinkState::Connected);
    assert!(state.retry.ready(established));
    let unknown = "11:22:33:44:55:66".parse().unwrap();
    state.connection_failed(unknown);
    assert!(state.retry.ready(unknown));
}

#[tokio::test(start_paused = true)]
async fn only_a_stable_stream_resets_session_suppression_at_disconnect() {
    let peer = Address::default();
    for (seconds, reset) in [(59, false), (60, true)] {
        let mut state = ConnectionState::default();
        state.retry.psm_unavailable(peer);
        state.mark_connected(peer);
        tokio::time::advance(Duration::from_secs(seconds)).await;
        state.ended(peer);
        assert_eq!(state.retry.l2cap_allowed(peer), reset);
        assert!(!state.retry.ready(peer));
    }
}

#[tokio::test]
async fn framed_transport_handles_backpressure_eof_and_propagates_failures() {
    let (writer, reader) = tokio::io::duplex(8);
    let (writes, writes_rx) = mpsc::channel(2);
    for (group, code, payload) in [
        (3, 1, &[1, 2, 3][..]),
        (8, 0x13, &[2, 0x28, 0x28, 0x20][..]),
    ] {
        writes
            .send(Frame::encoded(group, code, payload).unwrap())
            .await
            .unwrap();
    }
    drop(writes);
    let (frames, mut frames_rx) = mpsc::channel(1);
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(
            write_transport(writer, writes_rx),
            read_transport(reader, frames),
            async {
                let mut decoded = Vec::new();
                while let Some(frame) = frames_rx.recv().await {
                    decoded.push(frame);
                }
                decoded
            }
        )
    })
    .await
    .unwrap();
    assert!(result.0.is_ok() && result.1.is_ok());
    assert_eq!(result.2.len(), 2);
    assert_eq!(result.2[0].group, 3);
    assert_eq!(result.2[0].payload, [1, 2, 3]);
    assert_eq!(result.2[1].code, 0x13);
    assert_eq!(result.2[1].payload, [2, 0x28, 0x28, 0x20]);

    let (writer, reader) = tokio::io::duplex(8);
    drop(reader);
    let (writes, writes_rx) = mpsc::channel(1);
    writes.send(vec![1]).await.unwrap();
    drop(writes);
    assert!(write_transport(writer, writes_rx).await.is_err());
    let (frames, _frames_rx) = mpsc::channel(1);
    let [high, low] = ((MAX_FRAME_PAYLOAD + 1) as u16).to_be_bytes();
    assert!(
        read_transport(&[3, 1, high, low][..], frames)
            .await
            .unwrap_err()
            .to_string()
            .contains("too large")
    );
    let (frames, frames_rx) = mpsc::channel(1);
    drop(frames_rx);
    let encoded = Frame::encoded(3, 1, &[1, 2, 3]).unwrap();
    assert!(read_transport(encoded.as_slice(), frames).await.is_err());
}
