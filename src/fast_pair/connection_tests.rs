use super::{
    ConnectionState, Frame, MAX_FRAME_PAYLOAD, MessageStreamTransport, read_transport,
    select_transport, write_transport,
};
use bluer::Address;
use std::time::Duration;
use tokio::sync::mpsc;

#[tokio::test(start_paused = true)]
async fn connection_lifecycle_scopes_failures_and_resets_suppression_only_after_stable_streams() {
    let peer = Address::default();
    let other = "AA:BB:CC:DD:EE:FF".parse().unwrap();
    let rfcomm = select_transport(true, true, true).unwrap();
    assert_eq!(rfcomm, MessageStreamTransport::Rfcomm);
    let ble = select_transport(true, false, true).unwrap();
    assert_eq!(select_transport(false, true, true), Some(ble));
    assert_eq!(ble, MessageStreamTransport::L2cap);
    assert!(select_transport(true, false, false).is_none());
    for (seconds, reset) in [(59, false), (60, true)] {
        let mut state = ConnectionState::default();
        state.mark_connected(other);
        state.retry.psm_unavailable(peer);
        assert!(!state.begin(peer, ble));
        assert!(state.begin(peer, rfcomm));
        assert!(!state.begin(peer, rfcomm));
        state.connection_failed(peer, false);
        assert!(!state.retry.ready(peer));
        assert!(state.retry.ready(other));
        tokio::time::advance(Duration::from_secs(360)).await;
        assert!(state.begin(peer, rfcomm));
        assert!(state.mark_connected(peer));
        assert!(state.retry.ready(peer));
        let (writer, mut receiver) = mpsc::channel(1);
        state.writers.insert(peer, writer);
        state.connection_failed(peer, true); // A late busy failure must not tear down a live stream.
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(!state.begin(peer, rfcomm));
        tokio::time::advance(Duration::from_secs(seconds)).await;
        assert!(!state.mark_connected(peer)); // Duplicate notification must not reset stream age.
        state.ended(peer);
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
        assert_eq!(state.retry.l2cap_allowed(peer), reset);
        assert!(!state.retry.ready(peer));
        assert!(!state.mark_connected(other));
        state.retry.reset_session(peer);
        assert!(state.begin(peer, ble));
    }
}

#[tokio::test]
async fn framed_transport_handles_fragmentation_coalescing_backpressure_and_failures() {
    // Two bytes fragment even the header; 64 lets the writer queue both complete frames.
    for capacity in [2, 64] {
        let (writer, reader) = tokio::io::duplex(capacity);
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
    }
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
