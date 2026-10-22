use super::{
    ConnectionState, Frame, MAX_FRAME_PAYLOAD, MessageStreamTransport, PsmAvailability,
    decode_message_stream_psm, read_transport, select_transport, write_transport,
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
    let availability = decode_message_stream_psm(&[1, 0x80, 0]).unwrap();
    assert_eq!(availability, PsmAvailability::Ready(0x80));
    let ble = select_transport(
        true,
        false,
        matches!(availability, PsmAvailability::Ready(_)),
    )
    .unwrap();
    for invalid in [&[1, 0x7f, 0][..], &[1, 0][..], &[3, 0x80, 0][..]] {
        assert!(decode_message_stream_psm(invalid).is_err());
    }
    assert_eq!(
        decode_message_stream_psm(&[0, 0, 0]).unwrap(),
        PsmAvailability::Unknown
    );
    assert_eq!(
        decode_message_stream_psm(&[2, 0, 0]).unwrap(),
        PsmAvailability::Unavailable
    );
    assert_eq!(select_transport(false, true, true), Some(ble));
    assert_eq!(ble, MessageStreamTransport::L2cap);
    assert!(select_transport(true, false, false).is_none());
    for (seconds, reset) in [(59, false), (60, true)] {
        let mut state = ConnectionState::default();
        state.mark_connected(other);
        state.retry.psm_unavailable(other);
        for attempt in 1..=3 {
            state.retry.psm_unknown(peer);
            state.retry.connected(peer);
            assert_eq!(state.retry.l2cap_allowed(peer), attempt < 3);
        }
        assert!(state.begin(peer, ble).is_none());
        let attempt = state.begin(peer, rfcomm).unwrap();
        assert!(state.begin(peer, rfcomm).is_none());
        state.connection_failed(peer, attempt, false);
        assert!(!state.retry.ready(peer));
        assert!(state.retry.ready(other));
        tokio::time::advance(Duration::from_secs(360)).await;
        let attempt = state.begin(peer, rfcomm).unwrap();
        assert!(state.mark_connected(peer));
        assert!(state.retry.ready(peer));
        let (writer, mut receiver) = mpsc::channel(1);
        state.writers.insert(peer, writer);
        state.connection_failed(peer, attempt, true); // A late busy failure must not tear down a live stream.
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(state.begin(peer, rfcomm).is_none());
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
        assert!(state.begin(peer, ble).is_some());
        assert!(!state.retry.l2cap_allowed(other));
    }
}

#[tokio::test(start_paused = true)]
async fn successful_profile_call_without_stream_expires_with_backoff() {
    let peer = Address::default();
    let mut state = ConnectionState::default();
    let old = state.begin(peer, MessageStreamTransport::Rfcomm).unwrap();
    // A successful ConnectProfile reply makes no state change: only the stream
    // callback establishes Connected. Missing that callback must be bounded.
    tokio::time::advance(super::STALE_CONNECT - Duration::from_millis(1)).await;
    assert!(state.begin(peer, MessageStreamTransport::Rfcomm).is_none());
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(state.begin(peer, MessageStreamTransport::Rfcomm).is_none());
    assert!(!state.is_current_attempt(peer, old));
    let deadline = state.retry.next_deadline().unwrap();
    assert!(
        (Duration::from_secs(15)..=Duration::from_secs(18))
            .contains(&(deadline - tokio::time::Instant::now()))
    );
    assert!(state.begin(peer, MessageStreamTransport::Rfcomm).is_none());
    assert_eq!(state.retry.next_deadline(), Some(deadline));
    tokio::time::advance(deadline - tokio::time::Instant::now()).await;
    let new = state.begin(peer, MessageStreamTransport::Rfcomm).unwrap();
    assert_ne!(new, old);
    state.connection_failed(peer, old, true);
    assert!(!state.connect_completed(peer, old));
    assert!(state.is_current_attempt(peer, new));
    assert!(state.mark_connected(peer));
    tokio::time::advance(Duration::from_secs(360)).await;
    assert!(
        state.begin(peer, MessageStreamTransport::Rfcomm).is_none(),
        "live streams never expire as pending attempts"
    );
}

#[tokio::test(start_paused = true)]
async fn disconnect_clears_pending_attempts_and_fences_late_results() {
    let peer = Address::default();
    let other = "AA:BB:CC:DD:EE:FF".parse().unwrap();
    let mut state = ConnectionState::default();
    let old = state.begin(peer, MessageStreamTransport::L2cap).unwrap();
    let unrelated = state.begin(other, MessageStreamTransport::Rfcomm).unwrap();
    state.retry.psm_unavailable(peer);
    state.note_connection_change(peer, false);
    state.note_connection_change(peer, true);
    assert!(state.retry.l2cap_allowed(peer));
    // No time advance: attempt identity cannot just be a timestamp.
    let new = state.begin(peer, MessageStreamTransport::L2cap).unwrap();
    assert_ne!(old, new);
    state.connection_failed(peer, old, false);
    assert!(!state.connect_completed(peer, old));
    assert!(state.is_current_attempt(peer, new));
    assert!(state.is_current_attempt(other, unrelated));
    assert!(state.connect_completed(peer, new));
    state.note_connection_change(peer, false);
    assert!(
        state.begin(peer, MessageStreamTransport::Rfcomm).is_none(),
        "live reader retains teardown ownership"
    );
    state.ended(peer);
    state.note_connection_change(peer, true);
    assert!(state.begin(peer, MessageStreamTransport::Rfcomm).is_some());
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
