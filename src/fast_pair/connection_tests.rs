use super::{ConnectionState, Frame, LinkState, decode_chunks, read_transport, write_transport};
use bluer::Address;
use std::time::Duration;
use tokio::sync::mpsc;

#[test]
fn connected_transition_is_idempotent_and_preserves_connection_age() {
    let address = Address::default();
    let mut state = ConnectionState::default();
    state.retry.failed(address);
    assert!(!state.retry.ready(address));
    assert!(state.mark_connected(address));
    let since = state.connected_since[&address];
    assert!(state.retry.ready(address));
    assert_eq!(state.links[&address], LinkState::Connected);
    assert!(!state.mark_connected(address));
    assert_eq!(state.connected_since[&address], since);
}

#[test]
fn failed_connect_only_changes_the_matching_connecting_peer() {
    let pending = Address::default();
    let established = "AA:BB:CC:DD:EE:FF".parse().unwrap();
    let mut state = ConnectionState::default();
    state.links.insert(pending, LinkState::Connecting);
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

#[tokio::test]
async fn framed_transport_roundtrips_under_backpressure_and_stops_on_eof() {
    let (writer, reader) = tokio::io::duplex(8);
    let (writes, writes_rx) = mpsc::channel(2);
    writes
        .send(Frame::encoded(3, 1, &[1, 2, 3]).unwrap())
        .await
        .unwrap();
    writes
        .send(Frame::encoded(8, 0x13, &[2, 0x28, 0x28, 0x20]).unwrap())
        .await
        .unwrap();
    drop(writes);
    let (chunks, chunks_rx) = mpsc::channel(1);
    let (frames, mut frames_rx) = mpsc::channel(1);
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(
            write_transport(writer, writes_rx),
            read_transport(reader, chunks),
            decode_chunks(chunks_rx, frames),
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
    assert!(result.0.is_ok() && result.1.is_ok() && result.2.is_ok());
    assert_eq!(result.3.len(), 2);
    assert_eq!(result.3[0].group, 3);
    assert_eq!(result.3[0].payload, [1, 2, 3]);
    assert_eq!(result.3[1].code, 0x13);
    assert_eq!(result.3[1].payload, [2, 0x28, 0x28, 0x20]);
}

#[tokio::test]
async fn transport_and_decoder_errors_are_propagated() {
    let (chunks, chunks_rx) = mpsc::channel(1);
    drop(chunks_rx);
    assert!(read_transport(&b"data"[..], chunks).await.is_err());
    let (writer, reader) = tokio::io::duplex(8);
    drop(reader);
    let (writes, writes_rx) = mpsc::channel(1);
    writes.send(vec![1]).await.unwrap();
    drop(writes);
    assert!(write_transport(writer, writes_rx).await.is_err());
    let (chunks, chunks_rx) = mpsc::channel(1);
    chunks.send(vec![3, 1, 0xff, 0xff]).await.unwrap();
    drop(chunks);
    let (frames, _frames_rx) = mpsc::channel(1);
    assert!(decode_chunks(chunks_rx, frames).await.is_err());
    let (chunks, chunks_rx) = mpsc::channel(1);
    chunks
        .send(Frame::encoded(3, 1, &[1, 2, 3]).unwrap())
        .await
        .unwrap();
    drop(chunks);
    let (frames, frames_rx) = mpsc::channel(1);
    drop(frames_rx);
    assert!(decode_chunks(chunks_rx, frames).await.is_err());
}
