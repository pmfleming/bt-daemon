//! Cancellation-safe ownership of authenticated Message Stream commands.
use anyhow::{Result, bail};
use bluer::Address;
use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::oneshot;

type Key = (Address, u8, u8);
type Reply = std::result::Result<(), String>;

#[derive(Default)]
pub(super) struct PendingCommands {
    sequence: AtomicU64,
    entries: Mutex<HashMap<Key, (u64, oneshot::Sender<Reply>)>>,
}

pub(super) struct Reservation<'a> {
    pending: &'a PendingCommands,
    key: Key,
    generation: u64,
}

pub(super) fn authenticated_frame(
    group: u8,
    code: u8,
    message: &[u8],
    account_key: &[u8; 16],
    session_nonce: &[u8; 8],
    message_nonce: &[u8; 8],
) -> Result<Vec<u8>> {
    let mac = super::message_mac(account_key, session_nonce, message_nonce, message);
    let mut payload = Vec::with_capacity(message.len() + 16);
    payload.extend_from_slice(message);
    payload.extend_from_slice(message_nonce);
    payload.extend_from_slice(&mac);
    super::Frame::encoded(group, code, &payload)
}

impl PendingCommands {
    pub async fn execute(
        &self,
        key: Key,
        send: impl std::future::Future<Output = Result<()>>,
    ) -> Result<()> {
        let (_reservation, receiver) = self.reserve(key)?;
        // The deadline includes writer backpressure. Drop releases the slot on
        // send failure, disconnect, timeout and task cancellation alike.
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            send.await?;
            Ok::<_, anyhow::Error>(receiver.await)
        })
        .await;
        match result {
            Ok(Ok(Ok(Ok(())))) => Ok(()),
            Ok(Ok(Ok(Err(reason)))) => {
                bail!("Fast Pair provider rejected control command: {reason}")
            }
            Ok(Ok(Err(_))) => bail!("Fast Pair control acknowledgement was cancelled"),
            Ok(Err(error)) => Err(error),
            Err(_) => bail!("Fast Pair control acknowledgement timed out"),
        }
    }

    pub fn reserve(&self, key: Key) -> Result<(Reservation<'_>, oneshot::Receiver<Reply>)> {
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        if entries.contains_key(&key) {
            bail!("a matching Fast Pair control command is already pending");
        }
        let generation = self.sequence.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        entries.insert(key, (generation, sender));
        Ok((
            Reservation {
                pending: self,
                key,
                generation,
            },
            receiver,
        ))
    }

    pub fn resolve(&self, key: Key, result: Reply) {
        if let Some((_, sender)) = self
            .entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&key)
        {
            let _ = sender.send(result);
        }
    }

    pub fn disconnect(&self, address: Address) {
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|(peer, _, _), _| *peer != address);
    }

    pub fn clear(&self) {
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        // Synchronous Drop also runs when Tokio aborts the caller. Generation
        // checks prevent an old acknowledged command from removing a new one.
        let mut entries = self
            .pending
            .entries
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if entries
            .get(&self.key)
            .is_some_and(|(id, _)| *id == self.generation)
        {
            entries.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PendingCommands;
    use bluer::Address;

    #[tokio::test(start_paused = true)]
    async fn backpressure_and_missing_ack_share_a_bounded_deadline_and_release_reservations() {
        let pending = PendingCommands::default();
        let key = (Address::default(), 8, 0x12);
        for stalled_writer in [false, true] {
            let start = tokio::time::Instant::now();
            let error = pending
                .execute(key, async {
                    if stalled_writer {
                        std::future::pending::<()>().await;
                    }
                    Ok(())
                })
                .await
                .unwrap_err();
            assert!(error.to_string().contains("timed out"));
            assert_eq!(start.elapsed(), std::time::Duration::from_secs(2));
            assert!(pending.reserve(key).is_ok());
        }
    }

    #[tokio::test]
    async fn command_outcomes_release_only_the_matching_reservation() {
        use futures::FutureExt;
        let pending = PendingCommands::default();
        let key = (Address::default(), 8, 0x12);
        assert!(
            pending
                .execute(key, std::future::pending())
                .now_or_never()
                .is_none()
        );
        pending
            .execute(key, async {
                pending.resolve(key, Ok(()));
                Ok(())
            })
            .await
            .unwrap();
        let rejected = pending
            .execute(key, async {
                pending.resolve(key, Err("unsupported".into()));
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(rejected.to_string().contains("unsupported"));
        let failed = pending
            .execute(key, async { anyhow::bail!("transport closed") })
            .await
            .unwrap_err();
        assert_eq!(failed.to_string(), "transport closed");
        let disconnected = pending
            .execute(key, async {
                pending.disconnect(key.0);
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(disconnected.to_string().contains("cancelled"));
        let (old, _) = pending.reserve(key).unwrap();
        pending.resolve(key, Ok(()));
        let (_new, receiver) = pending.reserve(key).unwrap();
        drop(old);
        assert!(pending.reserve(key).is_err());
        pending.resolve(key, Ok(()));
        assert!(receiver.await.unwrap().is_ok());
    }

    #[test]
    fn encoding_binds_message_and_both_nonces() {
        let frame =
            super::authenticated_frame(8, 0x12, &[1], &[4; 16], &[0x11; 8], &[0x22; 8]).unwrap();
        assert_eq!(&frame[..5], &[8, 0x12, 0, 17, 1]);
        assert_eq!(&frame[5..13], &[0x22; 8]);
        // Independent Python hmac/SHA-256 reference for session || nonce || message.
        assert_eq!(hex::encode(&frame[13..]), "13cfdae51949437e");
        for other in [
            super::authenticated_frame(8, 0x12, &[0], &[4; 16], &[0x11; 8], &[0x22; 8]).unwrap(),
            super::authenticated_frame(8, 0x12, &[1], &[4; 16], &[0x10; 8], &[0x22; 8]).unwrap(),
            super::authenticated_frame(8, 0x12, &[1], &[4; 16], &[0x11; 8], &[0x23; 8]).unwrap(),
        ] {
            assert_ne!(&frame[13..], &other[13..]);
        }
    }
}
