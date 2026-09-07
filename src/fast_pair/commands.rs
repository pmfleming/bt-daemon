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

impl PendingCommands {
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
    use super::*;

    #[tokio::test]
    async fn aborting_a_wait_releases_the_command_slot() {
        let pending = std::sync::Arc::new(PendingCommands::default());
        let key = (Address::default(), 8, 0x12);
        let worker = pending.clone();
        let (ready, started) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (_reservation, receiver) = worker.reserve(key).unwrap();
            ready.send(()).unwrap();
            let _ = receiver.await;
        });
        started.await.unwrap();
        assert!(pending.reserve(key).is_err());
        task.abort();
        let _ = task.await;
        assert!(pending.reserve(key).is_ok());
    }

    #[test]
    fn old_guard_cannot_remove_a_new_command_after_ack() {
        let pending = PendingCommands::default();
        let key = (Address::default(), 7, 0x12);
        let (old, _) = pending.reserve(key).unwrap();
        pending.resolve(key, Ok(()));
        let (_new, _) = pending.reserve(key).unwrap();
        drop(old);
        assert!(pending.reserve(key).is_err());
        pending.disconnect(key.0);
        assert!(pending.reserve(key).is_ok());
    }
}
