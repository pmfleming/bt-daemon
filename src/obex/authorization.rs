//! Cancellation-safe ownership of authorization prompts and destination files.
use super::{IncomingBroker, ObexEvent};
use std::path::PathBuf;

pub(super) struct Destination {
    pub path: PathBuf,
    pub accepted: bool,
}

impl Drop for Destination {
    fn drop(&mut self) {
        if !self.accepted {
            super::remove_reservation(&self.path);
        }
    }
}

pub(super) struct Prompt<'a> {
    pub broker: &'a IncomingBroker,
    pub event: ObexEvent,
}

impl Drop for Prompt<'_> {
    fn drop(&mut self) {
        let pending = self
            .broker
            .pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.event.request_id);
        if pending.is_some() {
            let mut event = self.event.clone();
            event.event = "cancelled".into();
            event.status = "cancelled".into();
            event.timeout_ms = None;
            let _ = self.broker.events.send(event);
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn cancelled_destination_is_removed_but_accepted_transfer_is_retained() {
        let directory = tempfile::tempdir().unwrap();
        for accepted in [false, true] {
            let path = super::super::reserve_incoming_destination_in(directory.path(), "file.txt")
                .unwrap();
            let reserved = super::Destination {
                path: path.clone(),
                accepted,
            };
            assert!(path.is_file());
            drop(reserved);
            assert_eq!(path.exists(), accepted);
        }
    }
}
