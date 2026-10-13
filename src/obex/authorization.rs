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
