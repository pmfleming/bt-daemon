//! Atomic full-state publication. These revisions are not delivery sequences.
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::watch;

pub(super) const MAX_REVISION: u64 = (1_u64 << 53) - 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct Watermark {
    epoch: String,
    revision: u64,
}

#[derive(Clone, PartialEq)]
pub(super) struct Published<T> {
    pub(super) value: T,
    watermark: Watermark,
}

impl<T> Published<T> {
    pub(super) fn new(value: T) -> Self {
        Self {
            value,
            watermark: Watermark {
                epoch: uuid::Uuid::new_v4().to_string(),
                revision: 1,
            },
        }
    }

    pub(super) fn watermark(&self) -> Value {
        json!(self.watermark)
    }

    // Errors carry ordering metadata too: an old unavailable read must not
    // invalidate a newer available observation. Existing error identity stays put.
    pub(super) fn stamp(&self, mut response: Value) -> Value {
        response["data"]["state"] = self.watermark();
        response
    }
}

impl<T: PartialEq> Published<T> {
    pub(super) fn update(sender: &watch::Sender<Self>, next: T) {
        sender.send_if_modified(|current| {
            if current.value == next {
                return false;
            }
            if current.watermark.revision == MAX_REVISION {
                current.watermark.epoch = uuid::Uuid::new_v4().to_string();
                current.watermark.revision = 1;
            } else {
                current.watermark.revision += 1;
            }
            current.value = next;
            true
        });
    }
}

impl Published<Value> {
    pub(super) fn response(&self) -> Value {
        self.stamp(self.value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captured_reads_and_coalesced_watches_keep_their_own_watermark() {
        let (sender, mut receiver) =
            watch::channel(Published::new(json!({"ok": true, "data": {"n": 0}})));
        let before = receiver.borrow_and_update().response();
        Published::update(&sender, json!({"ok": true, "data": {"n": 0}}));
        assert!(!receiver.has_changed().unwrap());
        for n in 1..=3 {
            Published::update(&sender, json!({"ok": true, "data": {"n": n}}));
        }
        let latest = receiver.borrow_and_update().response();
        assert_eq!(before["data"]["n"], 0);
        assert_eq!(before["data"]["state"]["revision"], 1);
        assert_eq!(latest["data"]["n"], 3);
        assert_eq!(latest["data"]["state"]["revision"], 4);
        assert_eq!(
            latest["data"]["state"]["epoch"],
            before["data"]["state"]["epoch"]
        );
        assert_eq!(sender.subscribe().borrow().response(), latest);
        assert_eq!(sender.borrow().response(), latest);
    }

    #[test]
    fn errors_rollover_and_new_publishers_never_reuse_state_identity() {
        let initial = Published::new(crate::api::error("snapshot-loading", "Loading".into()));
        let epoch = initial.watermark.epoch.clone();
        assert_ne!(Published::new(initial.value.clone()).watermark.epoch, epoch);
        let (sender, _) = watch::channel(initial);
        sender.send_modify(|state| state.watermark.revision = MAX_REVISION);
        Published::update(
            &sender,
            crate::api::error("bluez-unavailable", "Unavailable".into()),
        );
        let next = sender.borrow().response();
        assert_eq!(next["ok"], false);
        assert_eq!(next["error"]["code"], "bluez-unavailable");
        assert_eq!(next["data"]["state"]["revision"], 1);
        assert_ne!(next["data"]["state"]["epoch"], epoch);
    }
}
