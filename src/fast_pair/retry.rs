//! Retry policy is scoped to a physical Bluetooth connection, not each timer tick.
use bluer::Address;
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use tokio::time::Instant;

// A connection still settling in BlueZ gets at most 4.75s of quick retries.
// Exhaustion falls back to the normal failure policy until a new/stable session.
const BUSY_RETRY_MS: [u64; 6] = [250, 500, 1000, 1000, 1000, 1000];

pub(super) fn is_connection_busy(error: &anyhow::Error) -> bool {
    error.downcast_ref::<bluer::Error>().is_some_and(|error| {
        error.kind == bluer::ErrorKind::InProgress
            || (error.kind == bluer::ErrorKind::Failed && error.message == "br-connection-busy")
    })
}

pub(super) async fn wait_for_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => futures::future::pending().await,
    }
}

#[derive(Default)]
pub(super) struct RetryPolicy {
    failures: HashMap<Address, u32>,
    busy_attempts: HashMap<Address, usize>,
    after: HashMap<Address, Instant>,
    unknown_psm: HashMap<Address, u8>,
    unavailable_psm: HashSet<Address>,
}

impl RetryPolicy {
    pub fn ready(&self, address: Address) -> bool {
        self.after
            .get(&address)
            .is_none_or(|deadline| *deadline <= Instant::now())
    }
    pub fn next_deadline(&self) -> Option<Instant> {
        self.after.values().copied().min()
    }
    pub fn clear_due(&mut self) {
        let now = Instant::now();
        self.after.retain(|_, deadline| *deadline > now);
    }
    pub fn connection_failed(&mut self, address: Address, busy: bool) {
        if busy {
            let attempts = self.busy_attempts.entry(address).or_default();
            if let Some(&millis) = BUSY_RETRY_MS.get(*attempts) {
                *attempts += 1;
                self.after
                    .insert(address, Instant::now() + Duration::from_millis(millis));
                return;
            }
        }
        self.failed(address);
    }
    pub fn failed(&mut self, address: Address) {
        let attempts = self.failures.entry(address).or_default();
        let seconds = (15_u64 * (1_u64 << (*attempts).min(5))).min(300);
        *attempts = attempts.saturating_add(1);
        // Jitter prevents all peers/recovered adapters retrying simultaneously.
        let jitter = u64::from(rand::random::<u8>()) % (seconds / 5 + 1);
        self.after.insert(
            address,
            Instant::now() + Duration::from_secs(seconds + jitter),
        );
    }
    pub fn connected(&mut self, address: Address) {
        self.after.remove(&address);
    }
    pub fn l2cap_allowed(&self, address: Address) -> bool {
        !self.unavailable_psm.contains(&address)
    }
    pub fn psm_unknown(&mut self, address: Address) {
        let attempts = self.unknown_psm.entry(address).or_default();
        *attempts = attempts.saturating_add(1);
        if *attempts >= 3 {
            self.psm_unavailable(address);
        }
    }
    pub fn psm_unavailable(&mut self, address: Address) {
        self.unavailable_psm.insert(address);
    }
    pub fn reset_session(&mut self, address: Address) {
        self.failures.remove(&address);
        self.busy_attempts.remove(&address);
        self.after.remove(&address);
        self.unknown_psm.remove(&address);
        self.unavailable_psm.remove(&address);
    }
}

#[cfg(test)]
mod tests {
    use super::{BUSY_RETRY_MS, RetryPolicy, is_connection_busy, wait_for_deadline};
    use bluer::Address;
    use std::time::Duration;
    use tokio::time::Instant;

    #[test]
    fn only_typed_bluez_busy_errors_get_short_retries() {
        for (kind, message, busy) in [
            (bluer::ErrorKind::InProgress, "br-connection-busy", true),
            (
                bluer::ErrorKind::InProgress,
                "Operation already in progress",
                true,
            ),
            (bluer::ErrorKind::Failed, "br-connection-busy", true),
            (bluer::ErrorKind::Failed, "br-connection-refused", false),
            (bluer::ErrorKind::NotReady, "", false),
            (bluer::ErrorKind::AuthenticationRejected, "", false),
        ] {
            let error = anyhow::Error::new(bluer::Error {
                kind,
                message: message.into(),
            })
            .context("Fast Pair profile connection failed");
            assert_eq!(is_connection_busy(&error), busy);
        }
        assert!(!is_connection_busy(&anyhow::anyhow!("br-connection-busy")));
    }

    #[tokio::test(start_paused = true)]
    async fn busy_retries_are_fast_bounded_and_do_not_inflate_normal_backoff() {
        let peer = Address::default();
        let mut retry = RetryPolicy::default();
        for millis in BUSY_RETRY_MS {
            retry.connection_failed(peer, true);
            assert_eq!(
                retry.next_deadline(),
                Some(Instant::now() + Duration::from_millis(millis))
            );
            assert!(!retry.ready(peer));
            wait_for_deadline(retry.next_deadline()).await;
            assert!(retry.ready(peer));
            retry.clear_due();
            assert!(retry.next_deadline().is_none());
        }
        retry.connection_failed(peer, true);
        let delay = retry.next_deadline().unwrap() - Instant::now();
        assert!((Duration::from_secs(15)..=Duration::from_secs(18)).contains(&delay));
        // A briefly connected stream must not replenish the quick-retry budget.
        retry.connected(peer);
        retry.connection_failed(peer, true);
        let delay = retry.next_deadline().unwrap() - Instant::now();
        assert!((Duration::from_secs(30)..=Duration::from_secs(36)).contains(&delay));
        retry.reset_session(peer);
        retry.connection_failed(peer, true);
        assert_eq!(
            retry.next_deadline(),
            Some(Instant::now() + Duration::from_millis(250))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn deadlines_wake_without_waiting_for_reconciliation_poll_and_do_not_spin() {
        use futures::FutureExt;
        let peer = Address::default();
        let other = "AA:BB:CC:DD:EE:FF".parse().unwrap();
        let mut retry = RetryPolicy::default();
        assert!(
            wait_for_deadline(retry.next_deadline())
                .now_or_never()
                .is_none()
        );
        retry.connection_failed(other, false);
        let later = retry.next_deadline();
        retry.connection_failed(peer, true);
        let start = Instant::now();
        wait_for_deadline(retry.next_deadline()).await;
        assert_eq!(start.elapsed(), Duration::from_millis(250));
        retry.clear_due();
        assert!(retry.ready(peer));
        assert!(!retry.ready(other));
        assert_eq!(retry.next_deadline(), later);
        wait_for_deadline(later).await;
        retry.clear_due();
        assert!(
            wait_for_deadline(retry.next_deadline())
                .now_or_never()
                .is_none()
        );
        retry.connection_failed(peer, true);
        retry.reset_session(peer);
        assert!(retry.next_deadline().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn retries_are_bounded_and_psm_suppression_requires_a_new_session() {
        let peer = Address::default();
        let mut retry = RetryPolicy::default();
        for _ in 0..12 {
            retry.failed(peer);
            let deadline = retry.after[&peer];
            let delay = deadline - tokio::time::Instant::now();
            assert!(delay >= std::time::Duration::from_secs(15));
            assert!(delay <= std::time::Duration::from_secs(360));
            assert!(!retry.ready(peer));
            tokio::time::advance(delay).await;
            assert!(retry.ready(peer));
        }
        retry.reset_session(peer);
        for _ in 0..2 {
            retry.psm_unknown(peer);
            assert!(retry.l2cap_allowed(peer));
        }
        retry.psm_unknown(peer);
        assert!(!retry.l2cap_allowed(peer));
        retry.failed(peer);
        assert!(!retry.ready(peer));
        retry.connected(peer);
        assert!(retry.ready(peer));
        assert!(!retry.l2cap_allowed(peer));
        retry.reset_session(peer);
        assert!(retry.ready(peer) && retry.l2cap_allowed(peer));
    }
}
