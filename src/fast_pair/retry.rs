//! Retry policy is scoped to a physical Bluetooth connection, not each timer tick.
use bluer::Address;
use std::{collections::HashMap, time::Duration};
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
struct PeerRetry {
    failures: u32,
    busy_attempts: usize,
    after: Option<Instant>,
    unknown_psm: u8,
    unavailable_psm: bool,
}

impl PeerRetry {
    fn failed(&mut self) {
        let seconds = (15_u64 * (1_u64 << self.failures.min(5))).min(300);
        self.failures = self.failures.saturating_add(1);
        // Jitter prevents all peers/recovered adapters retrying simultaneously.
        let jitter = u64::from(rand::random::<u8>()) % (seconds / 5 + 1);
        self.after = Some(Instant::now() + Duration::from_secs(seconds + jitter));
    }
}

#[derive(Default)]
pub(super) struct RetryPolicy(HashMap<Address, PeerRetry>);

impl RetryPolicy {
    pub fn ready(&self, address: Address) -> bool {
        self.0
            .get(&address)
            .and_then(|peer| peer.after)
            .is_none_or(|deadline| deadline <= Instant::now())
    }
    pub fn next_deadline(&self) -> Option<Instant> {
        self.0.values().filter_map(|peer| peer.after).min()
    }
    pub fn clear_due(&mut self) {
        let now = Instant::now();
        for peer in self.0.values_mut() {
            peer.after = peer.after.filter(|deadline| *deadline > now);
        }
    }
    pub fn connection_failed(&mut self, address: Address, busy: bool) {
        let peer = self.0.entry(address).or_default();
        if busy && let Some(&millis) = BUSY_RETRY_MS.get(peer.busy_attempts) {
            peer.busy_attempts += 1;
            peer.after = Some(Instant::now() + Duration::from_millis(millis));
        } else {
            peer.failed();
        }
    }
    pub fn failed(&mut self, address: Address) {
        self.0.entry(address).or_default().failed();
    }
    pub fn connected(&mut self, address: Address) {
        if let Some(peer) = self.0.get_mut(&address) {
            peer.after = None;
        }
    }
    pub fn l2cap_allowed(&self, address: Address) -> bool {
        self.0
            .get(&address)
            .is_none_or(|peer| !peer.unavailable_psm)
    }
    pub fn psm_unknown(&mut self, address: Address) {
        let peer = self.0.entry(address).or_default();
        peer.unknown_psm = peer.unknown_psm.saturating_add(1);
        peer.unavailable_psm |= peer.unknown_psm >= 3;
    }
    pub fn psm_unavailable(&mut self, address: Address) {
        self.0.entry(address).or_default().unavailable_psm = true;
    }
    pub fn reset_session(&mut self, address: Address) {
        self.0.remove(&address);
    }
}

#[cfg(test)]
mod tests {
    use super::{BUSY_RETRY_MS, RetryPolicy, is_connection_busy, wait_for_deadline};
    use bluer::Address;
    use futures::FutureExt;
    use std::time::Duration;
    use tokio::time::Instant;

    #[tokio::test(start_paused = true)]
    async fn busy_budget_and_deadlines_are_scoped_to_each_session() {
        let peer = Address::default();
        let other = "AA:BB:CC:DD:EE:FF".parse().unwrap();
        let mut retry = RetryPolicy::default();
        assert!(
            wait_for_deadline(retry.next_deadline())
                .now_or_never()
                .is_none()
        );
        let busy = is_connection_busy(
            &anyhow::Error::new(bluer::Error {
                kind: bluer::ErrorKind::Failed,
                message: "br-connection-busy".into(),
            })
            .context("connect"),
        );
        retry.connection_failed(
            other,
            is_connection_busy(&anyhow::anyhow!("br-connection-busy")),
        );
        let later = retry.next_deadline().unwrap();
        for _ in BUSY_RETRY_MS {
            retry.connection_failed(peer, busy);
            assert!(retry.next_deadline().unwrap() < later);
            assert!(!retry.ready(peer));
            wait_for_deadline(retry.next_deadline()).await;
            assert!(retry.ready(peer));
            retry.clear_due();
            assert_eq!(retry.next_deadline(), Some(later));
            assert!(!retry.ready(other));
        }
        retry.reset_session(other);
        retry.connection_failed(peer, true);
        let delay = retry.next_deadline().unwrap() - Instant::now();
        let quick_limit = Duration::from_millis(*BUSY_RETRY_MS.iter().max().unwrap());
        assert!(delay > quick_limit);
        // A briefly connected stream must not replenish either retry budget.
        retry.connected(peer);
        retry.connection_failed(peer, true);
        let delay = retry.next_deadline().unwrap() - Instant::now();
        assert!(delay > quick_limit);
        // Expiry preserves failure history, but never creates an unbounded wait.
        for _ in 0..12 {
            retry.failed(peer);
            let delay = retry.next_deadline().unwrap() - Instant::now();
            assert!((Duration::from_secs(1)..=Duration::from_secs(360)).contains(&delay));
            tokio::time::advance(delay).await;
            assert!(retry.ready(peer));
            retry.clear_due();
            assert!(retry.next_deadline().is_none());
        }
        retry.reset_session(peer);
        assert!(
            wait_for_deadline(retry.next_deadline())
                .now_or_never()
                .is_none()
        );
        retry.connection_failed(peer, true);
        assert_eq!(
            retry.next_deadline(),
            Some(Instant::now() + Duration::from_millis(BUSY_RETRY_MS[0]))
        );
    }
}
