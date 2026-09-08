//! Retry policy is scoped to a physical Bluetooth connection, not each timer tick.
use bluer::Address;
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

#[derive(Default)]
pub(super) struct RetryPolicy {
    failures: HashMap<Address, u32>,
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
        self.after.remove(&address);
        self.unknown_psm.remove(&address);
        self.unavailable_psm.remove(&address);
    }
}

#[cfg(test)]
mod tests {
    use super::RetryPolicy;
    use bluer::Address;
    #[test]
    fn unavailable_stays_suppressed_until_a_new_physical_connection() {
        let mut retry = RetryPolicy::default();
        let address = Address::default();
        retry.psm_unavailable(address);
        retry.connected(address);
        assert!(!retry.l2cap_allowed(address));
        retry.reset_session(address);
        assert!(retry.l2cap_allowed(address));
    }
    #[test]
    fn unknown_psm_has_a_bounded_budget_and_failures_back_off() {
        let mut retry = RetryPolicy::default();
        let address = Address::default();
        for _ in 0..2 {
            retry.psm_unknown(address);
            assert!(retry.l2cap_allowed(address));
        }
        retry.psm_unknown(address);
        assert!(!retry.l2cap_allowed(address));
        retry.failed(address);
        let first = retry.after[&address];
        retry.failed(address);
        assert!(retry.after[&address] > first);
        assert!(!retry.ready(address));
        retry.reset_session(address);
        assert!(retry.ready(address));
    }
}
