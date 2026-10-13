use bluer::Address;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use super::CACHE_TTL;
const LIVE_TTL: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
pub(super) struct Observation {
    pub rssi: Option<i16>,
    pub last_seen_ms: u64,
    observed: Instant,
}

impl Observation {
    pub fn live(&self, discovering: bool) -> bool {
        discovering && self.observed.elapsed() <= LIVE_TTL
    }
}

#[derive(Default)]
pub(super) struct Observations(HashMap<(String, Address), Observation>);
impl Observations {
    pub fn record(&mut self, adapter: &str, address: Address, rssi: Option<i16>) {
        self.0
            .retain(|_, seen| seen.observed.elapsed() <= CACHE_TTL);
        self.0.insert(
            (adapter.into(), address),
            Observation {
                rssi,
                last_seen_ms: crate::time::unix_time_ms(),
                observed: Instant::now(),
            },
        );
    }
    pub fn next_expiry(&mut self) -> Option<Instant> {
        let now = Instant::now();
        self.0
            .retain(|_, seen| now.duration_since(seen.observed) <= CACHE_TTL);
        self.0
            .values()
            .flat_map(|seen| [seen.observed + LIVE_TTL, seen.observed + CACHE_TTL])
            .filter(|deadline| *deadline > now)
            .min()
    }

    pub fn get(&self, adapter: &str, address: Address) -> Option<Observation> {
        self.0
            .get(&(adapter.into(), address))
            .filter(|seen| seen.observed.elapsed() <= CACHE_TTL)
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::Observations;
    use bluer::Address;
    use std::time::{Duration, Instant};
    #[test]
    fn reading_snapshots_does_not_refresh_last_seen_or_stopped_scan_signal() {
        let mut observations = Observations::default();
        let address = Address::default();
        observations.record("hci0", address, Some(-40));
        let first = observations.get("hci0", address).unwrap();
        assert!(first.live(true));
        assert!(!first.live(false));
        assert_eq!(
            first.last_seen_ms,
            observations.get("hci0", address).unwrap().last_seen_ms
        );
        observations
            .0
            .get_mut(&("hci0".into(), address))
            .unwrap()
            .observed = Instant::now() - Duration::from_secs(31);
        assert!(!observations.get("hci0", address).unwrap().live(true));
        assert!(observations.get("hci1", address).is_none());
    }
}
