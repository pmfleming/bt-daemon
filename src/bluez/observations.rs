use bluer::Address;
use std::{
    collections::HashMap,
    time::{Duration, Instant, SystemTime},
};

#[derive(Clone)]
pub(super) struct Observation {
    pub rssi: Option<i16>,
    pub last_seen_ms: u64,
    observed: Instant,
}

impl Observation {
    pub fn live(&self, discovering: bool) -> bool {
        discovering && self.observed.elapsed() <= Duration::from_secs(30)
    }
}

#[derive(Default)]
pub(super) struct Observations(HashMap<(String, Address), Observation>);
impl Observations {
    pub fn record(&mut self, adapter: &str, address: Address, rssi: Option<i16>) {
        self.0
            .retain(|_, seen| seen.observed.elapsed() <= super::DISCOVERED_DEVICE_CACHE_TTL);
        self.0.insert(
            (adapter.into(), address),
            Observation {
                rssi,
                last_seen_ms: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
                observed: Instant::now(),
            },
        );
    }
    pub fn next_expiry(&mut self) -> Option<Instant> {
        let now = Instant::now();
        self.0.retain(|_, seen| {
            now.duration_since(seen.observed) <= super::DISCOVERED_DEVICE_CACHE_TTL
        });
        self.0
            .values()
            .flat_map(|seen| {
                [
                    seen.observed + Duration::from_secs(30),
                    seen.observed + super::DISCOVERED_DEVICE_CACHE_TTL,
                ]
            })
            .filter(|deadline| *deadline > now)
            .min()
    }

    pub fn get(&self, adapter: &str, address: Address) -> Option<Observation> {
        self.0
            .get(&(adapter.into(), address))
            .filter(|seen| seen.observed.elapsed() <= super::DISCOVERED_DEVICE_CACHE_TTL)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
