//! Optional connected-link telemetry. This never starts discovery or connects a
//! device. Hardware I/O stays off snapshot paths and is bounded/cancellable.
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    io,
    time::Duration,
};

use bluer::Address;
use futures::{StreamExt, stream};
use tokio::{
    sync::{Mutex, broadcast},
    time::Instant,
};

use super::{BluezBackend, CACHE_TTL};
use crate::model::RssiKind;

use crate::rssi_helper::client as mgmt;

const POLL_INTERVAL: Duration = Duration::from_secs(5);
const FAILURE_BACKOFF: Duration = Duration::from_secs(30);
const LIVE_TTL: Duration = Duration::from_secs(12);
type Key = (String, Address);
type Peer = (Key, u16);

#[derive(Clone, Copy, Debug)]
pub(super) struct Reading {
    pub rssi: i16,
    pub kind: RssiKind,
    pub last_seen_ms: u64,
    pub live: bool,
}

struct Entry {
    reading: Option<Reading>,
    observed: Instant,
    attempted: Instant,
    succeeded: bool,
}

#[derive(Default)]
pub(super) struct Signals {
    entries: HashMap<Key, Entry>,
    epoch: u64,
    invalidated_devices: HashSet<Key>,
    invalidated_adapters: HashSet<String>,
}

impl Signals {
    pub fn get(&self, adapter: &str, address: Address, connected: bool) -> Option<Reading> {
        let entry = self.entries.get(&(adapter.into(), address))?;
        let mut reading = entry.reading?;
        if entry.observed.elapsed() > CACHE_TTL {
            return None;
        }
        reading.live = connected && entry.succeeded && entry.observed.elapsed() <= LIVE_TTL;
        Some(reading)
    }

    pub fn invalidate(&mut self, adapter: &str, address: Option<Address>) {
        // Fence only affected reads, including first reads without a cached entry.
        if let Some(address) = address {
            self.invalidated_devices.insert((adapter.into(), address));
        } else {
            self.invalidated_adapters.insert(adapter.into());
        }
        for ((name, peer), entry) in &mut self.entries {
            if name == adapter && address.is_none_or(|address| address == *peer) {
                entry.succeeded = false;
                entry.attempted = Instant::now() - FAILURE_BACKOFF;
            }
        }
    }

    fn suspend(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
        for entry in self.entries.values_mut() {
            entry.succeeded = false;
            entry.attempted = Instant::now() - FAILURE_BACKOFF;
        }
    }

    fn begin_poll(&mut self) -> u64 {
        // Polls are serialized. Retire the previous poll's tokens before clearing
        // its scoped fences, so discovery churn cannot grow them indefinitely.
        self.epoch = self.epoch.wrapping_add(1);
        self.invalidated_devices.clear();
        self.invalidated_adapters.clear();
        self.epoch
    }

    fn current(&self, key: &Key, epoch: u64) -> bool {
        epoch == self.epoch
            && !self.invalidated_devices.contains(key)
            && !self.invalidated_adapters.contains(&key.0)
    }

    fn due(&self, key: &Key) -> bool {
        self.entries.get(key).is_none_or(|entry| {
            // The monitor already schedules successful reads once per tick.
            // Comparing completion time with that tick would skip every other
            // poll due to the request's own latency.
            entry.succeeded || entry.attempted.elapsed() >= FAILURE_BACKOFF
        })
    }

    fn accept(&mut self, key: Key, epoch: u64, reading: Option<(i16, RssiKind)>) {
        if !self.current(&key, epoch) {
            return;
        }
        let now = Instant::now();
        let entry = self.entries.entry(key).or_insert(Entry {
            reading: None,
            observed: now,
            attempted: now,
            succeeded: false,
        });
        entry.attempted = now;
        entry.succeeded = reading.is_some();
        if let Some((rssi, kind)) = reading {
            entry.observed = now;
            entry.reading = Some(Reading {
                rssi,
                kind,
                live: true,
                last_seen_ms: crate::time::unix_time_ms(),
            });
        }
        // A failed read does not refresh or destroy the last successful sample.
    }
}

pub(super) async fn suspend(signals: &Mutex<Signals>, changes: &broadcast::Sender<()>) {
    signals.lock().await.suspend();
    // The polling monitor pauses during sleep. Publish invalidation now, without
    // waiting for a BlueZ event or runtime-state persistence to trigger a refresh.
    let _ = changes.send(());
}

pub(super) async fn run(backend: &BluezBackend) {
    let mut interval = tokio::time::interval(POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        if *backend.sleeping.borrow() {
            continue;
        }
        let had_entries = {
            let mut signals = backend.link_signals.lock().await;
            let had_entries = !signals.entries.is_empty();
            signals.entries.retain(|_, entry| {
                if entry.observed.elapsed() > CACHE_TTL {
                    entry.reading = None;
                }
                entry.attempted.elapsed() <= CACHE_TTL
            });
            had_entries
        };
        poll(
            &backend.link_signals,
            connected_peers(backend),
            |index, address| mgmt::read(&backend.system_bus, index, address),
        )
        .await;
        // Publish ageing too, but do not rebuild snapshots forever with no peers.
        if had_entries || !backend.link_signals.lock().await.entries.is_empty() {
            let _ = backend.changes.send(());
        }
    }
}

async fn connected_peers(backend: &BluezBackend) -> Vec<Peer> {
    let Ok(names) = backend.session.adapter_names().await else {
        return Vec::new();
    };
    let mut peers = Vec::new();
    for name in names {
        let Some(index) = name
            .strip_prefix("hci")
            .and_then(|index| index.parse::<u16>().ok())
        else {
            continue;
        };
        let Ok(adapter) = backend.session.adapter(&name) else {
            continue;
        };
        let Ok(addresses) = adapter.device_addresses().await else {
            continue;
        };
        for address in addresses {
            let Ok(device) = adapter.device(address) else {
                continue;
            };
            if !device.is_connected().await.unwrap_or(false) {
                continue;
            }
            // The privileged helper resolves and validates the address type.
            let key = (name.clone(), address);
            if backend.link_signals.lock().await.due(&key) {
                peers.push((key, index));
            }
        }
    }
    peers
}

async fn poll<F>(
    signals: &Mutex<Signals>,
    peers: impl Future<Output = Vec<Peer>>,
    read: impl Fn(u16, Address) -> F + Copy,
) where
    F: Future<Output = io::Result<(mgmt::Reply, bool)>>,
{
    let epoch = signals.lock().await.begin_poll();
    // Bound enumeration independently. A whole-poll deadline would cancel later
    // request batches before their own deadlines could record failure/backoff.
    let peers = match tokio::time::timeout(Duration::from_secs(4), peers).await {
        Ok(peers) => peers,
        Err(error) => {
            tracing::debug!(%error, "connected Bluetooth RSSI enumeration timed out");
            return;
        }
    };
    stream::iter(peers)
        .for_each_concurrent(4, |peer| poll_peer(signals, epoch, peer, read))
        .await;
}

async fn poll_peer<F>(
    signals: &Mutex<Signals>,
    epoch: u64,
    (key, index): Peer,
    read: impl FnOnce(u16, Address) -> F,
) where
    F: Future<Output = io::Result<(mgmt::Reply, bool)>>,
{
    if !signals.lock().await.current(&key, epoch) {
        return;
    }
    let reading = match read(index, key.1).await {
        Ok((mgmt::Reply::Reading(rssi), true)) => Some((rssi, RssiKind::BrEdrRelative)),
        Ok((mgmt::Reply::Reading(rssi), false)) => Some((rssi, RssiKind::Dbm)),
        Ok(_) => None,
        Err(error) => {
            tracing::debug!(%error, "connected RSSI helper read failed; retaining cached readings");
            None
        }
    };
    signals.lock().await.accept(key, epoch, reading);
}

#[cfg(test)]
mod tests {
    use super::{CACHE_TTL, FAILURE_BACKOFF, LIVE_TTL, POLL_INTERVAL, Signals, poll};
    use crate::model::RssiKind;
    use bluer::Address;
    use std::{io, time::Duration};
    use tokio::{sync::Mutex, time::Instant};

    #[tokio::test(start_paused = true)]
    async fn failures_disconnects_expiry_and_late_replies_preserve_only_cached_samples() {
        let mut signals = Signals::default();
        let address = Address::default();
        let key = ("hci0".into(), address);
        let sibling = "01:23:45:67:89:AB".parse().unwrap();
        let old = signals.begin_poll();
        signals.invalidate("hci0", Some(address));
        signals.accept(key.clone(), old, Some((-20, RssiKind::Dbm)));
        signals.accept(("hci0".into(), sibling), old, Some((-60, RssiKind::Dbm)));
        assert!(signals.get("hci0", address, true).is_none());
        assert!(signals.get("hci0", sibling, true).unwrap().live);
        let epoch = signals.begin_poll();
        signals.accept(key.clone(), old, Some((-20, RssiKind::Dbm)));
        assert!(signals.get("hci0", address, true).is_none());
        assert!(signals.due(&key));
        signals.accept(key.clone(), epoch, Some((-60, RssiKind::Dbm)));
        let first = signals.get("hci0", address, true).unwrap();
        assert!(first.live);
        assert!(signals.due(&key)); // Next scheduled tick may sample again.
        assert!(signals.get("hci1", address, true).is_none());
        assert!(!signals.get("hci0", address, false).unwrap().live);
        tokio::time::advance(POLL_INTERVAL).await;
        assert!(signals.due(&key));
        signals.accept(key.clone(), epoch, None);
        let cached = signals.get("hci0", address, true).unwrap();
        assert_eq!(cached.rssi, first.rssi);
        assert_eq!(cached.last_seen_ms, first.last_seen_ms);
        assert!(!cached.live);
        assert!(!signals.due(&key));
        tokio::time::advance(FAILURE_BACKOFF).await;
        assert!(signals.due(&key));
        signals.accept(key.clone(), epoch, Some((0, RssiKind::BrEdrRelative)));
        assert_eq!(signals.get("hci0", address, true).unwrap().rssi, 0);
        let other = ("hci1".into(), address);
        signals.accept(other.clone(), epoch, Some((-60, RssiKind::Dbm)));
        signals.accept(("hci0".into(), sibling), epoch, Some((-60, RssiKind::Dbm)));
        signals.invalidate("hci0", None);
        assert!(!signals.get("hci0", sibling, true).unwrap().live);
        signals.accept(key.clone(), epoch, Some((-20, RssiKind::Dbm)));
        assert!(signals.get(&other.0, other.1, true).unwrap().live);
        assert_eq!(signals.get("hci0", address, true).unwrap().rssi, 0);
        assert!(!signals.get("hci0", address, true).unwrap().live);
        let epoch = signals.begin_poll();
        signals.accept(key.clone(), epoch, Some((-42, RssiKind::Dbm)));
        tokio::time::advance(LIVE_TTL + Duration::from_millis(1)).await;
        assert!(!signals.get("hci0", address, true).unwrap().live);
        tokio::time::advance(CACHE_TTL).await;
        assert!(signals.get("hci0", address, true).is_none());
        // Helper/authorization failures back off, but must recover without a
        // session daemon restart after installation or an active-session change.
        signals.accept(key.clone(), epoch, None);
        assert!(!signals.due(&key));
        tokio::time::advance(FAILURE_BACKOFF).await;
        assert!(signals.due(&key));

        // A slow batch must not prevent later peers recording failure/backoff.
        let peers = (0..9)
            .map(|i| ((format!("hci{i}"), address), i))
            .collect::<Vec<_>>();
        let signals = Mutex::new(signals);
        poll(&signals, async { peers.clone() }, |_, _| async {
            tokio::time::sleep(Duration::from_secs(2)).await;
            Err(io::ErrorKind::TimedOut.into())
        })
        .await;
        assert!(
            peers
                .iter()
                .all(|(key, _)| !signals.try_lock().unwrap().due(key))
        );
        let started = Instant::now();
        poll(&signals, std::future::pending(), |_, _| async {
            unreachable!("enumeration supplied no peers")
        })
        .await;
        assert!(started.elapsed() <= Duration::from_secs(10));
    }
}
