//! Optional connected-link telemetry. This never starts discovery or connects a
//! device. Hardware I/O stays off snapshot paths and is bounded/cancellable.
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    io,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bluer::{Address, AddressType};
use futures::{StreamExt, stream};
use tokio::{
    sync::{Mutex, broadcast},
    time::Instant,
};

use super::{BluezBackend, DISCOVERED_DEVICE_CACHE_TTL};
use crate::model::RssiKind;

mod mgmt;

const POLL_INTERVAL: Duration = Duration::from_secs(5);
const FAILURE_BACKOFF: Duration = Duration::from_secs(30);
const LIVE_TTL: Duration = Duration::from_secs(12);
type Key = (String, Address);
type Peer = (Key, u16, bool);

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
    denied: bool,
}

impl Signals {
    pub fn get(&self, adapter: &str, address: Address, connected: bool) -> Option<Reading> {
        let entry = self.entries.get(&(adapter.into(), address))?;
        let mut reading = entry.reading?;
        if entry.observed.elapsed() > DISCOVERED_DEVICE_CACHE_TTL {
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
        !self.denied
            && self.entries.get(key).is_none_or(|entry| {
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
                last_seen_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
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
        let (denied, had_entries) = {
            let mut signals = backend.link_signals.lock().await;
            let had_entries = !signals.entries.is_empty();
            signals.entries.retain(|_, entry| {
                if entry.observed.elapsed() > DISCOVERED_DEVICE_CACHE_TTL {
                    entry.reading = None;
                }
                entry.attempted.elapsed() <= DISCOVERED_DEVICE_CACHE_TTL
            });
            (signals.denied, had_entries)
        };
        if !denied {
            poll(&backend.link_signals, connected_peers(backend), mgmt::read).await;
        }
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
            let Ok(address_type) = device.address_type().await else {
                continue;
            };
            let key = (name.clone(), address);
            if backend.link_signals.lock().await.due(&key) {
                peers.push((key, index, address_type == AddressType::LeRandom));
            }
        }
    }
    peers
}

async fn poll<F>(
    signals: &Mutex<Signals>,
    peers: impl Future<Output = Vec<Peer>>,
    read: impl Fn(u16, Address, bool) -> F + Copy,
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
    stream::iter(peers).for_each_concurrent(4, |(key, index, random)| async move {
        {
            let signals = signals.lock().await;
            if signals.denied || !signals.current(&key, epoch) { return; }
        }
        let result = read(index, key.1, random).await;
        let denied = matches!(&result, Ok((mgmt::Reply::Denied, _)))
            || result.as_ref().err().is_some_and(|error| error.kind() == std::io::ErrorKind::PermissionDenied);
        let reading = match result {
            Ok((mgmt::Reply::Reading(rssi), relative)) => Some((rssi, if relative { RssiKind::BrEdrRelative } else { RssiKind::Dbm })),
            _ => None,
        };
        let mut signals = signals.lock().await;
        if denied && !signals.denied {
            signals.denied = true;
            tracing::info!("connected Bluetooth RSSI unavailable: CAP_NET_ADMIN required; retaining discovery/cached signal readings");
        }
        signals.accept(key, epoch, reading);
    }).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn suspend_publishes_cached_freshness_without_a_poll_or_bluez_event() {
        let key = ("hci0".into(), Address::default());
        let mut signals = Signals::default();
        let epoch = signals.begin_poll();
        signals.accept(key.clone(), epoch, Some((0, RssiKind::BrEdrRelative)));
        let before = signals.get(&key.0, key.1, true).unwrap();
        assert!(before.live);
        let signals = Mutex::new(signals);
        let (changes, mut events) = broadcast::channel(4);
        let started = Instant::now();

        suspend(&signals, &changes).await;

        // No timer, hardware event or snapshot request drives this notification.
        assert_eq!(events.try_recv(), Ok(()));
        assert_eq!(started.elapsed(), Duration::ZERO);
        let mut signals = signals.lock().await;
        signals.accept(key.clone(), epoch, Some((-42, RssiKind::Dbm)));
        let after = signals.get(&key.0, key.1, true).unwrap();
        assert!(!after.live);
        assert_eq!(after.rssi, before.rssi);
        assert_eq!(after.kind, before.kind);
        assert_eq!(after.last_seen_ms, before.last_seen_ms);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_enumeration_and_multiple_batches_record_every_failure() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let mut signals = Signals::default();
        let peers: Vec<Peer> = (0..9)
            .map(|index| ((format!("hci{index}"), Address::default()), index, false))
            .collect();
        for (key, _, _) in &peers {
            signals.accept(key.clone(), 0, Some((-60, RssiKind::Dbm)));
        }
        let signals = Mutex::new(signals);
        let active = &AtomicUsize::new(0);
        let completed = &AtomicUsize::new(0);
        let started = Instant::now();
        poll(
            &signals,
            async {
                tokio::time::sleep(Duration::from_secs(3)).await;
                peers.clone()
            },
            |_, _, _| async move {
                assert!(active.fetch_add(1, Ordering::SeqCst) < 4);
                tokio::time::sleep(Duration::from_secs(2)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                completed.fetch_add(1, Ordering::SeqCst);
                Err(io::ErrorKind::TimedOut.into())
            },
        )
        .await;
        assert_eq!(started.elapsed(), Duration::from_secs(9));
        assert_eq!(completed.load(Ordering::SeqCst), peers.len());
        let signals = signals.lock().await;
        for (key, _, _) in &peers {
            let reading = signals.get(&key.0, key.1, true).unwrap();
            assert_eq!(reading.rssi, -60);
            assert!(!reading.live);
            assert!(!signals.due(key));
        }
        tokio::time::advance(FAILURE_BACKOFF).await;
        for (key, _, _) in &peers {
            assert!(signals.due(key));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_enumeration_is_bounded_without_starting_reads() {
        let signals = Mutex::new(Signals::default());
        let started = Instant::now();
        poll(&signals, std::future::pending(), |_, _, _| async {
            panic!("enumeration did not supply any peers");
        })
        .await;
        assert_eq!(started.elapsed(), Duration::from_secs(4));
        assert!(signals.lock().await.entries.is_empty());
    }

    #[tokio::test]
    async fn invalidation_is_scoped_even_before_the_first_read_completes() {
        let mut signals = Signals::default();
        let address = Address::default();
        let other_address = "01:23:45:67:89:AB".parse().unwrap();
        let removed = ("hci0".into(), address);
        let same_adapter = ("hci0".into(), other_address);
        let other_adapter = ("hci1".into(), address);
        let sample = Some((-60, RssiKind::Dbm));
        let epoch = signals.begin_poll();

        signals.invalidate("hci0", Some(address));
        signals.accept(removed.clone(), epoch, sample);
        signals.accept(same_adapter.clone(), epoch, sample);
        signals.accept(other_adapter.clone(), epoch, sample);
        assert!(signals.get("hci0", address, true).is_none());
        assert!(signals.get("hci0", other_address, true).unwrap().live);
        assert!(signals.get("hci1", address, true).unwrap().live);

        signals.invalidate("hci0", None);
        signals.accept(same_adapter.clone(), epoch, sample);
        signals.accept(other_adapter.clone(), epoch, Some((-42, RssiKind::Dbm)));
        assert!(!signals.get("hci0", other_address, true).unwrap().live);
        assert_eq!(signals.get("hci1", address, true).unwrap().rssi, -42);

        let next_epoch = signals.begin_poll();
        assert!(signals.invalidated_devices.is_empty());
        assert!(signals.invalidated_adapters.is_empty());
        signals.accept(removed.clone(), epoch, sample);
        assert!(signals.get("hci0", address, true).is_none());
        signals.accept(removed, next_epoch, sample);
        assert!(signals.get("hci0", address, true).unwrap().live);

        signals.suspend();
        for key in [same_adapter, other_adapter] {
            signals.accept(key.clone(), next_epoch, sample);
            assert!(!signals.get(&key.0, key.1, true).unwrap().live);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn failures_disconnects_expiry_and_late_replies_preserve_only_cached_samples() {
        let mut signals = Signals::default();
        let address = Address::default();
        let key = ("hci0".into(), address);
        assert!(signals.due(&key));
        assert!(signals.get("hci0", address, true).is_none());
        signals.accept(key.clone(), 0, Some((-60, RssiKind::Dbm)));
        let first = signals.get("hci0", address, true).unwrap();
        assert!(first.live);
        assert!(signals.due(&key)); // Next scheduled tick may sample again.
        assert!(signals.get("hci1", address, true).is_none());
        assert!(!signals.get("hci0", address, false).unwrap().live);
        tokio::time::advance(POLL_INTERVAL).await;
        assert!(signals.due(&key));
        signals.accept(key.clone(), 0, None);
        let cached = signals.get("hci0", address, true).unwrap();
        assert_eq!(cached.rssi, first.rssi);
        assert_eq!(cached.last_seen_ms, first.last_seen_ms);
        assert!(!cached.live);
        assert!(!signals.due(&key));
        tokio::time::advance(FAILURE_BACKOFF).await;
        assert!(signals.due(&key));
        signals.accept(key.clone(), 0, Some((0, RssiKind::BrEdrRelative)));
        assert_eq!(signals.get("hci0", address, true).unwrap().rssi, 0);
        signals.invalidate("hci0", Some(address));
        signals.accept(key.clone(), 0, Some((-20, RssiKind::Dbm)));
        assert_eq!(signals.get("hci0", address, true).unwrap().rssi, 0);
        assert!(!signals.get("hci0", address, true).unwrap().live);
        let epoch = signals.begin_poll();
        signals.accept(key.clone(), epoch, Some((-42, RssiKind::Dbm)));
        tokio::time::advance(LIVE_TTL + Duration::from_millis(1)).await;
        assert!(!signals.get("hci0", address, true).unwrap().live);
        tokio::time::advance(DISCOVERED_DEVICE_CACHE_TTL).await;
        assert!(signals.get("hci0", address, true).is_none());
        signals.denied = true;
        assert!(!signals.due(&key));
    }
}
