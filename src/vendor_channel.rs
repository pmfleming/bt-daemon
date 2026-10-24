//! Vendor RFCOMM channels kept open to each connected device offering a UUID.
//!
//! BlueZ hands each stream over through a registered client profile; a
//! reconciler requests it with ConnectProfile while the device is connected.
use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bluer::{
    Adapter, Address, Device, Session,
    rfcomm::{Profile, ProfileHandle, ReqError, Role, Stream},
};
use futures::StreamExt;
use tokio::{
    sync::{Mutex, broadcast},
    time::Instant,
};
use uuid::Uuid;

pub(crate) fn is_connection_busy(error: &anyhow::Error) -> bool {
    error.downcast_ref::<bluer::Error>().is_some_and(|error| {
        error.kind == bluer::ErrorKind::InProgress
            || (error.kind == bluer::ErrorKind::Failed && error.message == "br-connection-busy")
    })
}

const CONNECT_TIMEOUT: Duration = Duration::from_secs(12);
const RECONCILE_INTERVAL: Duration = Duration::from_secs(10);
const STALE_CONNECT: Duration = Duration::from_secs(30);
/// BlueZ reports busy while another profile (e.g. Fast Pair) is still connecting.
const BUSY_RETRIES: u32 = 6;

/// Profile identity, log label and task names of one vendor channel.
pub(crate) struct Spec {
    pub uuid: &'static str,
    pub profile_name: &'static str,
    /// Prefix for logs and errors, such as "Sony control".
    pub label: &'static str,
    pub request_task: &'static str,
    pub session_task: &'static str,
    pub reconciler_task: &'static str,
    pub connect_task: &'static str,
}

/// Vendor protocol run over each accepted channel.
#[async_trait]
pub(crate) trait Handler: Send + Sync + 'static {
    /// Serves one channel until the peer closes it.
    async fn run(&self, address: Address, stream: Stream) -> Result<()>;
    /// Drops per-peer state after a session ends, cleanly or not.
    async fn ended(&self, address: Address);
}

/// Connection attempts per peer, with exponential backoff that resets when the
/// peer disconnects or a session stays up for a minute.
#[derive(Default)]
struct Links {
    connecting: HashMap<Address, Attempt>,
    connected: HashMap<Address, Instant>,
    retry: HashMap<Address, Retry>,
    next_attempt: u64,
}

struct Attempt {
    id: u64,
    started: Instant,
}

struct Retry {
    failures: u32,
    busy: u32,
    after: Instant,
}

impl Links {
    fn begin(&mut self, address: Address) -> Option<u64> {
        let now = Instant::now();
        if self
            .connecting
            .get(&address)
            .is_some_and(|attempt| now.duration_since(attempt.started) >= STALE_CONNECT)
        {
            // A successful ConnectProfile call is not a stream. Bound missing
            // callbacks and back off, rather than retrying forever at a fixed rate.
            self.connecting.remove(&address);
            self.back_off(address);
        }
        if self.connected.contains_key(&address)
            || self.connecting.contains_key(&address)
            || self
                .retry
                .get(&address)
                .is_some_and(|retry| retry.after > now)
        {
            return None;
        }
        self.next_attempt = self.next_attempt.checked_add(1)?;
        self.connecting.insert(
            address,
            Attempt {
                id: self.next_attempt,
                started: now,
            },
        );
        Some(self.next_attempt)
    }

    fn accept(&mut self, address: Address) -> bool {
        self.connecting.remove(&address);
        if self.connected.contains_key(&address) {
            return false;
        }
        self.connected.insert(address, Instant::now());
        true
    }

    fn connect_failed(&mut self, address: Address, attempt: u64, busy: bool) {
        if self
            .connecting
            .get(&address)
            .is_none_or(|current| current.id != attempt)
        {
            return;
        }
        self.connecting.remove(&address);
        let retry = self.retry_state(address);
        if busy && retry.busy < BUSY_RETRIES {
            retry.busy += 1;
            retry.after = Instant::now() + Duration::from_secs(1);
        } else {
            self.back_off(address);
        }
    }

    fn ended(&mut self, address: Address) {
        if let Some(start) = self.connected.remove(&address) {
            if start.elapsed() >= Duration::from_secs(60) {
                self.retry.remove(&address);
            }
            self.back_off(address);
        }
    }

    fn reset(&mut self, address: Address) {
        self.retry.remove(&address);
        self.connecting.remove(&address);
        // An accepted stream retains teardown ownership until its reader ends.
    }

    fn retry_state(&mut self, address: Address) -> &mut Retry {
        self.retry.entry(address).or_insert(Retry {
            failures: 0,
            busy: 0,
            after: Instant::now(),
        })
    }

    fn back_off(&mut self, address: Address) {
        let retry = self.retry_state(address);
        let delay = Duration::from_secs((15_u64 << retry.failures.min(5)).min(300));
        retry.failures = retry.failures.saturating_add(1);
        retry.after = Instant::now() + delay;
    }
}

/// Keeps one channel open to each connected device that offers the spec's UUID.
pub(crate) struct Channels {
    session: Session,
    spec: &'static Spec,
    uuid: Uuid,
    links: Mutex<Links>,
    tasks: crate::task::TaskGroup,
}

impl Channels {
    /// Registers the client profile, builds the handler around the channels and
    /// starts connecting.
    pub async fn start<H: Handler>(
        session: Session,
        spec: &'static Spec,
        changes: broadcast::Receiver<()>,
        build: impl FnOnce(Arc<Self>) -> H,
    ) -> Result<Arc<H>> {
        let uuid: Uuid = spec
            .uuid
            .parse()
            .with_context(|| format!("parse {} UUID", spec.label))?;
        let requests = session
            .register_profile(Profile {
                uuid,
                name: Some(spec.profile_name.to_string()),
                role: Some(Role::Client),
                require_authentication: Some(true),
                require_authorization: Some(false),
                auto_connect: Some(false),
                ..Default::default()
            })
            .await
            .with_context(|| format!("register {} RFCOMM profile", spec.label))?;
        let channels = Arc::new(Self {
            session,
            spec,
            uuid,
            links: Mutex::default(),
            tasks: crate::task::TaskGroup::default(),
        });
        let handler = Arc::new(build(Arc::clone(&channels)));
        channels.spawn_request_handler(requests, Arc::clone(&handler) as Arc<dyn Handler>);
        channels.spawn_reconciler(changes);
        Ok(handler)
    }

    pub(crate) fn abort(&self) {
        self.tasks.abort();
    }

    pub async fn shutdown(&self) {
        self.tasks.shutdown().await;
        *self.links.lock().await = Links::default();
    }

    fn spawn_request_handler(
        self: &Arc<Self>,
        mut requests: ProfileHandle,
        handler: Arc<dyn Handler>,
    ) {
        let channels = Arc::clone(self);
        self.tasks.spawn(self.spec.request_task, async move {
            let label = channels.spec.label;
            while let Some(request) = requests.next().await {
                let address = request.device();
                if !channels.links.lock().await.accept(address) {
                    request.reject(ReqError::Rejected);
                    continue;
                }
                match request.accept() {
                    Ok(stream) => channels.spawn_session(Arc::clone(&handler), address, stream),
                    Err(error) => {
                        tracing::warn!(%address, %error, "could not accept {label} channel");
                        channels.links.lock().await.ended(address);
                    }
                }
            }
            tracing::warn!("{label} profile request stream ended");
        });
    }

    fn spawn_session(
        self: &Arc<Self>,
        handler: Arc<dyn Handler>,
        address: Address,
        stream: Stream,
    ) {
        let channels = Arc::clone(self);
        self.tasks.spawn(self.spec.session_task, async move {
            let label = channels.spec.label;
            tracing::info!(%address, "{label} channel started");
            match crate::task::catch(label, handler.run(address, stream)).await {
                Ok(()) => tracing::info!(%address, "{label} channel closed"),
                Err(error) => {
                    tracing::warn!(%address, error = %error, error_chain = %format!("{error:#}"), "{label} channel failed");
                }
            }
            // Clear the old handler's state before permitting a replacement
            // stream; otherwise its delayed cleanup could erase the new state.
            handler.ended(address).await;
            channels.links.lock().await.ended(address);
        });
    }

    fn spawn_reconciler(self: &Arc<Self>, mut changes: broadcast::Receiver<()>) {
        let channels = Arc::clone(self);
        self.tasks.spawn(self.spec.reconciler_task, async move {
            let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => {}
                    result = changes.recv() => match result {
                        Ok(()) | Err(broadcast::error::RecvError::Lagged(_)) => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            while changes.try_recv().is_ok() {}
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
                channels.reconcile().await;
            }
        });
    }

    async fn reconcile(self: &Arc<Self>) {
        let label = self.spec.label;
        let adapter_names = match self.session.adapter_names().await {
            Ok(names) => names,
            Err(error) => {
                tracing::warn!(%error, "could not list adapters for {label}");
                return;
            }
        };
        for adapter_name in adapter_names {
            if let Err(error) = self.reconcile_adapter(&adapter_name).await {
                tracing::warn!(%adapter_name, error = %error, error_chain = %format!("{error:#}"), "{label} reconciliation failed");
            }
        }
    }

    async fn reconcile_adapter(self: &Arc<Self>, adapter_name: &str) -> Result<()> {
        let adapter: Adapter = self.session.adapter(adapter_name)?;
        for address in adapter.device_addresses().await? {
            let device = adapter.device(address)?;
            if !device.is_connected().await? {
                self.links.lock().await.reset(address);
                continue;
            }
            // A cached BR/EDR profile UUID is sufficient: ConnectProfile can
            // finish resolution on reconnects where ServicesResolved stays false.
            // Existing busy retries/backoff protect connections still settling.
            if device
                .uuids()
                .await?
                .is_some_and(|uuids| uuids.contains(&self.uuid))
            {
                self.ensure_connected(device).await;
            }
        }
        Ok(())
    }

    async fn ensure_connected(self: &Arc<Self>, device: Device) {
        let address = device.address();
        let Some(attempt) = self.links.lock().await.begin(address) else {
            return;
        };
        let channels = Arc::clone(self);
        self.tasks.spawn(self.spec.connect_task, async move {
            let label = channels.spec.label;
            // The stream itself arrives through the registered profile handler.
            let result = tokio::time::timeout(CONNECT_TIMEOUT, device.connect_profile(&channels.uuid))
                .await
                .with_context(|| format!("{label} connection timed out"))
                .and_then(|result| {
                    result.with_context(|| format!("{label} profile connection failed"))
                });
            if let Err(error) = result {
                tracing::warn!(%address, error = %error, error_chain = %format!("{error:#}"), "{label} connection failed");
                let busy = is_connection_busy(&error);
                channels.links.lock().await.connect_failed(address, attempt, busy);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{BUSY_RETRIES, Duration, Instant, Links, STALE_CONNECT};

    #[tokio::test(start_paused = true)]
    async fn links_bound_busy_retries_and_reset_only_after_a_stable_stream() {
        let peer = bluer::Address::default();
        let mut links = Links::default();
        let mut attempt = links.begin(peer).unwrap();
        assert!(links.begin(peer).is_none());
        for _ in 0..BUSY_RETRIES {
            links.connect_failed(peer, attempt, true);
            assert_eq!(links.retry[&peer].failures, 0);
            assert_eq!(
                links.retry[&peer].after - Instant::now(),
                Duration::from_secs(1)
            );
            assert!(links.begin(peer).is_none());
            tokio::time::advance(Duration::from_secs(1)).await;
            attempt = links.begin(peer).unwrap();
        }
        links.connect_failed(peer, attempt, true);
        assert_eq!(links.retry[&peer].failures, 1);
        assert!(links.begin(peer).is_none());
        links.retry.get_mut(&peer).unwrap().failures = u32::MAX;
        tokio::time::advance(Duration::from_secs(15)).await;
        let attempt = links.begin(peer).unwrap();
        links.connect_failed(peer, attempt, false);
        assert_eq!(links.retry[&peer].failures, u32::MAX);
        assert_eq!(
            links.retry[&peer].after - Instant::now(),
            Duration::from_secs(300)
        );

        for (seconds, failures) in [(59, 2), (60, 1)] {
            links.reset(peer);
            let attempt = links.begin(peer).unwrap();
            links.connect_failed(peer, attempt, false);
            assert!(links.accept(peer));
            tokio::time::advance(Duration::from_secs(seconds)).await;
            assert!(
                !links.accept(peer),
                "duplicate acceptance must not restart stream age"
            );
            assert!(links.begin(peer).is_none());
            links.ended(peer);
            assert_eq!(links.retry[&peer].failures, failures);
            links.connect_failed(peer, attempt, false);
            assert_eq!(
                links.retry[&peer].failures, failures,
                "no attempt was in flight"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn expired_and_disconnected_attempts_cannot_mutate_replacements_or_live_streams() {
        let peer = bluer::Address::default();
        let other = bluer::Address::new([1; 6]);
        let mut links = Links::default();
        let old = links.begin(peer).unwrap();
        let unrelated = links.begin(other).unwrap();
        tokio::time::advance(STALE_CONNECT - Duration::from_nanos(1)).await;
        assert!(links.begin(peer).is_none());
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert!(links.begin(peer).is_none(), "missing stream must back off");
        let after = links.retry[&peer].after;
        assert_eq!(after - Instant::now(), Duration::from_secs(15));
        assert!(links.begin(peer).is_none());
        assert_eq!(
            links.retry[&peer].after, after,
            "polling must not postpone retry"
        );
        tokio::time::advance(after - Instant::now()).await;
        let replacement = links.begin(peer).unwrap();
        assert_ne!(replacement, old);
        for busy in [false, true] {
            links.connect_failed(peer, old, busy);
            assert_eq!(links.connecting[&peer].id, replacement);
            assert_eq!(links.retry[&peer].failures, 1);
        }
        links.reset(peer);
        let current = links.begin(peer).unwrap();
        assert_ne!(
            current, replacement,
            "same-clock reconnect needs a new generation"
        );
        links.connect_failed(peer, replacement, false);
        assert_eq!(links.connecting[&peer].id, current);
        assert_eq!(links.connecting[&other].id, unrelated);
        assert!(!links.retry.contains_key(&peer));
        assert!(links.accept(peer));
        links.connect_failed(peer, current, true);
        links.reset(peer);
        assert!(links.begin(peer).is_none(), "live reader owns teardown");
        links.ended(peer);
        assert_eq!(links.retry[&peer].failures, 1);
        links.connect_failed(other, unrelated, false);
        assert_eq!(links.retry[&other].failures, 1);

        let mut exhausted = Links {
            next_attempt: u64::MAX,
            ..Links::default()
        };
        assert!(
            exhausted.begin(peer).is_none(),
            "never wrap into an old generation"
        );
        assert!(exhausted.connecting.is_empty());
    }
}
