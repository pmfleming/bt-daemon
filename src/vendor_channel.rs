//! Vendor RFCOMM channels kept open to each connected device offering a UUID.
//!
//! BlueZ hands each stream over through a registered client profile; a
//! reconciler requests it with ConnectProfile while the device is connected.
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bluer::{
    Adapter, Address, Device, Session,
    rfcomm::{Profile, ProfileHandle, ReqError, Role, Stream},
};
use futures::StreamExt;
use tokio::sync::{Mutex, broadcast};
use uuid::Uuid;

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
    connecting: HashMap<Address, Instant>,
    connected: HashMap<Address, Instant>,
    retry: HashMap<Address, Retry>,
}

#[derive(Clone, Copy)]
struct Retry {
    failures: u32,
    busy: u32,
    after: Instant,
}

impl Links {
    fn begin(&mut self, address: Address) -> bool {
        let now = Instant::now();
        let busy = self.connected.contains_key(&address)
            || self
                .connecting
                .get(&address)
                .is_some_and(|start| now.duration_since(*start) < STALE_CONNECT)
            || self
                .retry
                .get(&address)
                .is_some_and(|retry| retry.after > now);
        if !busy {
            self.connecting.insert(address, now);
        }
        !busy
    }

    fn accept(&mut self, address: Address) -> bool {
        self.connecting.remove(&address);
        if self.connected.contains_key(&address) {
            return false;
        }
        self.connected.insert(address, Instant::now());
        true
    }

    fn connect_failed(&mut self, address: Address, busy: bool) {
        if self.connecting.remove(&address).is_none() {
            return;
        }
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
        retry.failures += 1;
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
        changes: &broadcast::Sender<()>,
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
        channels.spawn_reconciler(changes.subscribe());
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
            channels.links.lock().await.ended(address);
            handler.ended(address).await;
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
        if !self.links.lock().await.begin(address) {
            return;
        }
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
                let busy = crate::fast_pair::is_connection_busy(&error);
                channels.links.lock().await.connect_failed(address, busy);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{BUSY_RETRIES, Links};

    #[test]
    fn links_back_off_failures_but_retry_busy_connections_quickly() {
        let peer = bluer::Address::default();
        let mut links = Links::default();
        assert!(links.begin(peer));
        assert!(!links.begin(peer), "an attempt is already in flight");
        for _ in 0..BUSY_RETRIES {
            links.connect_failed(peer, true);
            let retry = links.retry[&peer];
            assert_eq!(retry.failures, 0);
            assert!(retry.after <= std::time::Instant::now() + std::time::Duration::from_secs(1));
            links.connecting.insert(peer, std::time::Instant::now());
        }
        links.connect_failed(peer, true);
        assert_eq!(links.retry[&peer].failures, 1, "busy budget is bounded");
        assert!(!links.begin(peer), "backing off");

        links.reset(peer);
        assert!(links.begin(peer));
        assert!(links.accept(peer));
        assert!(!links.accept(peer), "one channel per peer");
        assert!(!links.begin(peer));
        links.ended(peer);
        assert_eq!(links.retry[&peer].failures, 1);
        links.connect_failed(peer, false);
        assert_eq!(links.retry[&peer].failures, 1, "no attempt was in flight");
    }
}
