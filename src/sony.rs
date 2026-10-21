//! Sony headphones noise control over the Headphones Connect RFCOMM channel.
//!
//! Fast Pair carries battery on Sony models but not noise control, so this
//! provider keeps a second, vendor channel open to connected Sony headphones.
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use bluer::{
    Adapter, Address, Device, Session,
    rfcomm::{Profile, ProfileHandle, ReqError, Role},
};
use futures::StreamExt;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{Mutex, RwLock, broadcast, mpsc, oneshot},
};
use uuid::Uuid;

use crate::model::NoiseControl;

mod protocol;
use protocol::{Frame, FrameDecoder, NcAsm};

/// RFCOMM service UUID of the Sony Headphones Connect control channel.
pub const CONTROL_UUID: &str = "956c7b26-d49a-4ba8-b03f-b17d393cb6e2";
const SOURCE: &str = "sony-headphones-control";
const ACK_TIMEOUT: Duration = Duration::from_secs(1);
const STATE_TIMEOUT: Duration = Duration::from_secs(1);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(12);
const RECONCILE_INTERVAL: Duration = Duration::from_secs(10);
const STALE_CONNECT: Duration = Duration::from_secs(30);
/// BlueZ reports busy while another profile (e.g. Fast Pair) is still connecting.
const BUSY_RETRIES: u32 = 6;

struct Command {
    payload: Vec<u8>,
    done: Option<oneshot::Sender<Result<()>>>,
}

struct Channel {
    commands: mpsc::Sender<Command>,
    noise_control: Option<NcAsm>,
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

/// Keeps a control channel to each connected Sony headset and exposes its
/// noise-cancelling/ambient-sound state as vendor-neutral noise control.
pub struct SonyControlProvider {
    session: Session,
    uuid: Uuid,
    links: Mutex<Links>,
    channels: RwLock<HashMap<Address, Channel>>,
    changes: broadcast::Sender<()>,
    tasks: crate::task::TaskGroup,
}

impl SonyControlProvider {
    pub async fn start(session: Session, changes: broadcast::Sender<()>) -> Result<Arc<Self>> {
        let uuid: Uuid = CONTROL_UUID.parse().context("parse Sony control UUID")?;
        let requests = session
            .register_profile(Profile {
                uuid,
                name: Some("Shelllist Sony headphones control".to_string()),
                role: Some(Role::Client),
                require_authentication: Some(true),
                require_authorization: Some(false),
                auto_connect: Some(false),
                ..Default::default()
            })
            .await
            .context("register Sony control RFCOMM profile")?;
        let provider = Arc::new(Self {
            session,
            uuid,
            links: Mutex::default(),
            channels: RwLock::default(),
            changes,
            tasks: crate::task::TaskGroup::default(),
        });
        Self::spawn_request_handler(Arc::clone(&provider), requests);
        Self::spawn_reconciler(Arc::clone(&provider));
        Ok(provider)
    }

    pub(crate) fn abort(&self) {
        self.tasks.abort();
    }

    pub async fn shutdown(&self) {
        self.tasks.shutdown().await;
        *self.links.lock().await = Links::default();
        self.channels.write().await.clear();
    }

    /// Reported noise control, or `None` before the headphones answer.
    pub async fn noise_control(&self, address: Address) -> Option<NoiseControl> {
        let state = self.channels.read().await.get(&address)?.noise_control?;
        let modes: Vec<String> = protocol::MODES.into_iter().map(Into::into).collect();
        Some(NoiseControl {
            source: SOURCE.into(),
            available_modes: modes.clone(),
            settable_modes: modes,
            active_mode: state.active_mode().map(Into::into),
        })
    }

    pub async fn set_noise_control(&self, address: Address, mode: &str) -> Result<()> {
        let (commands, payload) = {
            let channels = self.channels.read().await;
            let channel = channels
                .get(&address)
                .context("Sony control channel is not connected")?;
            let state = channel
                .noise_control
                .context("Sony headphones have not reported noise control")?;
            (
                channel.commands.clone(),
                protocol::nc_asm_payload(&state, mode)?,
            )
        };
        // Subscribe before sending so the confirming notification is not missed.
        let mut changes = self.changes.subscribe();
        let (done, acknowledged) = oneshot::channel();
        commands
            .send(Command {
                payload,
                done: Some(done),
            })
            .await
            .map_err(|_| anyhow!("Sony control channel closed"))?;
        acknowledged
            .await
            .context("Sony control channel closed before acknowledging")??;
        // The returned snapshot should already show the new mode.
        let confirmed = tokio::time::timeout(STATE_TIMEOUT, async {
            while self.active_mode(address).await != Some(mode) {
                if let Err(broadcast::error::RecvError::Closed) = changes.recv().await {
                    break;
                }
            }
        })
        .await;
        if confirmed.is_err() {
            tracing::debug!(%address, mode, "Sony headphones acknowledged without a state notification");
        }
        Ok(())
    }

    async fn active_mode(&self, address: Address) -> Option<&'static str> {
        self.channels
            .read()
            .await
            .get(&address)?
            .noise_control?
            .active_mode()
    }

    fn spawn_request_handler(provider: Arc<Self>, mut requests: ProfileHandle) {
        let owner = Arc::clone(&provider);
        owner.tasks.spawn("sony-control-requests", async move {
            while let Some(request) = requests.next().await {
                let address = request.device();
                if !provider.links.lock().await.accept(address) {
                    request.reject(ReqError::Rejected);
                    continue;
                }
                match request.accept() {
                    Ok(stream) => Self::spawn_session(Arc::clone(&provider), address, stream),
                    Err(error) => {
                        tracing::warn!(%address, %error, "could not accept Sony control channel");
                        provider.links.lock().await.ended(address);
                    }
                }
            }
            tracing::warn!("Sony control profile request stream ended");
        });
    }

    fn spawn_session<S>(provider: Arc<Self>, address: Address, stream: S)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let owner = Arc::clone(&provider);
        owner.tasks.spawn("sony-control-session", async move {
            tracing::info!(%address, "Sony control channel started");
            let result =
                crate::task::catch("Sony control channel", provider.run_session(address, stream))
                    .await;
            match result {
                Ok(()) => tracing::info!(%address, "Sony control channel closed"),
                Err(error) => {
                    tracing::warn!(%address, error = %error, error_chain = %format!("{error:#}"), "Sony control channel failed");
                }
            }
            provider.links.lock().await.ended(address);
            if provider.channels.write().await.remove(&address).is_some() {
                let _ = provider.changes.send(());
            }
        });
    }

    async fn run_session<S>(&self, address: Address, stream: S) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let (reader, writer) = tokio::io::split(stream);
        let (commands_tx, commands_rx) = mpsc::channel(8);
        let (acks_tx, acks_rx) = mpsc::channel(16);
        let (acked_tx, acked_rx) = mpsc::channel(4);
        commands_tx
            .send(Command {
                payload: protocol::INIT_REQUEST.to_vec(),
                done: None,
            })
            .await
            .context("queue Sony init request")?;
        self.channels.write().await.insert(
            address,
            Channel {
                commands: commands_tx.clone(),
                noise_control: None,
            },
        );
        let link = Link {
            commands: commands_tx,
            acks: acks_tx,
            acked: acked_tx,
        };
        tokio::select! {
            result = self.read_session(address, reader, &link) => result,
            result = write_session(writer, commands_rx, acks_rx, acked_rx) => result,
        }
    }

    async fn read_session<R>(&self, address: Address, mut reader: R, link: &Link) -> Result<()>
    where
        R: AsyncRead + Unpin,
    {
        let mut bytes = [0_u8; 512];
        let mut decoder = FrameDecoder::default();
        loop {
            let count = reader
                .read(&mut bytes)
                .await
                .context("read Sony control channel")?;
            if count == 0 {
                return Ok(());
            }
            for frame in decoder.push(&bytes[..count]) {
                match frame {
                    Ok(frame) => self.apply_frame(address, frame, link).await?,
                    Err(error) => tracing::warn!(%address, %error, "ignored invalid Sony frame"),
                }
            }
        }
    }

    async fn apply_frame(&self, address: Address, frame: Frame, link: &Link) -> Result<()> {
        match frame.kind {
            protocol::ACK => {
                let _ = link.acked.try_send(frame.sequence);
            }
            protocol::COMMAND | protocol::COMMAND_2 => {
                link.acks
                    .send(frame.sequence)
                    .await
                    .context("queue Sony acknowledgement")?;
                if frame.kind == protocol::COMMAND {
                    self.apply_command(address, &frame.payload, link).await;
                }
            }
            kind => tracing::trace!(%address, kind, "ignored Sony frame kind"),
        }
        Ok(())
    }

    async fn store_noise_control(&self, address: Address, state: NcAsm) {
        {
            let mut channels = self.channels.write().await;
            let Some(channel) = channels.get_mut(&address) else {
                return;
            };
            if channel.noise_control == Some(state) {
                return;
            }
            channel.noise_control = Some(state);
        }
        tracing::debug!(%address, mode = ?state.active_mode(), "Sony noise control updated");
        let _ = self.changes.send(());
    }

    async fn apply_command(&self, address: Address, payload: &[u8], link: &Link) {
        match payload.first() {
            Some(&protocol::INIT_REPLY) => {
                tracing::debug!(%address, reply = ?payload, "Sony control channel initialized");
                for query in protocol::queries() {
                    if link
                        .commands
                        .try_send(Command {
                            payload: query,
                            done: None,
                        })
                        .is_err()
                    {
                        tracing::warn!(%address, "Sony noise-control query queue is full");
                    }
                }
            }
            Some(&(protocol::NC_ASM_RET | protocol::NC_ASM_NOTIFY)) => {
                match protocol::decode_nc_asm(payload) {
                    Ok(state) => self.store_noise_control(address, state).await,
                    Err(error) => {
                        tracing::debug!(%address, %error, "ignored Sony noise-control report")
                    }
                }
            }
            _ => tracing::trace!(%address, payload = ?payload, "ignored Sony command"),
        }
    }

    fn spawn_reconciler(provider: Arc<Self>) {
        let mut changes = provider.changes.subscribe();
        let owner = Arc::clone(&provider);
        owner.tasks.spawn("sony-control-reconciler", async move {
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
                provider.reconcile().await;
            }
        });
    }

    async fn reconcile(self: &Arc<Self>) {
        let adapter_names = match self.session.adapter_names().await {
            Ok(names) => names,
            Err(error) => {
                tracing::warn!(%error, "could not list adapters for Sony control");
                return;
            }
        };
        for adapter_name in adapter_names {
            if let Err(error) = self.reconcile_adapter(&adapter_name).await {
                tracing::warn!(%adapter_name, error = %error, error_chain = %format!("{error:#}"), "Sony control reconciliation failed");
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
            // Connected precedes service resolution; ServicesResolved wakes us via `changes`.
            if device.is_services_resolved().await?
                && device
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
        let provider = Arc::clone(self);
        self.tasks.spawn("sony-control-connect", async move {
            // The stream itself arrives through the registered profile handler.
            let result = tokio::time::timeout(CONNECT_TIMEOUT, device.connect_profile(&provider.uuid))
                .await
                .context("Sony control connection timed out")
                .and_then(|result| result.context("Sony control profile connection failed"));
            if let Err(error) = result {
                tracing::warn!(%address, error = %error, error_chain = %format!("{error:#}"), "Sony control connection failed");
                let busy = crate::fast_pair::is_connection_busy(&error);
                provider.links.lock().await.connect_failed(address, busy);
            }
        });
    }
}

struct Link {
    commands: mpsc::Sender<Command>,
    acks: mpsc::Sender<u8>,
    acked: mpsc::Sender<u8>,
}

/// Sends one command at a time and waits for its ACK, as the headphones expect.
/// ACKs for headphone-initiated messages are interleaved while waiting.
async fn write_session<W>(
    mut writer: W,
    mut commands: mpsc::Receiver<Command>,
    mut acks: mpsc::Receiver<u8>,
    mut acked: mpsc::Receiver<u8>,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut sequence = 0_u8;
    loop {
        tokio::select! {
            biased;
            Some(received) = acks.recv() => send_ack(&mut writer, received).await?,
            command = commands.recv() => {
                let Some(command) = command else {
                    return Ok(());
                };
                while acked.try_recv().is_ok() {}
                send(&mut writer, protocol::encode(protocol::COMMAND, sequence, &command.payload)?).await?;
                let deadline = tokio::time::sleep(ACK_TIMEOUT);
                tokio::pin!(deadline);
                let result = loop {
                    tokio::select! {
                        biased;
                        Some(received) = acks.recv() => send_ack(&mut writer, received).await?,
                        Some(next) = acked.recv() => {
                            sequence = next;
                            break Ok(());
                        }
                        () = &mut deadline => break Err(anyhow!("Sony headphones did not acknowledge the command")),
                    }
                };
                match command.done {
                    Some(done) => {
                        let _ = done.send(result);
                    }
                    None => {
                        if let Err(error) = result {
                            tracing::debug!(%error, "unacknowledged Sony background command");
                        }
                    }
                }
            }
        }
    }
}

async fn send_ack<W: AsyncWrite + Unpin>(writer: &mut W, received: u8) -> Result<()> {
    // ACKs carry the opposite sequence bit of the message they acknowledge.
    send(writer, protocol::encode(protocol::ACK, received ^ 1, &[])?).await
}

async fn send<W: AsyncWrite + Unpin>(writer: &mut W, frame: Vec<u8>) -> Result<()> {
    writer
        .write_all(&frame)
        .await
        .context("write Sony control frame")?;
    writer.flush().await.context("flush Sony control frame")
}

#[cfg(test)]
mod tests {
    use super::{BUSY_RETRIES, Command, Links, protocol, write_session};
    use protocol::{Frame, FrameDecoder};
    use tokio::{
        io::{AsyncReadExt, DuplexStream},
        sync::{mpsc, oneshot},
    };

    async fn next_frame(remote: &mut DuplexStream, decoder: &mut FrameDecoder) -> Frame {
        let mut bytes = [0_u8; 64];
        loop {
            let count = remote.read(&mut bytes).await.unwrap();
            if let Some(frame) = decoder.push(&bytes[..count]).pop() {
                return frame.unwrap();
            }
        }
    }

    fn command(payload: &[u8]) -> (Command, oneshot::Receiver<anyhow::Result<()>>) {
        let (done, result) = oneshot::channel();
        let command = Command {
            payload: payload.to_vec(),
            done: Some(done),
        };
        (command, result)
    }

    #[tokio::test(start_paused = true)]
    async fn writer_serializes_commands_on_acks_and_answers_headphone_messages() {
        let (local, mut remote) = tokio::io::duplex(256);
        let (commands, commands_rx) = mpsc::channel(4);
        let (acks, acks_rx) = mpsc::channel(4);
        let (acked, acked_rx) = mpsc::channel(4);
        let writer = tokio::spawn(write_session(local, commands_rx, acks_rx, acked_rx));
        let mut decoder = FrameDecoder::default();

        let (query, queried) = command(&[0x66, 0x17]);
        commands.send(query).await.unwrap();
        let frame = next_frame(&mut remote, &mut decoder).await;
        assert_eq!((frame.kind, frame.sequence), (protocol::COMMAND, 0));
        assert_eq!(frame.payload, [0x66, 0x17]);
        // A notification arriving before our ACK is acknowledged immediately.
        acks.send(0).await.unwrap();
        let ack = next_frame(&mut remote, &mut decoder).await;
        assert_eq!((ack.kind, ack.sequence), (protocol::ACK, 1));
        acked.send(1).await.unwrap();
        queried.await.unwrap().unwrap();

        // The next command continues from the acknowledged sequence.
        let (set, unacknowledged) = command(&[0x68]);
        commands.send(set).await.unwrap();
        let frame = next_frame(&mut remote, &mut decoder).await;
        assert_eq!(frame.sequence, 1);
        assert!(unacknowledged.await.unwrap().is_err());

        drop(commands);
        writer.await.unwrap().unwrap();
    }

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
