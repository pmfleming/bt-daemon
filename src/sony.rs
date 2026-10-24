//! Sony headphones noise control over the Headphones Connect RFCOMM channel.
//!
//! Fast Pair carries battery on Sony models but not noise control, so this
//! provider keeps a second, vendor channel open to connected Sony headphones.
use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use bluer::{Address, Session, rfcomm::Stream};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::{RwLock, broadcast, mpsc, oneshot},
};

use crate::{
    model::NoiseControl,
    vendor_channel::{self, Channels},
};

mod protocol;
use protocol::{Frame, FrameDecoder, NcAsm};

const SOURCE: &str = "sony-headphones-control";
const ACK_TIMEOUT: Duration = Duration::from_secs(1);
const STATE_TIMEOUT: Duration = Duration::from_secs(1);

static SPEC: vendor_channel::Spec = vendor_channel::Spec {
    // Sony Headphones Connect control channel.
    uuid: "956c7b26-d49a-4ba8-b03f-b17d393cb6e2",
    profile_name: "Shelllist Sony headphones control",
    label: "Sony control",
    request_task: "sony-control-requests",
    session_task: "sony-control-session",
    reconciler_task: "sony-control-reconciler",
    connect_task: "sony-control-connect",
};

struct Command {
    payload: Vec<u8>,
    done: Option<oneshot::Sender<Result<()>>>,
}

struct Channel {
    commands: mpsc::Sender<Command>,
    noise_control: Option<NcAsm>,
}

/// Keeps a control channel to each connected Sony headset and exposes its
/// noise-cancelling/ambient-sound state as vendor-neutral noise control.
pub struct SonyControlProvider {
    links: Arc<Channels>,
    channels: RwLock<HashMap<Address, Channel>>,
    changes: broadcast::Sender<()>,
}

#[async_trait]
impl vendor_channel::Handler for SonyControlProvider {
    async fn run(&self, address: Address, stream: Stream) -> Result<()> {
        self.run_session(address, stream).await
    }

    async fn ended(&self, address: Address) {
        if self.channels.write().await.remove(&address).is_some() {
            let _ = self.changes.send(());
        }
    }
}

impl SonyControlProvider {
    pub async fn start(session: Session, changes: broadcast::Sender<()>) -> Result<Arc<Self>> {
        Channels::start(session, &SPEC, changes.subscribe(), |links| Self {
            links,
            channels: RwLock::default(),
            changes,
        })
        .await
    }

    pub(crate) fn abort(&self) {
        self.links.abort();
    }

    pub async fn shutdown(&self) {
        self.links.shutdown().await;
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

    async fn read_session<R>(&self, address: Address, reader: R, link: &Link) -> Result<()>
    where
        R: AsyncRead + Unpin,
    {
        let mut decoder = FrameDecoder::default();
        let mut frames = crate::framing::Reader::new(
            reader,
            move |bytes| decoder.push(bytes),
            |error| tracing::warn!(%address, %error, "ignored invalid Sony frame"),
        );
        while let Some(frame) = frames.next().await.context("read Sony control channel")? {
            self.apply_frame(address, frame, link).await?;
        }
        Ok(())
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
    use super::{Command, protocol, write_session};
    use protocol::FrameDecoder;
    use tokio::sync::{mpsc, oneshot};

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
        let (local, remote) = tokio::io::duplex(256);
        let (commands, commands_rx) = mpsc::channel(4);
        let (acks, acks_rx) = mpsc::channel(4);
        let (acked, acked_rx) = mpsc::channel(4);
        let writer = tokio::spawn(write_session(local, commands_rx, acks_rx, acked_rx));
        let mut decoder = FrameDecoder::default();
        let mut frames = crate::framing::Reader::new(
            remote,
            move |bytes| decoder.push(bytes),
            |error| panic!("invalid outgoing frame: {error}"),
        );

        let (query, queried) = command(&[0x66, 0x17]);
        commands.send(query).await.unwrap();
        let frame = frames.next().await.unwrap().unwrap();
        assert_eq!((frame.kind, frame.sequence), (protocol::COMMAND, 0));
        assert_eq!(frame.payload, [0x66, 0x17]);
        // A notification arriving before our ACK is acknowledged immediately.
        acks.send(0).await.unwrap();
        let ack = frames.next().await.unwrap().unwrap();
        assert_eq!((ack.kind, ack.sequence), (protocol::ACK, 1));
        acked.send(1).await.unwrap();
        queried.await.unwrap().unwrap();

        // The next command continues from the acknowledged sequence.
        let (set, unacknowledged) = command(&[0x68]);
        commands.send(set).await.unwrap();
        let frame = frames.next().await.unwrap().unwrap();
        assert_eq!(frame.sequence, 1);
        assert!(unacknowledged.await.unwrap().is_err());

        drop(commands);
        writer.await.unwrap().unwrap();
        assert!(frames.next().await.unwrap().is_none());
    }
}
