use std::{collections::HashMap, sync::Arc, time::Duration};

use aes::{
    Aes128,
    cipher::{BlockDecrypt, BlockEncrypt, KeyInit, generic_array::GenericArray},
};
use anyhow::{Context, Result, bail, ensure};
use bluer::{
    Adapter, Address, AddressType, Device, Session,
    gatt::remote::{Characteristic, Service},
    l2cap::{
        PSM_LE_DYN_START, PSM_LE_MAX, Security, SecurityLevel, Socket as L2capSocket,
        SocketAddr as L2capSocketAddr, Stream as L2capStream,
    },
    rfcomm::{Profile, ProfileHandle, ReqError, Role},
};
use futures::StreamExt;
use p256::{PublicKey, ecdh::EphemeralSecret, elliptic_curve::sec1::ToEncodedPoint};
use rand::{RngCore, rngs::OsRng};
use sha2::{Digest, Sha256};
use tokio::time::Instant;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{Mutex, RwLock, broadcast, mpsc},
};
use uuid::Uuid;

use crate::{
    identity::DeviceIdentityRegistry,
    model::{Battery, FastPairFeatures, FastPairMultipoint, FastPairNoiseControl},
};

mod audio_switch;
mod capabilities;
mod commands;
mod retry;
use retry::RetryPolicy;
mod keys;
mod metadata;
use commands::PendingCommands;
use keys::AccountKeyStore;

/// RFCOMM service UUID for the Google Fast Pair Message Stream.
pub const MESSAGE_STREAM_UUID: &str = "df21fe2c-2515-4fdb-8886-f12c4d67927c";
/// Bluetooth-base UUID advertising the Fast Pair service.
pub const FAST_PAIR_SERVICE_UUID: &str = "0000fe2c-0000-1000-8000-00805f9b34fb";
const MESSAGE_STREAM_PSM_UUID: &str = "fe2c1239-8366-4814-8eb0-01de32100bea";
const DEVICE_INFORMATION_GROUP: u8 = 0x03;
const MODEL_ID_CODE: u8 = 0x01;
const BLE_ADDRESS_CODE: u8 = 0x02;
const BATTERY_UPDATED_CODE: u8 = 0x03;
const SESSION_NONCE_CODE: u8 = 0x0a;
const AUDIO_SWITCH_GROUP: u8 = 0x07;
const GET_AUDIO_SWITCH_CAPABILITY_CODE: u8 = 0x10;
const AUDIO_SWITCH_CAPABILITY_CODE: u8 = 0x11;
const SET_MULTIPOINT_CODE: u8 = 0x12;
const HEARABLE_CONTROLS_GROUP: u8 = 0x08;
const GET_ANC_STATE_CODE: u8 = 0x11;
const SET_ANC_STATE_CODE: u8 = 0x12;
const ANC_STATE_CODE: u8 = 0x13;
const ACKNOWLEDGEMENT_GROUP: u8 = 0xff;
const ACK_CODE: u8 = 0x01;
const NAK_CODE: u8 = 0x02;
const KEY_BASED_PAIRING_UUID: &str = "fe2c1234-8366-4814-8eb0-01de32100bea";
const ACCOUNT_KEY_UUID: &str = "fe2c1236-8366-4814-8eb0-01de32100bea";
const MAX_FRAME_PAYLOAD: usize = 4096;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(12);
const RECONCILE_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BatteryReport {
    left: Option<u8>,
    right: Option<u8>,
    case: Option<u8>,
    charging: [bool; 3],
}

impl BatteryReport {
    fn from_payload(payload: &[u8]) -> Result<Self> {
        if payload.len() != 3 {
            bail!(
                "Fast Pair battery update has {} bytes instead of 3",
                payload.len()
            );
        }
        Ok(Self {
            left: decode_component(payload[0])?,
            right: decode_component(payload[1])?,
            case: decode_component(payload[2])?,
            charging: [
                payload[0] & 0x80 != 0,
                payload[1] & 0x80 != 0,
                payload[2] & 0x80 != 0,
            ],
        })
    }

    fn model_batteries(self) -> Vec<Battery> {
        [
            ("fast-pair-left", "Left", "left", self.left),
            ("fast-pair-right", "Right", "right", self.right),
            ("fast-pair-case", "Case", "case", self.case),
        ]
        .into_iter()
        .enumerate()
        .filter_map(|(index, (id, label, component, reading))| {
            reading.map(|percentage| Battery {
                id: id.to_string(),
                label: label.to_string(),
                component: component.to_string(),
                percentage,
                charging: Some(self.charging[index]),
                source: "google-fast-pair-message-stream".to_string(),
                confidence: "high".to_string(),
            })
        })
        .collect()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RuntimeReport {
    session_nonce: Option<[u8; 8]>,
    model_id: Option<[u8; 3]>,
    ble_address: Option<Address>,
    multipoint: Option<FastPairMultipoint>,
    noise_control: Option<FastPairNoiseControl>,
    last_switch: Option<crate::model::FastPairSwitchEvent>,
}

fn decode_audio_switch_capability(payload: &[u8]) -> Result<FastPairMultipoint> {
    if payload.len() != 4 {
        bail!(
            "Fast Pair Audio Switch capability has {} bytes instead of 4",
            payload.len()
        );
    }
    let version = u16::from_be_bytes([payload[0], payload[1]]);
    let flags = payload[2];
    Ok(FastPairMultipoint {
        version,
        supported: version != 0,
        audio_switch_enabled: flags & 0x80 != 0,
        configurable: flags & 0x40 != 0,
        enabled: flags & 0x20 != 0,
    })
}

fn anc_modes(flags: u8) -> Vec<String> {
    [
        (0x80, "transparent"),
        (0x40, "adaptive"),
        (0x20, "off"),
        (0x08, "noise-cancelling"),
    ]
    .into_iter()
    .filter(|(flag, _)| flags & flag != 0)
    .map(|(_, name)| name.to_string())
    .collect()
}

fn anc_mode_flag(mode: &str) -> Result<u8> {
    match mode {
        "transparent" => Ok(0x80),
        "adaptive" => Ok(0x40),
        "off" => Ok(0x20),
        "noise-cancelling" => Ok(0x08),
        _ => bail!(
            "unsupported Fast Pair noise-control mode {mode}; expected transparent, adaptive, off, or noise-cancelling"
        ),
    }
}

fn decode_anc_state(payload: &[u8]) -> Result<FastPairNoiseControl> {
    if payload.len() != 4 {
        bail!(
            "Fast Pair ANC state has {} bytes instead of 4",
            payload.len()
        );
    }
    let active = anc_modes(payload[3]);
    if active.len() != 1 {
        bail!("Fast Pair ANC state must contain exactly one known active mode");
    }
    Ok(FastPairNoiseControl {
        version: payload[0],
        available_modes: anc_modes(payload[1]),
        settable_modes: anc_modes(payload[2]),
        active_mode: active.into_iter().next(),
    })
}

fn decode_address(payload: &[u8]) -> Result<Address> {
    if payload.len() != 6 {
        bail!(
            "Fast Pair BLE address has {} bytes instead of 6",
            payload.len()
        );
    }
    payload
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(":")
        .parse()
        .context("decode Fast Pair BLE address")
}

fn decode_component(value: u8) -> Result<Option<u8>> {
    let percentage = value & 0x7f;
    if percentage == 0x7f {
        return Ok(None);
    }
    if percentage > 100 {
        bail!("invalid Fast Pair battery percentage {percentage}");
    }
    Ok(Some(percentage))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PsmAvailability {
    Unknown,
    Ready(u16),
    Unavailable,
}

fn decode_message_stream_psm(value: &[u8]) -> Result<PsmAvailability> {
    if value.len() != 3 {
        bail!(
            "Fast Pair Message Stream PSM value has {} bytes instead of 3",
            value.len()
        );
    }
    match value[0] {
        0x00 => Ok(PsmAvailability::Unknown),
        0x01 => {
            let psm = u16::from_le_bytes([value[1], value[2]]);
            if !(PSM_LE_DYN_START..=PSM_LE_MAX).contains(&psm) {
                bail!("Fast Pair Message Stream PSM is out of range: 0x{psm:04x}");
            }
            Ok(PsmAvailability::Ready(psm))
        }
        0x02 => Ok(PsmAvailability::Unavailable),
        state => bail!("invalid Fast Pair Message Stream PSM state: 0x{state:02x}"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Frame {
    group: u8,
    code: u8,
    payload: Vec<u8>,
}

impl Frame {
    fn encoded(group: u8, code: u8, payload: &[u8]) -> Result<Vec<u8>> {
        let length: u16 = payload
            .len()
            .try_into()
            .context("Fast Pair frame payload exceeds the protocol length")?;
        let mut frame = Vec::with_capacity(4 + payload.len());
        frame.extend_from_slice(&[group, code]);
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(payload);
        Ok(frame)
    }
}

fn message_mac(
    account_key: &[u8; 16],
    session_nonce: &[u8; 8],
    message_nonce: &[u8; 8],
    message: &[u8],
) -> [u8; 8] {
    let mut inner_key = [0x36_u8; 64];
    let mut outer_key = [0x5c_u8; 64];
    for index in 0..account_key.len() {
        inner_key[index] ^= account_key[index];
        outer_key[index] ^= account_key[index];
    }
    let mut inner = Sha256::new();
    inner.update(inner_key);
    inner.update(session_nonce);
    inner.update(message_nonce);
    inner.update(message);
    let inner = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(outer_key);
    outer.update(inner);
    let digest = outer.finalize();
    let mut mac = [0; 8];
    mac.copy_from_slice(&digest[..8]);
    mac
}

fn derive_aes_key(shared_secret: &[u8]) -> [u8; 16] {
    let digest = Sha256::digest(shared_secret);
    let mut key = [0; 16];
    key.copy_from_slice(&digest[..16]);
    key
}

fn crypt_block(key: &[u8; 16], block: &mut [u8; 16], encrypt: bool) {
    let cipher = Aes128::new(GenericArray::from_slice(key));
    let block = GenericArray::from_mut_slice(block);
    if encrypt {
        cipher.encrypt_block(block);
    } else {
        cipher.decrypt_block(block);
    }
}

fn address_bytes(address: Address) -> [u8; 6] {
    address.0
}

fn parse_anti_spoofing_public_key(encoded: &str) -> Result<PublicKey> {
    let bytes = hex::decode(encoded).context("decode Fast Pair anti-spoofing public key")?;
    let encoded = match bytes.len() {
        64 => {
            let mut point = Vec::with_capacity(65);
            point.push(0x04);
            point.extend_from_slice(&bytes);
            point
        }
        65 if bytes[0] == 0x04 => bytes,
        length => bail!(
            "Fast Pair anti-spoofing public key has {length} bytes; expected 64-byte X/Y coordinates"
        ),
    };
    PublicKey::from_sec1_bytes(&encoded).context("parse Fast Pair anti-spoofing public key")
}

#[derive(Default)]
struct FrameDecoder {
    buffered: Vec<u8>,
}

impl FrameDecoder {
    fn push(&mut self, bytes: &[u8]) -> Result<Vec<Frame>> {
        self.buffered.extend_from_slice(bytes);
        let mut frames = Vec::new();
        loop {
            if self.buffered.len() < 4 {
                break;
            }
            let payload_len = u16::from_be_bytes([self.buffered[2], self.buffered[3]]) as usize;
            if payload_len > MAX_FRAME_PAYLOAD {
                self.buffered.clear();
                bail!("Fast Pair frame payload is too large: {payload_len}");
            }
            let frame_len = 4 + payload_len;
            if self.buffered.len() < frame_len {
                break;
            }
            let bytes: Vec<_> = self.buffered.drain(..frame_len).collect();
            frames.push(Frame {
                group: bytes[0],
                code: bytes[1],
                payload: bytes[4..].to_vec(),
            });
        }
        Ok(frames)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MessageStreamTransport {
    Rfcomm,
    L2cap,
}

fn select_transport(
    rfcomm_available: bool,
    supports_rfcomm: bool,
    supports_l2cap: bool,
) -> Option<MessageStreamTransport> {
    if rfcomm_available && supports_rfcomm {
        Some(MessageStreamTransport::Rfcomm)
    } else if supports_l2cap {
        Some(MessageStreamTransport::L2cap)
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LinkState {
    Connecting,
    Connected,
}

#[derive(Default)]
struct ConnectionState {
    links: HashMap<Address, LinkState>,
    writers: HashMap<Address, mpsc::Sender<Vec<u8>>>,
    retry: RetryPolicy,
    connected_since: HashMap<Address, Instant>,
}

impl ConnectionState {
    fn begin(&mut self, address: Address) -> bool {
        if self.links.contains_key(&address) || !self.retry.ready(address) {
            return false;
        }
        self.links.insert(address, LinkState::Connecting);
        true
    }

    fn ended(&mut self, address: Address) {
        self.links.remove(&address);
        self.writers.remove(&address);
        if self
            .connected_since
            .remove(&address)
            .is_some_and(|start| start.elapsed() >= Duration::from_secs(60))
        {
            self.retry.reset_session(address);
        }
        self.retry.failed(address);
    }

    fn mark_connected(&mut self, address: Address) -> bool {
        if self.links.get(&address) == Some(&LinkState::Connected) {
            return false;
        }
        self.links.insert(address, LinkState::Connected);
        self.retry.connected(address);
        self.connected_since.insert(address, Instant::now());
        true
    }

    fn connection_failed(&mut self, address: Address) {
        if self.links.get(&address) == Some(&LinkState::Connecting) {
            self.links.remove(&address);
            self.retry.failed(address);
        }
    }
}

#[cfg(test)]
mod connection_tests;

#[derive(Clone, Copy)]
struct FastPairUuids {
    message_stream: Uuid,
    service: Uuid,
    message_stream_psm: Uuid,
    key_based_pairing: Uuid,
    account_key: Uuid,
}

impl FastPairUuids {
    fn parse() -> Result<Self> {
        Ok(Self {
            message_stream: Uuid::parse_str(MESSAGE_STREAM_UUID)?,
            service: Uuid::parse_str(FAST_PAIR_SERVICE_UUID)?,
            message_stream_psm: Uuid::parse_str(MESSAGE_STREAM_PSM_UUID)?,
            key_based_pairing: Uuid::parse_str(KEY_BASED_PAIRING_UUID)?,
            account_key: Uuid::parse_str(ACCOUNT_KEY_UUID)?,
        })
    }
}

/// Owns Fast Pair message streams, battery observations and authenticated controls.
/// Account keys and operator-provisioned metadata are required for privileged actions.
pub struct FastPairBatteryProvider {
    session: Session,
    identities: Arc<DeviceIdentityRegistry>,
    account_keys: AccountKeyStore,
    catalog: metadata::Catalog,
    paired_at: RwLock<HashMap<String, Instant>>,
    reports: RwLock<HashMap<Address, BatteryReport>>,
    runtime: RwLock<HashMap<Address, RuntimeReport>>,
    connections: Mutex<ConnectionState>,
    pending_commands: PendingCommands,
    changes: broadcast::Sender<()>,
    uuids: FastPairUuids,
    rfcomm_available: bool,
    tasks: crate::task::TaskGroup,
}

async fn run_message_stream<S>(
    provider: Arc<FastPairBatteryProvider>,
    address: Address,
    stream: S,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, writer) = tokio::io::split(stream);
    let (chunks_tx, chunks_rx) = mpsc::channel(16);
    let (frames_tx, frames_rx) = mpsc::channel(32);
    let (writes_tx, writes_rx) = mpsc::channel(16);
    provider.install_writer(address, writes_tx.clone()).await;
    for (group, code) in [
        (AUDIO_SWITCH_GROUP, GET_AUDIO_SWITCH_CAPABILITY_CODE),
        (HEARABLE_CONTROLS_GROUP, GET_ANC_STATE_CODE),
    ] {
        writes_tx
            .send(Frame::encoded(group, code, &[])?)
            .await
            .context("queue Fast Pair capability query")?;
    }
    let reads = async {
        tokio::try_join!(
            read_transport(reader, chunks_tx),
            decode_chunks(chunks_rx, frames_tx),
            apply_frames(Arc::clone(&provider), address, frames_rx),
        )?;
        Ok::<_, anyhow::Error>(())
    };
    tokio::select! {
        result = reads => result?,
        result = write_transport(writer, writes_rx) => result?,
    }
    Ok(())
}

async fn write_transport<W>(mut writer: W, mut writes: mpsc::Receiver<Vec<u8>>) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    while let Some(frame) = writes.recv().await {
        writer
            .write_all(&frame)
            .await
            .context("write Fast Pair Message Stream frame")?;
        writer
            .flush()
            .await
            .context("flush Fast Pair Message Stream frame")?;
    }
    Ok(())
}

async fn read_transport<R>(mut stream: R, chunks: mpsc::Sender<Vec<u8>>) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = [0_u8; 512];
    loop {
        let count = stream
            .read(&mut bytes)
            .await
            .context("read Fast Pair Message Stream transport")?;
        if count == 0 {
            return Ok(());
        }
        tracing::trace!(bytes = count, "read Fast Pair transport chunk");
        chunks
            .send(bytes[..count].to_vec())
            .await
            .context("forward Fast Pair transport chunk")?;
    }
}

async fn decode_chunks(
    mut chunks: mpsc::Receiver<Vec<u8>>,
    frames: mpsc::Sender<Frame>,
) -> Result<()> {
    let mut decoder = FrameDecoder::default();
    while let Some(chunk) = chunks.recv().await {
        for frame in decoder.push(&chunk)? {
            tracing::trace!(
                group = frame.group,
                code = frame.code,
                bytes = frame.payload.len(),
                "decoded Fast Pair frame"
            );
            frames
                .send(frame)
                .await
                .context("forward decoded Fast Pair frame")?;
        }
    }
    Ok(())
}

async fn apply_frames(
    provider: Arc<FastPairBatteryProvider>,
    address: Address,
    mut frames: mpsc::Receiver<Frame>,
) -> Result<()> {
    while let Some(frame) = frames.recv().await {
        // An unsupported/malformed extension message must not tear down an
        // otherwise healthy battery stream and trigger reconnect churn.
        if let Err(error) = apply_frame(&provider, address, frame).await {
            tracing::warn!(%address, %error, "ignored invalid Fast Pair message");
        }
    }
    Ok(())
}

async fn apply_frame(
    provider: &Arc<FastPairBatteryProvider>,
    address: Address,
    frame: Frame,
) -> Result<()> {
    match frame.group {
        DEVICE_INFORMATION_GROUP => {
            apply_device_information(provider, address, frame.code, &frame.payload).await
        }
        AUDIO_SWITCH_GROUP if frame.code == GET_AUDIO_SWITCH_CAPABILITY_CODE => {
            let worker = Arc::clone(provider);
            provider.tasks.spawn("fast-pair-seeker-capability", async move {
                if let Err(error) = worker.answer_audio_switch_query(address).await {
                    // Per SASS, no response within 1s also means unsupported.
                    tracing::debug!(%address, %error, "Audio Switch seeker capability unavailable");
                }
            });
            Ok(())
        }
        AUDIO_SWITCH_GROUP if frame.code == 0x32 => {
            let mut event = audio_switch::decode_switch(&frame.payload)?;
            event.observed_at_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            tracing::info!(%address, reason = %event.reason, target = %event.target, "Fast Pair audio switched");
            provider
                .update_runtime(address, |state| state.last_switch = Some(event))
                .await;
            Ok(())
        }
        AUDIO_SWITCH_GROUP if frame.code == AUDIO_SWITCH_CAPABILITY_CODE => {
            let capability = decode_audio_switch_capability(&frame.payload)?;
            // SASS Notify Capability is bidirectional and requires an ACK.
            // Provider-to-Seeker notifications do not carry a MAC.
            provider
                .send_frame(
                    address,
                    Frame::encoded(
                        ACKNOWLEDGEMENT_GROUP,
                        ACK_CODE,
                        &[AUDIO_SWITCH_GROUP, AUDIO_SWITCH_CAPABILITY_CODE],
                    )?,
                )
                .await?;
            provider
                .update_runtime(address, |state| state.multipoint = Some(capability))
                .await;
            Ok(())
        }
        HEARABLE_CONTROLS_GROUP if frame.code == ANC_STATE_CODE => {
            let noise_control = decode_anc_state(&frame.payload)?;
            provider
                .update_runtime(address, |state| state.noise_control = Some(noise_control))
                .await;
            Ok(())
        }
        ACKNOWLEDGEMENT_GROUP => {
            apply_acknowledgement(provider, address, frame.code, &frame.payload).await;
            Ok(())
        }
        _ => {
            tracing::trace!(%address, group = frame.group, code = frame.code, "ignored Fast Pair frame");
            Ok(())
        }
    }
}

async fn apply_device_information(
    provider: &FastPairBatteryProvider,
    address: Address,
    code: u8,
    payload: &[u8],
) -> Result<()> {
    match code {
        BATTERY_UPDATED_CODE => {
            provider
                .update_report(address, BatteryReport::from_payload(payload)?)
                .await;
        }
        SESSION_NONCE_CODE => {
            let nonce = decode_fixed(payload, "session nonce")?;
            provider
                .update_runtime(address, |state| state.session_nonce = Some(nonce))
                .await;
        }
        MODEL_ID_CODE => {
            let model_id = decode_fixed(payload, "model ID")?;
            provider
                .update_runtime(address, |state| state.model_id = Some(model_id))
                .await;
        }
        BLE_ADDRESS_CODE => {
            let ble_address = decode_address(payload)?;
            provider
                .update_runtime(address, |state| state.ble_address = Some(ble_address))
                .await;
        }
        _ => {}
    }
    Ok(())
}

async fn apply_acknowledgement(
    provider: &FastPairBatteryProvider,
    address: Address,
    code: u8,
    payload: &[u8],
) {
    let (group, message, result) = match (code, payload) {
        (ACK_CODE, [group, message, ..]) => (*group, *message, Ok(())),
        (NAK_CODE, [reason, group, message, ..]) => {
            (*group, *message, Err(nak_reason(*reason).to_string()))
        }
        _ => return,
    };
    provider
        .pending_commands
        .resolve((address, group, message), result);
}

fn decode_fixed<const N: usize>(payload: &[u8], label: &str) -> Result<[u8; N]> {
    payload.try_into().map_err(|_| {
        anyhow::anyhow!(
            "Fast Pair {label} has {} bytes instead of {N}",
            payload.len()
        )
    })
}

fn nak_reason(reason: u8) -> &'static str {
    match reason {
        0x00 => "not supported",
        0x01 => "device busy",
        0x02 => "not allowed in the current state",
        0x03 => "message authentication failed",
        0x04 => "redundant device action",
        _ => "unknown provider rejection",
    }
}

async fn resolve_ble_device(adapter: &Adapter, address: Address) -> Result<Device> {
    discover_address(adapter, address).await?;
    let device = adapter
        .device(address)
        .context("open discovered Fast Pair BLE device")?;
    if !device
        .is_connected()
        .await
        .context("read Fast Pair BLE connection state")?
    {
        device
            .connect()
            .await
            .context("connect Fast Pair BLE device for provisioning")?;
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if device
                .is_services_resolved()
                .await
                .context("read Fast Pair BLE service resolution state")?
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("Fast Pair BLE service resolution timed out")??;
    Ok(device)
}

async fn discover_address(adapter: &Adapter, address: Address) -> Result<()> {
    if adapter
        .device_addresses()
        .await
        .context("list devices before Fast Pair BLE discovery")?
        .contains(&address)
    {
        return Ok(());
    }
    let events = adapter
        .discover_devices()
        .await
        .context("start discovery for Fast Pair BLE address")?;
    futures::pin_mut!(events);
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(event) = events.next().await {
            if matches!(event, bluer::AdapterEvent::DeviceAdded(found) if found == address) {
                return Ok(());
            }
        }
        bail!("Bluetooth discovery ended before the Fast Pair BLE address appeared")
    })
    .await
    .context("Fast Pair BLE address was not discovered")?
}

macro_rules! gatt_lookup {
    ($name:ident, $parent:ty, $child:ty, $items:ident) => {
        async fn $name(parent: &$parent, uuid: Uuid) -> Result<Option<$child>> {
            for item in parent
                .$items()
                .await
                .context(concat!("list Fast Pair GATT ", stringify!($items)))?
            {
                if item.uuid().await.context("read Fast Pair GATT UUID")? == uuid {
                    return Ok(Some(item));
                }
            }
            Ok(None)
        }
    };
}

gatt_lookup!(find_service, Device, Service, services);
gatt_lookup!(
    find_characteristic,
    Service,
    Characteristic,
    characteristics
);

fn provisioning_request(
    anti_spoofing_key: &str,
    remote_address: Address,
    local_address: Address,
) -> Result<([u8; 16], Vec<u8>)> {
    let provider_key = parse_anti_spoofing_public_key(anti_spoofing_key)?;
    let ephemeral = EphemeralSecret::random(&mut OsRng);
    let seeker_public = PublicKey::from(&ephemeral).to_encoded_point(false);
    let shared = ephemeral.diffie_hellman(&provider_key);
    let shared_key = derive_aes_key(shared.raw_secret_bytes());
    let mut request = [0_u8; 16];
    request[0] = 0x00;
    request[1] = 0x10;
    request[2..8].copy_from_slice(&address_bytes(remote_address));
    request[8..14].copy_from_slice(&address_bytes(local_address));
    OsRng.fill_bytes(&mut request[14..]);
    crypt_block(&shared_key, &mut request, true);
    let mut write = request.to_vec();
    write.extend_from_slice(&seeker_public.as_bytes()[1..]);
    Ok((shared_key, write))
}

async fn provisioning_characteristics(
    device: &Device,
    uuids: FastPairUuids,
) -> Result<(Characteristic, Characteristic)> {
    let service = find_service(device, uuids.service)
        .await?
        .context("Fast Pair GATT service is unavailable for account-key provisioning")?;
    let pairing = find_characteristic(&service, uuids.key_based_pairing)
        .await?
        .context("Fast Pair key-based pairing characteristic is unavailable")?;
    let account_key = find_characteristic(&service, uuids.account_key)
        .await?
        .context("Fast Pair account key characteristic is unavailable")?;
    Ok((pairing, account_key))
}

async fn complete_key_pairing(
    pairing: &Characteristic,
    request: &[u8],
    shared_key: &[u8; 16],
    device_address: Address,
) -> Result<()> {
    let notifications = pairing
        .notify()
        .await
        .context("subscribe to Fast Pair key-based pairing response")?;
    futures::pin_mut!(notifications);
    pairing
        .write(request)
        .await
        .context("write Fast Pair retroactive key-based pairing request")?;
    let response = tokio::time::timeout(Duration::from_secs(5), notifications.next())
        .await
        .context("Fast Pair key-based pairing response timed out")?
        .context("Fast Pair key-based pairing notification stream ended")?;
    let mut response = decode_fixed(&response, "key-based pairing response")?;
    crypt_block(shared_key, &mut response, false);
    ensure!(
        response[0] == 0x01,
        "Fast Pair provider returned an invalid key-based pairing response"
    );
    ensure!(
        response[1..7] == address_bytes(device_address),
        "Fast Pair provider response did not match the paired Bluetooth device"
    );
    Ok(())
}

async fn write_account_key(
    characteristic: &Characteristic,
    shared_key: &[u8; 16],
) -> Result<[u8; 16]> {
    let account_key = AccountKeyStore::generate();
    let mut encrypted = account_key;
    crypt_block(shared_key, &mut encrypted, true);
    characteristic
        .write(&encrypted)
        .await
        .context("write encrypted Fast Pair account key")?;
    Ok(account_key)
}

impl FastPairBatteryProvider {
    pub async fn start(
        session: Session,
        identities: Arc<DeviceIdentityRegistry>,
        changes: broadcast::Sender<()>,
    ) -> Result<Arc<Self>> {
        let uuids = FastPairUuids::parse().context("parse fixed Fast Pair UUIDs")?;
        let account_keys = AccountKeyStore::load_default()?;
        let catalog = metadata::Catalog::load_default().unwrap_or_else(|error| {
            tracing::warn!(%error, "trusted Fast Pair metadata unavailable; battery monitoring remains enabled");
            metadata::Catalog::default()
        });
        let profile = Profile {
            uuid: uuids.message_stream,
            name: Some("Shelllist Fast Pair battery provider".to_string()),
            role: Some(Role::Client),
            require_authentication: Some(true),
            require_authorization: Some(false),
            auto_connect: Some(false),
            ..Default::default()
        };
        let requests = match session.register_profile(profile).await {
            Ok(requests) => Some(requests),
            Err(error) => {
                tracing::warn!(%error, "Fast Pair RFCOMM transport is unavailable; BLE L2CAP remains enabled");
                None
            }
        };
        let provider = Arc::new(Self {
            session,
            identities,
            account_keys,
            catalog,
            paired_at: RwLock::new(HashMap::new()),
            reports: RwLock::new(HashMap::new()),
            runtime: RwLock::new(HashMap::new()),
            connections: Mutex::new(ConnectionState::default()),
            pending_commands: PendingCommands::default(),
            changes,
            uuids,
            rfcomm_available: requests.is_some(),
            tasks: crate::task::TaskGroup::default(),
        });
        if let Some(requests) = requests {
            Self::spawn_request_handler(Arc::clone(&provider), requests);
        }
        Self::spawn_reconciler(Arc::clone(&provider));
        Ok(provider)
    }

    pub fn forget_account_key(&self, device_key: &str) -> Result<()> {
        self.account_keys.remove(device_key)
    }

    pub(crate) fn abort(&self) {
        self.tasks.abort();
        self.pending_commands.clear();
    }

    pub async fn shutdown(&self) {
        self.tasks.shutdown().await;
        *self.connections.lock().await = ConnectionState::default();
        self.pending_commands.clear();
        self.runtime.write().await.clear();
        self.reports.write().await.clear();
    }

    pub async fn batteries(&self, address: Address) -> Vec<Battery> {
        self.reports
            .read()
            .await
            .get(&address)
            .copied()
            .map(BatteryReport::model_batteries)
            .unwrap_or_default()
    }

    pub async fn features(&self, device: &Device, connected: bool) -> Option<FastPairFeatures> {
        let reported = self.runtime.read().await.get(&device.address()).cloned();
        let mut runtime = reported.clone().unwrap_or_default();
        if runtime.model_id.is_none() {
            runtime.model_id = device.service_data().await.ok().flatten().and_then(|data| {
                data.get(&self.uuids.service)
                    .and_then(|data| metadata::advertised_model_id(data))
            });
        }
        if reported.is_none() && runtime.model_id.is_none() {
            return None;
        }
        let device_key = self
            .identities
            .device_key(device.adapter_name(), device.address());
        let account_key_available = self.account_keys.get(&device_key).is_some();
        let model = runtime
            .model_id
            .as_ref()
            .and_then(|id| self.catalog.get(id));
        let recent = self
            .paired_at
            .read()
            .await
            .get(&device_key)
            .is_some_and(|start| start.elapsed() < Duration::from_secs(60));
        Some(FastPairFeatures {
            authenticated_controls: connected
                && runtime.session_nonce.is_some()
                && account_key_available,
            ..capabilities::describe(runtime, account_key_available, model, recent)
        })
    }

    pub async fn note_connection_change(&self, address: Address) {
        // Observe real Connected transitions, including fast disconnect/reconnect
        // cycles that the reconciler's sampled is_connected() could miss.
        self.connections.lock().await.retry.reset_session(address);
    }

    pub async fn note_pairing(&self, adapter: &str, address: Address, paired: bool) {
        let key = self.identities.device_key(adapter, address);
        let mut windows = self.paired_at.write().await;
        windows.retain(|_, start| start.elapsed() < Duration::from_secs(60));
        if paired {
            if let std::collections::hash_map::Entry::Vacant(entry) = windows.entry(key) {
                entry.insert(Instant::now());
                let changes = self.changes.clone();
                self.tasks.spawn("fast-pair-pairing-window", async move {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    let _ = changes.send(());
                });
            }
        } else {
            windows.remove(&key);
        }
        let _ = self.changes.send(());
    }

    pub async fn provision_from_catalog(&self, adapter: &Adapter, device: &Device) -> Result<()> {
        let runtime = self
            .wait_for_provisioning_metadata(device.address())
            .await?;
        let model = runtime.model_id.as_ref().and_then(|id| self.catalog.get(id))
            .context("trusted Fast Pair model metadata is missing; configure /etc/bluetooth/fast-pair-models.json")?;
        self.provision_account_key(adapter, device, &model.anti_spoofing_public_key)
            .await
    }

    pub async fn auto_provision(&self, adapter: &Adapter, device: &Device) -> Result<()> {
        if self.catalog.is_empty() {
            return Ok(());
        }
        let key = self
            .identities
            .device_key(device.adapter_name(), device.address());
        if self.account_keys.get(&key).is_some() {
            return Ok(());
        }
        let uuids = device.uuids().await?.unwrap_or_default();
        if !uuids.contains(&self.uuids.message_stream) && !uuids.contains(&self.uuids.service) {
            return Ok(());
        }
        let runtime = self
            .wait_for_provisioning_metadata(device.address())
            .await?;
        if let Some(model) = runtime
            .model_id
            .as_ref()
            .and_then(|id| self.catalog.get(id))
        {
            self.provision_account_key(adapter, device, &model.anti_spoofing_public_key)
                .await?;
        }
        Ok(())
    }

    async fn answer_audio_switch_query(&self, address: Address) -> Result<()> {
        // Version zero is deliberate. Multipoint/ANC controls are implemented,
        // but encrypted-advertisement-driven Audio Switch seeker policy is not.
        // Never advertise 0x0102 just because the Provider supports that version.
        for name in self.session.adapter_names().await? {
            let adapter = self.session.adapter(&name)?;
            if adapter.device_addresses().await?.contains(&address) {
                return self
                    .send_authenticated(
                        &adapter.device(address)?,
                        AUDIO_SWITCH_GROUP,
                        AUDIO_SWITCH_CAPABILITY_CODE,
                        &[0, 0, 0, 0],
                    )
                    .await;
            }
        }
        bail!("Audio Switch peer disappeared")
    }

    pub async fn set_multipoint(&self, device: &Device, enabled: bool) -> Result<()> {
        let capability = self
            .runtime
            .read()
            .await
            .get(&device.address())
            .and_then(|state| state.multipoint)
            .context("Fast Pair multipoint capability has not been reported")?;
        if !capability.supported {
            bail!("Fast Pair Audio Switch is not supported by this device");
        }
        if !capability.configurable {
            bail!("multipoint cannot be changed on this device");
        }
        self.send_authenticated(
            device,
            AUDIO_SWITCH_GROUP,
            SET_MULTIPOINT_CODE,
            &[u8::from(enabled)],
        )
        .await
    }

    pub async fn set_noise_control(&self, device: &Device, mode: &str) -> Result<()> {
        let state = self
            .runtime
            .read()
            .await
            .get(&device.address())
            .and_then(|state| state.noise_control.clone())
            .context("Fast Pair ANC state has not been reported")?;
        ensure!(
            state.version == 2,
            "unsupported Fast Pair ANC protocol version"
        );
        let mode_flag = anc_mode_flag(mode)?;
        if !state
            .settable_modes
            .iter()
            .any(|candidate| candidate == mode)
        {
            bail!("Fast Pair noise-control mode {mode} is not currently settable");
        }
        let available_flags = state.available_modes.iter().try_fold(0_u8, |flags, mode| {
            Ok::<_, anyhow::Error>(flags | anc_mode_flag(mode)?)
        })?;
        let settable_flags = state.settable_modes.iter().try_fold(0_u8, |flags, mode| {
            Ok::<_, anyhow::Error>(flags | anc_mode_flag(mode)?)
        })?;
        self.send_authenticated(
            device,
            HEARABLE_CONTROLS_GROUP,
            SET_ANC_STATE_CODE,
            &[2, available_flags, settable_flags, mode_flag],
        )
        .await
    }

    pub async fn provision_account_key(
        &self,
        adapter: &Adapter,
        device: &Device,
        anti_spoofing_key: &str,
    ) -> Result<()> {
        ensure!(
            device.is_paired().await? && !device.is_blocked().await?,
            "pair and unblock the device before provisioning Fast Pair"
        );
        let key = self
            .identities
            .device_key(device.adapter_name(), device.address());
        ensure!(
            self.account_keys.get(&key).is_none(),
            "Fast Pair account key is already provisioned"
        );
        ensure!(
            self.paired_at
                .read()
                .await
                .get(&key)
                .is_some_and(|start| start.elapsed() < Duration::from_secs(60)),
            "Fast Pair retroactive provisioning requires pairing within the last minute; re-pair the device"
        );
        let runtime = self
            .wait_for_provisioning_metadata(device.address())
            .await?;
        let ble_address = runtime.ble_address.unwrap_or_else(|| device.address());
        let ble_device = resolve_ble_device(adapter, ble_address).await?;
        let (pairing, account_key_characteristic) =
            provisioning_characteristics(&ble_device, self.uuids).await?;
        let local_address = adapter
            .address()
            .await
            .context("read local address for Fast Pair provisioning")?;
        let (shared_key, request) =
            provisioning_request(anti_spoofing_key, ble_address, local_address)?;
        complete_key_pairing(&pairing, &request, &shared_key, device.address()).await?;

        let device_key = self
            .identities
            .promote_device(device.adapter_name(), device.address());
        self.identities.flush().await?;
        let account_key = write_account_key(&account_key_characteristic, &shared_key).await?;
        self.account_keys.insert(device_key, account_key)?;
        tracing::info!(
            address = %device.address(),
            model_id = %runtime.model_id.map(hex::encode).unwrap_or_default(),
            "Fast Pair account key provisioned"
        );
        let _ = self.changes.send(());
        Ok(())
    }

    async fn wait_for_provisioning_metadata(&self, address: Address) -> Result<RuntimeReport> {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Some(runtime) = self.runtime.read().await.get(&address).cloned()
                    && runtime.model_id.is_some()
                {
                    return runtime;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .context("timed out waiting for Fast Pair retroactive-pairing metadata")
    }

    async fn send_authenticated(
        &self,
        device: &Device,
        group: u8,
        code: u8,
        message: &[u8],
    ) -> Result<()> {
        ensure!(
            device.is_paired().await?
                && device.is_connected().await?
                && !device.is_blocked().await?,
            "Fast Pair controls require a connected, paired, unblocked device"
        );
        let address = device.address();
        let session_nonce = self
            .runtime
            .read()
            .await
            .get(&address)
            .and_then(|state| state.session_nonce)
            .context("Fast Pair session nonce is unavailable")?;
        let device_key = self.identities.device_key(device.adapter_name(), address);
        let account_key = self.account_keys.get(&device_key).context(
            "Fast Pair account key is unavailable; pair or provision this device through bt-daemon",
        )?;
        let mut message_nonce = [0_u8; 8];
        OsRng.fill_bytes(&mut message_nonce);
        let frame = commands::authenticated_frame(
            group,
            code,
            message,
            &account_key,
            &session_nonce,
            &message_nonce,
        )?;
        self.pending_commands
            .execute((address, group, code), self.send_frame(address, frame))
            .await
    }

    fn spawn_request_handler(provider: Arc<Self>, mut requests: ProfileHandle) {
        let owner = Arc::clone(&provider);
        owner.tasks.spawn("fast-pair-rfcomm-requests", async move {
            while let Some(request) = requests.next().await {
                let address = request.device();
                let newly_connected = provider.connections.lock().await.mark_connected(address);
                if !newly_connected {
                    request.reject(ReqError::Rejected);
                    continue;
                }
                match request.accept() {
                    Ok(stream) => {
                        tracing::debug!(%address, transport = "RFCOMM", "Fast Pair Message Stream connected");
                        Self::spawn_reader(Arc::clone(&provider), address, "RFCOMM", stream);
                    }
                    Err(error) => {
                        tracing::warn!(%address, %error, "could not accept Fast Pair Message Stream");
                        provider.connection_ended(address).await;
                    }
                }
            }
            tracing::warn!("Fast Pair profile request stream ended");
        });
    }

    fn spawn_reader<R>(provider: Arc<Self>, address: Address, transport: &'static str, stream: R)
    where
        R: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let owner = Arc::clone(&provider);
        owner.tasks.spawn("fast-pair-reader", async move {
            tracing::info!(%address, %transport, "Fast Pair battery stream started");
            let result = crate::task::catch(
                "Fast Pair message stream",
                run_message_stream(Arc::clone(&provider), address, stream),
            )
            .await;
            if let Err(error) = result {
                tracing::warn!(%address, %transport, error = %error, error_chain = %format!("{error:#}"), "Fast Pair battery stream failed");
            } else {
                tracing::info!(%address, %transport, "Fast Pair battery stream closed");
            }
            provider.connection_ended(address).await;
        });
    }

    fn spawn_reconciler(provider: Arc<Self>) {
        let mut changes = provider.changes.subscribe();
        let owner = Arc::clone(&provider);
        owner.tasks.spawn("fast-pair-reconciler", async move {
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
                tracing::warn!(%error, "could not list adapters for Fast Pair battery provider");
                return;
            }
        };
        for adapter_name in adapter_names {
            if let Err(error) = self.reconcile_adapter(&adapter_name).await {
                tracing::warn!(%adapter_name, error = %error, error_chain = %format!("{error:#}"), "Fast Pair adapter reconciliation failed");
            }
        }
    }

    async fn reconcile_adapter(self: &Arc<Self>, adapter_name: &str) -> Result<()> {
        let adapter = self
            .session
            .adapter(adapter_name)
            .context("open Fast Pair adapter")?;
        let addresses = adapter
            .device_addresses()
            .await
            .context("list Fast Pair adapter devices")?;
        for address in addresses {
            if let Err(error) = self.reconcile_device(&adapter, address).await {
                tracing::warn!(%address, error = %error, error_chain = %format!("{error:#}"), "Fast Pair device reconciliation failed");
            }
        }
        Ok(())
    }

    async fn reconcile_device(self: &Arc<Self>, adapter: &Adapter, address: Address) -> Result<()> {
        let device = adapter.device(address).context("open Fast Pair device")?;
        if !device
            .is_connected()
            .await
            .context("read Fast Pair device connection state")?
        {
            self.remove_report(address).await;
            self.connections.lock().await.retry.reset_session(address);
            return Ok(());
        }
        let Some(uuids) = device
            .uuids()
            .await
            .context("read Fast Pair device services")?
        else {
            return Ok(());
        };
        match select_transport(
            self.rfcomm_available,
            uuids.contains(&self.uuids.message_stream),
            uuids.contains(&self.uuids.service),
        ) {
            Some(MessageStreamTransport::Rfcomm) => self.ensure_rfcomm_connected(device).await,
            Some(MessageStreamTransport::L2cap) => self.ensure_l2cap_connected(device).await,
            None => {}
        }
        Ok(())
    }

    async fn ensure_rfcomm_connected(self: &Arc<Self>, device: Device) {
        if !self.connections.lock().await.begin(device.address()) {
            return;
        }
        let provider = Arc::clone(self);
        let address = device.address();
        self.tasks.spawn("fast-pair-rfcomm-connect", async move {
            let result = crate::task::catch("Fast Pair RFCOMM connection", async {
                tokio::time::timeout(
                    CONNECT_TIMEOUT,
                    device.connect_profile(&provider.uuids.message_stream),
                )
                .await
                .context("Fast Pair profile connection timed out")?
                .context("Fast Pair profile connection failed")
            })
            .await;
            if let Err(error) = result {
                tracing::warn!(%address, error = %error, error_chain = %format!("{error:#}"), "Fast Pair RFCOMM connection failed");
                provider.connections.lock().await.connection_failed(address);
            }
        });
    }

    async fn ensure_l2cap_connected(self: &Arc<Self>, device: Device) {
        if !self
            .connections
            .lock()
            .await
            .retry
            .l2cap_allowed(device.address())
        {
            return;
        }
        if !self.connections.lock().await.begin(device.address()) {
            return;
        }
        let provider = Arc::clone(self);
        let address = device.address();
        self.tasks.spawn("fast-pair-l2cap-connect", async move {
            let result = crate::task::catch("Fast Pair BLE L2CAP connection", async {
                tokio::time::timeout(CONNECT_TIMEOUT, provider.connect_l2cap(&device))
                    .await
                    .context("Fast Pair BLE L2CAP connection timed out")?
            })
            .await;
            match result {
                Ok(stream) if {
                    let mut state = provider.connections.lock().await;
                    state.mark_connected(address)
                } => {
                    tracing::debug!(%address, transport = "BLE L2CAP", "Fast Pair Message Stream connected");
                    Self::spawn_reader(Arc::clone(&provider), address, "BLE L2CAP", stream);
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(%address, error = %error, error_chain = %format!("{error:#}"), "Fast Pair BLE L2CAP connection failed");
                    provider.connections.lock().await.connection_failed(address);
                }
            }
        });
    }

    async fn connect_l2cap(&self, device: &Device) -> Result<L2capStream> {
        let psm = match self.read_message_stream_psm(device).await? {
            PsmAvailability::Ready(psm) => psm,
            PsmAvailability::Unknown => {
                self.connections
                    .lock()
                    .await
                    .retry
                    .psm_unknown(device.address());
                bail!("Fast Pair Message Stream PSM is not ready (bounded retry)")
            }
            PsmAvailability::Unavailable => {
                // Google BLE Device specification: do not use this component
                // again in this connection session; this is not a transient I/O error.
                self.connections
                    .lock()
                    .await
                    .retry
                    .psm_unavailable(device.address());
                bail!("Fast Pair Message Stream PSM is unavailable for this connection")
            }
        };
        let address_type = device
            .address_type()
            .await
            .context("read Fast Pair BLE address type")?;
        if address_type == AddressType::BrEdr {
            bail!("Fast Pair L2CAP requires a Bluetooth LE address");
        }
        let adapter = self
            .session
            .adapter(device.adapter_name())
            .context("open Fast Pair BLE adapter")?;
        let local_address = adapter
            .address()
            .await
            .context("read Fast Pair BLE adapter address")?;
        let socket = L2capSocket::<L2capStream>::new_stream()
            .context("create Fast Pair BLE L2CAP socket")?;
        socket
            .set_security(Security {
                level: SecurityLevel::Medium,
                key_size: 0,
            })
            .context("secure Fast Pair BLE L2CAP socket")?;
        socket
            .bind(L2capSocketAddr::new(
                local_address,
                AddressType::LePublic,
                0,
            ))
            .context("bind Fast Pair BLE L2CAP socket")?;
        socket
            .connect(L2capSocketAddr::new(device.address(), address_type, psm))
            .await
            .context("connect Fast Pair BLE L2CAP socket")
    }

    async fn read_message_stream_psm(&self, device: &Device) -> Result<PsmAvailability> {
        let service = find_service(device, self.uuids.service)
            .await?
            .context("Fast Pair GATT service is unavailable")?;
        let characteristic = find_characteristic(&service, self.uuids.message_stream_psm)
            .await?
            .context("Fast Pair Message Stream PSM characteristic is unavailable")?;
        let value = characteristic
            .read()
            .await
            .context("read Fast Pair Message Stream PSM")?;
        decode_message_stream_psm(&value)
    }

    async fn install_writer(&self, address: Address, writer: mpsc::Sender<Vec<u8>>) {
        self.connections
            .lock()
            .await
            .writers
            .insert(address, writer);
        self.runtime.write().await.entry(address).or_default();
        let _ = self.changes.send(());
    }

    async fn send_frame(&self, address: Address, frame: Vec<u8>) -> Result<()> {
        let writer = self
            .connections
            .lock()
            .await
            .writers
            .get(&address)
            .cloned()
            .context("Fast Pair Message Stream is not connected")?;
        tokio::time::timeout(Duration::from_secs(1), writer.send(frame))
            .await
            .context("Fast Pair Message Stream writer is stalled")?
            .context("Fast Pair Message Stream writer ended")
    }

    async fn update_runtime(&self, address: Address, update: impl FnOnce(&mut RuntimeReport)) {
        let changed = {
            let mut reports = self.runtime.write().await;
            let report = reports.entry(address).or_default();
            let previous = report.clone();
            update(report);
            *report != previous
        };
        if changed {
            let _ = self.changes.send(());
        }
    }

    async fn update_report(&self, address: Address, report: BatteryReport) {
        let changed = self.reports.write().await.insert(address, report) != Some(report);
        if changed {
            tracing::debug!(
                %address,
                left = ?report.left,
                right = ?report.right,
                case = ?report.case,
                "Fast Pair component battery updated"
            );
            let _ = self.changes.send(());
        }
    }

    async fn remove_report(&self, address: Address) {
        if self.reports.write().await.remove(&address).is_some() {
            let _ = self.changes.send(());
        }
    }

    async fn connection_ended(&self, address: Address) {
        self.connections.lock().await.ended(address);
        self.pending_commands.disconnect(address);
        let runtime_changed = self.runtime.write().await.remove(&address).is_some();
        self.remove_report(address).await;
        if runtime_changed {
            let _ = self.changes.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BATTERY_UPDATED_CODE, BatteryReport, DEVICE_INFORMATION_GROUP, FrameDecoder,
        MAX_FRAME_PAYLOAD, MessageStreamTransport, PsmAvailability, anc_mode_flag, crypt_block,
        decode_anc_state, decode_component, decode_message_stream_psm, derive_aes_key, message_mac,
        select_transport,
    };

    #[test]
    fn transport_selection_prefers_rfcomm_and_supports_ble_only_devices() {
        assert_eq!(
            select_transport(true, true, true),
            Some(MessageStreamTransport::Rfcomm)
        );
        assert_eq!(
            select_transport(true, false, true),
            Some(MessageStreamTransport::L2cap)
        );
        assert_eq!(
            select_transport(false, true, true),
            Some(MessageStreamTransport::L2cap)
        );
        assert_eq!(select_transport(true, false, false), None);
    }

    #[test]
    fn psm_characteristic_decodes_state_and_little_endian_value() {
        assert_eq!(
            decode_message_stream_psm(&[0x01, 0x80, 0x00]).unwrap(),
            PsmAvailability::Ready(0x80)
        );
        assert_eq!(
            decode_message_stream_psm(&[0x00, 0x00, 0x00]).unwrap(),
            PsmAvailability::Unknown
        );
        assert_eq!(
            decode_message_stream_psm(&[0x02, 0x00, 0x00]).unwrap(),
            PsmAvailability::Unavailable
        );
        assert!(decode_message_stream_psm(&[0x01, 0x7f, 0x00]).is_err());
        assert!(decode_message_stream_psm(&[0x01, 0x00]).is_err());
        assert!(decode_message_stream_psm(&[0x03, 0x80, 0x00]).is_err());
    }

    #[test]
    fn decoder_handles_fragmented_and_coalesced_frames() {
        let mut decoder = FrameDecoder::default();
        assert!(decoder.push(&[0x03, 0x03, 0x00]).unwrap().is_empty());
        let frames = decoder
            .push(&[
                0x03, 0x64, 0x5f, 0xce, 0x03, 0x09, 0x00, 0x03, b'1', b'.', b'0',
            ])
            .unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].group, DEVICE_INFORMATION_GROUP);
        assert_eq!(frames[0].code, BATTERY_UPDATED_CODE);
        assert_eq!(frames[0].payload, [0x64, 0x5f, 0xce]);
        assert_eq!(frames[1].payload, b"1.0");
    }

    #[test]
    fn decoder_rejects_unbounded_payloads() {
        let length = (MAX_FRAME_PAYLOAD + 1) as u16;
        let error = FrameDecoder::default()
            .push(&[0x03, 0x03, (length >> 8) as u8, length as u8])
            .unwrap_err();
        assert!(error.to_string().contains("too large"));
    }

    #[test]
    fn battery_values_mask_charging_and_decode_unknown_states() {
        let reports = BatteryReport::from_payload(&[0xe4, 0x32, 0xff])
            .unwrap()
            .model_batteries();
        assert_eq!(reports[0].charging, Some(true));
        assert_eq!(reports[1].charging, Some(false));
        assert_eq!(reports.len(), 2); // Unknown percentage is still not invented.
        assert_eq!(decode_component(0xe4).unwrap(), Some(100));
        assert_eq!(decode_component(0x7f).unwrap(), None);
        assert_eq!(decode_component(0xff).unwrap(), None);
        assert!(decode_component(0x7e).is_err());
    }

    #[test]
    fn component_reports_preserve_known_values_without_inferring_unknown_components() {
        for (payload, expected) in [
            (
                [0x64, 0x64, 0x4e],
                vec![("left", 100), ("right", 100), ("case", 78)],
            ),
            ([0x64, 0x64, 0xff], vec![("left", 100), ("right", 100)]),
        ] {
            let batteries = BatteryReport::from_payload(&payload)
                .unwrap()
                .model_batteries();
            assert_eq!(
                batteries
                    .iter()
                    .map(|battery| (battery.component.as_str(), battery.percentage))
                    .collect::<Vec<_>>(),
                expected,
                "{payload:?}"
            );
            assert!(
                batteries
                    .iter()
                    .all(|battery| battery.source == "google-fast-pair-message-stream")
            );
        }
        assert!(BatteryReport::from_payload(&[1, 2]).is_err());
    }

    #[test]
    fn anc_state_exposes_only_documented_modes() {
        let state = decode_anc_state(&[0x02, 0xe8, 0xa8, 0x20]).unwrap();
        assert_eq!(
            state.available_modes,
            ["transparent", "adaptive", "off", "noise-cancelling"]
        );
        assert_eq!(
            state.settable_modes,
            ["transparent", "off", "noise-cancelling"]
        );
        assert_eq!(state.active_mode.as_deref(), Some("off"));
        assert_eq!(anc_mode_flag("noise-cancelling").unwrap(), 0x08);
        assert!(decode_anc_state(&[2, 0xa8, 0xa8, 0xa0]).is_err());
        assert!(anc_mode_flag("wind").is_err());
    }

    #[test]
    fn ecdh_key_derivation_matches_the_google_fast_pair_test_vector() {
        let shared =
            hex::decode("9dade4f86ac3488bbac2ac34b5fe68a0ee5a6706f543d9061ad57889498ae6ba")
                .unwrap();
        assert_eq!(
            hex::encode(derive_aes_key(&shared)),
            "b07f1f17c236cbd33523c515f350ae57"
        );
    }

    #[test]
    fn aes_matches_the_google_fast_pair_test_vector() {
        let key = hex::decode("a0baf0bb951ff7b6cf5e3f4561c3321d").unwrap();
        let mut key_bytes = [0_u8; 16];
        key_bytes.copy_from_slice(&key);
        let input = hex::decode("f30f4e786c59a7bbf3873b5a49ba97ea").unwrap();
        let mut block = [0_u8; 16];
        block.copy_from_slice(&input);
        crypt_block(&key_bytes, &mut block, true);
        assert_eq!(hex::encode(block), "ac9a16f0953a3f223dd10cf536e09e9c");
        crypt_block(&key_bytes, &mut block, false);
        assert_eq!(block.as_slice(), input);
    }

    #[test]
    fn authenticated_messages_bind_both_nonces_and_payload() {
        let key = [0x04; 16];
        let session = [0x11; 8];
        let nonce = [0x22; 8];
        let mac = message_mac(&key, &session, &nonce, &[1]);
        // Independent Python hmac/SHA-256 reference for K, session || nonce || message.
        assert_eq!(hex::encode(mac), "13cfdae51949437e");
        assert_ne!(mac, message_mac(&key, &session, &nonce, &[0]));
        assert_ne!(mac, message_mac(&key, &[0x10; 8], &nonce, &[1]));
        assert_ne!(mac, message_mac(&key, &session, &[0x23; 8], &[1]));
    }
}
