use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use bluer::{
    Adapter, Address, AddressType, Device, Session,
    l2cap::{
        PSM_LE_DYN_START, PSM_LE_MAX, Security, SecurityLevel, Socket as L2capSocket,
        SocketAddr as L2capSocketAddr, Stream as L2capStream,
    },
    rfcomm::{Profile, ProfileHandle, ReqError, Role},
};
use futures::StreamExt;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::{Mutex, RwLock, broadcast, mpsc},
};
use uuid::Uuid;

use crate::model::Battery;

pub const MESSAGE_STREAM_UUID: &str = "df21fe2c-2515-4fdb-8886-f12c4d67927c";
pub const FAST_PAIR_SERVICE_UUID: &str = "0000fe2c-0000-1000-8000-00805f9b34fb";
pub const MESSAGE_STREAM_PSM_UUID: &str = "fe2c1239-8366-4814-8eb0-01de32100bea";
const DEVICE_INFORMATION_GROUP: u8 = 0x03;
const BATTERY_UPDATED_CODE: u8 = 0x03;
const MAX_FRAME_PAYLOAD: usize = 4096;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(12);
const RETRY_DELAY: Duration = Duration::from_secs(15);
const RECONCILE_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ComponentReading {
    percentage: u8,
    charging: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BatteryReport {
    left: Option<ComponentReading>,
    right: Option<ComponentReading>,
    case: Option<ComponentReading>,
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
        })
    }

    fn model_batteries(self) -> Vec<Battery> {
        [
            ("fast-pair-left", "Left", "left", self.left),
            ("fast-pair-right", "Right", "right", self.right),
            ("fast-pair-case", "Case", "case", self.case),
        ]
        .into_iter()
        .filter_map(|(id, label, component, reading)| {
            reading.map(|reading| Battery {
                id: id.to_string(),
                label: label.to_string(),
                component: component.to_string(),
                percentage: reading.percentage,
                source: "google-fast-pair-message-stream".to_string(),
                confidence: "high".to_string(),
            })
        })
        .collect()
    }
}

fn decode_component(value: u8) -> Result<Option<ComponentReading>> {
    let percentage = value & 0x7f;
    if percentage == 0x7f {
        return Ok(None);
    }
    if percentage > 100 {
        bail!("invalid Fast Pair battery percentage {percentage}");
    }
    Ok(Some(ComponentReading {
        percentage,
        charging: value & 0x80 != 0,
    }))
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
    retry_after: HashMap<Address, Instant>,
}

pub struct FastPairBatteryProvider {
    session: Session,
    reports: RwLock<HashMap<Address, BatteryReport>>,
    connections: Mutex<ConnectionState>,
    changes: broadcast::Sender<()>,
    message_stream_uuid: Uuid,
    fast_pair_service_uuid: Uuid,
    message_stream_psm_uuid: Uuid,
    rfcomm_available: bool,
}

async fn run_message_stream<R>(
    provider: Arc<FastPairBatteryProvider>,
    address: Address,
    stream: R,
) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let (chunks_tx, chunks_rx) = mpsc::channel(16);
    let (frames_tx, frames_rx) = mpsc::channel(32);
    tokio::try_join!(
        read_transport(stream, chunks_tx),
        decode_chunks(chunks_rx, frames_tx),
        apply_frames(provider, address, frames_rx),
    )?;
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
        if frame.group == DEVICE_INFORMATION_GROUP && frame.code == BATTERY_UPDATED_CODE {
            let report = BatteryReport::from_payload(&frame.payload)?;
            provider.update_report(address, report).await;
        } else {
            tracing::trace!(%address, group = frame.group, code = frame.code, "ignored Fast Pair frame");
        }
    }
    Ok(())
}

impl FastPairBatteryProvider {
    pub async fn start(session: Session, changes: broadcast::Sender<()>) -> Result<Arc<Self>> {
        let message_stream_uuid =
            Uuid::parse_str(MESSAGE_STREAM_UUID).expect("fixed Fast Pair UUID is valid");
        let fast_pair_service_uuid =
            Uuid::parse_str(FAST_PAIR_SERVICE_UUID).expect("fixed Fast Pair service UUID is valid");
        let message_stream_psm_uuid = Uuid::parse_str(MESSAGE_STREAM_PSM_UUID)
            .expect("fixed Fast Pair PSM characteristic UUID is valid");
        let profile = Profile {
            uuid: message_stream_uuid,
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
            reports: RwLock::new(HashMap::new()),
            connections: Mutex::new(ConnectionState::default()),
            changes,
            message_stream_uuid,
            fast_pair_service_uuid,
            message_stream_psm_uuid,
            rfcomm_available: requests.is_some(),
        });
        if let Some(requests) = requests {
            Self::spawn_request_handler(Arc::clone(&provider), requests);
        }
        Self::spawn_reconciler(Arc::clone(&provider));
        Ok(provider)
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

    fn spawn_request_handler(provider: Arc<Self>, mut requests: ProfileHandle) {
        crate::task::spawn("fast-pair-rfcomm-requests", async move {
            while let Some(request) = requests.next().await {
                let address = request.device();
                if !provider.mark_connected(address).await {
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
        R: AsyncRead + Unpin + Send + 'static,
    {
        crate::task::spawn("fast-pair-reader", async move {
            tracing::info!(%address, %transport, "Fast Pair battery stream started");
            let result = crate::task::catch(
                "Fast Pair message stream",
                run_message_stream(Arc::clone(&provider), address, stream),
            )
            .await
            .and_then(|result| result);
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
        crate::task::spawn("fast-pair-reconciler", async move {
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
            uuids.contains(&self.message_stream_uuid),
            uuids.contains(&self.fast_pair_service_uuid),
        ) {
            Some(MessageStreamTransport::Rfcomm) => self.ensure_rfcomm_connected(device).await,
            Some(MessageStreamTransport::L2cap) => self.ensure_l2cap_connected(device).await,
            None => {}
        }
        Ok(())
    }

    async fn ensure_rfcomm_connected(self: &Arc<Self>, device: Device) {
        let address = device.address();
        if !self.begin_connection(address).await {
            return;
        }
        let provider = Arc::clone(self);
        crate::task::spawn("fast-pair-rfcomm-connect", async move {
            let result = crate::task::catch("Fast Pair RFCOMM connection", async {
                tokio::time::timeout(
                    CONNECT_TIMEOUT,
                    device.connect_profile(&provider.message_stream_uuid),
                )
                .await
                .context("Fast Pair profile connection timed out")?
                .context("Fast Pair profile connection failed")
            })
            .await
            .and_then(|result| result);
            if let Err(error) = result {
                tracing::warn!(%address, error = %error, error_chain = %format!("{error:#}"), "Fast Pair RFCOMM connection failed");
                provider.connection_failed(address).await;
            }
        });
    }

    async fn ensure_l2cap_connected(self: &Arc<Self>, device: Device) {
        let address = device.address();
        if !self.begin_connection(address).await {
            return;
        }
        let provider = Arc::clone(self);
        crate::task::spawn("fast-pair-l2cap-connect", async move {
            let result = crate::task::catch("Fast Pair BLE L2CAP connection", async {
                tokio::time::timeout(CONNECT_TIMEOUT, provider.connect_l2cap(&device))
                    .await
                    .context("Fast Pair BLE L2CAP connection timed out")?
            })
            .await
            .and_then(|result| result);
            match result {
                Ok(stream) if provider.mark_connected(address).await => {
                    tracing::debug!(%address, transport = "BLE L2CAP", "Fast Pair Message Stream connected");
                    Self::spawn_reader(Arc::clone(&provider), address, "BLE L2CAP", stream);
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(%address, error = %error, error_chain = %format!("{error:#}"), "Fast Pair BLE L2CAP connection failed");
                    provider.connection_failed(address).await;
                }
            }
        });
    }

    async fn connect_l2cap(&self, device: &Device) -> Result<L2capStream> {
        let psm = match self.read_message_stream_psm(device).await? {
            PsmAvailability::Ready(psm) => psm,
            PsmAvailability::Unknown => bail!("Fast Pair Message Stream PSM is not ready"),
            PsmAvailability::Unavailable => {
                bail!("Fast Pair Message Stream PSM is unavailable")
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
        for service in device
            .services()
            .await
            .context("list Fast Pair GATT services")?
        {
            if service
                .uuid()
                .await
                .context("read Fast Pair service UUID")?
                != self.fast_pair_service_uuid
            {
                continue;
            }
            for characteristic in service
                .characteristics()
                .await
                .context("list Fast Pair GATT characteristics")?
            {
                if characteristic
                    .uuid()
                    .await
                    .context("read Fast Pair characteristic UUID")?
                    == self.message_stream_psm_uuid
                {
                    let value = characteristic
                        .read()
                        .await
                        .context("read Fast Pair Message Stream PSM")?;
                    return decode_message_stream_psm(&value);
                }
            }
        }
        bail!("Fast Pair Message Stream PSM characteristic is unavailable")
    }

    async fn begin_connection(&self, address: Address) -> bool {
        let mut state = self.connections.lock().await;
        if state.links.contains_key(&address)
            || state
                .retry_after
                .get(&address)
                .is_some_and(|retry| *retry > Instant::now())
        {
            false
        } else {
            state.links.insert(address, LinkState::Connecting);
            true
        }
    }

    async fn mark_connected(&self, address: Address) -> bool {
        let mut state = self.connections.lock().await;
        if state.links.get(&address) == Some(&LinkState::Connected) {
            false
        } else {
            state.links.insert(address, LinkState::Connected);
            state.retry_after.remove(&address);
            true
        }
    }

    async fn update_report(&self, address: Address, report: BatteryReport) {
        let changed = self.reports.write().await.insert(address, report) != Some(report);
        if changed {
            tracing::debug!(
                %address,
                left = ?report.left.map(|value| value.percentage),
                right = ?report.right.map(|value| value.percentage),
                case = ?report.case.map(|value| value.percentage),
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

    async fn connection_failed(&self, address: Address) {
        let mut state = self.connections.lock().await;
        if state.links.get(&address) == Some(&LinkState::Connecting) {
            state.links.remove(&address);
            state
                .retry_after
                .insert(address, Instant::now() + RETRY_DELAY);
        }
    }

    async fn connection_ended(&self, address: Address) {
        {
            let mut state = self.connections.lock().await;
            state.links.remove(&address);
            state
                .retry_after
                .insert(address, Instant::now() + RETRY_DELAY);
        }
        self.remove_report(address).await;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BATTERY_UPDATED_CODE, BatteryReport, ComponentReading, DEVICE_INFORMATION_GROUP,
        FrameDecoder, MAX_FRAME_PAYLOAD, MessageStreamTransport, PsmAvailability, decode_component,
        decode_message_stream_psm, select_transport,
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
    fn battery_values_decode_charging_and_unknown_states() {
        assert_eq!(
            decode_component(0xe4).unwrap(),
            Some(ComponentReading {
                percentage: 100,
                charging: true
            })
        );
        assert_eq!(decode_component(0x7f).unwrap(), None);
        assert_eq!(decode_component(0xff).unwrap(), None);
        assert!(decode_component(0x7e).is_err());
    }

    #[test]
    fn three_component_update_maps_to_component_model() {
        let report = BatteryReport::from_payload(&[0x64, 0x64, 0x4e]).unwrap();
        let batteries = report.model_batteries();
        assert_eq!(
            batteries
                .iter()
                .map(|battery| (battery.component.as_str(), battery.percentage))
                .collect::<Vec<_>>(),
            [("left", 100), ("right", 100), ("case", 78)]
        );
        assert!(
            batteries
                .iter()
                .all(|battery| battery.source == "google-fast-pair-message-stream")
        );
    }

    #[test]
    fn unknown_components_are_not_inferred() {
        let report = BatteryReport::from_payload(&[0x64, 0x64, 0xff]).unwrap();
        let batteries = report.model_batteries();
        assert_eq!(
            batteries
                .iter()
                .map(|battery| (battery.component.as_str(), battery.percentage))
                .collect::<Vec<_>>(),
            [("left", 100), ("right", 100)]
        );
        assert!(BatteryReport::from_payload(&[1, 2]).is_err());
    }
}
