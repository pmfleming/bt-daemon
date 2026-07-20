use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use bluer::{
    Address, Device, Session,
    rfcomm::{Profile, ProfileHandle, ReqError, Role, Stream},
};
use futures::StreamExt;
use tokio::{
    io::AsyncReadExt,
    sync::{Mutex, RwLock, broadcast},
};
use uuid::Uuid;

use crate::model::Battery;

pub const MESSAGE_STREAM_UUID: &str = "df21fe2c-2515-4fdb-8886-f12c4d67927c";
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
    uuid: Uuid,
}

impl FastPairBatteryProvider {
    pub async fn start(session: Session, changes: broadcast::Sender<()>) -> Result<Arc<Self>> {
        let uuid = Uuid::parse_str(MESSAGE_STREAM_UUID).expect("fixed Fast Pair UUID is valid");
        let profile = Profile {
            uuid,
            name: Some("Shelllist Fast Pair battery provider".to_string()),
            role: Some(Role::Client),
            require_authentication: Some(true),
            require_authorization: Some(false),
            auto_connect: Some(false),
            ..Default::default()
        };
        let requests = session
            .register_profile(profile)
            .await
            .context("register Fast Pair Message Stream profile")?;
        let provider = Arc::new(Self {
            session,
            reports: RwLock::new(HashMap::new()),
            connections: Mutex::new(ConnectionState::default()),
            changes,
            uuid,
        });
        Self::spawn_request_handler(Arc::clone(&provider), requests);
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
        tokio::spawn(async move {
            while let Some(request) = requests.next().await {
                let address = request.device();
                let duplicate = {
                    let mut state = provider.connections.lock().await;
                    if state.links.get(&address) == Some(&LinkState::Connected) {
                        true
                    } else {
                        state.links.insert(address, LinkState::Connected);
                        state.retry_after.remove(&address);
                        false
                    }
                };
                if duplicate {
                    request.reject(ReqError::Rejected);
                    continue;
                }
                match request.accept() {
                    Ok(stream) => {
                        tracing::debug!(%address, "Fast Pair Message Stream connected");
                        Self::spawn_reader(Arc::clone(&provider), address, stream);
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

    fn spawn_reader(provider: Arc<Self>, address: Address, mut stream: Stream) {
        tokio::spawn(async move {
            let result = async {
                let mut decoder = FrameDecoder::default();
                let mut bytes = [0_u8; 512];
                loop {
                    let count = stream
                        .read(&mut bytes)
                        .await
                        .context("read Fast Pair Message Stream")?;
                    if count == 0 {
                        break;
                    }
                    for frame in decoder.push(&bytes[..count])? {
                        if frame.group == DEVICE_INFORMATION_GROUP
                            && frame.code == BATTERY_UPDATED_CODE
                        {
                            let report = BatteryReport::from_payload(&frame.payload)?;
                            provider.update_report(address, report).await;
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(%address, error = %error, "Fast Pair battery stream ended with an error");
            } else {
                tracing::debug!(%address, "Fast Pair battery stream closed");
            }
            provider.connection_ended(address).await;
        });
    }

    fn spawn_reconciler(provider: Arc<Self>) {
        let mut changes = provider.changes.subscribe();
        tokio::spawn(async move {
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
                tracing::debug!(%error, "could not list adapters for Fast Pair battery provider");
                return;
            }
        };
        for adapter_name in adapter_names {
            let Ok(adapter) = self.session.adapter(&adapter_name) else {
                continue;
            };
            let Ok(addresses) = adapter.device_addresses().await else {
                continue;
            };
            for address in addresses {
                let Ok(device) = adapter.device(address) else {
                    continue;
                };
                let connected = device.is_connected().await.unwrap_or(false);
                if !connected {
                    self.remove_report(address).await;
                    continue;
                }
                let supports_fast_pair = device
                    .uuids()
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|uuids| uuids.contains(&self.uuid));
                if supports_fast_pair {
                    self.ensure_connected(device).await;
                }
            }
        }
    }

    async fn ensure_connected(self: &Arc<Self>, device: Device) {
        let address = device.address();
        let should_connect = {
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
        };
        if !should_connect {
            return;
        }
        let provider = Arc::clone(self);
        tokio::spawn(async move {
            let result =
                tokio::time::timeout(CONNECT_TIMEOUT, device.connect_profile(&provider.uuid)).await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::debug!(%address, %error, "Fast Pair profile connection failed");
                    provider.connection_failed(address).await;
                }
                Err(_) => {
                    tracing::debug!(%address, "Fast Pair profile connection timed out");
                    provider.connection_failed(address).await;
                }
            }
        });
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
        FrameDecoder, MAX_FRAME_PAYLOAD, decode_component,
    };

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
    fn live_wf_1000xm5_frame_maps_to_component_model() {
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
        let report = BatteryReport::from_payload(&[0x32, 0x7f, 0xff]).unwrap();
        let batteries = report.model_batteries();
        assert_eq!(batteries.len(), 1);
        assert_eq!(batteries[0].component, "left");
        assert_eq!(batteries[0].percentage, 50);
        assert!(BatteryReport::from_payload(&[1, 2]).is_err());
    }
}
