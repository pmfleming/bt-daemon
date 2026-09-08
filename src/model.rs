use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::management::{DevicePolicy, ManagementPolicy, RuntimeState};

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
/// A point-in-time bt-api view; cached presentation data is distinguished from live state.
pub struct Snapshot {
    pub radio: RadioState,
    pub management: ManagementPolicy,
    pub adapters: Vec<Adapter>,
    pub devices: Vec<Device>,
}

impl From<&Snapshot> for RuntimeState {
    fn from(snapshot: &Snapshot) -> Self {
        Self::observed(
            snapshot
                .adapters
                .iter()
                .map(|adapter| (adapter.key.clone(), adapter.powered))
                .collect(),
            snapshot
                .devices
                .iter()
                .filter(|device| device.state.connected)
                .map(|device| device.key.clone())
                .collect(),
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
/// Adapter power combined with Linux soft/hard rfkill state.
pub struct RadioState {
    pub available: bool,
    pub operational: bool,
    pub powered: bool,
    pub adapter_count: usize,
    pub rfkill_present: bool,
    pub soft_blocked: bool,
    pub hard_blocked: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
/// BlueZ adapter properties addressed by a stable opaque key.
pub struct Adapter {
    pub key: String,
    pub name: String,
    pub alias: String,
    pub address: String,
    pub address_type: String,
    pub powered: bool,
    pub discovering: bool,
    pub discoverable: bool,
    pub pairable: bool,
    pub discoverable_timeout: u32,
    pub pairable_timeout: u32,
    pub modalias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
/// A device's identity, connection state, presentation, policy and permitted actions.
/// The component structures are flattened when serialized for bt-api v1.
pub struct Device {
    pub key: String,
    pub adapter_key: String,
    #[serde(flatten)]
    pub identity: DeviceIdentity,
    #[serde(flatten)]
    pub state: DeviceState,
    #[serde(flatten)]
    pub services: DeviceServices,
    #[serde(flatten)]
    pub presentation: DevicePresentation,
    pub policy: DevicePolicy,
    pub capabilities: DeviceCapabilities,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
/// Display and transport identity; clients should address operations using `Device::key`.
pub struct DeviceIdentity {
    pub name: String,
    pub alias: String,
    pub remote_name: Option<String>,
    pub device_type: String,
    pub address: String,
    pub address_type: String,
    pub icon: Option<String>,
    pub modalias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
/// Observed BlueZ connection and security properties; absent optional values are unknown.
pub struct DeviceState {
    pub paired: bool,
    pub bonded: Option<bool>,
    pub connected: bool,
    pub trusted: bool,
    pub blocked: bool,
    pub wake_allowed: Option<bool>,
    pub legacy_pairing: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
/// Advertised service UUIDs and labels, with an explicit service-resolution flag.
pub struct DeviceServices {
    pub services_resolved: bool,
    pub uuids: Vec<String>,
    pub services: Vec<Service>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
/// UI metadata with freshness flags so remembered batteries and RSSI are not presented as live.
pub struct DevicePresentation {
    pub battery: Vec<Battery>,
    pub battery_live: bool,
    pub battery_last_known: bool,
    pub components: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fast_pair: Option<FastPairFeatures>,
    pub rssi: Option<i16>,
    pub signal_strength: Option<u8>,
    pub signal_live: bool,
    pub present: bool,
    pub last_seen_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
/// An advertised UUID and its human-readable Bluetooth service label.
pub struct Service {
    pub uuid: String,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
/// A component battery reading, including provenance and optional charging information.
pub struct Battery {
    pub id: String,
    pub label: String,
    pub component: String,
    pub percentage: u8,
    /// None means the source (e.g. BlueZ Battery1) does not report charging.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub charging: Option<bool>,
    pub source: String,
    pub confidence: String,
}

impl Battery {
    pub(crate) fn bluez_aggregate(percentage: u8) -> Self {
        Self {
            id: "aggregate".into(),
            label: "Battery".into(),
            component: "main".into(),
            percentage,
            charging: None,
            source: "bluez".into(),
            confidence: "standard".into(),
        }
    }
}

pub(crate) fn presentation_type(icon: Option<&str>, battery: &[Battery]) -> &'static str {
    if battery.iter().any(is_earbud_component) {
        return "Earbuds";
    }
    let icon = icon.unwrap_or_default().to_ascii_lowercase();
    TYPE_RULES
        .iter()
        .find(|(terms, _)| terms.iter().any(|term| icon.contains(term)))
        .map_or("Bluetooth device", |(_, device_type)| device_type)
}

fn is_earbud_component(report: &Battery) -> bool {
    ["left", "right"]
        .iter()
        .any(|component| report.component.eq_ignore_ascii_case(component))
}

pub(crate) fn presentation_components(battery: &[Battery]) -> Vec<String> {
    let mut components = battery
        .iter()
        .filter_map(|report| {
            ["left", "right", "case"]
                .into_iter()
                .find(|component| report.component.eq_ignore_ascii_case(component))
                .map(str::to_string)
        })
        .collect::<Vec<_>>();
    components.sort_by_key(|component| presentation_component_order(component));
    components.dedup();
    components
}

pub(crate) fn presentation_component_order(component: &str) -> u8 {
    match component {
        "left" => 0,
        "right" => 1,
        "case" => 2,
        _ => 3,
    }
}

const TYPE_RULES: &[(&[&str], &str)] = &[
    (&["headset"], "Headset"),
    (&["headphone"], "Headphones"),
    (&["speaker"], "Speaker"),
    (&["audio"], "Audio device"),
    (&["keyboard"], "Keyboard"),
    (&["mouse"], "Mouse"),
    (&["game", "joystick"], "Game controller"),
    (&["tablet"], "Tablet"),
    (&["phone"], "Phone"),
    (&["computer", "laptop"], "Computer"),
    (&["printer"], "Printer"),
    (&["camera"], "Camera"),
    (&["watch", "wearable"], "Wearable"),
];

#[derive(Debug, Clone, PartialEq, Serialize)]
/// Observed Fast Pair capabilities and the prerequisites for authenticated controls.
pub struct FastPairFeatures {
    pub model_id: Option<String>,
    pub ble_address: Option<String>,
    pub authenticated_controls: bool,
    pub account_key_available: bool,
    pub provisioning_available: bool,
    pub provisioning_reason: Option<String>,
    pub trusted_model_name: Option<String>,
    pub multipoint: Option<FastPairMultipoint>,
    pub noise_control: Option<FastPairNoiseControl>,
    pub last_switch: Option<FastPairSwitchEvent>,
    pub audio_switch_seeker_supported: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
/// The most recent provider-reported audio switch and its observation time.
pub struct FastPairSwitchEvent {
    pub reason: String,
    pub target: String,
    pub target_name: Option<String>,
    pub observed_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
/// Multipoint support, configurability and current state reported by Audio Switch.
pub struct FastPairMultipoint {
    pub version: u16,
    pub supported: bool,
    pub configurable: bool,
    pub enabled: bool,
    pub audio_switch_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
/// Provider-advertised ANC modes; supported and settable modes may differ.
pub struct FastPairNoiseControl {
    pub version: u8,
    pub available_modes: Vec<String>,
    pub settable_modes: Vec<String>,
    pub active_mode: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
/// Actions currently offered to clients, with reasons for unavailable operations.
/// These are UI hints: backends revalidate state before performing an action.
pub struct DeviceCapabilities {
    pub can_pair: bool,
    pub can_connect: bool,
    pub can_disconnect: bool,
    pub can_remove: bool,
    pub can_trust: bool,
    pub can_block: bool,
    pub can_wake: bool,
    pub can_rename: bool,
    pub can_send_file: bool,
    pub can_provision_fast_pair: bool,
    pub can_set_multipoint: bool,
    pub can_set_noise_control: bool,
    pub unsupported_reasons: HashMap<String, String>,
}
