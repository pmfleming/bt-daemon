use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::management::{DevicePolicy, ManagementPolicy};

#[derive(Debug, Clone, Default, Serialize)]
pub struct Snapshot {
    pub radio: RadioState,
    pub management: ManagementPolicy,
    pub adapters: Vec<Adapter>,
    pub devices: Vec<Device>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RadioState {
    pub available: bool,
    pub operational: bool,
    pub powered: bool,
    pub adapter_count: usize,
    pub rfkill_present: bool,
    pub soft_blocked: bool,
    pub hard_blocked: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
pub struct DeviceState {
    pub paired: bool,
    pub bonded: Option<bool>,
    pub connected: bool,
    pub trusted: bool,
    pub blocked: bool,
    pub wake_allowed: Option<bool>,
    pub legacy_pairing: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeviceServices {
    pub services_resolved: bool,
    pub uuids: Vec<String>,
    pub services: Vec<Service>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DevicePresentation {
    pub battery: Vec<Battery>,
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

#[derive(Debug, Clone, Serialize)]
pub struct Service {
    pub uuid: String,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Battery {
    pub id: String,
    pub label: String,
    pub component: String,
    pub percentage: u8,
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

#[derive(Debug, Clone, Serialize)]
pub struct FastPairFeatures {
    pub model_id: Option<String>,
    pub ble_address: Option<String>,
    pub authenticated_controls: bool,
    pub multipoint: Option<FastPairMultipoint>,
    pub noise_control: Option<FastPairNoiseControl>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct FastPairMultipoint {
    pub version: u16,
    pub supported: bool,
    pub configurable: bool,
    pub enabled: bool,
    pub audio_switch_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FastPairNoiseControl {
    pub version: u8,
    pub available_modes: Vec<String>,
    pub settable_modes: Vec<String>,
    pub active_mode: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
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
