use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize)]
pub struct Snapshot {
    pub adapters: Vec<Adapter>,
    pub devices: Vec<Device>,
}

#[derive(Debug, Clone, Serialize)]
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
    pub name: String,
    pub alias: String,
    pub address: String,
    pub address_type: String,
    pub icon: Option<String>,
    pub paired: bool,
    pub bonded: Option<bool>,
    pub connected: bool,
    pub services_resolved: bool,
    pub trusted: bool,
    pub blocked: bool,
    pub wake_allowed: Option<bool>,
    pub legacy_pairing: bool,
    pub modalias: Option<String>,
    pub uuids: Vec<String>,
    pub services: Vec<Service>,
    pub battery: Vec<Battery>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fast_pair: Option<FastPairFeatures>,
    pub rssi: Option<i16>,
    pub signal_strength: Option<u8>,
    pub signal_live: bool,
    pub present: bool,
    pub last_seen_ms: Option<u64>,
    pub capabilities: DeviceCapabilities,
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
