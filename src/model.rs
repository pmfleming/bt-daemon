use std::collections::HashMap;

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
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
    pub rssi: Option<i16>,
    pub signal_strength: Option<u8>,
    pub present: bool,
    pub last_seen_ms: Option<u64>,
    pub capabilities: DeviceCapabilities,
}

#[derive(Debug, Clone, Serialize)]
pub struct Service {
    pub uuid: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Battery {
    pub id: String,
    pub label: String,
    pub component: String,
    pub percentage: u8,
    pub source: String,
    pub confidence: String,
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
    pub unsupported_reasons: HashMap<String, String>,
}
