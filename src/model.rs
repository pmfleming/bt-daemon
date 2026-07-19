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
    pub powered: bool,
    pub discovering: bool,
    pub pairable: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Device {
    pub key: String,
    pub adapter_key: String,
    pub name: String,
    pub icon: Option<String>,
    pub paired: bool,
    pub connected: bool,
    pub trusted: bool,
    pub blocked: bool,
    pub wake_allowed: Option<bool>,
    pub battery: Vec<Battery>,
    pub signal_strength: Option<u8>,
    pub present: bool,
    pub capabilities: DeviceCapabilities,
}

#[derive(Debug, Clone, Serialize)]
pub struct Battery {
    pub component: String,
    pub percentage: u8,
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
}
