use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Mutex, MutexGuard},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{model::Snapshot, state};

const POLICY_VERSION: u8 = 1;
const RUNTIME_VERSION: u8 = 1;
const DEVICE_POLICY_VERSION: u8 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagementPolicy {
    #[serde(default = "default_policy_version")]
    pub version: u8,
    #[serde(default = "default_launch_state")]
    pub launch_state: String,
    #[serde(default = "default_true")]
    pub reconnect_on_resume: bool,
    #[serde(default = "default_true")]
    pub trust_after_pair: bool,
    #[serde(default)]
    pub preferred_adapter_key: String,
    #[serde(default)]
    pub show_blocked_devices: bool,
    #[serde(default)]
    pub show_recent_devices: bool,
}

impl Default for ManagementPolicy {
    fn default() -> Self {
        Self {
            version: POLICY_VERSION,
            launch_state: default_launch_state(),
            reconnect_on_resume: true,
            trust_after_pair: true,
            preferred_adapter_key: String::new(),
            show_blocked_devices: false,
            show_recent_devices: false,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DevicePolicy {
    pub reconnect_on_resume: bool,
    pub trust_after_pair: bool,
    pub power_on_connect: bool,
    pub wait_for_services: bool,
    pub audio_route_on_connect: String,
    pub preferred_audio_profile_key: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct DevicePolicyOverrides {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reconnect_on_resume: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trust_after_pair: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    power_on_connect: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    wait_for_services: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    audio_route_on_connect: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    preferred_audio_profile_key: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DevicePolicyFile {
    #[serde(default = "default_device_policy_version")]
    version: u8,
    #[serde(default)]
    devices: HashMap<String, DevicePolicyOverrides>,
}

impl Default for DevicePolicyFile {
    fn default() -> Self {
        Self {
            version: DEVICE_POLICY_VERSION,
            devices: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeState {
    #[serde(default = "default_runtime_version")]
    version: u8,
    #[serde(default)]
    adapter_power: HashMap<String, bool>,
    #[serde(default)]
    connected_device_keys: Vec<String>,
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self {
            version: RUNTIME_VERSION,
            adapter_power: HashMap::new(),
            connected_device_keys: Vec::new(),
        }
    }
}

pub struct ManagementStore {
    policy_path: Option<PathBuf>,
    runtime_path: Option<PathBuf>,
    device_policy_path: Option<PathBuf>,
    policy: Mutex<ManagementPolicy>,
    runtime: Mutex<RuntimeState>,
    device_policies: Mutex<DevicePolicyFile>,
}

impl ManagementStore {
    pub fn load_default() -> Result<Self> {
        let directory = state::directory()?;
        Self::load(
            Some(directory.join("management-policy.json")),
            Some(directory.join("runtime-state.json")),
            Some(directory.join("device-policy.json")),
        )
    }

    #[cfg(test)]
    pub fn in_memory() -> Self {
        Self {
            policy_path: None,
            runtime_path: None,
            device_policy_path: None,
            policy: Mutex::new(ManagementPolicy::default()),
            runtime: Mutex::new(RuntimeState::default()),
            device_policies: Mutex::new(DevicePolicyFile::default()),
        }
    }

    fn load(
        policy_path: Option<PathBuf>,
        runtime_path: Option<PathBuf>,
        device_policy_path: Option<PathBuf>,
    ) -> Result<Self> {
        let policy: ManagementPolicy =
            load_state(policy_path.as_deref(), "Bluetooth management policy")?;
        if policy.version != POLICY_VERSION {
            bail!(
                "unsupported Bluetooth management policy version {}",
                policy.version
            );
        }
        validate_launch_state(&policy.launch_state)?;
        let runtime: RuntimeState = load_state(runtime_path.as_deref(), "Bluetooth runtime state")?;
        if runtime.version != RUNTIME_VERSION {
            bail!(
                "unsupported Bluetooth runtime state version {}",
                runtime.version
            );
        }
        let device_policies: DevicePolicyFile =
            load_state(device_policy_path.as_deref(), "Bluetooth per-device policy")?;
        if device_policies.version != DEVICE_POLICY_VERSION {
            bail!(
                "unsupported Bluetooth per-device policy version {}",
                device_policies.version
            );
        }
        Ok(Self {
            policy_path,
            runtime_path,
            device_policy_path,
            policy: Mutex::new(policy),
            runtime: Mutex::new(runtime),
            device_policies: Mutex::new(device_policies),
        })
    }

    pub fn policy(&self) -> ManagementPolicy {
        self.policy_lock().clone()
    }

    pub fn update(&self, values: &Value) -> Result<ManagementPolicy> {
        let object = values
            .as_object()
            .context("management update must be an object")?;
        validate_setting_names(object)?;
        let mut policy = self.policy_lock();
        let mut updated = policy.clone();
        updated.apply(object)?;
        self.persist_policy(&updated)?;
        *policy = updated.clone();
        Ok(updated)
    }

    pub fn device_policy(&self, device_key: &str) -> DevicePolicy {
        let global = self.policy();
        let overrides = self
            .device_policy_lock()
            .devices
            .get(device_key)
            .cloned()
            .unwrap_or_default();
        overrides.effective(&global)
    }

    pub fn update_device_policy(&self, device_key: &str, values: &Value) -> Result<DevicePolicy> {
        let object = values
            .as_object()
            .context("device policy update must be an object")?;
        validate_device_setting_names(object)?;
        let mut policies = self.device_policy_lock();
        let mut updated = policies
            .devices
            .get(device_key)
            .cloned()
            .unwrap_or_default();
        updated.apply(object)?;
        if updated.is_empty() {
            policies.devices.remove(device_key);
        } else {
            policies.devices.insert(device_key.to_string(), updated);
        }
        self.persist_device_policies(&policies)?;
        drop(policies);
        Ok(self.device_policy(device_key))
    }

    pub fn forget_device_policy(&self, device_key: &str) {
        let mut policies = self.device_policy_lock();
        if policies.devices.remove(device_key).is_some()
            && let Err(error) = self.persist_device_policies(&policies)
        {
            tracing::warn!(%error, %device_key, "could not remove Bluetooth device policy");
        }
    }

    pub fn remember_snapshot(&self, snapshot: &Snapshot) {
        let runtime = RuntimeState {
            version: RUNTIME_VERSION,
            adapter_power: snapshot
                .adapters
                .iter()
                .map(|adapter| (adapter.key.clone(), adapter.powered))
                .collect(),
            connected_device_keys: snapshot
                .devices
                .iter()
                .filter(|device| device.state.connected)
                .map(|device| device.key.clone())
                .collect(),
        };
        let mut current = self.runtime_lock();
        if let Some(path) = &self.runtime_path
            && let Err(error) = state::write_json(path, &runtime, "Bluetooth runtime state")
        {
            tracing::warn!(%error, "could not persist Bluetooth runtime state");
            return;
        }
        *current = runtime;
    }

    pub fn runtime(&self) -> RuntimeState {
        self.runtime_lock().clone()
    }

    fn persist_device_policies(&self, policies: &DevicePolicyFile) -> Result<()> {
        if let Some(path) = &self.device_policy_path {
            state::write_json(path, policies, "Bluetooth per-device policy")?;
        }
        Ok(())
    }

    fn persist_policy(&self, policy: &ManagementPolicy) -> Result<()> {
        if let Some(path) = &self.policy_path {
            state::write_json(path, policy, "Bluetooth management policy")?;
        }
        Ok(())
    }

    fn policy_lock(&self) -> MutexGuard<'_, ManagementPolicy> {
        self.policy
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn runtime_lock(&self) -> MutexGuard<'_, RuntimeState> {
        self.runtime
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn device_policy_lock(&self) -> MutexGuard<'_, DevicePolicyFile> {
        self.device_policies
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

impl ManagementPolicy {
    fn apply(&mut self, object: &serde_json::Map<String, Value>) -> Result<()> {
        if let Some(value) = object.get("launch_state") {
            let value = value.as_str().context("launch_state must be a string")?;
            validate_launch_state(value)?;
            self.launch_state = value.into();
        }
        for (name, target) in [
            ("reconnect_on_resume", &mut self.reconnect_on_resume),
            ("trust_after_pair", &mut self.trust_after_pair),
            ("show_blocked_devices", &mut self.show_blocked_devices),
            ("show_recent_devices", &mut self.show_recent_devices),
        ] {
            update_bool(object, name, target)?;
        }
        if let Some(value) = object.get("preferred_adapter_key") {
            self.preferred_adapter_key = value
                .as_str()
                .context("preferred_adapter_key must be a string")?
                .into();
        }
        Ok(())
    }
}

impl DevicePolicyOverrides {
    fn effective(&self, global: &ManagementPolicy) -> DevicePolicy {
        DevicePolicy {
            reconnect_on_resume: self
                .reconnect_on_resume
                .unwrap_or(global.reconnect_on_resume),
            trust_after_pair: self.trust_after_pair.unwrap_or(global.trust_after_pair),
            power_on_connect: self.power_on_connect.unwrap_or(true),
            wait_for_services: self.wait_for_services.unwrap_or(true),
            audio_route_on_connect: self
                .audio_route_on_connect
                .clone()
                .unwrap_or_else(|| "keep".into()),
            preferred_audio_profile_key: self.preferred_audio_profile_key.clone(),
        }
    }

    fn apply(&mut self, object: &serde_json::Map<String, Value>) -> Result<()> {
        for (name, target) in [
            ("reconnect_on_resume", &mut self.reconnect_on_resume),
            ("trust_after_pair", &mut self.trust_after_pair),
            ("power_on_connect", &mut self.power_on_connect),
            ("wait_for_services", &mut self.wait_for_services),
        ] {
            update_optional_bool(object, name, target)?;
        }
        if let Some(value) = object.get("audio_route_on_connect") {
            match value {
                Value::Null => self.audio_route_on_connect = None,
                Value::String(value) if ["keep", "switch"].contains(&value.as_str()) => {
                    self.audio_route_on_connect = Some(value.clone());
                }
                _ => bail!("audio_route_on_connect must be keep, switch, or null"),
            }
        }
        if let Some(value) = object.get("preferred_audio_profile_key") {
            match value {
                Value::Null => self.preferred_audio_profile_key = None,
                Value::String(value) if !value.is_empty() => {
                    self.preferred_audio_profile_key = Some(value.clone());
                }
                _ => bail!("preferred_audio_profile_key must be a non-empty string or null"),
            }
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.reconnect_on_resume.is_none()
            && self.trust_after_pair.is_none()
            && self.power_on_connect.is_none()
            && self.wait_for_services.is_none()
            && self.audio_route_on_connect.is_none()
            && self.preferred_audio_profile_key.is_none()
    }
}

impl RuntimeState {
    pub fn adapter_power(&self) -> &HashMap<String, bool> {
        &self.adapter_power
    }
    pub fn connected_device_keys(&self) -> &[String] {
        &self.connected_device_keys
    }
}

fn load_state<T: DeserializeOwned + Default>(
    path: Option<&std::path::Path>,
    description: &str,
) -> Result<T> {
    path.map(|path| state::read_json(path, description))
        .transpose()
        .map(|value| value.flatten().unwrap_or_default())
}

fn validate_setting_names(object: &serde_json::Map<String, Value>) -> Result<()> {
    const ALLOWED: &[&str] = &[
        "launch_state",
        "reconnect_on_resume",
        "trust_after_pair",
        "preferred_adapter_key",
        "show_blocked_devices",
        "show_recent_devices",
    ];
    if let Some(name) = object.keys().find(|name| !ALLOWED.contains(&name.as_str())) {
        bail!("unsupported management setting '{name}'");
    }
    Ok(())
}

fn validate_device_setting_names(object: &serde_json::Map<String, Value>) -> Result<()> {
    const ALLOWED: &[&str] = &[
        "reconnect_on_resume",
        "trust_after_pair",
        "power_on_connect",
        "wait_for_services",
        "audio_route_on_connect",
        "preferred_audio_profile_key",
    ];
    if let Some(name) = object.keys().find(|name| !ALLOWED.contains(&name.as_str())) {
        bail!("unsupported per-device management setting '{name}'");
    }
    Ok(())
}

fn update_optional_bool(
    object: &serde_json::Map<String, Value>,
    name: &str,
    target: &mut Option<bool>,
) -> Result<()> {
    if let Some(value) = object.get(name) {
        match value {
            Value::Null => *target = None,
            Value::Bool(value) => *target = Some(*value),
            _ => bail!("{name} must be a boolean or null"),
        }
    }
    Ok(())
}

fn update_bool(
    object: &serde_json::Map<String, Value>,
    name: &str,
    target: &mut bool,
) -> Result<()> {
    if let Some(value) = object.get(name) {
        *target = value
            .as_bool()
            .with_context(|| format!("{name} must be a boolean"))?;
    }
    Ok(())
}

fn validate_launch_state(value: &str) -> Result<()> {
    if ["remember", "enable", "disable"].contains(&value) {
        Ok(())
    } else {
        bail!("launch_state must be remember, enable, or disable")
    }
}

const fn default_policy_version() -> u8 {
    POLICY_VERSION
}
const fn default_runtime_version() -> u8 {
    RUNTIME_VERSION
}
const fn default_device_policy_version() -> u8 {
    DEVICE_POLICY_VERSION
}
fn default_launch_state() -> String {
    "remember".into()
}
const fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::ManagementStore;

    #[test]
    fn policy_updates_are_validated_and_retained() {
        let store = ManagementStore::in_memory();
        let policy = store
            .update(&json!({
                "launch_state": "disable",
                "trust_after_pair": false,
                "preferred_adapter_key": "adapter-opaque"
            }))
            .unwrap();
        assert_eq!(policy.launch_state, "disable");
        assert!(!policy.trust_after_pair);
        assert_eq!(store.policy().preferred_adapter_key, "adapter-opaque");
        assert!(
            store
                .update(&json!({ "launch_state": "sometimes" }))
                .is_err()
        );
        assert!(store.update(&json!({ "unknown": true })).is_err());
    }

    #[test]
    fn invalid_updates_do_not_partially_mutate_the_live_policy() {
        let store = ManagementStore::in_memory();
        assert!(
            store
                .update(&json!({
                    "launch_state": "disable",
                    "reconnect_on_resume": "yes"
                }))
                .is_err()
        );
        let policy = store.policy();
        assert_eq!(policy.launch_state, "remember");
        assert!(policy.reconnect_on_resume);
    }

    #[test]
    fn absent_runtime_state_uses_the_current_version() {
        let directory =
            std::env::temp_dir().join(format!("bt-management-{}", uuid::Uuid::new_v4()));
        let store = ManagementStore::load(
            Some(directory.join("policy.json")),
            Some(directory.join("runtime.json")),
            Some(directory.join("devices.json")),
        )
        .unwrap();
        assert!(store.runtime().adapter_power().is_empty());
    }

    #[test]
    fn device_policy_overrides_and_clears_global_defaults() {
        let store = ManagementStore::in_memory();
        let policy = store
            .update_device_policy(
                "device-opaque",
                &json!({
                    "reconnect_on_resume": false,
                    "power_on_connect": false,
                    "audio_route_on_connect": "switch"
                }),
            )
            .unwrap();
        assert!(!policy.reconnect_on_resume);
        assert!(!policy.power_on_connect);
        assert_eq!(policy.audio_route_on_connect, "switch");
        let reset = store
            .update_device_policy(
                "device-opaque",
                &json!({
                    "reconnect_on_resume": null,
                    "power_on_connect": null,
                    "audio_route_on_connect": null
                }),
            )
            .unwrap();
        assert!(reset.reconnect_on_resume);
        assert!(reset.power_on_connect);
        assert_eq!(reset.audio_route_on_connect, "keep");
    }
}
