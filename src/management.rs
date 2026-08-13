use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Mutex, MutexGuard},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{model::Snapshot, state};

const POLICY_VERSION: u8 = 1;
const RUNTIME_VERSION: u8 = 1;

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
    policy: Mutex<ManagementPolicy>,
    runtime: Mutex<RuntimeState>,
}

impl ManagementStore {
    pub fn load_default() -> Result<Self> {
        let directory = state::directory()?;
        Self::load(
            Some(directory.join("management-policy.json")),
            Some(directory.join("runtime-state.json")),
        )
    }

    #[cfg(test)]
    pub fn in_memory() -> Self {
        Self {
            policy_path: None,
            runtime_path: None,
            policy: Mutex::new(ManagementPolicy::default()),
            runtime: Mutex::new(RuntimeState::default()),
        }
    }

    fn load(policy_path: Option<PathBuf>, runtime_path: Option<PathBuf>) -> Result<Self> {
        let policy: ManagementPolicy = policy_path
            .as_deref()
            .map(|path| state::read_json(path, "Bluetooth management policy"))
            .transpose()?
            .flatten()
            .unwrap_or_default();
        if policy.version != POLICY_VERSION {
            bail!(
                "unsupported Bluetooth management policy version {}",
                policy.version
            );
        }
        validate_launch_state(&policy.launch_state)?;
        let runtime: RuntimeState = runtime_path
            .as_deref()
            .map(|path| state::read_json(path, "Bluetooth runtime state"))
            .transpose()?
            .flatten()
            .unwrap_or_default();
        if runtime.version != RUNTIME_VERSION {
            bail!(
                "unsupported Bluetooth runtime state version {}",
                runtime.version
            );
        }
        Ok(Self {
            policy_path,
            runtime_path,
            policy: Mutex::new(policy),
            runtime: Mutex::new(runtime),
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
        policy.apply(object)?;
        self.persist_policy(&policy)?;
        Ok(policy.clone())
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
                .filter(|device| device.connected)
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

impl RuntimeState {
    pub fn adapter_power(&self) -> &HashMap<String, bool> {
        &self.adapter_power
    }
    pub fn connected_device_keys(&self) -> &[String] {
        &self.connected_device_keys
    }
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
    fn absent_runtime_state_uses_the_current_version() {
        let directory =
            std::env::temp_dir().join(format!("bt-management-{}", uuid::Uuid::new_v4()));
        let store = ManagementStore::load(
            Some(directory.join("policy.json")),
            Some(directory.join("runtime.json")),
        )
        .unwrap();
        assert!(store.runtime().adapter_power().is_empty());
    }
}
