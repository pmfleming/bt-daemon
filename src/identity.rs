use std::{
    collections::HashMap,
    fs,
    fs::OpenOptions,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, bail};
use bluer::Address;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::Battery;

const REGISTRY_VERSION: u8 = 1;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct RememberedPresentation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    icon: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    battery: Vec<Battery>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct RegistryFile {
    version: u8,
    #[serde(default)]
    adapters: HashMap<String, String>,
    devices: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    presentations: HashMap<String, RememberedPresentation>,
}

pub struct DeviceIdentityRegistry {
    path: Option<PathBuf>,
    state: Mutex<RegistryFile>,
}

impl DeviceIdentityRegistry {
    pub fn load_default() -> Result<Arc<Self>> {
        let path = state_directory()?.join("device-identities.json");
        Self::load(Some(path))
    }

    #[cfg(test)]
    pub fn in_memory() -> Arc<Self> {
        Self::load(None).expect("create in-memory identity registry")
    }

    fn load(path: Option<PathBuf>) -> Result<Arc<Self>> {
        let state = match path.as_deref() {
            Some(path) if path.exists() => {
                let bytes = fs::read(path)
                    .with_context(|| format!("read identity registry {}", path.display()))?;
                let state: RegistryFile = serde_json::from_slice(&bytes)
                    .with_context(|| format!("decode identity registry {}", path.display()))?;
                if state.version != REGISTRY_VERSION {
                    bail!(
                        "unsupported Bluetooth identity registry version {}",
                        state.version
                    );
                }
                state
            }
            _ => RegistryFile {
                version: REGISTRY_VERSION,
                adapters: HashMap::new(),
                devices: HashMap::new(),
                presentations: HashMap::new(),
            },
        };
        Ok(Arc::new(Self {
            path,
            state: Mutex::new(state),
        }))
    }

    pub fn register_adapter(&self, adapter: &str, stable_identity: &str) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let previous = state.adapters.get(adapter).cloned();
        if previous.as_deref() == Some(stable_identity) {
            return;
        }
        state
            .adapters
            .insert(adapter.to_string(), stable_identity.to_string());
        if previous.is_none() {
            let legacy_prefix = format!("{adapter}:");
            let legacy = state
                .devices
                .iter()
                .filter(|(identity, _)| identity.starts_with(&legacy_prefix))
                .map(|(identity, key)| (identity.clone(), key.clone()))
                .collect::<Vec<_>>();
            for (identity, key) in legacy {
                state.devices.remove(&identity);
                let address = &identity[legacy_prefix.len()..];
                state
                    .devices
                    .entry(format!("{stable_identity}:{address}"))
                    .or_insert(key);
            }
        }
        if let Some(path) = &self.path
            && let Err(error) = persist(path, &state)
        {
            tracing::warn!(%error, "could not persist Bluetooth adapter identity");
        }
    }

    pub fn device_key(&self, adapter: &str, address: Address) -> String {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let adapter_identity = state.adapters.get(adapter).map_or(adapter, String::as_str);
        let identity = format!("{adapter_identity}:{address}");
        if let Some(key) = state.devices.get(&identity) {
            return key.clone();
        }
        let key = format!("device-{}", Uuid::new_v4().simple());
        state.devices.insert(identity, key.clone());
        if let Some(path) = &self.path
            && let Err(error) = persist(path, &state)
        {
            tracing::warn!(%error, "could not persist Bluetooth device identity");
        }
        key
    }

    pub fn remember_presentation(
        &self,
        device_key: &str,
        icon: Option<&str>,
        battery: &[Battery],
    ) -> (Option<String>, Vec<Battery>) {
        let icon = icon.filter(|value| !value.trim().is_empty());
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let presentation = state
            .presentations
            .entry(device_key.to_string())
            .or_default();
        let mut changed = false;
        if let Some(icon) = icon
            && presentation.icon.as_deref() != Some(icon)
        {
            presentation.icon = Some(icon.to_string());
            changed = true;
        }
        if !battery.is_empty() && presentation.battery != battery {
            presentation.battery = battery.to_vec();
            changed = true;
        }
        let remembered = presentation.clone();
        if changed
            && let Some(path) = &self.path
            && let Err(error) = persist(path, &state)
        {
            tracing::warn!(%error, "could not persist Bluetooth device presentation");
        }
        (remembered.icon, remembered.battery)
    }

    pub fn forget_presentation(&self, device_key: &str) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if state.presentations.remove(device_key).is_none() {
            return;
        }
        if let Some(path) = &self.path
            && let Err(error) = persist(path, &state)
        {
            tracing::warn!(%error, "could not forget Bluetooth device presentation");
        }
    }
}

pub(crate) fn state_directory() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("BT_DAEMON_STATE_DIR") {
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = std::env::var_os("STATE_DIRECTORY") {
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = std::env::var_os("XDG_STATE_HOME") {
        return Ok(PathBuf::from(path).join("bt-daemon"));
    }
    let home = std::env::var_os("HOME").context("HOME is unavailable for identity storage")?;
    Ok(PathBuf::from(home).join(".local/state/bt-daemon"))
}

fn persist(path: &Path, state: &RegistryFile) -> Result<()> {
    let parent = path
        .parent()
        .context("identity registry path has no parent")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create identity registry directory {}", parent.display()))?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("secure identity registry directory {}", parent.display()))?;
    let temporary = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(state).context("serialize identity registry")?;
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true).mode(0o600);
    let mut file = options
        .open(&temporary)
        .with_context(|| format!("open identity registry {}", temporary.display()))?;
    use std::io::Write;
    file.write_all(&bytes)
        .with_context(|| format!("write identity registry {}", temporary.display()))?;
    file.sync_all()
        .with_context(|| format!("sync identity registry {}", temporary.display()))?;
    fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("secure identity registry {}", temporary.display()))?;
    fs::rename(&temporary, path)
        .with_context(|| format!("replace identity registry {}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::model::Battery;

    use super::DeviceIdentityRegistry;

    fn battery(percentage: u8) -> Battery {
        Battery {
            id: "aggregate".into(),
            label: "Battery".into(),
            component: "main".into(),
            percentage,
            source: "bluez".into(),
            confidence: "standard".into(),
        }
    }

    #[test]
    fn keys_are_opaque_and_stable_in_memory() {
        let registry = DeviceIdentityRegistry::in_memory();
        let address = "AA:BB:CC:DD:EE:FF".parse().unwrap();
        let first = registry.device_key("hci0", address);
        assert_eq!(first, registry.device_key("hci0", address));
        assert!(!first.contains("AA"));
        assert_ne!(first, registry.device_key("hci1", address));
    }

    #[test]
    fn keys_use_stable_adapter_identity_across_kernel_renames() {
        let registry = DeviceIdentityRegistry::in_memory();
        let address = "AA:BB:CC:DD:EE:FF".parse().unwrap();
        registry.register_adapter("hci0", "00:11:22:33:44:55");
        let first = registry.device_key("hci0", address);
        registry.register_adapter("hci1", "00:11:22:33:44:55");
        assert_eq!(first, registry.device_key("hci1", address));
    }

    #[test]
    fn keys_survive_registry_reload() {
        let directory = std::env::temp_dir().join(format!("bt-daemon-{}", uuid::Uuid::new_v4()));
        let path = directory.join("identities.json");
        let address = "AA:BB:CC:DD:EE:FF".parse().unwrap();
        let first = DeviceIdentityRegistry::load(Some(path.clone()))
            .unwrap()
            .device_key("hci0", address);
        let second = DeviceIdentityRegistry::load(Some(path))
            .unwrap()
            .device_key("hci0", address);
        assert_eq!(first, second);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn registries_without_presentations_remain_compatible() {
        let directory = std::env::temp_dir().join(format!("bt-daemon-{}", uuid::Uuid::new_v4()));
        let path = directory.join("identities.json");
        fs::create_dir_all(&directory).unwrap();
        fs::write(&path, r#"{"version":1,"adapters":{},"devices":{}}"#).unwrap();

        DeviceIdentityRegistry::load(Some(path)).unwrap();
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn presentation_survives_registry_reload_and_empty_observations() {
        let directory = std::env::temp_dir().join(format!("bt-daemon-{}", uuid::Uuid::new_v4()));
        let path = directory.join("identities.json");
        let registry = DeviceIdentityRegistry::load(Some(path.clone())).unwrap();
        let expected_battery = vec![battery(64)];
        registry.remember_presentation("device-known", Some("audio-headphones"), &expected_battery);
        drop(registry);

        let registry = DeviceIdentityRegistry::load(Some(path)).unwrap();
        let (icon, remembered_battery) = registry.remember_presentation("device-known", None, &[]);
        assert_eq!(icon.as_deref(), Some("audio-headphones"));
        assert_eq!(remembered_battery, expected_battery);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn forgetting_a_presentation_keeps_the_stable_device_key() {
        let registry = DeviceIdentityRegistry::in_memory();
        let address = "AA:BB:CC:DD:EE:FF".parse().unwrap();
        let key = registry.device_key("hci0", address);
        registry.remember_presentation(&key, Some("input-mouse"), &[battery(80)]);
        registry.forget_presentation(&key);

        assert_eq!(key, registry.device_key("hci0", address));
        assert_eq!(
            registry.remember_presentation(&key, None, &[]),
            (None, vec![])
        );
    }
}
