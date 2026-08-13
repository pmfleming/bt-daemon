use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
};

use anyhow::{Result, bail};
use bluer::Address;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    model::{Battery, presentation_type},
    state,
};

const REGISTRY_VERSION: u8 = 1;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct RememberedPresentation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    icon: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    device_type: Option<String>,
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
        let path = state::directory()?.join("device-identities.json");
        Self::load(Some(path))
    }

    #[cfg(test)]
    pub fn in_memory() -> Arc<Self> {
        Arc::new(Self {
            path: None,
            state: Mutex::new(RegistryFile {
                version: REGISTRY_VERSION,
                ..RegistryFile::default()
            }),
        })
    }

    fn load(path: Option<PathBuf>) -> Result<Arc<Self>> {
        let stored: Option<RegistryFile> = path
            .as_deref()
            .map(|path| state::read_json(path, "identity registry"))
            .transpose()?
            .flatten();
        let state = match stored {
            Some(state) if state.version != REGISTRY_VERSION => bail!(
                "unsupported Bluetooth identity registry version {}",
                state.version
            ),
            Some(state) => state,
            None => RegistryFile {
                version: REGISTRY_VERSION,
                ..RegistryFile::default()
            },
        };
        Ok(Arc::new(Self {
            path,
            state: Mutex::new(state),
        }))
    }

    pub fn register_adapter(&self, adapter: &str, stable_identity: &str) {
        let mut state = self.state();
        if state
            .adapters
            .get(adapter)
            .is_some_and(|identity| identity == stable_identity)
        {
            return;
        }
        let first_registration = !state.adapters.contains_key(adapter);
        state
            .adapters
            .insert(adapter.into(), stable_identity.into());
        if first_registration {
            migrate_legacy_devices(&mut state, adapter, stable_identity);
        }
        self.persist(&state, "adapter identity");
    }

    pub fn device_key(&self, adapter: &str, address: Address) -> String {
        let mut state = self.state();
        let adapter_identity = state.adapters.get(adapter).map_or(adapter, String::as_str);
        let identity = format!("{adapter_identity}:{address}");
        if let Some(key) = state.devices.get(&identity) {
            return key.clone();
        }
        let key = format!("device-{}", Uuid::new_v4().simple());
        state.devices.insert(identity, key.clone());
        self.persist(&state, "device identity");
        key
    }

    pub fn remember_presentation(
        &self,
        device_key: &str,
        icon: Option<&str>,
        battery: &[Battery],
    ) -> (Option<String>, Vec<Battery>, String) {
        let mut state = self.state();
        let (changed, remembered) = {
            let presentation = state
                .presentations
                .entry(device_key.to_string())
                .or_default();
            (presentation.update(icon, battery), presentation.values())
        };
        self.persist_if(changed, &state, "device presentation");
        remembered
    }

    pub fn forget_presentation(&self, device_key: &str) {
        let mut state = self.state();
        let changed = state.presentations.remove(device_key).is_some();
        self.persist_if(changed, &state, "forgotten device presentation");
    }

    fn state(&self) -> MutexGuard<'_, RegistryFile> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn persist_if(&self, changed: bool, state: &RegistryFile, description: &str) {
        if changed {
            self.persist(state, description);
        }
    }

    fn persist(&self, state: &RegistryFile, description: &str) {
        if let Some(path) = &self.path
            && let Err(error) = state::write_json(path, state, "identity registry")
        {
            tracing::warn!(%error, %description, "could not persist Bluetooth registry state");
        }
    }
}

impl RememberedPresentation {
    fn update(&mut self, icon: Option<&str>, battery: &[Battery]) -> bool {
        let icon = icon.filter(|value| !value.trim().is_empty());
        let observed_type = presentation_type(icon, battery);
        let resolved_type = self.resolved_type(observed_type).to_string();
        let type_changed = resolved_type != "Bluetooth device"
            && self.device_type.as_deref() != Some(resolved_type.as_str());
        if type_changed {
            self.device_type = Some(resolved_type);
        }
        let icon_changed = self.update_icon(icon);
        let battery_changed = self.update_battery(battery);
        type_changed || icon_changed || battery_changed
    }

    fn resolved_type<'a>(&'a self, observed: &'a str) -> &'a str {
        let remembered = self.device_type();
        if type_confidence(observed) > type_confidence(remembered) {
            observed
        } else {
            remembered
        }
    }

    fn device_type(&self) -> &str {
        self.device_type
            .as_deref()
            .unwrap_or_else(|| presentation_type(self.icon.as_deref(), &self.battery))
    }

    fn update_icon(&mut self, icon: Option<&str>) -> bool {
        if icon.is_none_or(|icon| self.icon.as_deref() == Some(icon)) {
            return false;
        }
        self.icon = icon.map(Into::into);
        true
    }

    fn update_battery(&mut self, battery: &[Battery]) -> bool {
        if battery.is_empty() || self.battery == battery {
            return false;
        }
        self.battery = battery.to_vec();
        true
    }

    fn values(&self) -> (Option<String>, Vec<Battery>, String) {
        (
            self.icon.clone(),
            self.battery.clone(),
            self.device_type().into(),
        )
    }
}

fn type_confidence(device_type: &str) -> u8 {
    match device_type {
        "Earbuds" => 3,
        "Bluetooth device" => 0,
        "Audio device" => 1,
        _ => 2,
    }
}

fn migrate_legacy_devices(state: &mut RegistryFile, adapter: &str, stable_identity: &str) {
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

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::model::Battery;

    use super::DeviceIdentityRegistry;

    fn battery(percentage: u8) -> Battery {
        Battery::bluez_aggregate(percentage)
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
    fn registry_state_is_compatible_and_survives_reload() {
        let directory = std::env::temp_dir().join(format!("bt-daemon-{}", uuid::Uuid::new_v4()));
        let path = directory.join("identities.json");
        let address = "AA:BB:CC:DD:EE:FF".parse().unwrap();
        let registry = DeviceIdentityRegistry::load(Some(path.clone())).unwrap();
        let key = registry.device_key("hci0", address);
        let expected_battery = vec![battery(64)];
        registry.remember_presentation("device-known", Some("audio-headphones"), &expected_battery);
        drop(registry);

        let registry = DeviceIdentityRegistry::load(Some(path)).unwrap();
        assert_eq!(key, registry.device_key("hci0", address));
        let (icon, remembered_battery, device_type) =
            registry.remember_presentation("device-known", None, &[]);
        assert_eq!(icon.as_deref(), Some("audio-headphones"));
        assert_eq!(remembered_battery, expected_battery);
        assert_eq!(device_type, "Headphones");

        let legacy_path = directory.join("legacy.json");
        fs::write(&legacy_path, r#"{"version":1,"adapters":{},"devices":{}}"#).unwrap();
        DeviceIdentityRegistry::load(Some(legacy_path)).unwrap();
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
            (None, vec![], "Bluetooth device".to_string())
        );
    }

    #[test]
    fn known_device_type_ignores_weaker_transient_observations() {
        let registry = DeviceIdentityRegistry::in_memory();
        let component_battery = vec![Battery {
            id: "left".into(),
            label: "Left".into(),
            component: "left".into(),
            percentage: 80,
            source: "test".into(),
            confidence: "standard".into(),
        }];
        assert_eq!(
            registry
                .remember_presentation("device-known", Some("audio-headset"), &component_battery)
                .2,
            "Earbuds"
        );
        assert_eq!(
            registry
                .remember_presentation("device-known", Some("audio-headphones"), &[battery(79)])
                .2,
            "Earbuds"
        );
    }

    #[test]
    fn stronger_type_evidence_can_refine_a_known_generic_type() {
        let registry = DeviceIdentityRegistry::in_memory();
        assert_eq!(
            registry
                .remember_presentation("device-known", Some("audio-headphones"), &[])
                .2,
            "Headphones"
        );
        let component_battery = vec![Battery {
            id: "right".into(),
            label: "Right".into(),
            component: "right".into(),
            percentage: 75,
            source: "test".into(),
            confidence: "standard".into(),
        }];
        assert_eq!(
            registry
                .remember_presentation("device-known", None, &component_battery)
                .2,
            "Earbuds"
        );
    }
}
