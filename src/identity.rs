use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use bluer::Address;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    model::{Battery, presentation_component_order, presentation_components, presentation_type},
    state,
};

mod persistence;
const REGISTRY_VERSION: u8 = 1;
const EPHEMERAL_LIMIT: usize = 4096;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct RememberedPresentation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    icon: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    device_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    components: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    battery: Vec<Battery>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RememberedPresentationValues {
    pub icon: Option<String>,
    pub device_type: String,
    pub model_id: Option<String>,
    pub components: Vec<String>,
    pub battery: Vec<Battery>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct RegistryFile {
    version: u8,
    #[serde(default)]
    adapters: HashMap<String, String>,
    devices: HashMap<String, String>,
    #[serde(skip)]
    ephemeral: HashMap<String, (String, Instant)>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    presentations: HashMap<String, RememberedPresentation>,
}

pub struct DeviceIdentityRegistry {
    writer: Option<persistence::Writer>,
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
            writer: None,
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
            writer: path.map(persistence::Writer::new).transpose()?,
            state: Mutex::new(state),
        }))
    }

    /// Flush before associating durable credentials with an opaque device ID.
    pub async fn flush(self: &Arc<Self>) -> Result<()> {
        let registry = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            if let Some(writer) = &registry.writer {
                writer.flush()?;
            }
            Ok(())
        })
        .await?
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
        if let Some((key, seen)) = state.ephemeral.get_mut(&identity) {
            *seen = Instant::now();
            return key.clone();
        }
        state
            .ephemeral
            .retain(|_, (_, seen)| seen.elapsed() < Duration::from_secs(300));
        if state.ephemeral.len() >= EPHEMERAL_LIMIT
            && let Some(oldest) = state
                .ephemeral
                .iter()
                .min_by_key(|(_, (_, seen))| *seen)
                .map(|(identity, _)| identity.clone())
        {
            state.ephemeral.remove(&oldest);
        }
        let key = format!("device-{}", Uuid::new_v4().simple());
        state
            .ephemeral
            .insert(identity, (key.clone(), Instant::now()));
        key
    }

    /// Promote the same opaque ID only once BlueZ confirms pairing. Merely
    /// observing an advertising/private address must not leave a disk history.
    pub fn promote_device(&self, adapter: &str, address: Address) -> String {
        let key = self.device_key(adapter, address);
        let mut state = self.state();
        let stable = state.adapters.get(adapter).map_or(adapter, String::as_str);
        let identity = format!("{stable}:{address}");
        if !state.devices.contains_key(&identity) {
            state.ephemeral.remove(&identity);
            state.devices.insert(identity, key.clone());
            self.persist(&state, "paired device identity");
        }
        key
    }

    pub(crate) fn remember_presentation(
        &self,
        device_key: &str,
        icon: Option<&str>,
        model_id: Option<&str>,
        battery: &[Battery],
    ) -> RememberedPresentationValues {
        let mut state = self.state();
        let (changed, remembered) = {
            let presentation = state
                .presentations
                .entry(device_key.to_string())
                .or_default();
            (
                presentation.update(icon, model_id, battery),
                presentation.values(),
            )
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

    fn persist(&self, state: &RegistryFile, _description: &str) {
        if let Some(writer) = &self.writer {
            writer.schedule(RegistryFile {
                version: state.version,
                adapters: state.adapters.clone(),
                devices: state.devices.clone(),
                presentations: state.presentations.clone(),
                ephemeral: HashMap::new(),
            });
        }
    }
}

impl RememberedPresentation {
    fn update(&mut self, icon: Option<&str>, model_id: Option<&str>, battery: &[Battery]) -> bool {
        let icon = icon.filter(|value| !value.trim().is_empty());
        let observed_type = presentation_type(icon, battery);
        let resolved_type = self.resolved_type(observed_type).to_string();
        let type_changed = resolved_type != "Bluetooth device"
            && self.device_type.as_deref() != Some(resolved_type.as_str());
        if type_changed {
            self.device_type = Some(resolved_type);
        }
        let icon_changed = self.update_icon(icon);
        let model_changed = self.update_model_id(model_id);
        let components_changed = self.update_components(battery);
        let battery_changed = self.update_battery(battery);
        type_changed || icon_changed || model_changed || components_changed || battery_changed
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

    fn update_model_id(&mut self, model_id: Option<&str>) -> bool {
        let model_id = model_id.filter(|value| !value.trim().is_empty());
        if model_id.is_none_or(|model_id| self.model_id.as_deref() == Some(model_id)) {
            return false;
        }
        self.model_id = model_id.map(Into::into);
        true
    }

    fn update_components(&mut self, battery: &[Battery]) -> bool {
        let observed = presentation_components(battery);
        if observed.is_empty() {
            return false;
        }
        let previous = self.components.clone();
        self.components.extend(observed);
        self.components
            .sort_by_key(|component| presentation_component_order(component));
        self.components.dedup();
        self.components != previous
    }

    fn update_battery(&mut self, battery: &[Battery]) -> bool {
        if battery.is_empty() || self.battery == battery {
            return false;
        }
        self.battery = battery.to_vec();
        true
    }

    fn values(&self) -> RememberedPresentationValues {
        RememberedPresentationValues {
            icon: self.icon.clone(),
            device_type: self.device_type().into(),
            model_id: self.model_id.clone(),
            components: self.components.clone(),
            battery: self.battery.clone(),
        }
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

    fn component_battery(component: &str, percentage: u8) -> Vec<Battery> {
        vec![Battery {
            id: component.into(),
            label: component.into(),
            component: component.into(),
            percentage,
            source: "test".into(),
            confidence: "standard".into(),
        }]
    }

    #[test]
    fn registry_preserves_identity_and_presentation_across_reload_and_adapter_renames() {
        let directory = std::env::temp_dir().join(format!("bt-daemon-{}", uuid::Uuid::new_v4()));
        let path = directory.join("identities.json");
        let address = "AA:BB:CC:DD:EE:FF".parse().unwrap();
        let registry = DeviceIdentityRegistry::load(Some(path.clone())).unwrap();
        registry.register_adapter("hci0", "00:11:22:33:44:55");
        let key = registry.promote_device("hci0", address);
        let expected_battery = component_battery("left", 64);
        registry.remember_presentation(
            "device-known",
            Some("audio-headphones"),
            Some("a1b2c3"),
            &expected_battery,
        );
        drop(registry);

        let registry = DeviceIdentityRegistry::load(Some(path)).unwrap();
        assert_eq!(key, registry.device_key("hci0", address));
        registry.register_adapter("hci1", "00:11:22:33:44:55");
        assert_eq!(key, registry.device_key("hci1", address));
        let presentation = registry.remember_presentation("device-known", None, None, &[]);
        assert_eq!(presentation.icon.as_deref(), Some("audio-headphones"));
        assert_eq!(presentation.battery, expected_battery);
        assert_eq!(presentation.device_type, "Earbuds");
        assert_eq!(presentation.model_id.as_deref(), Some("a1b2c3"));
        assert_eq!(presentation.components, ["left"]);

        drop(registry);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn discovery_identities_are_ephemeral_until_promoted() {
        let registry = DeviceIdentityRegistry::in_memory();
        let address = "AA:BB:CC:DD:EE:FF".parse().unwrap();
        let key = registry.device_key("hci0", address);
        let encoded = serde_json::to_string(&*registry.state()).unwrap();
        assert!(!encoded.contains("AA:BB:CC:DD:EE:FF"));
        assert_eq!(registry.promote_device("hci0", address), key);
        assert!(
            serde_json::to_string(&*registry.state())
                .unwrap()
                .contains("AA:BB:CC:DD:EE:FF")
        );
    }

    #[test]
    fn forgetting_a_presentation_keeps_the_stable_device_key() {
        let registry = DeviceIdentityRegistry::in_memory();
        let address = "AA:BB:CC:DD:EE:FF".parse().unwrap();
        let key = registry.device_key("hci0", address);
        registry.remember_presentation(&key, Some("input-mouse"), None, &[battery(80)]);
        registry.forget_presentation(&key);

        assert_eq!(key, registry.device_key("hci0", address));
        let presentation = registry.remember_presentation(&key, None, None, &[]);
        assert_eq!(presentation.icon, None);
        assert_eq!(presentation.battery, vec![]);
        assert_eq!(presentation.device_type, "Bluetooth device");
        assert_eq!(presentation.model_id, None);
        assert_eq!(presentation.components, Vec::<String>::new());
    }

    #[test]
    fn remembered_components_and_model_survive_transient_connection_metadata() {
        let registry = DeviceIdentityRegistry::in_memory();
        let mut component_reports = component_battery("right", 75);
        component_reports.extend(component_battery("left", 80));
        let observed = registry.remember_presentation(
            "device-known",
            Some("audio-headset"),
            Some("02fc97"),
            &component_reports,
        );
        assert_eq!(observed.components, ["left", "right"]);

        let restored = registry.remember_presentation(
            "device-known",
            Some("audio-headphones"),
            None,
            &[battery(79)],
        );
        assert_eq!(restored.device_type, "Earbuds");
        assert_eq!(restored.model_id.as_deref(), Some("02fc97"));
        assert_eq!(restored.components, ["left", "right"]);
    }
}
