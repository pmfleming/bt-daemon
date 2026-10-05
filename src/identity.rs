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
    // Immutable readings are shared by registry views and queued disk snapshots.
    #[serde(default, skip_serializing_if = "<[Battery]>::is_empty")]
    battery: Arc<[Battery]>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RememberedPresentationValues {
    pub icon: Option<String>,
    pub device_type: String,
    pub model_id: Option<String>,
    pub components: Vec<String>,
    pub battery: Arc<[Battery]>,
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

/// Stable opaque device identities and remembered presentation metadata.
/// Discovery-only identities remain ephemeral until promoted after pairing.
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
        self.persist(&state);
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
            self.persist(&state);
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
        self.persist_if(changed, &state);
        remembered
    }

    pub fn forget_presentation(&self, device_key: &str) {
        let mut state = self.state();
        let changed = state.presentations.remove(device_key).is_some();
        self.persist_if(changed, &state);
    }

    fn state(&self) -> MutexGuard<'_, RegistryFile> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn persist_if(&self, changed: bool, state: &RegistryFile) {
        if changed {
            self.persist(state);
        }
    }

    fn persist(&self, state: &RegistryFile) {
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
        let resolved_type = self.resolved_type(observed_type);
        let type_changed = resolved_type != "Bluetooth device"
            && self.device_type.as_deref() != Some(resolved_type);
        if type_changed {
            self.device_type = Some(resolved_type.into());
        }
        let icon_changed = update_text(&mut self.icon, icon);
        let model_changed = update_text(&mut self.model_id, model_id);
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
        if battery.is_empty() || self.battery.as_ref() == battery {
            return false;
        }
        self.battery = battery.into();
        true
    }

    fn values(&self) -> RememberedPresentationValues {
        RememberedPresentationValues {
            icon: self.icon.clone(),
            device_type: self.device_type().into(),
            model_id: self.model_id.clone(),
            components: self.components.clone(),
            battery: Arc::clone(&self.battery),
        }
    }
}

fn update_text(target: &mut Option<String>, value: Option<&str>) -> bool {
    let value = value.filter(|value| !value.trim().is_empty());
    if value.is_none_or(|value| target.as_deref() == Some(value)) {
        return false;
    }
    *target = value.map(Into::into);
    true
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
        .extract_if(|identity, _| identity.starts_with(&legacy_prefix))
        .collect::<Vec<_>>();
    for (identity, key) in legacy {
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

    #[test]
    fn legacy_migration_moves_only_matching_devices_and_preserves_stable_ids() {
        let mut state = super::RegistryFile::default();
        state.devices.extend([
            ("hci0:AA".into(), "old-a".into()),
            ("hci0:BB".into(), "old-b".into()),
            ("stable:BB".into(), "stable-b".into()),
            ("hci01:CC".into(), "other".into()),
        ]);
        super::migrate_legacy_devices(&mut state, "hci0", "stable");
        assert_eq!(state.devices.len(), 3);
        assert_eq!(state.devices["stable:AA"], "old-a");
        assert_eq!(state.devices["stable:BB"], "stable-b");
        assert_eq!(state.devices["hci01:CC"], "other");
    }

    #[test]
    fn battery_views_and_persistence_snapshots_share_immutable_readings() {
        let registry = DeviceIdentityRegistry::in_memory();
        let battery = [Battery::bluez_aggregate(75)];
        let first = registry.remember_presentation("peer", None, None, &battery);
        let frozen = registry.state().clone();
        let unchanged = registry.remember_presentation("peer", None, None, &battery);
        let missing = registry.remember_presentation("peer", None, None, &[]);
        for retained in [
            unchanged.battery.as_ref(),
            missing.battery.as_ref(),
            frozen.presentations["peer"].battery.as_ref(),
        ] {
            assert_eq!(first.battery.as_ptr(), retained.as_ptr());
        }
        let changed =
            registry.remember_presentation("peer", None, None, &[Battery::bluez_aggregate(74)]);
        assert_ne!(first.battery.as_ptr(), changed.battery.as_ptr());
        assert_eq!(first.battery[0].percentage, 75);
        assert_eq!(changed.battery[0].percentage, 74);
        let saved = serde_json::to_value(&frozen).unwrap();
        assert_eq!(
            saved["presentations"]["peer"]["battery"],
            serde_json::json!(battery)
        );
        registry.forget_presentation("peer");
        assert!(
            registry
                .remember_presentation("peer", None, None, &[])
                .battery
                .is_empty()
        );
        assert_eq!(first.battery[0].percentage, 75);
    }

    #[test]
    fn legacy_battery_arrays_and_missing_readings_keep_their_disk_format() {
        let legacy = serde_json::json!({
            "version": 1, "devices": {}, "presentations": {
                "peer": {"battery": [Battery::bluez_aggregate(75)]},
                "empty": {"battery": []}, "missing": {}
            }
        });
        let loaded: super::RegistryFile = serde_json::from_value(legacy.clone()).unwrap();
        let saved = serde_json::to_value(&loaded).unwrap();
        assert_eq!(
            saved["presentations"]["peer"]["battery"],
            legacy["presentations"]["peer"]["battery"]
        );
        for key in ["empty", "missing"] {
            assert!(loaded.presentations[key].battery.is_empty());
            assert!(saved["presentations"][key].get("battery").is_none());
        }
    }

    fn component_battery(component: &str, percentage: u8) -> Vec<Battery> {
        vec![Battery {
            id: component.into(),
            label: component.into(),
            component: component.into(),
            percentage,
            charging: None,
            source: "test".into(),
            confidence: "standard".into(),
        }]
    }

    #[tokio::test]
    async fn discovery_stays_private_until_pairing_and_paired_history_survives_reload() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("identities.json");
        let address = "AA:BB:CC:DD:EE:FF".parse().unwrap();
        let registry = DeviceIdentityRegistry::load(Some(path.clone())).unwrap();
        registry.register_adapter("hci0", "00:11:22:33:44:55");
        let key = registry.device_key("hci0", address);
        registry.flush().await.unwrap();
        assert!(
            !fs::read_to_string(&path)
                .unwrap()
                .contains("AA:BB:CC:DD:EE:FF")
        );
        assert_eq!(registry.promote_device("hci0", address), key);
        let mut components = component_battery("right", 75);
        components.extend(component_battery("left", 80));
        components.extend(component_battery("LEFT", 80));
        components.extend(component_battery("main", 79));
        registry.remember_presentation(&key, Some("audio-headset"), Some("02fc97"), &components);
        let expected_battery = vec![Battery::bluez_aggregate(79)];
        registry.remember_presentation(&key, Some("audio-headphones"), None, &expected_battery);
        drop(registry);
        assert!(
            fs::read_to_string(&path)
                .unwrap()
                .contains("AA:BB:CC:DD:EE:FF")
        );
        let registry = DeviceIdentityRegistry::load(Some(path)).unwrap();
        assert_eq!(key, registry.device_key("hci0", address));
        registry.register_adapter("hci1", "00:11:22:33:44:55");
        assert_eq!(key, registry.device_key("hci1", address));
        let presentation = registry.remember_presentation(&key, None, None, &[]);
        assert_eq!(presentation.icon.as_deref(), Some("audio-headphones"));
        assert_eq!(presentation.battery.as_ref(), expected_battery);
        assert_eq!(presentation.device_type, "Earbuds");
        assert_eq!(presentation.model_id.as_deref(), Some("02fc97"));
        assert_eq!(presentation.components, ["left", "right"]);
        assert_eq!(
            registry.remember_presentation(&key, Some("  "), Some("\t"), &[]),
            presentation
        );
        assert_eq!(
            registry.remember_presentation(
                &key,
                Some("audio-headphones"),
                Some("02fc97"),
                &expected_battery
            ),
            presentation
        );
    }
}
