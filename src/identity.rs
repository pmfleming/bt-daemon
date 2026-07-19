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

const REGISTRY_VERSION: u8 = 1;

#[derive(Debug, Default, Deserialize, Serialize)]
struct RegistryFile {
    version: u8,
    devices: HashMap<String, String>,
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
                devices: HashMap::new(),
            },
        };
        Ok(Arc::new(Self {
            path,
            state: Mutex::new(state),
        }))
    }

    pub fn device_key(&self, adapter: &str, address: Address) -> String {
        let identity = format!("{adapter}:{address}");
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
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
}

fn state_directory() -> Result<PathBuf> {
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

    use super::DeviceIdentityRegistry;

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
}
