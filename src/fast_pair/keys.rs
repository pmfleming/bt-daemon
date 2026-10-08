use std::{collections::HashMap, path::PathBuf, sync::Mutex};

use anyhow::{Context, Result, bail};
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};

const STORE_VERSION: u8 = 1;

#[derive(Debug, Serialize, Deserialize)]
struct KeyFile {
    version: u8,
    #[serde(default)]
    account_keys: HashMap<String, String>,
}

pub(super) struct AccountKeyStore {
    path: Option<PathBuf>,
    keys: Mutex<HashMap<String, [u8; 16]>>,
}

impl AccountKeyStore {
    pub(super) fn load_default() -> Result<Self> {
        Self::load(Some(
            crate::state::directory()?.join("fast-pair-account-keys.json"),
        ))
    }

    fn load(path: Option<PathBuf>) -> Result<Self> {
        let mut keys = HashMap::new();
        if let Some(file) = path
            .as_deref()
            .map(|path| crate::state::read_json::<KeyFile>(path, "Fast Pair account key store"))
            .transpose()?
            .flatten()
        {
            if file.version != STORE_VERSION {
                bail!(
                    "unsupported Fast Pair account key store version {}",
                    file.version
                );
            }
            for (device, encoded) in file.account_keys {
                let decoded = hex::decode(&encoded)
                    .with_context(|| format!("decode Fast Pair account key for {device}"))?;
                let key: [u8; 16] = decoded.try_into().map_err(|value: Vec<u8>| {
                    anyhow::anyhow!(
                        "Fast Pair account key for {device} has {} bytes instead of 16",
                        value.len()
                    )
                })?;
                validate_key(&key, &format!("Fast Pair account key for {device}"))?;
                keys.insert(device, key);
            }
        }
        Ok(Self {
            path,
            keys: Mutex::new(keys),
        })
    }

    pub(super) fn get(&self, device_key: &str) -> Option<[u8; 16]> {
        self.keys
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(device_key)
            .copied()
    }

    pub(super) fn generate() -> [u8; 16] {
        let mut key = [0_u8; 16];
        OsRng.fill_bytes(&mut key);
        key[0] = 0x04;
        key
    }

    pub(super) fn remove(&self, device_key: &str) -> Result<()> {
        let mut keys = self.keys.lock().unwrap_or_else(|p| p.into_inner());
        if !keys.contains_key(device_key) {
            return Ok(());
        }
        self.persist(
            keys.iter()
                .filter(|(device, _)| device.as_str() != device_key),
        )?;
        keys.remove(device_key);
        Ok(())
    }

    fn persist<'a>(&self, keys: impl Iterator<Item = (&'a String, &'a [u8; 16])>) -> Result<()> {
        if let Some(path) = &self.path {
            let file = KeyFile {
                version: STORE_VERSION,
                account_keys: keys
                    .map(|(device, key)| (device.clone(), hex::encode(key)))
                    .collect(),
            };
            crate::state::write_json(path, &file, "Fast Pair account key store")?;
        }
        Ok(())
    }

    pub(super) fn insert(&self, device_key: String, key: [u8; 16]) -> Result<()> {
        validate_key(&key, "Fast Pair account key")?;
        let mut keys = self
            .keys
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        // Serialize the candidate view before committing; failed saves leave every key intact.
        self.persist(
            keys.iter()
                .filter(|(device, _)| *device != &device_key)
                .chain(std::iter::once((&device_key, &key))),
        )?;
        keys.insert(device_key, key);
        Ok(())
    }
}

fn validate_key(key: &[u8; 16], label: &str) -> Result<()> {
    if key[0] != 0x04 {
        bail!("{label} has an invalid type byte");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt};

    use super::AccountKeyStore;

    #[test]
    fn failed_key_writes_leave_insert_replace_and_remove_retryable() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("keys.json");
        let mut store = AccountKeyStore::load(Some(path.clone())).unwrap();
        let original = [4; 16];
        let replacement = [4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        store.insert("peer".into(), original).unwrap();
        store.insert("other".into(), original).unwrap();
        store.path = Some(path.join("blocked.json"));
        assert!(store.insert("new".into(), replacement).is_err());
        assert!(store.insert("peer".into(), replacement).is_err());
        assert!(store.remove("peer").is_err());
        assert_eq!(store.get("peer"), Some(original));
        assert_eq!(store.get("other"), Some(original));
        assert_eq!(store.get("new"), None);
        store.remove("missing").unwrap();
        store.path = Some(path.clone());
        assert!(store.insert("peer".into(), [0; 16]).is_err());
        let saved = AccountKeyStore::load(Some(path.clone())).unwrap();
        assert_eq!(saved.get("peer"), Some(original));
        assert_eq!(saved.get("new"), None);
        store.insert("peer".into(), replacement).unwrap();
        store.remove("other").unwrap();
        let saved = AccountKeyStore::load(Some(path)).unwrap();
        assert_eq!(saved.get("peer"), Some(replacement));
        assert_eq!(saved.get("other"), None);
    }

    #[test]
    fn keys_survive_a_secure_store_reload() {
        let directory = std::env::temp_dir().join(format!("bt-fast-pair-{}", uuid::Uuid::new_v4()));
        let path = directory.join("keys.json");
        let key = AccountKeyStore::generate();
        AccountKeyStore::load(Some(path.clone()))
            .unwrap()
            .insert("device-test".into(), key)
            .unwrap();
        assert_eq!(
            AccountKeyStore::load(Some(path.clone()))
                .unwrap()
                .get("device-test"),
            Some(key)
        );
        let store = AccountKeyStore::load(Some(path.clone())).unwrap();
        store.remove("device-test").unwrap();
        assert_eq!(store.get("device-test"), None);
        assert_eq!(
            AccountKeyStore::load(Some(path.clone()))
                .unwrap()
                .get("device-test"),
            None
        );
        let mode = fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        fs::remove_dir_all(directory).unwrap();
    }
}
