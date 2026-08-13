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

    pub(super) fn insert(&self, device_key: String, key: [u8; 16]) -> Result<()> {
        validate_key(&key, "Fast Pair account key")?;
        let mut keys = self
            .keys
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        keys.insert(device_key, key);
        if let Some(path) = &self.path {
            let file = KeyFile {
                version: STORE_VERSION,
                account_keys: keys
                    .iter()
                    .map(|(device, key)| (device.clone(), hex::encode(key)))
                    .collect(),
            };
            crate::state::write_json(path, &file, "Fast Pair account key store")?;
        }
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
    fn generated_keys_have_the_account_key_type() {
        let first = AccountKeyStore::generate();
        let second = AccountKeyStore::generate();
        assert_eq!(first[0], 0x04);
        assert_eq!(second[0], 0x04);
        assert_ne!(first, second);
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
        let mode = fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        fs::remove_dir_all(directory).unwrap();
    }
}
