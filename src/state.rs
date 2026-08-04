use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::{Serialize, de::DeserializeOwned};

pub(crate) fn directory() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("BT_DAEMON_STATE_DIR") {
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = std::env::var_os("STATE_DIRECTORY") {
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = std::env::var_os("XDG_STATE_HOME") {
        return Ok(PathBuf::from(path).join("bt-daemon"));
    }
    let home = std::env::var_os("HOME").context("HOME is unavailable for state storage")?;
    Ok(PathBuf::from(home).join(".local/state/bt-daemon"))
}

pub(crate) fn read_json<T: DeserializeOwned>(path: &Path, description: &str) -> Result<Option<T>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path).with_context(|| format!("read {description} {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("decode {description} {}", path.display()))
        .map(Some)
}

pub(crate) fn write_json<T: Serialize>(path: &Path, value: &T, description: &str) -> Result<()> {
    let parent = path.parent().context("state file path has no parent")?;
    fs::create_dir_all(parent).context("create state directory")?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
        .context("secure state directory")?;
    let temporary = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value).context("serialize state")?;
    let mut output = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)
        .with_context(|| format!("open {description} {}", temporary.display()))?;
    output.write_all(&bytes).context("write state file")?;
    output.sync_all().context("sync state file")?;
    fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))
        .context("secure state file")?;
    fs::rename(&temporary, path)
        .with_context(|| format!("replace {description} {}", path.display()))
}
