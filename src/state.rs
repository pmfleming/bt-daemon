use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Serialize, de::DeserializeOwned};
use shelllist_daemon_core::{AtomicWritePolicy, XdgRoot};

pub(crate) fn directory() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("BT_DAEMON_STATE_DIR") {
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = std::env::var_os("STATE_DIRECTORY") {
        return Ok(PathBuf::from(path));
    }
    shelllist_daemon_core::resolve_xdg_root(XdgRoot::State)
        .map(|path| path.join("bt-daemon"))
        .context("HOME is unavailable for state storage")
}

pub(crate) fn read_json<T: DeserializeOwned>(path: &Path, description: &str) -> Result<Option<T>> {
    shelllist_daemon_core::read_json(path)
        .with_context(|| format!("read and decode {description} {}", path.display()))
}

pub(crate) fn write_json<T: Serialize>(path: &Path, value: &T, description: &str) -> Result<()> {
    shelllist_daemon_core::write_json_atomic(path, value, AtomicWritePolicy::PRIVATE)
        .with_context(|| format!("replace {description} {}", path.display()))
}
