use super::Writer;
use crate::identity::{REGISTRY_VERSION, RegistryFile};
use std::fs;

fn state(key: &str) -> RegistryFile {
    RegistryFile {
        version: REGISTRY_VERSION,
        devices: [("identity".into(), key.into())].into(),
        ..RegistryFile::default()
    }
}

#[test]
fn flush_persists_the_latest_state_and_drop_drains_updates() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("identities.json");
    let writer = Writer::new(path.clone())?;
    writer.flush()?;
    assert!(!path.exists());
    writer.schedule(state("first"));
    writer.schedule(state("latest"));
    std::thread::scope(|scope| {
        let first = scope.spawn(|| writer.flush());
        let second = scope.spawn(|| writer.flush());
        first.join().unwrap()?;
        second.join().unwrap()
    })?;
    let saved: RegistryFile = serde_json::from_slice(&fs::read(&path)?)?;
    assert_eq!(saved.devices["identity"], "latest");
    writer.schedule(state("on-drop"));
    drop(writer);
    let saved: RegistryFile = serde_json::from_slice(&fs::read(&path)?)?;
    assert_eq!(saved.devices["identity"], "on-drop");
    Ok(())
}

#[test]
fn flush_reports_write_failures_until_a_successful_retry() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let directory = root.path().join("blocked");
    fs::write(&directory, "not a directory")?;
    let writer = Writer::new(directory.join("identities.json"))?;
    writer.schedule(state("first"));
    assert!(writer.flush().is_err());
    assert!(writer.flush().is_err());
    fs::remove_file(&directory)?;
    writer.schedule(state("recovered"));
    writer.flush()?;
    let saved: RegistryFile =
        serde_json::from_slice(&fs::read(directory.join("identities.json"))?)?;
    assert_eq!(saved.devices["identity"], "recovered");
    Ok(())
}
