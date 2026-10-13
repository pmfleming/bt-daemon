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
fn writer_flushes_latest_state_recovers_failed_saves_and_drains_on_drop() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("identities.json");
    let writer = Writer::new(path.clone())?;
    writer.flush()?;
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

    // A directory at the destination prevents atomic replacement by a file.
    fs::remove_file(&path)?;
    fs::create_dir(&path)?;
    writer.schedule(state("failed"));
    assert!(writer.flush().is_err());
    assert!(writer.flush().is_err());
    fs::remove_dir(&path)?;
    writer.schedule(state("recovered"));
    writer.flush()?;
    let saved: RegistryFile = serde_json::from_slice(&fs::read(&path)?)?;
    assert_eq!(saved.devices["identity"], "recovered");

    writer.schedule(state("on-drop"));
    drop(writer);
    let saved: RegistryFile = serde_json::from_slice(&fs::read(&path)?)?;
    assert_eq!(saved.devices["identity"], "on-drop");
    Ok(())
}
