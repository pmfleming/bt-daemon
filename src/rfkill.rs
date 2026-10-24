use std::{
    fs,
    io::{self, Write},
    path::Path,
};

use anyhow::{Context, Result};

use crate::model::RadioState;

const RFKILL_TYPE_BLUETOOTH: u8 = 2;
const RFKILL_OP_CHANGE_ALL: u8 = 3;

/// Combine adapter availability/power with readable Bluetooth rfkill entries.
/// Missing or unreadable sysfs state is reported as unavailable rfkill evidence.
pub fn radio_state(adapter_count: usize, powered: bool) -> RadioState {
    let (rfkill_present, soft_blocked, hard_blocked) = read_bluetooth_state("/sys/class/rfkill")
        .unwrap_or_else(|error| {
            tracing::debug!(%error, "Bluetooth rfkill state is unavailable");
            (false, false, false)
        });
    RadioState {
        available: adapter_count > 0 || rfkill_present,
        adapter_count,
        powered,
        operational: adapter_count > 0 && powered && !soft_blocked && !hard_blocked,
        rfkill_present,
        soft_blocked,
        hard_blocked,
    }
}

/// Change the Bluetooth software radio block through `/dev/rfkill`.
/// Requires write permission and never overrides a hardware radio block.
pub fn set_bluetooth_soft_blocked(blocked: bool) -> Result<()> {
    let mut device = fs::OpenOptions::new()
        .write(true)
        .open("/dev/rfkill")
        .context("open /dev/rfkill to update Bluetooth radio state")?;
    // Linux struct rfkill_event: native u32 index followed by type, operation, soft and hard bytes.
    let event = [
        0,
        0,
        0,
        0,
        RFKILL_TYPE_BLUETOOTH,
        RFKILL_OP_CHANGE_ALL,
        u8::from(blocked),
        0,
    ];
    device
        .write_all(&event)
        .context("write Bluetooth rfkill state")
}

fn read_bluetooth_state(root: impl AsRef<Path>) -> io::Result<(bool, bool, bool)> {
    let mut present = false;
    let mut soft = false;
    let mut hard = false;
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok((false, false, false)),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let path = entry?.path();
        if fs::read_to_string(path.join("type"))?.trim() != "bluetooth" {
            continue;
        }
        present = true;
        soft |= read_flag(&path.join("soft"))?;
        hard |= read_flag(&path.join("hard"))?;
    }
    Ok((present, soft, hard))
}

fn read_flag(path: &Path) -> io::Result<bool> {
    Ok(fs::read_to_string(path)?.trim() == "1")
}

#[cfg(test)]
mod tests {
    use super::read_bluetooth_state;
    use std::fs;

    #[test]
    fn sysfs_aggregates_bluetooth_only_and_distinguishes_missing_from_unreadable()
    -> std::io::Result<()> {
        let root = tempfile::tempdir()?;
        assert_eq!(read_bluetooth_state(root.path())?, (false, false, false));
        for (name, kind, soft, hard) in [
            ("a", "bluetooth\n", "1\n", "0\n"),
            ("b", "bluetooth", "0", "1"),
            ("wifi", "wlan", "1", "1"),
        ] {
            let path = root.path().join(name);
            fs::create_dir(&path)?;
            for (field, value) in [("type", kind), ("soft", soft), ("hard", hard)] {
                fs::write(path.join(field), value)?;
            }
        }
        fs::remove_file(root.path().join("wifi/soft"))?;
        assert_eq!(read_bluetooth_state(root.path())?, (true, true, true));
        fs::remove_file(root.path().join("b/hard"))?;
        assert!(read_bluetooth_state(root.path()).is_err());
        assert!(read_bluetooth_state(root.path().join("a/type")).is_err());
        assert_eq!(
            read_bluetooth_state(root.path().join("absent"))?,
            (false, false, false)
        );
        Ok(())
    }
}
