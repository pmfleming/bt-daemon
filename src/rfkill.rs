use std::{
    fs,
    io::{self, Write},
    path::Path,
};

use anyhow::{Context, Result};

use crate::model::RadioState;

const RFKILL_TYPE_BLUETOOTH: u8 = 2;
const RFKILL_OP_CHANGE_ALL: u8 = 3;

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
    use std::fs;

    use super::read_bluetooth_state;

    #[test]
    fn aggregates_bluetooth_rfkill_entries_only() {
        let root = std::env::temp_dir().join(format!("bt-rfkill-{}", uuid::Uuid::new_v4()));
        for (name, kind, soft, hard) in [
            ("rfkill0", "wlan", "1", "0"),
            ("rfkill1", "bluetooth", "0", "0"),
            ("rfkill2", "bluetooth", "1", "0"),
        ] {
            let path = root.join(name);
            fs::create_dir_all(&path).unwrap();
            fs::write(path.join("type"), kind).unwrap();
            fs::write(path.join("soft"), soft).unwrap();
            fs::write(path.join("hard"), hard).unwrap();
        }
        assert_eq!(read_bluetooth_state(&root).unwrap(), (true, true, false));
        fs::remove_dir_all(root).unwrap();
    }
}
