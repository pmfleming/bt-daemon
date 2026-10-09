//! Wall-clock timestamps for published observations, never for expiry/deadlines.
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
