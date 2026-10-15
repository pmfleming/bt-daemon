//! Google Audio Switch Message Stream notifications. Full advertisement-driven
//! Audio Switch seeker policy is not advertised: Shelllist does not implement it.
use crate::model::FastPairSwitchEvent;
use anyhow::{Result, ensure};

/// Decode and timestamp a received notification, without advertising seeker support.
pub(super) fn notification(payload: &[u8]) -> Result<FastPairSwitchEvent> {
    ensure!(
        payload.len() >= 2,
        "Fast Pair switch notification is missing reason/target"
    );
    let reason = match payload[0] {
        0 => "unspecified",
        1 => "media",
        2 => "call",
        _ => "unknown",
    };
    let target = match payload[1] {
        1 => "this-device",
        2 => "another-device",
        _ => "unknown",
    };
    // This is a remote-controlled display string, not markup, a shell argument,
    // or a trustworthy identity. Strip controls/bidi formatting and bound it.
    let name = std::str::from_utf8(&payload[2..])?
        .chars()
        .filter(|c| {
            !c.is_control() && !matches!(*c, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        })
        .take(128)
        .collect::<String>();
    Ok(FastPairSwitchEvent {
        reason: reason.into(),
        target: target.into(),
        target_name: (!name.is_empty()).then_some(name),
        observed_at_ms: crate::time::unix_time_ms(),
    })
}

#[cfg(test)]
mod tests {
    use super::notification;

    #[test]
    fn notifications_are_timestamped_and_sanitize_bounded_remote_names() {
        assert!(notification(&[1]).is_err());
        assert!(notification(&[1, 2, 0xff]).is_err());
        let mut payload = vec![1, 2];
        payload.extend_from_slice("\n\u{202e}Buds".as_bytes());
        let event = notification(&payload).unwrap();
        assert_eq!(event.reason, "media");
        assert_eq!(event.target, "another-device");
        assert_eq!(event.target_name.as_deref(), Some("Buds"));
        assert!(event.observed_at_ms > 0);
        assert!(notification(&[9, 9]).unwrap().target_name.is_none());
        payload.extend_from_slice(&[b'x'; 200]);
        assert_eq!(
            notification(&payload).unwrap().target_name.unwrap().len(),
            128
        );
    }
}
