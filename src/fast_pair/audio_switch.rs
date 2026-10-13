//! Google Audio Switch Message Stream notifications. Full advertisement-driven
//! Audio Switch seeker policy is not advertised: Shelllist does not implement it.
use crate::model::FastPairSwitchEvent;
use anyhow::{Result, ensure};

pub(super) fn decode_switch(payload: &[u8]) -> Result<FastPairSwitchEvent> {
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
        observed_at_ms: 0,
    })
}
