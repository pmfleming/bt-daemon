//! Sony Headphones Connect framing and noise-cancelling/ambient-sound (NC/ASM) payloads.
//!
//! A frame is `3e kind seq length(be32) payload checksum 3c`. Bytes 3c/3d/3e
//! inside the frame are sent as `3d (byte & ef)`. The checksum is the byte sum
//! of kind, sequence, length and payload. Only the v2 command set (WF-1000XM4
//! and newer; eight-byte init reply) is handled here.
use anyhow::{Context, Result, bail, ensure};

use crate::framing::{self, DelimitedFrames};

const START: u8 = 0x3e;
const END: u8 = 0x3c;
const ESCAPE: u8 = 0x3d;
const ESCAPE_MASK: u8 = 0xef;
pub(super) const ACK: u8 = 0x01;
pub(super) const COMMAND: u8 = 0x0c;
pub(super) const COMMAND_2: u8 = 0x0e;
pub(super) const INIT_REQUEST: [u8; 2] = [0x00, 0x00];
pub(super) const INIT_REPLY: u8 = 0x01;
const NC_ASM_GET: u8 = 0x66;
pub(super) const NC_ASM_RET: u8 = 0x67;
const NC_ASM_SET: u8 = 0x68;
pub(super) const NC_ASM_NOTIFY: u8 = 0x69;
/// NC/ASM inquired types. 0x17 is the newer variant (wind-noise capable models,
/// WF-1000XM5); 0x15 is the older one. Query 0x17 last so it wins when both answer.
const NC_ASM_TYPES: [u8; 2] = [0x15, 0x17];
pub(super) const MODES: [&str; 3] = ["transparent", "off", "noise-cancelling"];

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Frame {
    pub kind: u8,
    pub sequence: u8,
    pub payload: Vec<u8>,
}

pub(super) fn encode(kind: u8, sequence: u8, payload: &[u8]) -> Result<Vec<u8>> {
    let length = u32::try_from(payload.len()).context("Sony frame payload is too large")?;
    let mut body = Vec::with_capacity(payload.len() + 7);
    body.extend_from_slice(&[kind, sequence]);
    body.extend_from_slice(&length.to_be_bytes());
    body.extend_from_slice(payload);
    body.push(checksum(&body));
    Ok(framing::escape(&body, (START, END), ESCAPE, |byte| {
        byte & ESCAPE_MASK
    }))
}

fn checksum(bytes: &[u8]) -> u8 {
    bytes.iter().fold(0_u8, |sum, byte| sum.wrapping_add(*byte))
}

fn decode(escaped: &[u8]) -> Result<Frame> {
    let bytes = framing::unescape(escaped, ESCAPE, |byte| byte | !ESCAPE_MASK)
        .context("decode Sony frame escaping")?;
    let Some((&received, body)) = bytes.split_last() else {
        bail!("empty Sony frame");
    };
    let [kind, sequence, a, b, c, d, payload @ ..] = body else {
        bail!("Sony frame header is truncated");
    };
    let length = u32::from_be_bytes([*a, *b, *c, *d]) as usize;
    ensure!(
        length == payload.len(),
        "Sony frame length {length} does not match payload {}",
        payload.len()
    );
    ensure!(checksum(body) == received, "Sony frame checksum mismatch");
    Ok(Frame {
        kind: *kind,
        sequence: *sequence,
        payload: payload.to_vec(),
    })
}

/// Splits the byte stream on start/end markers; bytes outside a frame are noise.
#[derive(Default)]
pub(super) struct FrameDecoder(DelimitedFrames);

impl FrameDecoder {
    pub fn push(&mut self, bytes: &[u8]) -> Vec<Result<Frame>> {
        self.0.push(bytes, (START, END), "Sony", decode)
    }
}

/// NC/ASM state as reported by the headphones. Focus-on-voice and the ambient
/// level are echoed back unchanged when only the mode changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NcAsm {
    inquired: u8,
    enabled: bool,
    ambient: bool,
    /// Present on wind-noise capable models: 0x02 normal, 0x03/0x05 wind reduction.
    wind: Option<u8>,
    focus_on_voice: bool,
    level: u8,
}

impl NcAsm {
    /// Wind-noise reduction has no vendor-neutral mode, so it reports as unknown.
    pub fn active_mode(&self) -> Option<&'static str> {
        match (self.enabled, self.ambient, self.wind) {
            (false, _, _) => Some("off"),
            (true, _, Some(0x03 | 0x05)) => None,
            (true, true, _) => Some("transparent"),
            (true, false, _) => Some("noise-cancelling"),
        }
    }
}

pub(super) fn queries() -> impl Iterator<Item = Vec<u8>> {
    NC_ASM_TYPES
        .into_iter()
        .map(|inquired| vec![NC_ASM_GET, inquired])
}

/// Decodes NC/ASM return (0x67) and notify (0x69) payloads.
pub(super) fn decode_nc_asm(payload: &[u8]) -> Result<NcAsm> {
    let (inquired, enabled, ambient, wind, voice, level) = match *payload {
        [_, inquired, _, enabled, ambient, voice, level] => {
            (inquired, enabled, ambient, None, voice, level)
        }
        [_, inquired, _, enabled, ambient, wind, voice, level] => {
            (inquired, enabled, ambient, Some(wind), voice, level)
        }
        _ => bail!("Sony noise-control report has length {}", payload.len()),
    };
    ensure!(
        NC_ASM_TYPES.contains(&inquired),
        "unsupported Sony noise-control type 0x{inquired:02x}"
    );
    ensure!(
        enabled <= 1 && ambient <= 1 && voice <= 1 && level <= 20,
        "invalid Sony noise-control report"
    );
    Ok(NcAsm {
        inquired,
        enabled: enabled == 1,
        ambient: ambient == 1,
        wind,
        focus_on_voice: voice == 1,
        level,
    })
}

pub(super) fn nc_asm_payload(state: &NcAsm, mode: &str) -> Result<Vec<u8>> {
    let (enabled, ambient) = match mode {
        "noise-cancelling" => (1, 0),
        "transparent" => (1, 1),
        "off" => (0, 0),
        _ => bail!(
            "unsupported Sony noise-control mode {mode}; expected transparent, off, or noise-cancelling"
        ),
    };
    // 0x01 marks a settled change; the Sony app sends 0x00 while a slider moves.
    let mut payload = vec![NC_ASM_SET, state.inquired, 0x01, enabled, ambient];
    if state.wind.is_some() {
        payload.push(0x02);
    }
    payload.extend_from_slice(&[u8::from(state.focus_on_voice), state.level]);
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::{FrameDecoder, decode_nc_asm, encode, nc_asm_payload};

    // Captured from a WF-1000XM5 (firmware reply 01 00 03 00 30 18 00 00).
    const XM5_NC: [u8; 7] = [0x67, 0x17, 0x01, 0x01, 0x00, 0x00, 0x14];

    #[test]
    fn frames_escape_markers_and_round_trip_through_noise() {
        assert_eq!(
            encode(0x0c, 0, &[0x00, 0x00]).unwrap(),
            [0x3e, 0x0c, 0, 0, 0, 0, 2, 0, 0, 0x0e, 0x3c]
        );
        // 0x3e in the payload and a 0x3c checksum must both be escaped.
        let escaped = encode(0x0c, 1, &[0x3e, 0xef]).unwrap();
        assert_eq!(
            escaped,
            [
                0x3e, 0x0c, 1, 0, 0, 0, 2, 0x3d, 0x2e, 0xef, 0x3d, 0x2c, 0x3c
            ]
        );

        let mut decoder = FrameDecoder::default();
        let mut stream = vec![0xaa];
        stream.extend(encode(0x01, 1, &[]).unwrap());
        stream.extend(&escaped);
        let (first, rest) = stream.split_at(9);
        assert!(decoder.push(first).is_empty());
        let frames: Vec<_> = decoder.push(rest).into_iter().map(Result::unwrap).collect();
        assert_eq!(frames.len(), 2);
        assert_eq!((frames[0].kind, frames[0].sequence), (0x01, 1));
        assert!(frames[0].payload.is_empty());
        assert_eq!(frames[1].payload, [0x3e, 0xef]);

        let mut corrupt = encode(0x0c, 0, &[0x66, 0x17]).unwrap();
        corrupt[8] ^= 1;
        assert!(decoder.push(&corrupt)[0].is_err());
        assert!(decoder.push(&[0x3e, 0x0c, 0x3c])[0].is_err());
        assert!(decoder.push(&[0x3e, 0x0c, 0x3d, 0x3c])[0].is_err());
        assert!(
            decoder.push(&[0x3e, 0x3c])[0].is_err(),
            "empty Sony frames are invalid"
        );
        assert!(decoder.push(&[0x3e]).is_empty());
        assert!(decoder.push(&[0; 4095]).is_empty());
        assert!(decoder.push(&[0])[0].is_err(), "bound incomplete frames");
        assert_eq!(
            decoder.push(&escaped)[0].as_ref().unwrap().payload,
            [0x3e, 0xef]
        );
    }

    #[test]
    fn nc_asm_modes_preserve_layout_voice_and_level_and_reject_invalid_reports() {
        for (report, active, tail) in [
            (&XM5_NC[..], Some("noise-cancelling"), &[0, 20][..]),
            (&[0x69, 0x17, 1, 1, 0, 3, 1, 10], None, &[2, 1, 10]),
            (&[0x69, 0x17, 1, 1, 0, 5, 1, 10], None, &[2, 1, 10]),
            (&[0x67, 0x15, 1, 0, 1, 0, 20], Some("off"), &[0, 20]),
        ] {
            let state = decode_nc_asm(report).unwrap();
            assert_eq!(state.active_mode(), active);
            for (mode, enabled, ambient) in [
                ("transparent", 1, 1),
                ("off", 0, 0),
                ("noise-cancelling", 1, 0),
            ] {
                let mut expected = vec![0x68, report[1], 1, enabled, ambient];
                expected.extend_from_slice(tail);
                assert_eq!(nc_asm_payload(&state, mode).unwrap(), expected);
                expected[0] = 0x69;
                assert_eq!(decode_nc_asm(&expected).unwrap().active_mode(), Some(mode));
            }
            assert!(nc_asm_payload(&state, "adaptive").is_err());
        }
        for invalid in [
            &[0x67, 0x17, 1, 1, 0, 0][..],
            &[0x67, 0x22, 1, 1, 0, 0, 20],
            &[0x67, 0x17, 1, 2, 0, 0, 20],
            &[0x67, 0x17, 1, 1, 2, 0, 20],
            &[0x67, 0x17, 1, 1, 0, 2, 20],
            &[0x67, 0x17, 1, 1, 0, 0, 21],
        ] {
            assert!(decode_nc_asm(invalid).is_err(), "accepted {invalid:?}");
        }
    }
}
