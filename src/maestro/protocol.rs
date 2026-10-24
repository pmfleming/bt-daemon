//! Maestro framing, pw_rpc packets and the runtime-info battery fields.
//!
//! Each RPC packet is a protobuf `pw.rpc.packet.RpcPacket` carried in an HDLC
//! U-frame: `7e address control(03) packet crc32(le) 7e`, where 7d/7e inside
//! the frame are sent as `7d (byte ^ 20)` and the address is a varint holding
//! the local and remote Maestro peers. Service and method ids are the pw_rpc
//! 65599 hashes of their names. Field numbers follow pbpctrl's maestro_pw.proto.
use anyhow::{Context, Result, bail, ensure};

use crate::framing::{self, DelimitedFrames};

const FLAG: u8 = 0x7e;
const ESCAPE: u8 = 0x7d;
const ESCAPE_MASK: u8 = 0x20;
const UNNUMBERED_INFORMATION: u8 = 0x03;

const REQUEST: u32 = 0;
const RESPONSE: u32 = 1;
const SERVER_ERROR: u32 = 5;
const SERVER_STREAM: u32 = 7;

const MAESTRO_SERVICE: u32 = id("maestro_pw.Maestro");
const GET_SOFTWARE_INFO: u32 = id("GetSoftwareInfo");
const SUBSCRIBE_RUNTIME_INFO: u32 = id("SubscribeRuntimeInfo");
const SUBSCRIBE_CALL: u32 = 0;

const CASE: u32 = 2;
const LEFT_BT_CORE: u32 = 3;
const RIGHT_BT_CORE: u32 = 4;
const MAESTRO_A: u32 = 10;
const MAESTRO_B: u32 = 13;
/// Channels the buds may answer on, with their local and remote peers.
const CHANNELS: [(u32, u32, u32); 6] = [
    (18, MAESTRO_A, CASE),
    (19, MAESTRO_A, LEFT_BT_CORE),
    (21, MAESTRO_A, RIGHT_BT_CORE),
    (23, MAESTRO_B, CASE),
    (24, MAESTRO_B, LEFT_BT_CORE),
    (26, MAESTRO_B, RIGHT_BT_CORE),
];

/// pw_rpc's 65599 hash of a service or method name.
const fn id(name: &str) -> u32 {
    let bytes = name.as_bytes();
    let mut hash = bytes.len() as u32;
    let mut coefficient = 65599_u32;
    let mut index = 0;
    while index < bytes.len() {
        hash = hash.wrapping_add(coefficient.wrapping_mul(bytes[index] as u32));
        coefficient = coefficient.wrapping_mul(65599);
        index += 1;
    }
    hash
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Packet {
    kind: u32,
    channel: u32,
    service: u32,
    method: u32,
    payload: Vec<u8>,
    status: u32,
    call: u32,
}

/// The RPC channel the buds answered on and its HDLC address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Channel {
    pub id: u32,
    address: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Reading {
    pub level: u8,
    pub charging: Option<bool>,
}

/// One runtime-info battery report; the case is unknown unless a bud is docked.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Batteries {
    pub left: Option<Reading>,
    pub right: Option<Reading>,
    pub case: Option<Reading>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Update {
    Batteries(Batteries),
    /// The buds completed or rejected the subscription with this status.
    Ended(u32),
    Ignored,
}

/// The buds send a GetSoftwareInfo response on their channel once the link opens.
pub(super) fn announced_channel(packet: &Packet) -> Option<Channel> {
    if (packet.kind, packet.service, packet.method)
        != (RESPONSE, MAESTRO_SERVICE, GET_SOFTWARE_INFO)
    {
        return None;
    }
    CHANNELS
        .into_iter()
        .find(|(id, ..)| *id == packet.channel)
        .map(|(id, local, remote)| Channel {
            id,
            address: (local << 6) | (remote << 10),
        })
}

pub(super) fn subscribe_runtime_info(channel: Channel) -> Vec<u8> {
    let packet = Packet {
        kind: REQUEST,
        channel: channel.id,
        service: MAESTRO_SERVICE,
        method: SUBSCRIBE_RUNTIME_INFO,
        call: SUBSCRIBE_CALL,
        ..Packet::default()
    };
    encode_frame(channel.address, &encode_packet(&packet))
}

pub(super) fn subscription_update(packet: &Packet, channel: Channel) -> Result<Update> {
    if (packet.channel, packet.service, packet.method, packet.call)
        != (
            channel.id,
            MAESTRO_SERVICE,
            SUBSCRIBE_RUNTIME_INFO,
            SUBSCRIBE_CALL,
        )
    {
        return Ok(Update::Ignored);
    }
    match packet.kind {
        SERVER_STREAM => {
            Ok(runtime_batteries(&packet.payload)?.map_or(Update::Ignored, Update::Batteries))
        }
        RESPONSE | SERVER_ERROR => Ok(Update::Ended(packet.status)),
        _ => Ok(Update::Ignored),
    }
}

/// Decodes `RuntimeInfo.battery_info` (field 6); reports without it carry no battery.
fn runtime_batteries(payload: &[u8]) -> Result<Option<Batteries>> {
    let Some(info) = fields(payload)?.into_iter().find_map(|field| match field {
        (6, Value::Bytes(info)) => Some(info),
        _ => None,
    }) else {
        return Ok(None);
    };
    let mut batteries = Batteries::default();
    for field in fields(info)? {
        let (slot, reading) = match field {
            (1, Value::Bytes(reading)) => (&mut batteries.case, reading),
            (2, Value::Bytes(reading)) => (&mut batteries.left, reading),
            (3, Value::Bytes(reading)) => (&mut batteries.right, reading),
            _ => continue,
        };
        *slot = Some(decode_reading(reading)?);
    }
    Ok(Some(batteries))
}

fn decode_reading(bytes: &[u8]) -> Result<Reading> {
    let (mut level, mut state) = (0, 0);
    for field in fields(bytes)? {
        match field {
            (1, Value::Varint(value)) => level = value,
            (2, Value::Varint(value)) => state = value,
            _ => {}
        }
    }
    let level = u8::try_from(level)
        .ok()
        .filter(|level| *level <= 100)
        .with_context(|| format!("invalid Maestro battery level {level}"))?;
    let charging = match state {
        1 => Some(false),
        2 => Some(true),
        _ => None,
    };
    Ok(Reading { level, charging })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Value<'a> {
    Varint(u64),
    Fixed32(u32),
    Bytes(&'a [u8]),
}

fn fields(mut bytes: &[u8]) -> Result<Vec<(u32, Value<'_>)>> {
    let mut fields = Vec::new();
    while !bytes.is_empty() {
        let key = read_varint(&mut bytes)?;
        let field = u32::try_from(key >> 3).context("protobuf field number is too large")?;
        let value = match key & 0x07 {
            0 => Value::Varint(read_varint(&mut bytes)?),
            1 => {
                // No Maestro field read here is a fixed64.
                take(&mut bytes, 8)?;
                continue;
            }
            2 => {
                let length = usize::try_from(read_varint(&mut bytes)?)?;
                Value::Bytes(take(&mut bytes, length)?)
            }
            5 => Value::Fixed32(u32::from_le_bytes(take(&mut bytes, 4)?.try_into()?)),
            wire => bail!("unsupported protobuf wire type {wire}"),
        };
        fields.push((field, value));
    }
    Ok(fields)
}

fn take<'a>(bytes: &mut &'a [u8], length: usize) -> Result<&'a [u8]> {
    ensure!(bytes.len() >= length, "protobuf field is truncated");
    let (taken, rest) = bytes.split_at(length);
    *bytes = rest;
    Ok(taken)
}

fn read_varint(bytes: &mut &[u8]) -> Result<u64> {
    let mut value = 0_u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = bytes
            .split_first()
            .context("protobuf varint is truncated")?;
        *bytes = rest;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    bail!("protobuf varint is too long")
}

fn put_varint(bytes: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        bytes.push((value as u8) | 0x80);
        value >>= 7;
    }
    bytes.push(value as u8);
}

/// Encodes like protobuf3, in field order: zero and empty fields are omitted.
fn encode_packet(packet: &Packet) -> Vec<u8> {
    fn varint(bytes: &mut Vec<u8>, field: u64, value: u32) {
        if value != 0 {
            put_varint(bytes, field << 3);
            put_varint(bytes, value.into());
        }
    }
    fn fixed32(bytes: &mut Vec<u8>, field: u64, value: u32) {
        if value != 0 {
            put_varint(bytes, (field << 3) | 5);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
    }
    let mut bytes = Vec::new();
    varint(&mut bytes, 1, packet.kind);
    varint(&mut bytes, 2, packet.channel);
    fixed32(&mut bytes, 3, packet.service);
    fixed32(&mut bytes, 4, packet.method);
    if !packet.payload.is_empty() {
        put_varint(&mut bytes, (5 << 3) | 2);
        put_varint(&mut bytes, packet.payload.len() as u64);
        bytes.extend_from_slice(&packet.payload);
    }
    varint(&mut bytes, 6, packet.status);
    varint(&mut bytes, 7, packet.call);
    bytes
}

fn decode_packet(bytes: &[u8]) -> Result<Packet> {
    let mut packet = Packet::default();
    for field in fields(bytes)? {
        let (target, value) = match field {
            (1, Value::Varint(value)) => (&mut packet.kind, value),
            (2, Value::Varint(value)) => (&mut packet.channel, value),
            (3, Value::Fixed32(value)) => (&mut packet.service, u64::from(value)),
            (4, Value::Fixed32(value)) => (&mut packet.method, u64::from(value)),
            (5, Value::Bytes(value)) => {
                packet.payload = value.to_vec();
                continue;
            }
            (6, Value::Varint(value)) => (&mut packet.status, value),
            (7, Value::Varint(value)) => (&mut packet.call, value),
            _ => continue,
        };
        *target = u32::try_from(value).context("pw_rpc packet field is too large")?;
    }
    Ok(packet)
}

/// Bitwise CRC-32 (IEEE, reflected), as used by pw_hdlc.
fn crc32(bytes: &[u8]) -> u32 {
    !bytes.iter().fold(!0_u32, |crc, byte| {
        (0..8).fold(crc ^ u32::from(*byte), |crc, _| {
            if crc & 1 == 0 {
                crc >> 1
            } else {
                (crc >> 1) ^ 0xedb8_8320
            }
        })
    })
}

/// HDLC addresses use 7 bits per byte, shifted left; bit 0 marks the last byte.
fn put_address(bytes: &mut Vec<u8>, mut address: u32) {
    while address >> 7 != 0 {
        bytes.push(((address & 0x7f) as u8) << 1);
        address >>= 7;
    }
    bytes.push(((address as u8) << 1) | 1);
}

fn encode_frame(address: u32, data: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(data.len() + 8);
    put_address(&mut body, address);
    body.push(UNNUMBERED_INFORMATION);
    body.extend_from_slice(data);
    body.extend_from_slice(&crc32(&body).to_le_bytes());
    framing::escape(&body, (FLAG, FLAG), ESCAPE, |byte| byte ^ ESCAPE_MASK)
}

fn decode_frame(escaped: &[u8]) -> Result<Packet> {
    let body = framing::unescape(escaped, ESCAPE, |byte| byte ^ ESCAPE_MASK)
        .context("decode Maestro frame escaping")?;
    ensure!(body.len() >= 6, "Maestro frame is truncated");
    let (content, received) = body.split_at(body.len() - 4);
    ensure!(
        crc32(content) == u32::from_le_bytes(received.try_into()?),
        "Maestro frame checksum mismatch"
    );
    let address_length = content
        .iter()
        .position(|byte| byte & 1 == 1)
        .context("Maestro frame address is unterminated")?;
    let [control, packet @ ..] = &content[address_length + 1..] else {
        bail!("Maestro frame has no control byte");
    };
    ensure!(
        *control == UNNUMBERED_INFORMATION,
        "unsupported Maestro HDLC control 0x{control:02x}"
    );
    decode_packet(packet)
}

/// Splits the byte stream on flags; a closing flag may also open the next frame.
#[derive(Default)]
pub(super) struct FrameDecoder(DelimitedFrames);

impl FrameDecoder {
    pub fn push(&mut self, bytes: &[u8]) -> Vec<Result<Packet>> {
        self.0.push(bytes, (FLAG, FLAG), "Maestro", decode_frame)
    }
}

/// Packets as the buds send them on the channel they announced in hardware
/// testing (MaestroB to RightBtCore, channel 26).
#[cfg(test)]
pub(super) mod fixtures {
    use super::{
        GET_SOFTWARE_INFO, MAESTRO_SERVICE, Packet, RESPONSE, SERVER_STREAM,
        SUBSCRIBE_RUNTIME_INFO, encode_frame, encode_packet,
    };

    /// RightBtCore (4) to MaestroB (13), the reverse of our request address.
    const BUDS_ADDRESS: u32 = (4 << 6) | (13 << 10);

    /// A `DeviceBatteryInfo` with the level and `BatteryState` (1 idle, 2 charging).
    pub fn reading(level: u8, state: u8) -> Vec<u8> {
        vec![0x08, level, 0x10, state]
    }

    fn nested(field: u8, bytes: &[u8]) -> Vec<u8> {
        let mut encoded = vec![(field << 3) | 2, bytes.len() as u8];
        encoded.extend_from_slice(bytes);
        encoded
    }

    /// `RuntimeInfo` with the given `BatteryInfo` fields (1 case, 2 left, 3 right).
    pub fn runtime_info(batteries: &[(u8, Vec<u8>)]) -> Vec<u8> {
        let info: Vec<u8> = batteries
            .iter()
            .flat_map(|(field, reading)| nested(*field, reading))
            .collect();
        // timestamp_ms (2) precedes battery_info (6), as on the wire.
        let mut payload = vec![0x10, 0x96, 0x01];
        payload.extend(nested(6, &info));
        payload
    }

    pub fn announcement() -> Packet {
        Packet {
            kind: RESPONSE,
            channel: 26,
            service: MAESTRO_SERVICE,
            method: GET_SOFTWARE_INFO,
            call: u32::MAX,
            payload: vec![0x0a, 0x00],
            ..Packet::default()
        }
    }

    pub fn runtime_stream(payload: Vec<u8>) -> Packet {
        Packet {
            kind: SERVER_STREAM,
            channel: 26,
            service: MAESTRO_SERVICE,
            method: SUBSCRIBE_RUNTIME_INFO,
            payload,
            ..Packet::default()
        }
    }

    pub fn frame(packet: &Packet) -> Vec<u8> {
        encode_frame(BUDS_ADDRESS, &encode_packet(packet))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Batteries, FrameDecoder, MAESTRO_SERVICE, Packet, Reading, SUBSCRIBE_RUNTIME_INFO, Update,
        announced_channel, crc32, decode_frame, encode_frame,
        fixtures::{announcement, frame, reading, runtime_info, runtime_stream},
        id, subscribe_runtime_info, subscription_update,
    };

    #[test]
    fn ids_and_checksums_match_pw_rpc_and_pw_hdlc() {
        assert_eq!(MAESTRO_SERVICE, 0x7ede_71ea);
        assert_eq!(id("GetSoftwareInfo"), 0x7199_fa44);
        assert_eq!(id("SubscribeToSettingsChanges"), 0x2821_adf5);
        assert_eq!(crc32(b"test test test"), 0x235b_6a02);
        assert_eq!(crc32(b"1234321"), 0xd981_751c);
        assert_eq!(
            encode_frame(0x010203, &[]),
            [0x7e, 0x06, 0x08, 0x09, 0x03, 0x8b, 0x3b, 0xf7, 0x42, 0x7e]
        );
        assert_eq!(
            encode_frame(0x010203, &[0x05, 0x06, 0x07, 0x7d, 0x7e, 0x7f, 0xff]),
            [
                0x7e, 0x06, 0x08, 0x09, 0x03, 0x05, 0x06, 0x07, 0x7d, 0x5d, 0x7d, 0x5e, 0x7f, 0xff,
                0xe6, 0x2d, 0x17, 0xc6, 0x7e
            ]
        );
    }

    #[test]
    fn channel_announcement_selects_the_subscription_address() {
        let channel = announced_channel(&announcement()).unwrap();
        assert_eq!(channel.id, 26);
        // MaestroB (13) to RightBtCore (4): (13 << 6) | (4 << 10) = 0x1340.
        let request = subscribe_runtime_info(channel);
        assert_eq!(&request[..4], [0x7e, 0x80, 0x4d, 0x03]);
        let decoded = decode_frame(&request[1..request.len() - 1]).unwrap();
        assert_eq!(
            decoded,
            Packet {
                channel: 26,
                service: MAESTRO_SERVICE,
                method: SUBSCRIBE_RUNTIME_INFO,
                ..Packet::default()
            }
        );

        for ignored in [
            Packet {
                channel: 20,
                ..announcement()
            },
            Packet {
                kind: 7,
                ..announcement()
            },
            Packet {
                method: SUBSCRIBE_RUNTIME_INFO,
                ..announcement()
            },
        ] {
            assert_eq!(announced_channel(&ignored), None);
        }
    }

    #[test]
    fn runtime_info_reports_docked_case_and_charging_buds() {
        let channel = announced_channel(&announcement()).unwrap();
        // Buds docked: case 43% idle, both buds 100% charging.
        let docked = runtime_stream(runtime_info(&[
            (1, reading(43, 1)),
            (2, reading(100, 2)),
            (3, reading(100, 2)),
        ]));
        let observed = |level, charging| Some(Reading { level, charging });
        let buds = observed(100, Some(true));
        assert_eq!(
            subscription_update(&docked, channel).unwrap(),
            Update::Batteries(Batteries {
                case: observed(43, Some(false)),
                left: buds,
                right: buds,
            })
        );
        // Out of the case the buds omit the case reading.
        let worn = runtime_stream(runtime_info(&[(2, reading(90, 1)), (3, reading(88, 0))]));
        let Update::Batteries(worn) = subscription_update(&worn, channel).unwrap() else {
            panic!("expected a battery update");
        };
        assert_eq!(worn.case, None);
        assert_eq!(worn.right, observed(88, None));

        assert_eq!(
            subscription_update(&runtime_stream(vec![0x10, 0x01]), channel).unwrap(),
            Update::Ignored,
            "runtime info without battery_info"
        );
        let mut packet = docked;
        packet.channel = 21;
        assert_eq!(
            subscription_update(&packet, channel).unwrap(),
            Update::Ignored
        );
        packet.channel = channel.id;
        packet.kind = 5;
        packet.status = 12;
        assert_eq!(
            subscription_update(&packet, channel).unwrap(),
            Update::Ended(12)
        );
        let invalid = runtime_stream(runtime_info(&[(1, reading(101, 1))]));
        assert!(subscription_update(&invalid, channel).is_err());
    }

    #[test]
    fn decoder_shares_flags_and_rejects_corrupt_frames() {
        // Payload bytes that need escaping.
        let packet = runtime_stream(vec![0x7e, 0x7d]);
        let frame = frame(&packet);
        let mut stream = vec![0xaa];
        stream.extend(&frame);
        // A second frame reusing the first one's closing flag.
        stream.extend(&frame[1..]);
        let mut decoder = FrameDecoder::default();
        let (first, rest) = stream.split_at(6);
        assert!(decoder.push(first).is_empty());
        let packets: Vec<_> = decoder.push(rest).into_iter().map(Result::unwrap).collect();
        assert_eq!(packets.iter().collect::<Vec<_>>(), [&packet, &packet]);

        let mut corrupt = frame.clone();
        corrupt[5] ^= 1;
        assert!(decoder.push(&corrupt)[0].is_err());
        assert!(decoder.push(&[0x7e, 0x01, 0x03, 0x7e])[0].is_err());
        assert!(decoder.push(&[0x7e, 0x7d, 0x7e])[0].is_err());
        assert!(
            decoder.push(&[0x7e, 0x7e]).is_empty(),
            "idle flags are not packets"
        );
        assert!(decoder.push(&[0; 4095]).is_empty());
        assert!(decoder.push(&[0])[0].is_err(), "bound incomplete frames");
        assert!(decoder.push(&frame)[0].is_ok(), "recover after overflow");
    }
}
