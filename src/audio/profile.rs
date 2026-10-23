use pipewire as pw;

use super::AudioProfile;

pub(super) fn parse_profile(pod: &pw::spa::pod::Pod) -> Option<AudioProfile> {
    use pw::spa::pod::{Value, deserialize::PodDeserializer};

    let (_, Value::Object(object)) =
        PodDeserializer::deserialize_from::<Value>(pod.as_bytes()).ok()?
    else {
        return None;
    };
    if object.type_ != pw::spa::sys::SPA_TYPE_OBJECT_ParamProfile {
        return None;
    }
    let mut values = ProfileValues::default();
    for property in object.properties {
        values.apply(property.key, property.value);
    }
    values.finish()
}

#[derive(Default)]
struct ProfileValues {
    index: Option<u32>,
    name: Option<String>,
    description: Option<String>,
    available: bool,
    priority: i32,
}

impl ProfileValues {
    fn apply(&mut self, key: u32, value: pw::spa::pod::Value) {
        use pw::spa::pod::Value;

        match (key, value) {
            (pw::spa::sys::SPA_PARAM_PROFILE_index, Value::Int(value)) => {
                self.index = u32::try_from(value).ok()
            }
            (pw::spa::sys::SPA_PARAM_PROFILE_name, Value::String(value)) => self.name = Some(value),
            (pw::spa::sys::SPA_PARAM_PROFILE_description, Value::String(value)) => {
                self.description = Some(value)
            }
            (pw::spa::sys::SPA_PARAM_PROFILE_available, Value::Id(value)) => {
                self.available = value.0 != pw::spa::sys::SPA_PARAM_AVAILABILITY_no
            }
            (pw::spa::sys::SPA_PARAM_PROFILE_priority, Value::Int(value)) => self.priority = value,
            _ => {}
        }
    }

    fn finish(self) -> Option<AudioProfile> {
        let name = self.name.unwrap_or_default();
        let description = self.description.unwrap_or_default();
        Some(AudioProfile {
            index: self.index?,
            mode: profile_mode(&name).into(),
            codec: profile_codec(&name),
            name,
            description,
            available: self.available,
            priority: self.priority,
        })
    }
}

pub(super) fn profile_mode(name: &str) -> &'static str {
    if name == "off" {
        return "off";
    }
    [
        ("a2dp-", "high-fidelity"),
        ("headset-", "headset"),
        ("handsfree-", "headset"),
        ("bap-", "le-audio"),
        ("asha-", "hearing-aid"),
    ]
    .into_iter()
    .find_map(|(prefix, mode)| name.starts_with(prefix).then_some(mode))
    .unwrap_or("other")
}

#[cfg(test)]
mod tests;

/// Profile names are machine identifiers; translated descriptions are not data.
/// The highest-priority codec has an unsuffixed name, so its codec remains unknown
/// until an active endpoint reports api.bluez5.codec.
pub(super) fn profile_codec(name: &str) -> Option<String> {
    [
        "a2dp-sink",
        "a2dp-source",
        "a2dp-duplex",
        "headset-head-unit",
        "handsfree-head-unit",
        "bap-broadcast-sink",
        "bap-broadcast-source",
        "bap-sink",
        "bap-source",
        "bap-duplex",
        "asha-sink",
    ]
    .into_iter()
    .find_map(|base| {
        name.strip_prefix(base)?
            .strip_prefix('-')
            .and_then(codec_name)
    })
}

pub(super) fn codec_name(value: &str) -> Option<String> {
    (!value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.')))
    .then(|| value.to_owned())
}
