use pipewire as pw;
use sha2::{Digest, Sha256};

use super::AudioProfile;

pub fn profile_key(device_key: &str, profile_name: &str) -> String {
    opaque_audio_key("profile", device_key, profile_name)
}

pub fn endpoint_key(device_key: &str, kind: &str) -> String {
    opaque_audio_key("endpoint", device_key, kind)
}

fn opaque_audio_key(kind: &str, device_key: &str, value: &str) -> String {
    let digest = Sha256::digest(format!("{device_key}:{value}").as_bytes());
    format!("audio-{kind}-{}", hex::encode(&digest[..12]))
}

pub(super) fn parse_profile(pod: &pw::spa::pod::Pod) -> Option<AudioProfile> {
    use pw::spa::pod::{Value, deserialize::PodDeserializer};

    let (_, Value::Object(object)) =
        PodDeserializer::deserialize_from::<Value>(pod.as_bytes()).ok()?
    else {
        return None;
    };
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

        match value {
            Value::Int(value) if key == pw::spa::sys::SPA_PARAM_PROFILE_index => {
                self.index = u32::try_from(value).ok();
            }
            Value::String(value) if key == pw::spa::sys::SPA_PARAM_PROFILE_name => {
                self.name = Some(value);
            }
            Value::String(value) if key == pw::spa::sys::SPA_PARAM_PROFILE_description => {
                self.description = Some(value);
            }
            Value::Id(value) if key == pw::spa::sys::SPA_PARAM_PROFILE_available => {
                self.available = value.0 != pw::spa::sys::SPA_PARAM_AVAILABILITY_no;
            }
            Value::Int(value) if key == pw::spa::sys::SPA_PARAM_PROFILE_priority => {
                self.priority = value;
            }
            _ => {}
        }
    }

    fn finish(self) -> Option<AudioProfile> {
        let name = self.name.unwrap_or_default();
        let description = self.description.unwrap_or_default();
        Some(AudioProfile {
            index: self.index?,
            mode: profile_mode(&name).into(),
            codec: profile_codec(&description),
            name,
            description,
            available: self.available,
            priority: self.priority,
        })
    }
}

pub(super) fn profile_mode(name: &str) -> &'static str {
    if name.starts_with("a2dp-") {
        "high-fidelity"
    } else if name.starts_with("headset-") || name.starts_with("handsfree-") {
        "headset"
    } else if name == "off" {
        "off"
    } else {
        "other"
    }
}

pub(super) fn profile_codec(description: &str) -> Option<String> {
    let marker = "codec ";
    let start = description.rfind(marker)? + marker.len();
    let value = description[start..].trim_end_matches(')').trim();
    (!value.is_empty()).then(|| value.to_string())
}
