use std::{cell::RefCell, collections::HashMap, rc::Rc, time::Duration};

use anyhow::{Context, Result};
use pipewire as pw;
use pw::{
    device::Device,
    proxy::{Listener, ProxyT},
    types::ObjectType,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize)]
pub struct AudioDevice {
    pub pipewire_id: u32,
    pub address: String,
    pub adapter: String,
    pub name: String,
    pub active_profile: Option<u32>,
    pub profiles: Vec<AudioProfile>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AudioProfile {
    pub index: u32,
    pub name: String,
    pub description: String,
    pub available: bool,
    pub priority: i32,
    pub mode: String,
    pub codec: Option<String>,
}

#[derive(Default)]
struct Objects {
    proxies: Vec<Box<dyn ProxyT>>,
    listeners: Vec<Box<dyn Listener>>,
}

pub fn probe() -> Result<Vec<AudioDevice>> {
    pw::init();
    let result = probe_inner();
    unsafe { pw::deinit() };
    result
}

fn probe_inner() -> Result<Vec<AudioDevice>> {
    let main_loop = pw::main_loop::MainLoopRc::new(None).context("create PipeWire main loop")?;
    let context =
        pw::context::ContextRc::new(&main_loop, None).context("create PipeWire context")?;
    let core = context.connect_rc(None).context("connect to PipeWire")?;
    let registry = core.get_registry_rc().context("open PipeWire registry")?;
    let devices = Rc::new(RefCell::new(HashMap::<u32, AudioDevice>::new()));
    let objects = Rc::new(RefCell::new(Objects::default()));
    let registry_weak = registry.downgrade();
    let devices_for_registry = Rc::clone(&devices);
    let objects_for_registry = Rc::clone(&objects);

    let _registry_listener = registry
        .add_listener_local()
        .global(move |global| {
            if global.type_ != ObjectType::Device {
                return;
            }
            let Some(properties) = global.props else {
                return;
            };
            if properties.get("device.api") != Some("bluez5") {
                return;
            }
            let Some(registry) = registry_weak.upgrade() else {
                return;
            };
            let Ok(device) = registry.bind::<Device, _>(global) else {
                return;
            };
            devices_for_registry.borrow_mut().insert(
                global.id,
                AudioDevice {
                    pipewire_id: global.id,
                    address: properties
                        .get("api.bluez5.address")
                        .unwrap_or_default()
                        .to_string(),
                    adapter: String::new(),
                    name: properties
                        .get("device.description")
                        .or_else(|| properties.get("device.alias"))
                        .unwrap_or("Bluetooth audio device")
                        .to_string(),
                    active_profile: None,
                    profiles: Vec::new(),
                },
            );
            let devices_for_info = Rc::clone(&devices_for_registry);
            let devices_for_params = Rc::clone(&devices_for_registry);
            let device_id = global.id;
            let listener = device
                .add_listener_local()
                .info(move |info| {
                    let Some(properties) = info.props() else {
                        return;
                    };
                    let mut devices = devices_for_info.borrow_mut();
                    let Some(audio_device) = devices.get_mut(&device_id) else {
                        return;
                    };
                    if let Some(address) = properties.get("api.bluez5.address") {
                        audio_device.address = address.to_string();
                    }
                    if let Some(path) = properties.get("api.bluez5.path") {
                        audio_device.adapter = adapter_from_bluez_path(path).unwrap_or_default();
                    }
                    if let Some(name) = properties
                        .get("device.description")
                        .or_else(|| properties.get("device.alias"))
                    {
                        audio_device.name = name.to_string();
                    }
                })
                .param(move |_sequence, parameter, _index, _next, pod| {
                    let Some(pod) = pod else { return };
                    let Some(profile) = parse_profile(pod) else {
                        return;
                    };
                    let mut devices = devices_for_params.borrow_mut();
                    let Some(audio_device) = devices.get_mut(&device_id) else {
                        return;
                    };
                    if parameter == pw::spa::param::ParamType::Profile {
                        audio_device.active_profile = Some(profile.index);
                    } else if parameter == pw::spa::param::ParamType::EnumProfile
                        && !audio_device
                            .profiles
                            .iter()
                            .any(|item| item.index == profile.index)
                    {
                        audio_device.profiles.push(profile);
                    }
                })
                .register();
            device.enum_params(1, Some(pw::spa::param::ParamType::EnumProfile), 0, u32::MAX);
            device.enum_params(2, Some(pw::spa::param::ParamType::Profile), 0, 1);
            let mut objects = objects_for_registry.borrow_mut();
            objects.proxies.push(Box::new(device));
            objects.listeners.push(Box::new(listener));
        })
        .register();

    let main_loop_for_timer = main_loop.clone();
    let timer = main_loop
        .loop_()
        .add_timer(move |_| main_loop_for_timer.quit());
    timer
        .update_timer(Some(Duration::from_millis(750)), None)
        .into_result()
        .context("arm PipeWire probe timer")?;
    main_loop.run();

    let mut result = devices.borrow().values().cloned().collect::<Vec<_>>();
    for device in &mut result {
        device.profiles.sort_by_key(|profile| -profile.priority);
    }
    result.sort_by_key(|device| device.name.to_lowercase());
    Ok(result)
}

pub fn profile_key(device_key: &str, profile_name: &str) -> String {
    let digest = Sha256::digest(format!("{device_key}:{profile_name}").as_bytes());
    format!("audio-profile-{}", hex::encode(&digest[..12]))
}

fn adapter_from_bluez_path(path: &str) -> Option<String> {
    path.split('/')
        .find(|part| part.starts_with("hci"))
        .map(str::to_string)
}

fn parse_profile(pod: &pw::spa::pod::Pod) -> Option<AudioProfile> {
    use pw::spa::pod::{Value, deserialize::PodDeserializer};

    let (_, Value::Object(object)) =
        PodDeserializer::deserialize_from::<Value>(pod.as_bytes()).ok()?
    else {
        return None;
    };
    let mut index = None;
    let mut name = None;
    let mut description = None;
    let mut available = true;
    let mut priority = 0;
    for property in object.properties {
        match (property.key, property.value) {
            (key, Value::Int(value)) if key == pw::spa::sys::SPA_PARAM_PROFILE_index => {
                index = u32::try_from(value).ok();
            }
            (key, Value::String(value)) if key == pw::spa::sys::SPA_PARAM_PROFILE_name => {
                name = Some(value);
            }
            (key, Value::String(value)) if key == pw::spa::sys::SPA_PARAM_PROFILE_description => {
                description = Some(value);
            }
            (key, Value::Id(value)) if key == pw::spa::sys::SPA_PARAM_PROFILE_available => {
                available = value.0 != pw::spa::sys::SPA_PARAM_AVAILABILITY_no;
            }
            (key, Value::Int(value)) if key == pw::spa::sys::SPA_PARAM_PROFILE_priority => {
                priority = value;
            }
            _ => {}
        }
    }
    let name = name.unwrap_or_default();
    let description = description.unwrap_or_default();
    Some(AudioProfile {
        index: index?,
        mode: profile_mode(&name).to_string(),
        codec: profile_codec(&description),
        name,
        description,
        available,
        priority,
    })
}

fn profile_mode(name: &str) -> &'static str {
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

fn profile_codec(description: &str) -> Option<String> {
    let marker = "codec ";
    let start = description.rfind(marker)? + marker.len();
    let value = description[start..].trim_end_matches(')').trim();
    (!value.is_empty()).then(|| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::{adapter_from_bluez_path, profile_codec, profile_key, profile_mode};

    #[test]
    fn classifies_bluetooth_audio_profiles() {
        assert_eq!(profile_mode("a2dp-sink-sbc_xq"), "high-fidelity");
        assert_eq!(profile_mode("headset-head-unit"), "headset");
        assert_eq!(profile_mode("off"), "off");
        assert_eq!(
            profile_codec("High Fidelity Playback (A2DP Sink, codec AAC)").as_deref(),
            Some("AAC")
        );
        assert_eq!(
            adapter_from_bluez_path("/org/bluez/hci0/dev_AA"),
            Some("hci0".into())
        );
        let key = profile_key("device-opaque", "a2dp-sink");
        assert!(key.starts_with("audio-profile-"));
        assert!(!key.contains("a2dp"));
    }
}
