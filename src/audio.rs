use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, HashSet},
    io::Cursor,
    rc::Rc,
    sync::Once,
    time::Duration,
};

use anyhow::{Context, Result};
use pipewire as pw;
use pw::{
    device::Device,
    metadata::Metadata,
    node::{Node, NodeState},
    proxy::{Listener, ProxyT},
    types::ObjectType,
};
use serde::Serialize;

mod profile;
use profile::parse_profile;
pub use profile::{endpoint_key, profile_key};

macro_rules! bind_or_return {
    ($registry:expr, $global:expr, $kind:ty) => {
        match $registry.bind::<$kind, _>($global) {
            Ok(proxy) => proxy,
            Err(error) => {
                tracing::warn!(id = $global.id, %error, kind = stringify!($kind), "could not bind PipeWire object");
                return;
            }
        }
    };
}

#[derive(Debug, Clone, Serialize)]
pub struct AudioDevice {
    pub pipewire_id: u32,
    pub address: String,
    pub adapter: String,
    pub name: String,
    pub active_profile: Option<u32>,
    pub profiles: Vec<AudioProfile>,
    pub sink: Option<AudioEndpoint>,
    pub source: Option<AudioEndpoint>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AudioEndpoint {
    pub name: String,
    pub state: String,
    pub is_default: bool,
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

type RetainedObject = (Box<dyn ProxyT>, Box<dyn Listener>);

#[derive(Default)]
struct Objects {
    retained: HashMap<u32, RetainedObject>,
}

impl Objects {
    fn retain(&mut self, id: u32, proxy: impl ProxyT + 'static, listener: impl Listener + 'static) {
        self.retained
            .insert(id, (Box::new(proxy), Box::new(listener)));
    }

    fn remove(&mut self, id: u32) {
        self.retained.remove(&id);
    }
}

#[derive(Default)]
struct Defaults {
    sink: String,
    source: String,
}

#[derive(Default)]
struct DeviceEndpoints {
    sink: Option<AudioEndpoint>,
    source: Option<AudioEndpoint>,
}

type ChangeCallback = std::sync::Arc<dyn Fn() + Send + Sync>;

fn initialize() {
    static INITIALIZE: Once = Once::new();
    INITIALIZE.call_once(pw::init);
}

fn monitor_relevant(global: &pw::registry::GlobalObject<&pw::spa::utils::dict::DictRef>) -> bool {
    match global.type_ {
        ObjectType::Device => global
            .props
            .is_some_and(|properties| properties.get("device.api") == Some("bluez5")),
        ObjectType::Node => global.props.is_some_and(|properties| {
            properties.get("device.api") == Some("bluez5")
                || properties
                    .get("node.name")
                    .is_some_and(|name| name.starts_with("bluez_"))
        }),
        ObjectType::Metadata => global
            .props
            .is_some_and(|properties| properties.get("metadata.name") == Some("default")),
        _ => false,
    }
}

fn bind_monitor_object(
    registry: &pw::registry::RegistryRc,
    global: &pw::registry::GlobalObject<&pw::spa::utils::dict::DictRef>,
    objects: &Rc<RefCell<Objects>>,
    event: ChangeCallback,
) -> Result<()> {
    match global.type_ {
        ObjectType::Device => {
            let device = registry
                .bind::<Device, _>(global)
                .context("bind PipeWire Bluetooth monitor device")?;
            let info_event = std::sync::Arc::clone(&event);
            let listener = device
                .add_listener_local()
                .info(move |_| info_event())
                .param(move |_, _, _, _, _| event())
                .register();
            objects.borrow_mut().retain(global.id, device, listener);
        }
        ObjectType::Node => {
            let node = registry
                .bind::<Node, _>(global)
                .context("bind PipeWire Bluetooth monitor node")?;
            let listener = node.add_listener_local().info(move |_| event()).register();
            objects.borrow_mut().retain(global.id, node, listener);
        }
        ObjectType::Metadata => {
            let metadata = registry
                .bind::<Metadata, _>(global)
                .context("bind PipeWire default metadata monitor")?;
            let listener = metadata
                .add_listener_local()
                .property(move |_, _, _, _| {
                    event();
                    0
                })
                .register();
            objects.borrow_mut().retain(global.id, metadata, listener);
        }
        _ => {}
    }
    Ok(())
}

pub fn monitor(on_change: ChangeCallback) -> Result<()> {
    initialize();
    let main_loop = pw::main_loop::MainLoopRc::new(None).context("create PipeWire monitor loop")?;
    let context =
        pw::context::ContextRc::new(&main_loop, None).context("create PipeWire monitor context")?;
    let core = context
        .connect_rc(None)
        .context("connect PipeWire monitor")?;
    let registry = core
        .get_registry_rc()
        .context("open PipeWire monitor registry")?;
    let registry_weak = registry.downgrade();
    let objects = Rc::new(RefCell::new(Objects::default()));
    let objects_for_registry = Rc::clone(&objects);
    let relevant_ids = Rc::new(RefCell::new(HashSet::new()));
    let relevant_for_global = Rc::clone(&relevant_ids);
    let relevant_for_remove = Rc::clone(&relevant_ids);
    let objects_for_remove = Rc::clone(&objects);
    let changes_for_global = std::sync::Arc::clone(&on_change);
    let changes_for_remove = std::sync::Arc::clone(&on_change);
    let _listener = registry
        .add_listener_local()
        .global(move |global| {
            if !monitor_relevant(global) {
                return;
            }
            tracing::debug!(id = global.id, object_type = ?global.type_, "PipeWire Bluetooth object added");
            relevant_for_global.borrow_mut().insert(global.id);
            changes_for_global();
            let Some(registry) = registry_weak.upgrade() else {
                tracing::warn!(id = global.id, "PipeWire registry disappeared while binding object");
                return;
            };
            if let Err(error) = bind_monitor_object(
                &registry,
                global,
                &objects_for_registry,
                std::sync::Arc::clone(&changes_for_global),
            ) {
                tracing::warn!(id = global.id, error = %error, error_chain = %format!("{error:#}"), "could not monitor PipeWire Bluetooth object");
            }
        })
        .global_remove(move |id| {
            if relevant_for_remove.borrow_mut().remove(&id) {
                objects_for_remove.borrow_mut().remove(id);
                tracing::debug!(id, "PipeWire Bluetooth object removed");
                changes_for_remove();
            }
        })
        .register();
    main_loop.run();
    anyhow::bail!("PipeWire monitor loop ended")
}

pub fn probe() -> Result<Vec<AudioDevice>> {
    initialize();
    probe_inner()
}

pub fn set_profile(address: &str, index: u32) -> Result<()> {
    initialize();
    set_profile_inner(address, index)
}

pub fn set_default_sink(address: &str) -> Result<()> {
    set_default_endpoint(address, EndpointKind::Sink)
}

pub fn set_default_source(address: &str) -> Result<()> {
    set_default_endpoint(address, EndpointKind::Source)
}

fn set_default_endpoint(address: &str, kind: EndpointKind) -> Result<()> {
    initialize();
    let devices = probe_inner()?;
    let device = devices
        .iter()
        .find(|device| device.address.eq_ignore_ascii_case(address))
        .context("Bluetooth audio device is unavailable")?;
    let endpoint = match kind {
        EndpointKind::Sink => device.sink.as_ref(),
        EndpointKind::Source => device.source.as_ref(),
    }
    .context("requested Bluetooth audio endpoint is unavailable")?;
    if endpoint.name.is_empty() {
        anyhow::bail!("requested Bluetooth audio endpoint has no PipeWire node name");
    }
    set_default_node(kind, &endpoint.name)
}

fn set_default_node(kind: EndpointKind, node_name: &str) -> Result<()> {
    let main_loop = pw::main_loop::MainLoopRc::new(None).context("create PipeWire main loop")?;
    let context =
        pw::context::ContextRc::new(&main_loop, None).context("create PipeWire context")?;
    let core = context.connect_rc(None).context("connect to PipeWire")?;
    let registry = core.get_registry_rc().context("open PipeWire registry")?;
    let registry_weak = registry.downgrade();
    let applied = Rc::new(Cell::new(false));
    let applied_for_registry = Rc::clone(&applied);
    let retained = Rc::new(RefCell::new(None::<Metadata>));
    let retained_for_registry = Rc::clone(&retained);
    let key = match kind {
        EndpointKind::Sink => "default.audio.sink",
        EndpointKind::Source => "default.audio.source",
    };
    let value = serde_json::json!({ "name": node_name }).to_string();
    let _registry_listener = registry
        .add_listener_local()
        .global(move |global| {
            if global.type_ != ObjectType::Metadata
                || global.props.and_then(|props| props.get("metadata.name")) != Some("default")
            {
                return;
            }
            let Some(registry) = registry_weak.upgrade() else {
                return;
            };
            match registry.bind::<Metadata, _>(global) {
                Ok(metadata) => {
                    metadata.set_property(0, key, Some("Spa:String:JSON"), Some(&value));
                    *retained_for_registry.borrow_mut() = Some(metadata);
                    applied_for_registry.set(true);
                }
                Err(error) => tracing::warn!(%error, "could not bind PipeWire default metadata"),
            }
        })
        .register();
    pipewire_roundtrip(&main_loop, &core)?;
    pipewire_roundtrip(&main_loop, &core)?;
    if !applied.get() {
        anyhow::bail!("PipeWire default metadata is unavailable");
    }
    Ok(())
}

fn set_profile_inner(address: &str, index: u32) -> Result<()> {
    let requested_index = index;
    let bytes = profile_parameter(index)?;
    let main_loop = pw::main_loop::MainLoopRc::new(None).context("create PipeWire main loop")?;
    let context =
        pw::context::ContextRc::new(&main_loop, None).context("create PipeWire context")?;
    let core = context.connect_rc(None).context("connect to PipeWire")?;
    let registry = core.get_registry_rc().context("open PipeWire registry")?;
    let registry_weak = registry.downgrade();
    let expected_name = format!("bluez_card.{}", address.replace(':', "_"));
    let confirmed = Rc::new(Cell::new(false));
    let confirmed_for_registry = Rc::clone(&confirmed);
    let objects = Rc::new(RefCell::new(Objects::default()));
    let objects_for_registry = Rc::clone(&objects);
    let _registry_listener = registry
        .add_listener_local()
        .global(move |global| {
            if global.type_ != ObjectType::Device
                || global.props.and_then(|props| props.get("device.name"))
                    != Some(expected_name.as_str())
            {
                return;
            }
            let Some(registry) = registry_weak.upgrade() else {
                return;
            };
            if let Err(error) = apply_profile(
                &registry,
                global,
                &bytes,
                requested_index,
                Rc::clone(&confirmed_for_registry),
                &objects_for_registry,
            ) {
                tracing::warn!(id = global.id, %error, "could not apply PipeWire Bluetooth profile");
            }
        })
        .register();
    pipewire_roundtrip(&main_loop, &core)?;
    pipewire_roundtrip(&main_loop, &core)?;
    if !confirmed.get() {
        anyhow::bail!("PipeWire did not activate the requested Bluetooth audio profile");
    }
    Ok(())
}

fn profile_parameter(index: u32) -> Result<Vec<u8>> {
    use pw::spa::pod::{Object, Property, Value, serialize::PodSerializer};

    let index = i32::try_from(index).context("PipeWire profile index exceeds i32")?;
    let value = Value::Object(Object {
        type_: pw::spa::sys::SPA_TYPE_OBJECT_ParamProfile,
        id: pw::spa::sys::SPA_PARAM_Profile,
        properties: vec![
            Property::new(pw::spa::sys::SPA_PARAM_PROFILE_index, Value::Int(index)),
            Property::new(pw::spa::sys::SPA_PARAM_PROFILE_save, Value::Bool(true)),
        ],
    });
    Ok(PodSerializer::serialize(Cursor::new(Vec::new()), &value)
        .context("serialize PipeWire profile parameter")?
        .0
        .into_inner())
}

fn apply_profile(
    registry: &pw::registry::RegistryRc,
    global: &pw::registry::GlobalObject<&pw::spa::utils::dict::DictRef>,
    bytes: &[u8],
    requested_index: u32,
    confirmed: Rc<Cell<bool>>,
    objects: &Rc<RefCell<Objects>>,
) -> Result<()> {
    let device = registry
        .bind::<Device, _>(global)
        .context("bind PipeWire device for profile change")?;
    let pod = pw::spa::pod::Pod::from_bytes(bytes)
        .context("decode serialized PipeWire profile parameter")?;
    let listener = device
        .add_listener_local()
        .param(move |_, parameter, _, _, pod| {
            if parameter == pw::spa::param::ParamType::Profile
                && pod
                    .and_then(parse_profile)
                    .is_some_and(|profile| profile.index == requested_index)
            {
                confirmed.set(true);
            }
        })
        .register();
    device.set_param(pw::spa::param::ParamType::Profile, 0, pod);
    device.enum_params(1, Some(pw::spa::param::ParamType::Profile), 0, 1);
    objects.borrow_mut().retain(global.id, device, listener);
    Ok(())
}

fn probe_inner() -> Result<Vec<AudioDevice>> {
    let main_loop = pw::main_loop::MainLoopRc::new(None).context("create PipeWire main loop")?;
    let context =
        pw::context::ContextRc::new(&main_loop, None).context("create PipeWire context")?;
    let core = context.connect_rc(None).context("connect to PipeWire")?;
    let registry = core.get_registry_rc().context("open PipeWire registry")?;
    let registry_weak = registry.downgrade();
    let state = ProbeState::default();
    let state_for_registry = state.clone();
    let _registry_listener = registry
        .add_listener_local()
        .global(move |global| {
            if let Some(registry) = registry_weak.upgrade() {
                state_for_registry.bind_global(&registry, global);
            }
        })
        .register();

    pipewire_roundtrip(&main_loop, &core)?;
    pipewire_roundtrip(&main_loop, &core)?;
    Ok(state.finish())
}

fn pipewire_roundtrip(
    main_loop: &pw::main_loop::MainLoopRc,
    core: &pw::core::CoreRc,
) -> Result<()> {
    let pending = core.sync(0).context("start PipeWire synchronization")?;
    let done = Rc::new(Cell::new(false));
    let done_for_listener = Rc::clone(&done);
    let loop_for_listener = main_loop.clone();
    let _listener = core
        .add_listener_local()
        .done(move |id, sequence| {
            if id == pw::core::PW_ID_CORE && sequence == pending {
                done_for_listener.set(true);
                loop_for_listener.quit();
            }
        })
        .register();
    let timed_out = Rc::new(Cell::new(false));
    let timed_out_for_timer = Rc::clone(&timed_out);
    let loop_for_timer = main_loop.clone();
    let timer = main_loop.loop_().add_timer(move |_| {
        timed_out_for_timer.set(true);
        loop_for_timer.quit();
    });
    timer
        .update_timer(Some(Duration::from_secs(3)), None)
        .into_result()
        .context("arm PipeWire synchronization timeout")?;
    while !done.get() && !timed_out.get() {
        main_loop.run();
    }
    if timed_out.get() {
        anyhow::bail!("PipeWire synchronization timed out");
    }
    Ok(())
}

#[derive(Clone, Default)]
struct ProbeState {
    devices: Rc<RefCell<HashMap<u32, AudioDevice>>>,
    endpoints: Rc<RefCell<HashMap<u32, DeviceEndpoints>>>,
    defaults: Rc<RefCell<Defaults>>,
    objects: Rc<RefCell<Objects>>,
}

impl ProbeState {
    fn bind_global(
        &self,
        registry: &pw::registry::RegistryRc,
        global: &pw::registry::GlobalObject<&pw::spa::utils::dict::DictRef>,
    ) {
        let Some(properties) = global.props else {
            return;
        };
        match global.type_ {
            ObjectType::Device if properties.get("device.api") == Some("bluez5") => {
                self.bind_device(registry, global, properties);
            }
            ObjectType::Node => self.bind_node(registry, global, properties),
            ObjectType::Metadata if properties.get("metadata.name") == Some("default") => {
                self.bind_metadata(registry, global);
            }
            _ => {}
        }
    }

    fn bind_device(
        &self,
        registry: &pw::registry::RegistryRc,
        global: &pw::registry::GlobalObject<&pw::spa::utils::dict::DictRef>,
        properties: &pw::spa::utils::dict::DictRef,
    ) {
        let device = bind_or_return!(registry, global, Device);
        self.devices.borrow_mut().insert(
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
                sink: None,
                source: None,
            },
        );
        let devices_for_info = Rc::clone(&self.devices);
        let devices_for_params = Rc::clone(&self.devices);
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
                update_device_properties(audio_device, properties);
            })
            .param(move |_sequence, parameter, _index, _next, pod| {
                let Some(profile) = pod.and_then(parse_profile) else {
                    return;
                };
                let mut devices = devices_for_params.borrow_mut();
                let Some(audio_device) = devices.get_mut(&device_id) else {
                    return;
                };
                update_profile(audio_device, parameter, profile);
            })
            .register();
        device.enum_params(1, Some(pw::spa::param::ParamType::EnumProfile), 0, u32::MAX);
        device.enum_params(2, Some(pw::spa::param::ParamType::Profile), 0, 1);
        self.retain(global.id, device, listener);
    }

    fn bind_node(
        &self,
        registry: &pw::registry::RegistryRc,
        global: &pw::registry::GlobalObject<&pw::spa::utils::dict::DictRef>,
        properties: &pw::spa::utils::dict::DictRef,
    ) {
        let Some(device_id) = properties
            .get("device.id")
            .and_then(|value| value.parse::<u32>().ok())
        else {
            return;
        };
        let Some(kind) = endpoint_kind(properties.get("media.class")) else {
            return;
        };
        set_endpoint(
            &mut self.endpoints.borrow_mut(),
            device_id,
            kind,
            AudioEndpoint {
                name: properties.get("node.name").unwrap_or_default().to_string(),
                state: "creating".to_string(),
                is_default: false,
            },
        );
        let node = bind_or_return!(registry, global, Node);
        let endpoints_for_info = Rc::clone(&self.endpoints);
        let listener = node
            .add_listener_local()
            .info(move |info| {
                let mut endpoints = endpoints_for_info.borrow_mut();
                let Some(endpoint) = endpoint_mut(&mut endpoints, device_id, kind) else {
                    return;
                };
                endpoint.state = node_state(info.state()).to_string();
            })
            .register();
        self.retain(global.id, node, listener);
    }

    fn bind_metadata(
        &self,
        registry: &pw::registry::RegistryRc,
        global: &pw::registry::GlobalObject<&pw::spa::utils::dict::DictRef>,
    ) {
        let metadata = bind_or_return!(registry, global, Metadata);
        let defaults_for_events = Rc::clone(&self.defaults);
        let listener = metadata
            .add_listener_local()
            .property(move |_subject, key, _type, value| {
                if let (Some(key), Some(value)) = (key, value.and_then(default_node_name)) {
                    update_default(&mut defaults_for_events.borrow_mut(), key, value);
                }
                0
            })
            .register();
        self.retain(global.id, metadata, listener);
    }

    fn retain(&self, id: u32, proxy: impl ProxyT + 'static, listener: impl Listener + 'static) {
        self.objects.borrow_mut().retain(id, proxy, listener);
    }

    fn finish(&self) -> Vec<AudioDevice> {
        finalize_devices(
            self.devices.borrow().values().cloned().collect(),
            &self.endpoints.borrow(),
            &self.defaults.borrow(),
        )
    }
}

fn update_device_properties(device: &mut AudioDevice, properties: &pw::spa::utils::dict::DictRef) {
    if let Some(address) = properties.get("api.bluez5.address") {
        device.address = address.to_string();
    }
    if let Some(path) = properties.get("api.bluez5.path") {
        device.adapter = adapter_from_bluez_path(path).unwrap_or_default();
    }
    if let Some(name) = properties
        .get("device.description")
        .or_else(|| properties.get("device.alias"))
    {
        device.name = name.to_string();
    }
}

fn update_profile(
    device: &mut AudioDevice,
    parameter: pw::spa::param::ParamType,
    profile: AudioProfile,
) {
    if parameter == pw::spa::param::ParamType::Profile {
        device.active_profile = Some(profile.index);
    } else if parameter == pw::spa::param::ParamType::EnumProfile
        && !device
            .profiles
            .iter()
            .any(|item| item.index == profile.index)
    {
        device.profiles.push(profile);
    }
}

fn update_default(defaults: &mut Defaults, key: &str, value: String) {
    match key {
        "default.audio.sink" => defaults.sink = value,
        "default.audio.source" => defaults.source = value,
        _ => {}
    }
}

fn finalize_devices(
    mut devices: Vec<AudioDevice>,
    endpoints: &HashMap<u32, DeviceEndpoints>,
    defaults: &Defaults,
) -> Vec<AudioDevice> {
    for device in &mut devices {
        device.profiles.sort_by_key(|profile| -profile.priority);
        if let Some(device_endpoints) = endpoints.get(&device.pipewire_id) {
            device.sink = device_endpoints.sink.clone();
            device.source = device_endpoints.source.clone();
        }
        if let Some(sink) = &mut device.sink {
            sink.is_default = sink.name == defaults.sink;
        }
        if let Some(source) = &mut device.source {
            source.is_default = source.name == defaults.source;
        }
    }
    devices.sort_by_key(|device| device.name.to_lowercase());
    devices
}

#[derive(Clone, Copy)]
enum EndpointKind {
    Sink,
    Source,
}

fn endpoint_kind(media_class: Option<&str>) -> Option<EndpointKind> {
    match media_class {
        Some("Audio/Sink") => Some(EndpointKind::Sink),
        Some("Audio/Source") => Some(EndpointKind::Source),
        _ => None,
    }
}

fn set_endpoint(
    endpoints: &mut HashMap<u32, DeviceEndpoints>,
    device_id: u32,
    kind: EndpointKind,
    endpoint: AudioEndpoint,
) {
    let device = endpoints.entry(device_id).or_default();
    match kind {
        EndpointKind::Sink => device.sink = Some(endpoint),
        EndpointKind::Source => device.source = Some(endpoint),
    }
}

fn endpoint_mut(
    endpoints: &mut HashMap<u32, DeviceEndpoints>,
    device_id: u32,
    kind: EndpointKind,
) -> Option<&mut AudioEndpoint> {
    let device = endpoints.get_mut(&device_id)?;
    match kind {
        EndpointKind::Sink => device.sink.as_mut(),
        EndpointKind::Source => device.source.as_mut(),
    }
}

fn node_state(state: NodeState<'_>) -> &'static str {
    match state {
        NodeState::Error(_) => "error",
        NodeState::Creating => "creating",
        NodeState::Suspended => "suspended",
        NodeState::Idle => "idle",
        NodeState::Running => "running",
    }
}

fn default_node_name(value: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(value)
        .ok()?
        .get("name")?
        .as_str()
        .map(str::to_string)
}

fn adapter_from_bluez_path(path: &str) -> Option<String> {
    path.split('/')
        .find(|part| part.starts_with("hci"))
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        AudioDevice, AudioEndpoint, AudioProfile, Defaults, DeviceEndpoints,
        adapter_from_bluez_path, default_node_name, endpoint_kind, finalize_devices, node_state,
        profile::{profile_codec, profile_mode},
        profile_key, update_profile,
    };

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
        assert!(endpoint_kind(Some("Audio/Sink")).is_some());
        assert_eq!(node_state(pipewire::node::NodeState::Idle), "idle");
        assert_eq!(
            default_node_name(r#"{"name":"bluez_output.opaque"}"#).as_deref(),
            Some("bluez_output.opaque")
        );
    }

    #[test]
    fn probe_results_are_deduplicated_and_linked_to_defaults() {
        let mut device = audio_device();
        let profile = audio_profile(2, 100);
        update_profile(
            &mut device,
            pipewire::spa::param::ParamType::EnumProfile,
            profile.clone(),
        );
        update_profile(
            &mut device,
            pipewire::spa::param::ParamType::EnumProfile,
            profile,
        );
        update_profile(
            &mut device,
            pipewire::spa::param::ParamType::Profile,
            audio_profile(2, 0),
        );
        let endpoints = HashMap::from([(
            7,
            DeviceEndpoints {
                sink: Some(AudioEndpoint {
                    name: "bluez_output.7".into(),
                    state: "idle".into(),
                    is_default: false,
                }),
                source: None,
            },
        )]);
        let devices = finalize_devices(
            vec![device],
            &endpoints,
            &Defaults {
                sink: "bluez_output.7".into(),
                source: String::new(),
            },
        );
        assert_eq!(devices[0].profiles.len(), 1);
        assert_eq!(devices[0].active_profile, Some(2));
        assert!(devices[0].sink.as_ref().unwrap().is_default);
    }

    fn audio_device() -> AudioDevice {
        AudioDevice {
            pipewire_id: 7,
            address: "AA:BB:CC:DD:EE:FF".into(),
            adapter: "hci0".into(),
            name: "Headset".into(),
            active_profile: None,
            profiles: vec![],
            sink: None,
            source: None,
        }
    }

    fn audio_profile(index: u32, priority: i32) -> AudioProfile {
        AudioProfile {
            index,
            name: "a2dp-sink".into(),
            description: "High Fidelity".into(),
            available: true,
            priority,
            mode: "high-fidelity".into(),
            codec: None,
        }
    }
}
