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
    proxy::Listener,
    types::ObjectType,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

mod profile;
mod wait;
use profile::parse_profile;
use wait::Completion;

const OPERATION_TIMEOUT: Duration = Duration::from_secs(8);

/// Derive a stable profile identifier scoped to an opaque Bluetooth device key.
pub fn profile_key(device_key: &str, profile_name: &str) -> String {
    opaque_audio_key("profile", device_key, profile_name)
}

/// Derive a stable sink/source identifier scoped to an opaque Bluetooth device key.
pub fn endpoint_key(device_key: &str, kind: &str) -> String {
    opaque_audio_key("endpoint", device_key, kind)
}

fn opaque_audio_key(kind: &str, device_key: &str, value: &str) -> String {
    let digest = Sha256::digest(format!("{device_key}:{value}").as_bytes());
    format!("audio-{kind}-{}", hex::encode(&digest[..12]))
}

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
/// A probed Bluetooth PipeWire device and its profiles/endpoints.
/// Transport addresses are internal; the daemon maps them to opaque keys for clients.
pub struct AudioDevice {
    pub pipewire_id: u32,
    pub serial: Option<u64>,
    pub address: String,
    pub adapter: String,
    pub name: String,
    pub active_profile: Option<u32>,
    pub profiles: Vec<AudioProfile>,
    /// Public logical nodes, never internal per-component transport nodes.
    pub endpoints: Vec<AudioEndpoint>,
    pub internal_endpoints: Vec<AudioEndpoint>,
    /// Deterministic compatibility aliases for older clients.
    pub sink: Option<AudioEndpoint>,
    pub source: Option<AudioEndpoint>,
}

#[derive(Debug, Clone, Serialize)]
/// Current PipeWire node lifecycle state and default-route status.
pub struct AudioEndpoint {
    pub pipewire_id: u32,
    pub serial: Option<u64>,
    pub name: String,
    pub kind: EndpointKind,
    pub state: String,
    pub is_default: bool,
    pub codec: Option<String>,
    pub priority: i32,
    pub internal: bool,
}

impl AudioEndpoint {
    /// Stable opaque identity for this logical endpoint, scoped to its Bluetooth device.
    pub fn key(&self, device_key: &str) -> String {
        endpoint_key(device_key, &format!("{}:{}", self.kind.as_str(), self.name))
    }
}

#[derive(Debug, Clone, Serialize)]
/// PipeWire profile metadata, including availability, selection index and codec.
pub struct AudioProfile {
    pub index: u32,
    pub name: String,
    pub description: String,
    pub available: bool,
    pub priority: i32,
    pub mode: String,
    pub codec: Option<String>,
}

impl AudioDevice {
    pub(crate) fn available_profile(&self, device_key: &str, key: &str) -> Option<&AudioProfile> {
        self.profiles
            .iter()
            .find(|profile| profile.available && profile_key(device_key, &profile.name) == key)
    }
}

pub(crate) struct PolicyRequest {
    pub device_key: String,
    pub adapter: String,
    pub address: String,
    pub preferred_profile_key: Option<String>,
    pub switch_output: bool,
}

#[derive(Debug)]
pub(crate) struct PolicyMutationFailed;

impl std::fmt::Display for PolicyMutationFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("audio policy write failed or is uncertain; do not automatically replay")
    }
}

impl std::error::Error for PolicyMutationFailed {}

impl PolicyRequest {
    pub(crate) fn apply(&self) -> Result<()> {
        let device = probe()?
            .into_iter()
            .find(|device| {
                device.adapter == self.adapter && device.address.eq_ignore_ascii_case(&self.address)
            })
            .context("Bluetooth audio card is not ready")?;
        if let Some(profile) = self.preferred_profile(&device)? {
            set_profile(&device, profile).context(PolicyMutationFailed)?;
        }
        if self.switch_output {
            set_default_sink(&self.adapter, &self.address).context(PolicyMutationFailed)?;
        }
        Ok(())
    }

    fn preferred_profile(&self, device: &AudioDevice) -> Result<Option<u32>> {
        let Some(key) = &self.preferred_profile_key else {
            return Ok(None);
        };
        let profile = device
            .available_profile(&self.device_key, key)
            .context("preferred Bluetooth audio profile is unavailable")?;
        Ok((device.active_profile != Some(profile.index)).then_some(profile.index))
    }
}

// Drop listeners before their proxies, including Rc proxies whose callbacks use Weak.
struct RetainedObject {
    _listener: Box<dyn Listener>,
    _proxy: Box<dyn std::any::Any>,
}

#[derive(Default)]
struct Objects {
    retained: HashMap<u32, RetainedObject>,
}

impl Objects {
    fn retain(&mut self, id: u32, proxy: impl std::any::Any, listener: impl Listener + 'static) {
        self.retained.insert(
            id,
            RetainedObject {
                _listener: Box::new(listener),
                _proxy: Box::new(proxy),
            },
        );
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

type DeviceEndpoints = HashMap<u32, AudioEndpoint>;

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
            device.subscribe_params(&[
                pw::spa::param::ParamType::Profile,
                pw::spa::param::ParamType::EnumProfile,
            ]);
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

/// Run the blocking PipeWire change monitor until its connection ends.
/// Invoke on a dedicated thread; callbacks should return promptly.
pub fn monitor(on_change: ChangeCallback) -> Result<()> {
    let PipewireSession {
        main_loop,
        core,
        registry,
    } = pipewire_session()?;
    // A disconnected PipeWire core does not stop MainLoop by itself. Without
    // this listener the outer retry loop can never build a fresh connection.
    let failed = Rc::new(RefCell::new(None::<String>));
    let error_state = Rc::clone(&failed);
    let error_loop = main_loop.clone();
    let _core_listener = core
        .add_listener_local()
        .error(move |id, _, result, message| {
            if id == pw::core::PW_ID_CORE {
                *error_state.borrow_mut() =
                    Some(format!("PipeWire core disconnected ({result}): {message}"));
                error_loop.quit();
            } else {
                tracing::warn!(id, result, message, "PipeWire monitor object error");
            }
        })
        .register();
    let registry_weak = registry.downgrade();
    let objects = Rc::new(RefCell::new(Objects::default()));
    let objects_for_registry = Rc::clone(&objects);
    let relevant_ids = Rc::new(RefCell::new(HashSet::new()));
    let relevant_for_global = Rc::clone(&relevant_ids);
    let changes_for_global = std::sync::Arc::clone(&on_change);
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
            if relevant_ids.borrow_mut().remove(&id) {
                objects.borrow_mut().remove(id);
                tracing::debug!(id, "PipeWire Bluetooth object removed");
                on_change();
            }
        })
        .register();
    main_loop.run();
    anyhow::bail!(
        failed
            .borrow_mut()
            .take()
            .unwrap_or_else(|| "PipeWire monitor loop ended".into())
    )
}

/// Probe Bluetooth PipeWire objects using bounded synchronization round trips.
/// This is blocking I/O and should not run on a Tokio worker thread.
pub fn probe() -> Result<Vec<AudioDevice>> {
    let session = pipewire_session()?;
    let registry_weak = session.registry.downgrade();
    let state = ProbeState::default();
    let state_for_registry = state.clone();
    let state_for_removal = state.clone();
    let _registry_listener = session
        .registry
        .add_listener_local()
        .global(move |global| {
            if let Some(registry) = registry_weak.upgrade() {
                state_for_registry.bind_global(&registry, global);
            }
        })
        .global_remove(move |id| state_for_removal.remove(id))
        .register();
    session.settle()?;
    Ok(state.finish())
}

/// Make this Bluetooth device's sink the PipeWire default; performs blocking I/O.
pub fn set_default_sink(adapter: &str, address: &str) -> Result<DefaultRouteOutcome> {
    set_default_endpoint(adapter, address, EndpointKind::Sink)
}

/// Make this Bluetooth device's source the PipeWire default; performs blocking I/O.
pub fn set_default_source(adapter: &str, address: &str) -> Result<DefaultRouteOutcome> {
    set_default_endpoint(adapter, address, EndpointKind::Source)
}

fn set_default_endpoint(
    adapter: &str,
    address: &str,
    kind: EndpointKind,
) -> Result<DefaultRouteOutcome> {
    let devices = probe()?;
    let device = devices
        .iter()
        .find(|device| device.adapter == adapter && device.address.eq_ignore_ascii_case(address))
        .context("Bluetooth audio device is unavailable")?;
    let endpoint = match kind {
        EndpointKind::Sink => device.sink.as_ref(),
        EndpointKind::Source => device.source.as_ref(),
    }
    .context("requested Bluetooth audio endpoint is unavailable")?;
    if endpoint.name.is_empty() {
        anyhow::bail!("requested Bluetooth audio endpoint has no PipeWire node name");
    }
    set_default_node(device, endpoint)
}

/// Separately observed configured preference and effective policy-selected route.
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct DefaultRouteOutcome {
    pub configured: bool,
    pub effective: bool,
}

impl DefaultRouteOutcome {
    fn confirmed(&self, written: bool) -> bool {
        written && self.configured && self.effective
    }

    fn observe(
        &mut self,
        kind: EndpointKind,
        expected: &str,
        key: Option<&str>,
        value: Option<&str>,
    ) {
        let matches = value.and_then(default_node_name).as_deref() == Some(expected);
        match key {
            Some(key) if key == configured_default_key(kind) => self.configured = matches,
            Some(key) if key == effective_default_key(kind) => self.effective = matches,
            None => *self = Self::default(),
            _ => {}
        }
    }
}

/// Only transport identity crosses into callbacks, not a cloned device snapshot.
struct DeviceIdentity {
    id: u32,
    serial: Option<u64>,
    adapter: String,
    address: String,
}

impl From<&AudioDevice> for DeviceIdentity {
    fn from(device: &AudioDevice) -> Self {
        Self {
            id: device.pipewire_id,
            serial: device.serial,
            adapter: device.adapter.clone(),
            address: device.address.clone(),
        }
    }
}

struct RouteChange {
    device: DeviceIdentity,
    node_id: u32,
    node_serial: Option<u64>,
    name: String,
    kind: EndpointKind,
    completion: Completion,
    outcome: RefCell<DefaultRouteOutcome>,
    device_seen: Cell<bool>,
    node_seen: Cell<bool>,
    written: Cell<bool>,
    metadata_id: Cell<Option<u32>>,
    metadata: RefCell<Option<Rc<Metadata>>>,
}

impl RouteChange {
    fn confirm(&self) {
        if self.outcome.borrow().confirmed(self.written.get()) {
            self.completion.succeed();
        }
    }

    fn write_if_ready(&self) {
        if !self.device_seen.get()
            || !self.node_seen.get()
            || self.written.get()
            || self.completion.failed()
        {
            return;
        }
        let metadata = self.metadata.borrow();
        let Some(metadata) = metadata.as_ref() else {
            return;
        };
        self.written.set(true);
        metadata.set_property(
            0,
            configured_default_key(self.kind),
            Some("Spa:String:JSON"),
            Some(&serde_json::json!({ "name": self.name }).to_string()),
        );
        self.confirm();
    }

    fn bind_device(
        self: &Rc<Self>,
        registry: &pw::registry::RegistryRc,
        global: &pw::registry::GlobalObject<&pw::spa::utils::dict::DictRef>,
        objects: &mut Objects,
    ) -> Result<()> {
        let device = registry
            .bind::<Device, _>(global)
            .context("bind PipeWire routing device")?;
        let state = Rc::clone(self);
        let listener = device
            .add_listener_local()
            .info(move |info| {
                let Some(properties) = info.props() else {
                    return;
                };
                if !device_properties_match(
                    &state.device.adapter,
                    &state.device.address,
                    properties,
                ) {
                    state
                        .completion
                        .fail("PipeWire routing device changed identity");
                    return;
                }
                state.device_seen.set(true);
                state.write_if_ready();
            })
            .register();
        objects.retain(global.id, device, listener);
        Ok(())
    }

    fn bind_metadata(
        self: &Rc<Self>,
        registry: &pw::registry::RegistryRc,
        global: &pw::registry::GlobalObject<&pw::spa::utils::dict::DictRef>,
        objects: &mut Objects,
    ) -> Result<()> {
        if let Some(previous) = self.metadata_id.replace(Some(global.id)) {
            objects.remove(previous);
        }
        let metadata = Rc::new(
            registry
                .bind::<Metadata, _>(global)
                .context("bind PipeWire default metadata")?,
        );
        let state = Rc::clone(self);
        let listener = metadata
            .add_listener_local()
            .property(move |subject, key, _, value| {
                if subject == 0 {
                    state
                        .outcome
                        .borrow_mut()
                        .observe(state.kind, &state.name, key, value);
                    state.confirm();
                }
                0
            })
            .register();
        *self.metadata.borrow_mut() = Some(Rc::clone(&metadata));
        // Listeners live outside state so their Rc callbacks cannot form a cycle.
        objects.retain(global.id, metadata, listener);
        Ok(())
    }

    fn observe(
        self: &Rc<Self>,
        registry: &pw::registry::RegistryRc,
        global: &pw::registry::GlobalObject<&pw::spa::utils::dict::DictRef>,
        objects: &mut Objects,
    ) -> Result<()> {
        match global.type_ {
            ObjectType::Device if object_matches(global, self.device.id, self.device.serial) => {
                self.bind_device(registry, global, objects)?;
            }
            ObjectType::Node if object_matches(global, self.node_id, self.node_serial) => {
                anyhow::ensure!(
                    global.props.is_some_and(|props| {
                        props.get("device.id").and_then(|id| id.parse::<u32>().ok())
                            == Some(self.device.id)
                            && props.get("node.name") == Some(self.name.as_str())
                            && endpoint_kind(props.get("media.class")) == Some(self.kind)
                    }),
                    "PipeWire endpoint changed identity"
                );
                self.node_seen.set(true);
            }
            ObjectType::Metadata
                if global.props.and_then(|props| props.get("metadata.name")) == Some("default") =>
            {
                self.bind_metadata(registry, global, objects)?;
            }
            _ => {}
        }
        // Serial checks alone do not protect against server restarts: device
        // info must also confirm the adapter/address before any write.
        self.write_if_ready();
        Ok(())
    }
}

/// Route to a current public endpoint after validating its device and object serials.
pub fn set_default_node(
    target: &AudioDevice,
    endpoint: &AudioEndpoint,
) -> Result<DefaultRouteOutcome> {
    anyhow::ensure!(
        target.serial.is_some()
            && !target.adapter.is_empty()
            && endpoint.serial.is_some()
            && !endpoint.name.is_empty()
            && !endpoint.internal
            && target
                .endpoints
                .iter()
                .any(|node| node.pipewire_id == endpoint.pipewire_id
                    && node.serial == endpoint.serial),
        "PipeWire endpoint identity is unavailable or does not belong to this device"
    );
    let session = pipewire_session()?;
    let completion = Completion::new(&session.main_loop);
    let _core_listener = completion.watch_core(&session.core);
    let state = Rc::new(RouteChange {
        device: target.into(),
        node_id: endpoint.pipewire_id,
        node_serial: endpoint.serial,
        name: endpoint.name.clone(),
        kind: endpoint.kind,
        completion: completion.clone(),
        outcome: RefCell::default(),
        device_seen: Cell::new(false),
        node_seen: Cell::new(false),
        written: Cell::new(false),
        metadata_id: Cell::new(None),
        metadata: RefCell::default(),
    });
    let observed = Rc::clone(&state);
    let removed = Rc::clone(&state);
    let registry_weak = session.registry.downgrade();
    let objects = RefCell::new(Objects::default());
    let _registry_listener = session
        .registry
        .add_listener_local()
        .global(move |global| {
            if let Some(registry) = registry_weak.upgrade()
                && let Err(error) = observed.observe(&registry, global, &mut objects.borrow_mut())
            {
                observed.completion.fail(format!("{error:#}"));
            }
        })
        .global_remove(move |id| {
            if removed.metadata_id.get() == Some(id)
                || id == removed.device.id
                || id == removed.node_id
            {
                removed
                    .completion
                    .fail("PipeWire routing target or default metadata disappeared");
            }
        })
        .register();
    completion.wait(OPERATION_TIMEOUT)?;
    let outcome = *state.outcome.borrow();
    anyhow::ensure!(
        state.written.get() && outcome.configured,
        "PipeWire did not confirm the requested default before the deadline (target may have disappeared)"
    );
    Ok(outcome)
}

/// Activate a profile on the adapter-scoped, serial-validated device and confirm it.
/// Returns an error if the device/profile disappears or activation is not observed.
pub fn set_profile(target: &AudioDevice, index: u32) -> Result<()> {
    anyhow::ensure!(
        target.serial.is_some() && !target.adapter.is_empty(),
        "PipeWire device identity is unavailable"
    );
    let bytes = profile_parameter(index)?;
    let session = pipewire_session()?;
    let registry = &session.registry;
    let registry_weak = registry.downgrade();
    let completion = Completion::new(&session.main_loop);
    let _core_listener = completion.watch_core(&session.core);
    let change = Rc::new(ProfileChange {
        target: DeviceIdentity::from(target),
        index,
        bytes,
        completion: completion.clone(),
        written: Cell::new(false),
    });
    let target_id = Rc::new(Cell::new(None));
    let removed_id = target_id.clone();
    let removed = completion.clone();
    let objects = RefCell::new(Objects::default());
    let _registry_listener = registry
        .add_listener_local()
        .global(move |global| {
            if global.type_ != ObjectType::Device
                || !object_matches(global, change.target.id, change.target.serial)
            {
                return;
            }
            let Some(registry) = registry_weak.upgrade() else {
                return;
            };
            target_id.set(Some(global.id));
            if let Err(error) = apply_profile(&registry, global, &change, &objects) {
                change
                    .completion
                    .fail(format!("apply PipeWire Bluetooth profile: {error:#}"));
            }
        })
        .global_remove(move |id| {
            if removed_id.get() == Some(id) {
                removed.fail("PipeWire Bluetooth device disappeared during profile change");
            }
        })
        .register();
    if !completion.wait(OPERATION_TIMEOUT)? {
        anyhow::bail!(
            "PipeWire profile activation was not observed before the deadline; the write may still complete"
        );
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

// One operation state is shared by all bindings/callbacks. A write is recorded
// only after the writer runs successfully; any terminal failure prevents replay.
struct ProfileChange {
    target: DeviceIdentity,
    index: u32,
    bytes: Vec<u8>,
    completion: Completion,
    written: Cell<bool>,
}

impl ProfileChange {
    fn info(
        &self,
        properties: Option<&pw::spa::utils::dict::DictRef>,
        write: impl FnOnce() -> Result<()>,
    ) {
        if self.completion.failed() {
            return;
        }
        let Some(properties) = properties else { return };
        if !device_properties_match(&self.target.adapter, &self.target.address, properties) {
            self.completion
                .fail("PipeWire Bluetooth device changed identity");
            return;
        }
        if self.written.get() {
            return;
        }
        match write() {
            Ok(()) => self.written.set(true),
            Err(error) => self
                .completion
                .fail(format!("write PipeWire profile: {error:#}")),
        }
    }

    fn observe(&self, parameter: pw::spa::param::ParamType, pod: Option<&pw::spa::pod::Pod>) {
        if self.written.get()
            && parameter == pw::spa::param::ParamType::Profile
            && pod
                .and_then(parse_profile)
                .is_some_and(|profile| profile.index == self.index)
        {
            self.completion.succeed();
        }
    }
}

fn apply_profile(
    registry: &pw::registry::RegistryRc,
    global: &pw::registry::GlobalObject<&pw::spa::utils::dict::DictRef>,
    change: &Rc<ProfileChange>,
    objects: &RefCell<Objects>,
) -> Result<()> {
    let device = Rc::new(
        registry
            .bind::<Device, _>(global)
            .context("bind PipeWire device for profile change")?,
    );
    let weak = Rc::downgrade(&device);
    let checked = Rc::clone(change);
    let applied = Rc::clone(change);
    let listener = device
        .add_listener_local()
        .info(move |info| {
            checked.info(info.props(), || {
                let device = weak
                    .upgrade()
                    .context("PipeWire profile device disappeared")?;
                let pod = pw::spa::pod::Pod::from_bytes(&checked.bytes)
                    .context("decode serialized PipeWire profile parameter")?;
                device.set_param(pw::spa::param::ParamType::Profile, 0, pod);
                Ok(())
            })
        })
        .param(move |_, parameter, _, _, pod| applied.observe(parameter, pod))
        .register();
    // Rebinding must retain observations even after this operation has written.
    device.subscribe_params(&[pw::spa::param::ParamType::Profile]);
    objects.borrow_mut().retain(global.id, device, listener);
    Ok(())
}

struct PipewireSession {
    main_loop: pw::main_loop::MainLoopRc,
    core: pw::core::CoreRc,
    registry: pw::registry::RegistryRc,
}

impl PipewireSession {
    fn settle(&self) -> Result<()> {
        // First discover/bind globals, then receive their parameters or confirm writes.
        pipewire_roundtrip(&self.main_loop, &self.core)?;
        pipewire_roundtrip(&self.main_loop, &self.core)
    }
}

fn pipewire_session() -> Result<PipewireSession> {
    initialize();
    let main_loop = pw::main_loop::MainLoopRc::new(None).context("create PipeWire main loop")?;
    let context =
        pw::context::ContextRc::new(&main_loop, None).context("create PipeWire context")?;
    let core = context.connect_rc(None).context("connect to PipeWire")?;
    let registry = core.get_registry_rc().context("open PipeWire registry")?;
    Ok(PipewireSession {
        main_loop,
        core,
        registry,
    })
}

fn pipewire_roundtrip(
    main_loop: &pw::main_loop::MainLoopRc,
    core: &pw::core::CoreRc,
) -> Result<()> {
    let completion = Completion::new(main_loop);
    let _errors = completion.watch_core(core);
    let pending = core.sync(0).context("start PipeWire synchronization")?;
    let done_for_listener = completion.clone();
    let _listener = core
        .add_listener_local()
        .done(move |id, sequence| {
            if id == pw::core::PW_ID_CORE && sequence == pending {
                done_for_listener.succeed();
            }
        })
        .register();
    if !completion.wait(Duration::from_secs(3))? {
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
                serial: properties
                    .get("object.serial")
                    .and_then(|value| value.parse().ok()),
                address: properties
                    .get("api.bluez5.address")
                    .unwrap_or_default()
                    .to_string(),
                adapter: properties
                    .get("api.bluez5.path")
                    .and_then(adapter_from_bluez_path)
                    .unwrap_or_default(),
                name: properties
                    .get("device.description")
                    .or_else(|| properties.get("device.alias"))
                    .unwrap_or("Bluetooth audio device")
                    .to_string(),
                active_profile: None,
                profiles: Vec::new(),
                endpoints: Vec::new(),
                internal_endpoints: Vec::new(),
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
        self.objects
            .borrow_mut()
            .retain(global.id, device, listener);
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
        let mut endpoint = AudioEndpoint {
            pipewire_id: global.id,
            serial: properties
                .get("object.serial")
                .and_then(|value| value.parse().ok()),
            name: properties.get("node.name").unwrap_or_default().to_string(),
            kind,
            state: "creating".to_string(),
            is_default: false,
            codec: None,
            priority: 0,
            internal: false,
        };
        update_endpoint_properties(&mut endpoint, properties);
        self.endpoints
            .borrow_mut()
            .entry(device_id)
            .or_default()
            .insert(global.id, endpoint);
        let node = bind_or_return!(registry, global, Node);
        let endpoints_for_info = Rc::clone(&self.endpoints);
        let node_id = global.id;
        let listener = node
            .add_listener_local()
            .info(move |info| {
                let mut endpoints = endpoints_for_info.borrow_mut();
                let Some(endpoint) = endpoints
                    .get_mut(&device_id)
                    .and_then(|device| device.get_mut(&node_id))
                else {
                    return;
                };
                endpoint.state = node_state(info.state()).to_string();
                if let Some(properties) = info.props() {
                    update_endpoint_properties(endpoint, properties);
                }
            })
            .register();
        self.objects.borrow_mut().retain(global.id, node, listener);
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
            .property(move |subject, key, _type, value| {
                if subject == 0 {
                    match key {
                        Some(key) => update_default(
                            &mut defaults_for_events.borrow_mut(),
                            key,
                            value.and_then(default_node_name).unwrap_or_default(),
                        ),
                        None => *defaults_for_events.borrow_mut() = Defaults::default(),
                    }
                }
                0
            })
            .register();
        self.objects
            .borrow_mut()
            .retain(global.id, metadata, listener);
    }

    fn remove(&self, id: u32) {
        self.devices.borrow_mut().remove(&id);
        let mut endpoints = self.endpoints.borrow_mut();
        endpoints.remove(&id);
        for device in endpoints.values_mut() {
            device.remove(&id);
        }
        self.objects.borrow_mut().remove(id);
    }

    fn finish(&self) -> Vec<AudioDevice> {
        finalize_devices(
            self.devices
                .borrow_mut()
                .drain()
                .map(|(_, device)| device)
                .collect(),
            &mut self.endpoints.borrow_mut(),
            &self.defaults.borrow(),
        )
    }
}

fn object_matches(
    global: &pw::registry::GlobalObject<&pw::spa::utils::dict::DictRef>,
    id: u32,
    serial: Option<u64>,
) -> bool {
    global.id == id
        && serial.is_some()
        && global
            .props
            .and_then(|props| props.get("object.serial"))
            .and_then(|value| value.parse::<u64>().ok())
            == serial
}

fn device_properties_match(
    adapter: &str,
    address: &str,
    properties: &pw::spa::utils::dict::DictRef,
) -> bool {
    properties
        .get("api.bluez5.address")
        .is_some_and(|value| value.eq_ignore_ascii_case(address))
        && properties
            .get("api.bluez5.path")
            .and_then(adapter_from_bluez_path)
            .as_deref()
            == Some(adapter)
}

fn update_endpoint_properties(
    endpoint: &mut AudioEndpoint,
    properties: &pw::spa::utils::dict::DictRef,
) {
    if let Some(value) = properties.get("api.bluez5.codec") {
        endpoint.codec = profile::codec_name(value);
    }
    if let Some(value) = properties.get("api.bluez5.internal") {
        endpoint.internal = matches!(value, "true" | "1");
    }
    if let Some(priority) = properties
        .get("priority.session")
        .and_then(|value| value.parse().ok())
    {
        endpoint.priority = priority;
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
    endpoints: &mut HashMap<u32, DeviceEndpoints>,
    defaults: &Defaults,
) -> Vec<AudioDevice> {
    for device in &mut devices {
        if let Some(nodes) = endpoints.remove(&device.pipewire_id) {
            (device.internal_endpoints, device.endpoints) =
                nodes.into_values().partition(|node| node.internal);
        }
        device.finalize(defaults);
    }
    devices.sort_by_cached_key(|device| (device.name.to_lowercase(), device.pipewire_id));
    devices
}

impl AudioDevice {
    fn finalize(&mut self, defaults: &Defaults) {
        self.profiles
            .sort_by_key(|profile| (std::cmp::Reverse(profile.priority), profile.index));
        for endpoint in &mut self.endpoints {
            endpoint.is_default = endpoint.name
                == match endpoint.kind {
                    EndpointKind::Sink => &defaults.sink,
                    EndpointKind::Source => &defaults.source,
                }
                .as_str();
        }
        // The ID breaks ties even when PipeWire publishes duplicate node names.
        self.endpoints.sort_by(|a, b| {
            b.is_default
                .cmp(&a.is_default)
                .then_with(|| b.priority.cmp(&a.priority))
                .then_with(|| a.name.cmp(&b.name))
                .then_with(|| a.pipewire_id.cmp(&b.pipewire_id))
        });
        [self.sink, self.source] = [EndpointKind::Sink, EndpointKind::Source].map(|kind| {
            self.endpoints
                .iter()
                .find(|node| node.kind == kind)
                .cloned()
        });
        if let Some(codec) = common_codec(&self.endpoints)
            && let Some(active) = self
                .profiles
                .iter_mut()
                .find(|profile| Some(profile.index) == self.active_profile)
        {
            active.codec = Some(codec.to_owned());
        }
    }
}

// Unknown readings do not disagree; only a single known codec can refine a profile.
fn common_codec(endpoints: &[AudioEndpoint]) -> Option<&str> {
    let mut codecs = endpoints.iter().filter_map(|node| node.codec.as_deref());
    let first = codecs.next()?;
    codecs.all(|codec| codec == first).then_some(first)
}

/// Logical input/output direction; internal transport nodes are not selectable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(test, derive(serde::Deserialize))]
pub enum EndpointKind {
    Sink,
    Source,
}

impl EndpointKind {
    /// Stable API name for endpoint keys and snapshots.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sink => "sink",
            Self::Source => "source",
        }
    }
}

fn configured_default_key(kind: EndpointKind) -> &'static str {
    match kind {
        EndpointKind::Sink => "default.configured.audio.sink",
        EndpointKind::Source => "default.configured.audio.source",
    }
}

fn effective_default_key(kind: EndpointKind) -> &'static str {
    match kind {
        EndpointKind::Sink => "default.audio.sink",
        EndpointKind::Source => "default.audio.source",
    }
}

fn endpoint_kind(media_class: Option<&str>) -> Option<EndpointKind> {
    match media_class {
        Some("Audio/Sink") => Some(EndpointKind::Sink),
        Some("Audio/Source") => Some(EndpointKind::Source),
        _ => None,
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

    use super::{AudioDevice, AudioEndpoint, AudioProfile, Defaults, finalize_devices};

    fn endpoint(id: u32, name: &str) -> AudioEndpoint {
        AudioEndpoint {
            pipewire_id: id,
            serial: Some(u64::from(id) * 10),
            name: name.into(),
            kind: super::EndpointKind::Sink,
            state: "running".into(),
            is_default: false,
            codec: Some("lc3".into()),
            priority: 0,
            internal: false,
        }
    }

    #[test]
    fn logical_endpoints_survive_multiple_nodes_and_internal_nodes_are_not_routable() {
        let mut device = audio_device();
        device.active_profile = Some(2);
        device.profiles.push(audio_profile(2, 100));
        let mut internal = endpoint(12, "transport");
        internal.internal = true;
        internal.priority = 9999;
        internal.codec = Some("internal-only".into());
        let mut second = endpoint(11, "second");
        second.priority = 100;
        let mut endpoints = HashMap::from([(
            7,
            HashMap::from([(10, endpoint(10, "first")), (11, second), (12, internal)]),
        )]);
        let mut result = finalize_devices(
            vec![device],
            &mut endpoints,
            &Defaults {
                sink: "first".into(),
                source: String::new(),
            },
        );
        let mut device = result.pop().unwrap();
        assert_eq!(device.endpoints.len(), 2);
        assert_eq!(device.internal_endpoints.len(), 1);
        assert_eq!(device.sink.as_ref().unwrap().name, "first");
        assert_eq!(device.profiles[0].codec.as_deref(), Some("lc3"));
        assert_ne!(
            device.endpoints[0].key("peer"),
            device.endpoints[1].key("peer")
        );
        let state = super::ProbeState::default();
        state.endpoints.borrow_mut().insert(
            7,
            device
                .endpoints
                .drain(..)
                .map(|node| (node.pipewire_id, node))
                .collect(),
        );
        state.devices.borrow_mut().insert(7, device);
        state.remove(10);
        assert_eq!(state.endpoints.borrow()[&7].len(), 1);
        state.remove(7);
        assert!(state.devices.borrow().is_empty() && state.endpoints.borrow().is_empty());
    }

    #[test]
    fn finalization_is_deterministic_and_only_refines_unambiguous_active_codecs() {
        for (codecs, active, expected) in [
            ([None, None, None], Some(2), "configured"),
            ([Some("lc3"), None, Some("lc3")], Some(2), "lc3"),
            ([Some("lc3"), Some("aac"), None], Some(2), "configured"),
            ([Some("lc3"), None, None], None, "configured"),
            ([Some("lc3"), None, None], Some(999), "configured"),
        ] {
            let mut device = audio_device();
            device.active_profile = active;
            device.profiles = vec![audio_profile(2, 10), audio_profile(1, 10)];
            device.profiles[0].codec = Some("configured".into());
            device.endpoints = codecs
                .into_iter()
                .enumerate()
                .map(|(id, codec)| {
                    let mut node = endpoint(id as u32, "same-name");
                    node.codec = codec.map(Into::into);
                    node.kind = [
                        super::EndpointKind::Sink,
                        super::EndpointKind::Sink,
                        super::EndpointKind::Source,
                    ][id];
                    node
                })
                .collect();
            let defaults = Defaults {
                sink: "same-name".into(),
                source: String::new(),
            };
            device.finalize(&defaults);
            device.endpoints.reverse();
            device.finalize(&defaults);
            assert_eq!(
                device.profiles.iter().map(|p| p.index).collect::<Vec<_>>(),
                [1, 2]
            );
            assert_eq!(device.profiles[1].codec.as_deref(), Some(expected));
            assert_eq!(device.sink.as_ref().unwrap().pipewire_id, 0);
            assert!(device.sink.as_ref().unwrap().is_default);
            assert_eq!(device.source.as_ref().unwrap().pipewire_id, 2);
            assert!(!device.source.as_ref().unwrap().is_default);
        }
        let mut first = audio_device();
        first.pipewire_id = 1;
        let ordered = finalize_devices(
            vec![audio_device(), first],
            &mut HashMap::new(),
            &Defaults::default(),
        );
        assert_eq!(
            ordered
                .iter()
                .map(|device| device.pipewire_id)
                .collect::<Vec<_>>(),
            [1, 7]
        );
        assert!(
            ordered
                .iter()
                .all(|device| device.sink.is_none() && device.source.is_none())
        );
    }

    #[test]
    fn object_serials_reject_reused_ids_and_properties_preserve_structured_codecs() {
        super::initialize();
        let props = pipewire::properties::properties! {
            "object.serial" => "70",
            "api.bluez5.address" => "AA:BB:CC:DD:EE:FF",
            "api.bluez5.path" => "/org/bluez/hci1/dev_AA_BB_CC_DD_EE_FF",
            "api.bluez5.codec" => "lc3",
            "api.bluez5.internal" => "true",
            "priority.session" => "100",
        };
        let global = pipewire::registry::GlobalObject {
            id: 7,
            permissions: pipewire::permissions::PermissionFlags::empty(),
            type_: pipewire::types::ObjectType::Device,
            version: 1,
            props: Some(props.dict()),
        };
        assert!(super::device_properties_match(
            "hci1",
            "aa:bb:cc:dd:ee:ff",
            props.dict()
        ));
        assert!(!super::device_properties_match(
            "hci0",
            "AA:BB:CC:DD:EE:FF",
            props.dict()
        ));
        assert!(!super::device_properties_match(
            "hci1",
            "00:00:00:00:00:00",
            props.dict()
        ));
        assert!(super::object_matches(&global, 7, Some(70)));
        assert!(!super::object_matches(&global, 7, Some(69)));
        assert!(!super::object_matches(&global, 8, Some(70)));
        assert!(!super::object_matches(&global, 7, None));
        let mut node = endpoint(10, "logical");
        super::update_endpoint_properties(&mut node, props.dict());
        assert!(node.internal);
        assert_eq!(node.codec.as_deref(), Some("lc3"));
        assert_eq!(node.priority, 100);
    }

    #[test]
    fn profile_callbacks_require_a_write_and_fail_closed_without_replaying() -> anyhow::Result<()> {
        use super::{Cell, Completion, DeviceIdentity, Duration, ProfileChange, profile_parameter};
        use pipewire::{
            main_loop::MainLoopRc,
            spa::{param::ParamType, pod::Pod},
        };

        super::initialize();
        let main_loop = MainLoopRc::new(None)?;
        let valid = pipewire::properties::properties! {
            "api.bluez5.address" => "aa:bb:cc:dd:ee:ff",
            "api.bluez5.path" => "/org/bluez/hci0/dev_AA_BB_CC_DD_EE_FF",
        };
        let wrong = pipewire::properties::properties! {
            "api.bluez5.address" => "aa:bb:cc:dd:ee:ff",
            "api.bluez5.path" => "/org/bluez/hci1/dev_AA_BB_CC_DD_EE_FF",
        };
        let new_change = || ProfileChange {
            target: DeviceIdentity::from(&audio_device()),
            index: 2,
            bytes: profile_parameter(2).unwrap(),
            completion: Completion::new(&main_loop),
            written: Cell::new(false),
        };
        let change = new_change();
        let matching = Pod::from_bytes(&change.bytes).unwrap();
        let deadline = Duration::from_millis(1);
        change.observe(ParamType::Profile, Some(matching));
        assert!(
            !change.completion.wait(deadline)?,
            "pre-write observation is not confirmation"
        );
        let writes = Cell::new(0);
        let write = || {
            writes.set(writes.get() + 1);
            Ok(())
        };
        change.info(None, write);
        assert_eq!(writes.get(), 0);
        change.info(Some(valid.dict()), write);
        change.info(Some(valid.dict()), write);
        assert_eq!(writes.get(), 1);
        change.observe(ParamType::EnumProfile, Some(matching));
        change.observe(ParamType::Profile, None);
        let other = profile_parameter(3)?;
        change.observe(ParamType::Profile, Pod::from_bytes(&other));
        assert!(!change.completion.wait(deadline)?);
        change.observe(ParamType::Profile, Some(matching));
        assert!(change.completion.wait(deadline)?);
        change.info(Some(wrong.dict()), write);
        change.info(Some(valid.dict()), write);
        change.observe(ParamType::Profile, Some(matching));
        assert!(
            change.completion.failed(),
            "late identity failure overrides success"
        );
        assert_eq!(writes.get(), 1);

        for (properties, write_ok, pre_failed, expected_writes) in [
            (valid.dict(), false, false, 1),
            (wrong.dict(), true, false, 0),
            (valid.dict(), true, true, 0),
        ] {
            let failed = new_change();
            let attempts = Cell::new(0);
            if pre_failed {
                failed
                    .completion
                    .fail("core disconnected or target removed");
            }
            failed.info(Some(properties), || {
                attempts.set(attempts.get() + 1);
                anyhow::ensure!(write_ok, "write unavailable");
                Ok(())
            });
            failed.info(Some(valid.dict()), || {
                attempts.set(attempts.get() + 1);
                Ok(())
            });
            failed.observe(ParamType::Profile, Some(matching));
            assert_eq!(attempts.get(), expected_writes);
            assert!(!failed.written.get());
            assert!(failed.completion.failed());
        }
        assert!(profile_parameter(u32::MAX).is_err());
        Ok(())
    }

    #[test]
    fn route_confirmation_requires_a_write_and_matching_configured_and_effective_defaults() {
        use super::{
            DefaultRouteOutcome, EndpointKind, configured_default_key, effective_default_key,
        };
        let expected = Some(r#"{"name":"logical"}"#);
        for kind in [EndpointKind::Sink, EndpointKind::Source] {
            let mut outcome = DefaultRouteOutcome::default();
            let configured = Some(configured_default_key(kind));
            let effective = Some(effective_default_key(kind));
            outcome.observe(kind, "logical", configured, expected);
            for value in [None, Some("invalid JSON"), Some(r#"{"name":"other"}"#)] {
                outcome.observe(kind, "logical", effective, value);
                assert!(!outcome.confirmed(true));
            }
            outcome.observe(kind, "logical", effective, expected);
            assert!(outcome.confirmed(true));
            assert!(!outcome.confirmed(false));
            outcome.observe(kind, "logical", Some("unrelated"), None);
            assert!(outcome.confirmed(true));
            outcome.observe(kind, "logical", None, None);
            assert!(!outcome.configured && !outcome.effective);
        }
    }

    fn audio_device() -> AudioDevice {
        AudioDevice {
            pipewire_id: 7,
            serial: Some(70),
            address: "AA:BB:CC:DD:EE:FF".into(),
            adapter: "hci0".into(),
            name: "Headset".into(),
            active_profile: None,
            profiles: vec![],
            endpoints: vec![],
            internal_endpoints: vec![],
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
