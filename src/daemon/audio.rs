use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tokio::sync::broadcast;

use crate::{api, audio, backend::Params, pairing::PairingBroker};

pub(super) fn start_monitor(events: broadcast::Sender<()>) -> Result<()> {
    std::thread::Builder::new()
        .name("bt-pipewire-monitor".into())
        .spawn(move || {
            loop {
                let sender = events.clone();
                let notify = Arc::new(move || drop(sender.send(())));
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| audio::monitor(notify))) {
                    Ok(Err(error)) => tracing::warn!(error = %error, error_chain = %format!("{error:#}"), "PipeWire audio monitor is retrying"),
                    Err(_) => tracing::error!("PipeWire audio monitor panicked; rebuilding connection"),
                    Ok(Ok(())) => {}
                }
                // Force a probe/unavailable event even if no globals were removed.
                let _ = events.send(());
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        })
        .context("start PipeWire audio monitor thread")?;
    Ok(())
}

async fn devices() -> Result<Vec<audio::AudioDevice>, Value> {
    tokio::task::spawn_blocking(audio::probe)
        .await
        .context("PipeWire audio probe task failed")
        .and_then(std::convert::identity)
        .map_err(|error| api::error("audio-unavailable", format!("{error:#}")))
}

fn resolved_device_key(pairing: &PairingBroker, device: &audio::AudioDevice) -> Option<String> {
    match device.address.parse() {
        Ok(address) => {
            (!device.adapter.is_empty()).then(|| pairing.device_key(&device.adapter, address))
        }
        Err(error) => {
            tracing::warn!(pipewire_id = device.pipewire_id, %error, "PipeWire Bluetooth device has an invalid address");
            None
        }
    }
}

type AudioOperation = Box<dyn FnOnce() -> Result<Option<audio::DefaultRouteOutcome>> + Send>;
type SelectOperation = fn(&str, &str, audio::AudioDevice) -> Option<AudioOperation>;

fn select_default(
    device_key: &str,
    requested_key: &str,
    device: audio::AudioDevice,
) -> Option<AudioOperation> {
    let endpoint = device
        .endpoints
        .iter()
        .find(|node| node.key(device_key) == requested_key)
        .or_else(|| {
            // Preserve old sink/source keys as deterministic compatibility aliases.
            [
                ("sink", device.sink.as_ref()),
                ("source", device.source.as_ref()),
            ]
            .into_iter()
            .find_map(|(kind, node)| {
                (audio::endpoint_key(device_key, kind) == requested_key)
                    .then_some(node)
                    .flatten()
            })
        })?
        .clone();
    Some(Box::new(move || {
        audio::set_default_node(&device, &endpoint).map(Some)
    }))
}

fn select_profile(
    device_key: &str,
    requested_key: &str,
    device: audio::AudioDevice,
) -> Option<AudioOperation> {
    let index = device.available_profile(device_key, requested_key)?.index;
    Some(Box::new(move || {
        audio::set_profile(&device, index).map(|()| None)
    }))
}

pub(super) struct Change {
    parameter: &'static str,
    unavailable_code: &'static str,
    unavailable_message: &'static str,
    select: SelectOperation,
}

pub(super) const DEFAULT: Change = Change {
    parameter: "endpoint_key",
    unavailable_code: "audio-endpoint-unavailable",
    unavailable_message: "Bluetooth audio endpoint is not available",
    select: select_default,
};
pub(super) const PROFILE: Change = Change {
    parameter: "profile_key",
    unavailable_code: "audio-profile-unavailable",
    unavailable_message: "Bluetooth audio profile is not available",
    select: select_profile,
};

impl Change {
    fn request<'a>(&self, params: &'a Value) -> Result<(&'a str, &'a str, bool)> {
        let remember = match params.get("remember") {
            None => false,
            Some(Value::Bool(value)) if self.parameter == "profile_key" => *value,
            _ => anyhow::bail!("remember must be a boolean on setProfile"),
        };
        let (device, requested) = params.require_strings("device_key", self.parameter)?;
        Ok((device, requested, remember))
    }
}

// An apply failure never persists; a persistence failure never claims that the
// already-applied hardware change was rolled back.
fn apply_then_remember<T>(
    apply: impl FnOnce() -> Result<T>,
    remember: impl FnOnce() -> Result<()>,
) -> Result<(T, Option<String>)> {
    let outcome = apply()?;
    Ok((outcome, remember().err().map(|error| format!("{error:#}"))))
}

pub(super) async fn apply_change(
    pairing: &PairingBroker,
    params: &Value,
    change: Change,
    persist: impl FnOnce(&str, &str) -> Result<()> + Send + 'static,
) -> Value {
    let (device_key, requested_key, remember) = match change.request(params) {
        Ok(params) => params,
        Err(error) => return api::error("validation-error", error.to_string()),
    };
    let gate = crate::task::device_gate(device_key);
    let exclusive = gate.lock_owned().await;
    let devices = match devices().await {
        Ok(devices) => devices,
        Err(error) => return error,
    };
    let operation = devices
        .into_iter()
        .filter(|device| resolved_device_key(pairing, device).as_deref() == Some(device_key))
        .find_map(|device| (change.select)(device_key, requested_key, device));
    let Some(operation) = operation else {
        return api::error(
            change.unavailable_code,
            change.unavailable_message.to_string(),
        );
    };
    let policy = remember.then(|| (device_key.to_owned(), requested_key.to_owned()));
    // Keep both effects and the gate inside a non-abortable worker: disconnecting
    // a caller cannot release serialization while PipeWire is still mutating.
    match tokio::task::spawn_blocking(move || {
        let _exclusive = exclusive;
        apply_then_remember(operation, || {
            policy.map_or(Ok(()), |(device, profile)| persist(&device, &profile))
        })
    })
    .await
    {
        Ok(Ok((route, persistence_error))) => {
            applied_response(snapshot(pairing).await, remember, persistence_error, route)
        }
        Ok(Err(error)) => api::error("audio-operation-failed", format!("{error:#}")),
        Err(error) => api::error("audio-operation-failed", error.to_string()),
    }
}

// Refresh failure must not hide a successfully applied profile or a failed policy save.
fn applied_response(
    mut response: Value,
    remember: bool,
    persistence_error: Option<String>,
    route: Option<audio::DefaultRouteOutcome>,
) -> Value {
    if !remember && route.is_none() {
        return response;
    }
    if response["ok"] != true {
        response = api::success(json!({"refresh_error": response["error"]}));
    }
    if remember {
        response["data"]["profile_outcome"] = json!({
            "applied": true, "remembered": persistence_error.is_none(),
            "persistence_error": persistence_error
        });
    }
    if let Some(route) = route {
        response["data"]["default_route_outcome"] = json!(route);
    }
    response
}

pub(super) async fn snapshot(pairing: &PairingBroker) -> Value {
    let devices = match devices().await {
        Ok(devices) => devices,
        Err(error) => return error,
    };
    let devices = devices
        .into_iter()
        .filter_map(|device| device_snapshot(pairing, device))
        .collect::<Vec<_>>();
    api::success(json!({ "audio_devices": devices }))
}

fn device_snapshot(pairing: &PairingBroker, device: audio::AudioDevice) -> Option<Value> {
    let device_key = resolved_device_key(pairing, &device)?;
    let active_profile_key = device
        .active_profile
        .and_then(|active| {
            device
                .profiles
                .iter()
                .find(|profile| profile.index == active)
        })
        .map(|profile| audio::profile_key(&device_key, &profile.name));
    let profiles = device
        .profiles
        .into_iter()
        .map(|profile| crate::model::audio_wire::Profile {
            key: audio::profile_key(&device_key, &profile.name),
            label: profile.description,
            mode: profile.mode,
            codec: profile.codec,
            available: profile.available,
            priority: profile.priority,
        })
        .collect::<Vec<_>>();
    Some(json!(crate::model::audio_wire::Device {
        endpoints: device
            .endpoints
            .into_iter()
            .map(|node| {
                let key = node.key(&device_key);
                node_snapshot(node, key)
            })
            .collect(),
        sink: endpoint_snapshot(&device_key, "sink", device.sink),
        source: endpoint_snapshot(&device_key, "source", device.source),
        device_key,
        active_profile_key,
        profiles,
    }))
}

fn endpoint_snapshot(
    device_key: &str,
    kind: &str,
    endpoint: Option<audio::AudioEndpoint>,
) -> Option<crate::model::audio_wire::Endpoint> {
    endpoint.map(|endpoint| node_snapshot(endpoint, audio::endpoint_key(device_key, kind)))
}

fn node_snapshot(
    endpoint: audio::AudioEndpoint,
    key: String,
) -> crate::model::audio_wire::Endpoint {
    crate::model::audio_wire::Endpoint {
        key,
        kind: endpoint.kind,
        codec: endpoint.codec,
        ready: !matches!(endpoint.state.as_str(), "creating" | "error"),
        state: endpoint.state,
        is_default: endpoint.is_default,
    }
}

#[cfg(test)]
mod tests;
