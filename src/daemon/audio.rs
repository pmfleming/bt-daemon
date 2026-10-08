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

async fn devices() -> Result<Vec<audio::AudioDevice>> {
    tokio::task::spawn_blocking(audio::probe)
        .await
        .context("PipeWire audio probe task failed")?
}

fn device_address(device: &audio::AudioDevice) -> Option<bluer::Address> {
    match device.address.parse() {
        Ok(address) => Some(address),
        Err(error) => {
            tracing::warn!(pipewire_id = device.pipewire_id, %error, "PipeWire Bluetooth device has an invalid address");
            None
        }
    }
}

type AudioOperation = Box<dyn FnOnce() -> Result<()> + Send>;
type SelectOperation = fn(&str, &str, audio::AudioDevice) -> Option<AudioOperation>;

fn select_default(
    device_key: &str,
    requested_key: &str,
    device: audio::AudioDevice,
) -> Option<AudioOperation> {
    let kind = [
        ("sink", device.sink.is_some()),
        ("source", device.source.is_some()),
    ]
    .into_iter()
    .find(|(kind, available)| *available && audio::endpoint_key(device_key, kind) == requested_key)?
    .0;
    let address = device.address;
    match kind {
        "sink" => Some(Box::new(move || audio::set_default_sink(&address))),
        _ => Some(Box::new(move || audio::set_default_source(&address))),
    }
}

fn select_profile(
    device_key: &str,
    requested_key: &str,
    device: audio::AudioDevice,
) -> Option<AudioOperation> {
    let index = device.available_profile(device_key, requested_key)?.index;
    Some(Box::new(move || audio::set_profile(&device.address, index)))
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
fn apply_then_remember(
    apply: impl FnOnce() -> Result<()>,
    remember: impl FnOnce() -> Result<()>,
) -> Result<Option<String>> {
    apply()?;
    Ok(remember().err().map(|error| format!("{error:#}")))
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
        Err(error) => return api::error("audio-unavailable", format!("{error:#}")),
    };
    let operation = devices.into_iter().find_map(|device| {
        let address = device_address(&device)?;
        if device.adapter.is_empty() || pairing.device_key(&device.adapter, address) != device_key {
            return None;
        }
        (change.select)(device_key, requested_key, device)
    });
    let Some(operation) = operation else {
        return api::error(
            change.unavailable_code,
            change.unavailable_message.to_string(),
        );
    };
    let device_key = device_key.to_owned();
    let requested_key = requested_key.to_owned();
    // Keep both effects and the gate inside a non-abortable worker: disconnecting
    // a caller cannot release serialization while PipeWire is still mutating.
    match tokio::task::spawn_blocking(move || {
        let _exclusive = exclusive;
        apply_then_remember(operation, || {
            if remember {
                persist(&device_key, &requested_key)
            } else {
                Ok(())
            }
        })
    })
    .await
    {
        Ok(Ok(persistence_error)) => {
            applied_response(snapshot(pairing).await, remember, persistence_error)
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
) -> Value {
    if !remember {
        return response;
    }
    if response["ok"] != true {
        response = api::success(json!({"refresh_error": response["error"]}));
    }
    response["data"]["profile_outcome"] = json!({
        "applied": true, "remembered": persistence_error.is_none(),
        "persistence_error": persistence_error
    });
    response
}

pub(super) async fn snapshot(pairing: &PairingBroker) -> Value {
    let devices = match devices().await {
        Ok(devices) => devices,
        Err(error) => return api::error("audio-unavailable", format!("{error:#}")),
    };
    let devices = devices
        .into_iter()
        .filter_map(|device| device_snapshot(pairing, device))
        .collect::<Vec<_>>();
    api::success(json!({ "audio_devices": devices }))
}

fn device_snapshot(pairing: &PairingBroker, device: audio::AudioDevice) -> Option<Value> {
    let address = device_address(&device)?;
    if device.adapter.is_empty() {
        return None;
    }
    let device_key = pairing.device_key(&device.adapter, address);
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
        .map(|profile| {
            json!({
                "key": audio::profile_key(&device_key, &profile.name),
                "label": profile.description, "mode": profile.mode, "codec": profile.codec,
                "available": profile.available, "priority": profile.priority,
            })
        })
        .collect::<Vec<_>>();
    Some(json!({
        "device_key": device_key, "active_profile_key": active_profile_key, "profiles": profiles,
        "sink": endpoint_snapshot(&device_key, "sink", device.sink),
        "source": endpoint_snapshot(&device_key, "source", device.source),
    }))
}

fn endpoint_snapshot(
    device_key: &str,
    kind: &str,
    endpoint: Option<audio::AudioEndpoint>,
) -> Option<Value> {
    endpoint.map(|endpoint| {
        json!({
            "key": audio::endpoint_key(device_key, kind),
            "ready": !matches!(endpoint.state.as_str(), "creating" | "error"),
            "state": endpoint.state, "is_default": endpoint.is_default,
        })
    })
}

#[cfg(test)]
mod tests;
