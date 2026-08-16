use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tokio::sync::broadcast;

use crate::{api, audio, backend::Params, pairing::PairingBroker};

pub(super) fn start_monitor(events: broadcast::Sender<()>) -> Result<()> {
    std::thread::Builder::new()
        .name("bt-pipewire-monitor".into())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| loop {
                let sender = events.clone();
                let notify = Arc::new(move || drop(sender.send(())));
                if let Err(error) = audio::monitor(notify) {
                    tracing::warn!(error = %error, error_chain = %format!("{error:#}"), "PipeWire audio monitor is retrying");
                }
                std::thread::sleep(std::time::Duration::from_secs(1));
            }));
            if result.is_err() {
                tracing::error!("PipeWire audio monitor thread panicked");
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
    let profile = device.profiles.into_iter().find(|profile| {
        profile.available && audio::profile_key(device_key, &profile.name) == requested_key
    })?;
    Some(Box::new(move || {
        audio::set_profile(&device.address, profile.index)
    }))
}

pub(super) async fn set_default(pairing: &Arc<PairingBroker>, params: &Value) -> Value {
    apply_change(
        pairing,
        params,
        "endpoint_key",
        "audio-endpoint-unavailable",
        "Bluetooth audio endpoint is not available",
        select_default,
    )
    .await
}

pub(super) async fn set_profile(pairing: &Arc<PairingBroker>, params: &Value) -> Value {
    apply_change(
        pairing,
        params,
        "profile_key",
        "audio-profile-unavailable",
        "Bluetooth audio profile is not available",
        select_profile,
    )
    .await
}

async fn apply_change(
    pairing: &Arc<PairingBroker>,
    params: &Value,
    parameter: &str,
    unavailable_code: &str,
    unavailable_message: &str,
    select: SelectOperation,
) -> Value {
    let (device_key, requested_key) = match params.require_strings("device_key", parameter) {
        Ok(params) => params,
        Err(error) => return api::error("validation-error", error.to_string()),
    };
    let devices = match devices().await {
        Ok(devices) => devices,
        Err(error) => return api::error("audio-unavailable", format!("{error:#}")),
    };
    let operation = devices.into_iter().find_map(|device| {
        let address = device_address(&device)?;
        if device.adapter.is_empty() || pairing.device_key(&device.adapter, address) != device_key {
            return None;
        }
        select(device_key, requested_key, device)
    });
    let Some(operation) = operation else {
        return api::error(unavailable_code, unavailable_message.to_string());
    };
    match tokio::task::spawn_blocking(operation).await {
        Ok(Ok(())) => snapshot(Arc::clone(pairing)).await,
        Ok(Err(error)) => api::error("audio-operation-failed", format!("{error:#}")),
        Err(error) => api::error("audio-operation-failed", error.to_string()),
    }
}

pub(super) async fn snapshot(pairing: Arc<PairingBroker>) -> Value {
    let devices = match devices().await {
        Ok(devices) => devices,
        Err(error) => return api::error("audio-unavailable", format!("{error:#}")),
    };
    let devices = devices
        .into_iter()
        .filter_map(|device| {
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
                        "label": profile.description,
                        "mode": profile.mode,
                        "codec": profile.codec,
                        "available": profile.available,
                        "priority": profile.priority,
                    })
                })
                .collect::<Vec<_>>();
            let endpoint = |kind: &str, value: Option<audio::AudioEndpoint>| {
                value.map(|endpoint| {
                    json!({
                        "key": audio::endpoint_key(&device_key, kind),
                        "ready": !matches!(endpoint.state.as_str(), "creating" | "error"),
                        "state": endpoint.state,
                        "is_default": endpoint.is_default,
                    })
                })
            };
            Some(json!({
                "device_key": device_key,
                "active_profile_key": active_profile_key,
                "profiles": profiles,
                "sink": endpoint("sink", device.sink),
                "source": endpoint("source", device.source),
            }))
        })
        .collect::<Vec<_>>();
    api::success(json!({ "audio_devices": devices }))
}
