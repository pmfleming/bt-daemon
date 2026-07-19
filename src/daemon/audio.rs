use std::sync::Arc;

use serde_json::{Value, json};

use crate::{api, audio, pairing::PairingBroker, params::Params};

pub(super) async fn set_profile(pairing: &Arc<PairingBroker>, params: &Value) -> Value {
    let (device_key, profile_key) = match params.require_strings("device_key", "profile_key") {
        Ok(params) => params,
        Err(error) => return api::error("validation-error", error.to_string()),
    };
    let devices = match tokio::task::spawn_blocking(audio::probe).await {
        Ok(Ok(devices)) => devices,
        Ok(Err(error)) => return api::error("audio-unavailable", format!("{error:#}")),
        Err(error) => return api::error("audio-unavailable", error.to_string()),
    };
    let selection = devices.into_iter().find_map(|device| {
        let address = device.address.parse().ok()?;
        if device.adapter.is_empty() || pairing.device_key(&device.adapter, address) != device_key {
            return None;
        }
        let profile = device.profiles.into_iter().find(|profile| {
            audio::profile_key(device_key, &profile.name) == profile_key && profile.available
        })?;
        Some((device.address, profile.index))
    });
    let Some((address, index)) = selection else {
        return api::error(
            "audio-profile-unavailable",
            "Bluetooth audio profile is not available".to_string(),
        );
    };
    match tokio::task::spawn_blocking(move || audio::set_profile(&address, index)).await {
        Ok(Ok(())) => snapshot(Arc::clone(pairing)).await,
        Ok(Err(error)) => api::error("audio-operation-failed", format!("{error:#}")),
        Err(error) => api::error("audio-operation-failed", error.to_string()),
    }
}

pub(super) async fn snapshot(pairing: Arc<PairingBroker>) -> Value {
    let devices = match tokio::task::spawn_blocking(audio::probe).await {
        Ok(Ok(devices)) => devices,
        Ok(Err(error)) => return api::error("audio-unavailable", format!("{error:#}")),
        Err(error) => return api::error("audio-unavailable", error.to_string()),
    };
    let devices = devices
        .into_iter()
        .filter_map(|device| {
            let address = device.address.parse().ok()?;
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
            let endpoint = |value: Option<audio::AudioEndpoint>| {
                value.map(|endpoint| {
                    json!({
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
                "sink": endpoint(device.sink),
                "source": endpoint(device.source),
            }))
        })
        .collect::<Vec<_>>();
    api::success(json!({ "audio_devices": devices }))
}
