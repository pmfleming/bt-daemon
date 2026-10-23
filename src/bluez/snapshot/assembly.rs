//! Snapshot construction from captured evidence; no clocks, locks or hardware I/O.
use super::{
    BluezDeviceState, CachedDevice, DeviceMetadata, ResolvedPresentation, device_capabilities,
    has_fast_pair_service, service_label, signal_level, signal_strength,
};
use crate::{
    bluez::{link_signal, observations::Observation},
    management::DevicePolicy,
    model::{
        Device, DeviceIdentity, DevicePresentation, DeviceServices, DeviceState, FastPairFeatures,
        NoiseControl, RssiKind, Service,
    },
};

pub(super) struct Context {
    pub key: String,
    pub adapter_key: String,
    pub address: String,
    pub policy: DevicePolicy,
}

pub(super) struct Reading {
    pub state: BluezDeviceState,
    pub metadata: DeviceMetadata,
    pub bonded: Option<bool>,
    pub features: Option<FastPairFeatures>,
    /// Noise control from a non-Fast Pair vendor channel; preferred when present.
    pub vendor_noise_control: Option<NoiseControl>,
    pub last_disconnect: Option<crate::model::DeviceDisconnect>,
}

pub(super) struct Signal {
    pub rssi: Option<i16>,
    pub kind: Option<RssiKind>,
    pub last_seen_ms: Option<u64>,
    pub live: bool,
    pub present: bool,
}

impl Signal {
    pub fn resolve(
        observed: Option<&Observation>,
        cached: Option<&CachedDevice>,
        live: bool,
    ) -> Self {
        let measured = observed.filter(|seen| {
            seen.rssi.is_some()
                && (live
                    || cached.is_none_or(|cache| {
                        cache.device.presentation.rssi.is_none()
                            || Some(seen.last_seen_ms) >= cache.device.presentation.last_seen_ms
                    }))
        });
        Self {
            rssi: measured
                .and_then(|seen| seen.rssi)
                .or_else(|| cached.and_then(|cache| cache.device.presentation.rssi)),
            kind: measured
                .map(|_| RssiKind::Dbm)
                .or_else(|| cached.and_then(|cache| cache.device.presentation.rssi_kind)),
            last_seen_ms: measured
                .map(|seen| seen.last_seen_ms)
                .or_else(|| cached.and_then(|cache| cache.device.presentation.last_seen_ms))
                .or_else(|| observed.map(|seen| seen.last_seen_ms)),
            live: live && measured.is_some(),
            present: live,
        }
    }

    pub fn with_link(mut self, link: Option<link_signal::Reading>) -> Self {
        if let Some(link) = link
            && (link.live
                || (!self.live
                    && (self.rssi.is_none() || Some(link.last_seen_ms) >= self.last_seen_ms)))
        {
            self.rssi = Some(link.rssi);
            self.kind = Some(link.kind);
            self.last_seen_ms = Some(link.last_seen_ms);
            self.live = link.live;
        }
        self
    }
}

pub(super) fn build(
    context: Context,
    reading: Reading,
    presentation: ResolvedPresentation,
    signal: Signal,
) -> Device {
    let Reading {
        state,
        metadata,
        bonded,
        features,
        vendor_noise_control,
        last_disconnect,
    } = reading;
    let services: Vec<Service> = metadata
        .uuids
        .iter()
        .map(|uuid| Service {
            uuid: uuid.clone(),
            label: service_label(uuid).into(),
        })
        .collect();
    let has_fast_pair = has_fast_pair_service(&metadata.uuids);
    let capabilities = device_capabilities(
        state.paired,
        state.connected,
        state.blocked,
        state.wake_allowed,
        has_fast_pair,
        features.as_ref(),
        vendor_noise_control.as_ref(),
    );
    let noise_control = vendor_noise_control.or_else(|| {
        features
            .as_ref()
            .and_then(|features| features.noise_control.as_ref())
            .map(NoiseControl::from_fast_pair)
    });
    let icon_id = crate::model::icon_id(
        &presentation.device_type,
        presentation.icon.as_deref(),
        &presentation.battery,
        &services,
    )
    .into();
    Device {
        key: context.key,
        adapter_key: context.adapter_key,
        identity: DeviceIdentity {
            name: metadata.alias.clone(),
            alias: metadata.alias,
            remote_name: metadata.remote_name,
            device_type: presentation.device_type,
            address: context.address,
            address_type: metadata.address_type,
            icon: presentation.icon,
            modalias: metadata.modalias,
        },
        state: DeviceState {
            paired: state.paired,
            bonded,
            connected: state.connected,
            trusted: state.trusted,
            blocked: state.blocked,
            wake_allowed: state.wake_allowed,
            legacy_pairing: metadata.legacy_pairing,
            last_disconnect,
        },
        services: DeviceServices {
            services_resolved: metadata.services_resolved,
            uuids: metadata.uuids,
            services,
        },
        presentation: DevicePresentation {
            icon_id,
            battery: presentation.battery,
            battery_live: presentation.battery_live,
            battery_last_known: presentation.battery_last_known,
            components: presentation.components,
            model_id: presentation.model_id,
            fast_pair: features,
            noise_control,
            rssi: signal.rssi,
            rssi_kind: signal.kind,
            signal_strength: signal
                .rssi
                .filter(|_| signal.kind != Some(RssiKind::BrEdrRelative))
                .map(signal_strength),
            signal_level: signal.rssi.map(|rssi| signal_level(rssi, signal.kind)),
            signal_live: signal.live,
            present: state.connected || signal.present,
            last_seen_ms: signal.last_seen_ms,
        },
        policy: context.policy,
        capabilities,
    }
}

pub(super) fn cache_entry(device: &Device, now_ms: u64) -> Option<CachedDevice> {
    device.presentation.present.then(|| CachedDevice {
        device: device.clone(),
        observed_at_ms: if device.state.connected {
            now_ms
        } else {
            device.presentation.last_seen_ms.unwrap_or(now_ms)
        },
    })
}
