//! Snapshot construction from captured evidence; no clocks, locks or hardware I/O.
use super::{
    BluezDeviceState, CachedDevice, DeviceMetadata, ResolvedPresentation, device_capabilities,
    has_fast_pair_service, service_label, signal_strength,
};
use crate::{
    bluez::{link_signal, observations::Observation},
    management::DevicePolicy,
    model::{
        Device, DeviceIdentity, DevicePresentation, DeviceServices, DeviceState, FastPairFeatures,
        RssiKind, Service,
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
        last_disconnect,
    } = reading;
    let services = metadata
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
    );
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
            battery: presentation.battery,
            battery_live: presentation.battery_live,
            battery_last_known: presentation.battery_last_known,
            components: presentation.components,
            model_id: presentation.model_id,
            fast_pair: features,
            rssi: signal.rssi,
            rssi_kind: signal.kind,
            signal_strength: signal
                .rssi
                .filter(|_| signal.kind != Some(RssiKind::BrEdrRelative))
                .map(signal_strength),
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

#[cfg(test)]
mod tests {
    use super::{Context, Reading, Signal, build, cache_entry};
    use crate::{
        bluez::{
            observations::Observations,
            snapshot::{BluezDeviceState, DeviceMetadata, ResolvedPresentation},
        },
        management::ManagementStore,
    };

    #[test]
    fn disconnect_reports_are_additive_and_do_not_override_current_connection_state() {
        let mut reading = reading();
        reading.state.connected = true;
        reading.last_disconnect = Some(crate::model::DeviceDisconnect {
            reason: crate::model::DisconnectReason::Remote,
            message: "terminated".into(),
            observed_at_ms: 42,
        });
        let device = assemble(reading, Signal::resolve(None, None, false));
        let value = serde_json::to_value(&device).unwrap();
        assert_eq!(value["connected"], true);
        assert_eq!(value["last_disconnect"]["reason"], "remote");
        assert_eq!(value["last_disconnect"]["observed_at_ms"], 42);
        let device = assemble(self::reading(), Signal::resolve(None, None, false));
        assert!(
            serde_json::to_value(device)
                .unwrap()
                .get("last_disconnect")
                .is_none()
        );
    }

    fn reading() -> Reading {
        Reading {
            state: BluezDeviceState {
                paired: false,
                connected: false,
                trusted: false,
                blocked: false,
                wake_allowed: None,
            },
            metadata: DeviceMetadata {
                alias: "Buds".into(),
                remote_name: None,
                address_type: "public".into(),
                icon: None,
                services_resolved: false,
                legacy_pairing: false,
                modalias: None,
                uuids: vec![],
            },
            bonded: None,
            features: None,
            last_disconnect: None,
        }
    }
    fn assemble(reading: Reading, signal: Signal) -> crate::model::Device {
        build(
            Context {
                key: "opaque-peer".into(),
                adapter_key: "opaque-adapter".into(),
                address: "AA:BB:CC:DD:EE:FF".into(),
                policy: ManagementStore::in_memory().device_policy("opaque-peer"),
            },
            reading,
            ResolvedPresentation {
                icon: None,
                device_type: "unknown".into(),
                model_id: None,
                components: vec![],
                battery: vec![],
                battery_live: false,
                battery_last_known: false,
            },
            signal,
        )
    }

    #[test]
    fn connected_signal_precedes_discovery_and_failed_reads_retain_honest_units() {
        use crate::{bluez::link_signal, model::RssiKind};
        let mut initial = reading();
        initial.metadata.uuids = vec![
            "00001105-0000-1000-8000-00805F9B34FB".into(),
            "00001105-0000-1000-8000-ffffffffffff".into(),
        ];
        let unknown = assemble(initial, Signal::resolve(None, None, false));
        assert_eq!(unknown.services.services[0].label, "Object Push");
        assert_eq!(unknown.services.services[1].label, "Bluetooth service");
        assert!(unknown.state.bonded.is_none() && unknown.state.wake_allowed.is_none());
        assert!(unknown.identity.remote_name.is_none() && unknown.presentation.rssi.is_none());
        assert!(!unknown.presentation.present && cache_entry(&unknown, 100).is_none());
        let mut observations = Observations::default();
        let peer = bluer::Address::default();
        observations.record("hci0", peer, Some(-70));
        let seen = observations.get("hci0", peer).unwrap();
        assert!(seen.live(true) && !seen.live(false));
        assert_eq!(
            observations.get("hci0", peer).unwrap().last_seen_ms,
            seen.last_seen_ms
        );
        assert!(observations.get("hci1", peer).is_none());
        let discovered = assemble(reading(), Signal::resolve(Some(&seen), None, true));
        assert_eq!(
            cache_entry(&discovered, seen.last_seen_ms + 10)
                .unwrap()
                .observed_at_ms,
            seen.last_seen_ms
        );
        let link = link_signal::Reading {
            rssi: 0,
            kind: RssiKind::BrEdrRelative,
            live: true,
            last_seen_ms: seen.last_seen_ms + 10,
        };
        let signal = Signal::resolve(Some(&seen), None, true).with_link(Some(link));
        let mut connected = reading();
        connected.state.connected = true;
        let device = assemble(connected, signal);
        assert_eq!(device.presentation.rssi, Some(0));
        assert_eq!(device.presentation.rssi_kind, Some(RssiKind::BrEdrRelative));
        assert_eq!(device.presentation.signal_strength, None);
        assert!(device.presentation.signal_live);
        let json = serde_json::to_value(&device).unwrap();
        assert_eq!(json["rssi_kind"], "bredr-relative");
        let cached = cache_entry(&device, link.last_seen_ms).unwrap();
        // A stale discovery must not replace a newer cached connected reading,
        // even after the optional sampler no longer holds that sample.
        let fallback = Signal::resolve(Some(&seen), Some(&cached), false).with_link(None);
        assert_eq!(fallback.rssi, Some(0));
        assert_eq!(fallback.kind, Some(RssiKind::BrEdrRelative));
        assert!(!fallback.live);
        // A fresh discovery beats a failed/stale link sample, regardless of its timestamp.
        let stale = link_signal::Reading {
            live: false,
            ..link
        };
        let discovery = Signal::resolve(Some(&seen), Some(&cached), true).with_link(Some(stale));
        assert_eq!(discovery.rssi, Some(-70));
        assert_eq!(discovery.kind, Some(RssiKind::Dbm));
        let le = link_signal::Reading {
            rssi: -58,
            kind: RssiKind::Dbm,
            ..link
        };
        let mut connected = reading();
        connected.state.connected = true;
        let device = assemble(
            connected,
            Signal::resolve(None, None, false).with_link(Some(le)),
        );
        assert_eq!(device.presentation.signal_strength, Some(70));
        assert!(device.presentation.signal_live && device.presentation.present);
        for (rssi, expected) in [(i16::MIN, 0), (i16::MAX, 100)] {
            let signal = Signal::resolve(None, None, false)
                .with_link(Some(link_signal::Reading { rssi, ..le }));
            assert_eq!(
                assemble(reading(), signal).presentation.signal_strength,
                Some(expected)
            );
        }
        // An observation without RSSI cannot relabel a cached number as live.
        observations.record("hci0", peer, None);
        let missing = observations.get("hci0", peer).unwrap();
        let unknown = Signal::resolve(Some(&missing), Some(&cached), true);
        assert!(unknown.present);
        assert!(!unknown.live);
        assert_eq!(unknown.rssi, Some(0));
    }
}
