//! Pure presentation of trusted provisioning evidence and runtime capabilities.
use super::{RuntimeReport, metadata::Model};
use crate::model::FastPairFeatures;

pub(super) fn describe(
    runtime: RuntimeReport,
    account_key_available: bool,
    model: Option<&Model>,
    recent: bool,
) -> FastPairFeatures {
    let reason = provisioning_reason(account_key_available, model.is_some(), recent);
    FastPairFeatures {
        model_id: runtime.model_id.map(hex::encode),
        ble_address: runtime.ble_address.map(|address| address.to_string()),
        // The caller revalidates connection, nonce, credentials and operator policy.
        authenticated_controls: false,
        account_key_available,
        provisioning_available: reason.is_none(),
        provisioning_reason: reason.map(Into::into),
        trusted_model_name: model.map(|model| model.name.clone()),
        multipoint: runtime.multipoint,
        noise_control: runtime.noise_control,
        last_switch: runtime.last_switch,
        audio_switch_seeker_supported: false,
    }
}

fn provisioning_reason(
    has_key: bool,
    trusted_model: bool,
    recent_pairing: bool,
) -> Option<&'static str> {
    if has_key {
        Some("Account key already provisioned")
    } else if !trusted_model {
        Some("Trusted model metadata is missing; configure fast-pair-models.json")
    } else if !recent_pairing {
        Some("Re-pair this device to open its one-minute Fast Pair provisioning window")
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{describe, provisioning_reason};
    use crate::fast_pair::{RuntimeReport, metadata::Model};

    #[test]
    fn provisioning_requires_all_prerequisites_and_never_replaces_an_existing_key() {
        for key in [false, true] {
            for trusted in [false, true] {
                for recent in [false, true] {
                    assert_eq!(
                        provisioning_reason(key, trusted, recent).is_none(),
                        !key && trusted && recent
                    );
                }
            }
        }
        assert_eq!(
            provisioning_reason(true, false, false),
            Some("Account key already provisioned")
        );
    }

    #[test]
    fn presentation_keeps_trusted_identity_separate_from_observed_identity() {
        let runtime = RuntimeReport {
            model_id: Some([0xaa, 0xbb, 0xcc]),
            ..RuntimeReport::default()
        };
        let unknown = describe(runtime.clone(), false, None, true);
        assert_eq!(unknown.model_id.as_deref(), Some("aabbcc"));
        assert!(unknown.trusted_model_name.is_none());
        assert!(!unknown.provisioning_available);
        assert!(!unknown.authenticated_controls);
        let model = Model {
            name: "Trusted buds".into(),
            anti_spoofing_public_key: "not used by presentation".into(),
        };
        let known = describe(runtime, false, Some(&model), true);
        assert_eq!(known.trusted_model_name.as_deref(), Some("Trusted buds"));
        assert!(known.provisioning_available);
        assert!(known.provisioning_reason.is_none());
    }
}
