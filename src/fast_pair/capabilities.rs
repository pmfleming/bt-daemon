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
    use super::describe;
    use crate::fast_pair::{RuntimeReport, metadata::Model};

    #[test]
    fn advertised_features_keep_trust_separate_and_require_all_provisioning_prerequisites() {
        let model = Model {
            name: "Trusted buds".into(),
            anti_spoofing_public_key: "not used by presentation".into(),
        };
        for (key, trusted, recent) in [
            (false, false, false),
            (false, false, true),
            (false, true, false),
            (false, true, true),
            (true, false, false),
            (true, false, true),
            (true, true, false),
            (true, true, true),
        ] {
            let features = describe(
                RuntimeReport {
                    model_id: Some([0xaa, 0xbb, 0xcc]),
                    ..RuntimeReport::default()
                },
                key,
                trusted.then_some(&model),
                recent,
            );
            assert_eq!(features.model_id.as_deref(), Some("aabbcc"));
            assert_eq!(
                features.trusted_model_name.as_deref(),
                trusted.then_some("Trusted buds")
            );
            assert!(!features.authenticated_controls);
            assert_eq!(features.account_key_available, key);
            assert_eq!(
                features.provisioning_available,
                !key && trusted && recent,
                "key={key}, trusted={trusted}, recent={recent}"
            );
            assert_eq!(
                features.provisioning_reason.is_none(),
                features.provisioning_available
            );
            if key {
                assert_eq!(
                    features.provisioning_reason.as_deref(),
                    Some("Account key already provisioned")
                );
            }
        }
    }
}
