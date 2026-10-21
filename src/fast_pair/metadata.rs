//! Operator-provisioned model metadata is a trust root, never supplied by an
//! advertisement or silently downloaded from an unauthenticated endpoint.
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use std::{collections::HashMap, fs::File, io::Read, os::unix::fs::PermissionsExt, path::Path};

#[derive(Default)]
pub(super) struct Catalog(HashMap<[u8; 3], Model>);
#[derive(Deserialize)]
struct MetadataFile {
    version: u8,
    models: HashMap<String, Model>,
}
#[derive(Deserialize)]
pub(super) struct Model {
    pub name: String,
    pub anti_spoofing_public_key: String,
}

impl Catalog {
    pub fn load_default() -> Result<Self> {
        let path = std::env::var_os("BT_DAEMON_FAST_PAIR_METADATA")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| "/etc/bluetooth/fast-pair-models.json".into());
        Self::load(&path)
    }
    fn load(path: &Path) -> Result<Self> {
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => return Err(error.into()),
        };
        let metadata = file.metadata()?;
        ensure!(
            metadata.is_file(),
            "Fast Pair metadata must be a regular file"
        );
        ensure!(
            metadata.permissions().mode() & 0o022 == 0,
            "Fast Pair metadata must not be group/world writable"
        );
        let mut data = Vec::new();
        file.take(1_048_577).read_to_end(&mut data)?;
        ensure!(data.len() <= 1_048_576, "Fast Pair metadata exceeds 1 MiB");
        Self::parse(&data).context("validate trusted Fast Pair model metadata")
    }
    fn parse(data: &[u8]) -> Result<Self> {
        let file: MetadataFile = serde_json::from_slice(data)?;
        ensure!(file.version == 1, "unsupported Fast Pair metadata version");
        ensure!(
            file.models.len() <= 1024,
            "too many Fast Pair model entries"
        );
        let mut models = HashMap::new();
        for (id, model) in file.models {
            let bytes = hex::decode(id)?;
            let id: [u8; 3] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("model ID must be three bytes"))?;
            ensure!(
                !model.name.is_empty()
                    && model.name.len() <= 256
                    && !model.name.chars().any(char::is_control),
                "invalid model display name"
            );
            super::parse_anti_spoofing_public_key(&model.anti_spoofing_public_key)?;
            ensure!(models.insert(id, model).is_none(), "duplicate model ID");
        }
        Ok(Self(models))
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn get(&self, id: &[u8; 3]) -> Option<&Model> {
        self.0.get(id)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Advertisement {
    pub model_id: [u8; 3],
    pub capabilities: Option<crate::model::FastPairAdvertisementCapabilities>,
}

/// Parse legacy Model ID data or GFPS 3.3.1 LE Audio Sharing data.
/// This is untrusted presentation metadata, never evidence of pairing or identity.
/// Unknown versions and Model ID widths cannot select our 24-bit model catalog.
pub(super) fn advertisement(data: &[u8]) -> Option<Advertisement> {
    if let Ok(model_id) = data.try_into() {
        return Some(Advertisement {
            model_id,
            capabilities: None,
        });
    }
    let (&version_flags, mut fields) = data.split_first()?;
    if version_flags != 0 {
        return None;
    }
    let mut model_id = None;
    let mut capabilities = None;
    while let Some((&header, rest)) = fields.split_first() {
        let length = usize::from(header >> 4);
        if length == 0 || rest.len() < length {
            return None;
        }
        let (value, remaining) = rest.split_at(length);
        fields = remaining;
        let duplicate = match header & 0x0f {
            // Account-key filters (show/hide UI) are not discoverable Model IDs.
            0 | 2 => return None,
            7 => model_id.replace(value.try_into().ok()?).is_some(),
            8 => capabilities
                .replace(crate::model::FastPairAdvertisementCapabilities {
                    le_audio_sharing: value[0] & 2 != 0,
                    factory_new: value[0] & 1 != 0,
                })
                .is_some(),
            _ => false, // Length-delimited extension; reserved capability bits are ignored.
        };
        if duplicate {
            return None;
        }
    }
    Some(Advertisement {
        model_id: model_id?,
        capabilities: Some(capabilities?),
    })
}

#[cfg(test)]
mod tests {
    use super::{Catalog, advertisement};
    #[test]
    fn sharing_advertisements_are_bounded_versioned_and_not_account_filters() {
        let parsed = advertisement(&[0, 0x37, 0xaa, 0xbb, 0xcc, 0x18, 3]).unwrap();
        assert_eq!(parsed.model_id, [0xaa, 0xbb, 0xcc]);
        let capabilities = parsed.capabilities.unwrap();
        assert!(capabilities.le_audio_sharing && capabilities.factory_new);
        // Extension fields, longer capability maps and reserved bits are harmless.
        let extended = [0, 0x19, 0xff, 0x28, 0xfc, 0xff, 0x37, 1, 2, 3];
        let parsed = advertisement(&extended).unwrap();
        assert_eq!(parsed.model_id, [1, 2, 3]);
        assert!(!parsed.capabilities.unwrap().le_audio_sharing);
        for bad in [
            &[][..],
            &[0],
            &[0, 0x37],
            &[0, 0x37, 1, 2],
            &[0x10, 0x37, 1, 2, 3, 0x18, 3],
            &[1, 0x37, 1, 2, 3, 0x18, 3],
            &[0, 0x37, 1, 2, 3],                   // Missing capability map.
            &[0, 0x18, 3, 0x18, 3, 0x37, 1, 2, 3], // Duplicate.
            &[0, 0x37, 1, 2, 3, 0x37, 1, 2, 3, 0x18, 3],
            &[0, 0x27, 1, 2, 0x18, 3],    // Unsupported model width.
            &[0, 0x37, 1, 2, 3, 0x08],    // Empty capability map.
            &[0, 0x37, 1, 2, 3, 0x28, 3], // Truncated map.
            &[0, 0x30, 1, 2, 3, 0x18, 3], // Account-key filter.
            &[0, 0x32, 1, 2, 3, 0x18, 3],
        ] {
            assert!(advertisement(bad).is_none(), "accepted {bad:?}");
        }
    }

    #[test]
    fn catalog_checks_public_key_and_model_identity() {
        // SEC1 P-256 generator coordinates, for parser tests only (not production metadata).
        let key = "6b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c2964fe342e2fe1a7f9b8ee7eb4a7c0f9e162bce33576b315ececbb6406837bf51f5";
        let data = serde_json::json!({"version":1,"models":{"aabbcc":{"name":"Test","anti_spoofing_public_key":key}}});
        let catalog = Catalog::parse(&serde_json::to_vec(&data).unwrap()).unwrap();
        assert!(catalog.get(&[0xaa, 0xbb, 0xcc]).is_some());
        assert!(catalog.get(&[0, 0, 0]).is_none());
        assert!(Catalog::parse(br#"{"version":2,"models":{}}"#).is_err());
        assert!(Catalog::parse(br#"{"version":1,"models":{"aabbcc":{"name":"Invalid","anti_spoofing_public_key":"00"}}}"#).is_err());
        assert_eq!(advertisement(&[1, 2, 3]).unwrap().model_id, [1, 2, 3]);
        assert_eq!(advertisement(&[0, 1, 2, 3]), None);
    }
}
