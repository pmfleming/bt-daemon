//! Public audio projection, shared by live snapshot serialization and contract tests.
use serde::Serialize;
#[derive(Serialize)]
#[cfg_attr(test, derive(serde::Deserialize))]
pub(crate) struct Device {
    pub device_key: String,
    pub active_profile_key: Option<String>,
    pub profiles: Vec<Profile>,
    pub endpoints: Vec<Endpoint>,
    pub sink: Option<Endpoint>,
    pub source: Option<Endpoint>,
}
#[derive(Serialize)]
#[cfg_attr(test, derive(serde::Deserialize))]
pub(crate) struct Profile {
    pub key: String,
    pub label: String,
    pub mode: String,
    pub codec: Option<String>,
    pub available: bool,
    pub priority: i32,
}
#[derive(Serialize)]
#[cfg_attr(test, derive(serde::Deserialize))]
pub(crate) struct Endpoint {
    pub key: String,
    pub kind: crate::audio::EndpointKind,
    pub codec: Option<String>,
    pub ready: bool,
    pub state: String,
    pub is_default: bool,
}
