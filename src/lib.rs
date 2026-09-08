//! Bluetooth policy service with a versioned JSON API and session D-Bus transport.
//!
//! Backends expose testable operations independently of the service transport.
//! Hardware operations require BlueZ/PipeWire; protocol metadata is available offline.
#![recursion_limit = "256"]
/// Versioned response envelopes and backend request validation.
pub mod api;
/// PipeWire Bluetooth profiles, endpoints and change monitoring.
pub mod audio;
/// Injectable Bluetooth operations and typed failure classification.
pub mod backend;
/// BlueZ access, monitoring, identity and recovery integration.
pub mod bluez;
/// Frontend-owned JSON Lines bridge to the session service.
pub mod client;
/// Session D-Bus service, request ownership and subscriptions.
pub mod daemon;
mod fast_pair;
mod identity;
mod management;
/// Serialized snapshots, device capabilities and presentation metadata.
pub mod model;
mod obex;
/// Pairing-agent prompts, responses and cancellation.
pub mod pairing;
/// Stable method, stream and schema registry for bt-api clients.
pub mod protocol;
mod rfkill;
mod state;
mod task;
