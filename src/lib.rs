#![recursion_limit = "256"]
pub mod api;
pub mod audio;
pub mod backend;
pub mod bluez;
pub mod client;
pub mod daemon;
mod fast_pair;
mod identity;
mod management;
pub mod model;
mod obex;
pub mod pairing;
pub mod protocol;
mod rfkill;
mod state;
mod task;
