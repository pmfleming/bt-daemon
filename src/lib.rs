#![recursion_limit = "256"]

pub mod api;
pub mod audio;
pub mod backend;
pub mod bluez;
pub mod client;
pub mod daemon;
pub mod fast_pair;
pub mod identity;
pub mod management;
pub mod model;
pub mod obex;
pub mod pairing;
pub mod protocol;
mod rfkill;
mod state;
mod task;
