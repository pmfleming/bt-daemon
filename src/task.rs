use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock, Weak},
};

pub(crate) use shelllist_daemon_tokio::{TaskGroup, catch_task as catch, spawn_named as spawn};

/// Device serialization also covers resume policy and native audio operations.
pub(crate) fn device_gate(key: &str) -> Arc<tokio::sync::Mutex<()>> {
    mutation_gate(&format!("device:{key}"))
}

/// Serialize submitted adapter settings, including legacy single-setting requests.
pub(crate) fn adapter_gate(key: &str) -> Arc<tokio::sync::Mutex<()>> {
    mutation_gate(&format!("adapter:{key}"))
}

fn mutation_gate(key: &str) -> Arc<tokio::sync::Mutex<()>> {
    static GATES: OnceLock<Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>> = OnceLock::new();
    let mut gates = GATES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    gates.retain(|_, gate| gate.strong_count() > 0);
    if let Some(gate) = gates.get(key).and_then(Weak::upgrade) {
        return gate;
    }
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    gates.insert(key.to_owned(), Arc::downgrade(&gate));
    gate
}
