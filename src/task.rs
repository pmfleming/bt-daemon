use std::{
    collections::HashMap,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex, OnceLock, Weak},
};

use anyhow::{Result, anyhow};
use futures::FutureExt;
use tokio::task::JoinHandle;

/// A shared device gate also covers resume policy and native audio operations,
/// which do not originate in OperationCoordinator.
pub(crate) fn device_gate(key: &str) -> Arc<tokio::sync::Mutex<()>> {
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

/// Owns one backend generation's workers. Shutdown closes the spawn gate before
/// aborting and joining, so a racing reconnect cannot install an orphan worker.
#[derive(Default)]
pub(crate) struct TaskGroup(Mutex<TaskGroupState>);

#[derive(Default)]
struct TaskGroupState {
    closed: bool,
    handles: Vec<JoinHandle<()>>,
}

impl TaskGroup {
    pub fn spawn(&self, name: &'static str, future: impl Future<Output = ()> + Send + 'static) {
        let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if state.closed {
            return;
        }
        state.handles.retain(|handle| !handle.is_finished());
        state.handles.push(spawn(name, future));
    }

    pub async fn shutdown(&self) {
        let handles = {
            let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
            state.closed = true;
            std::mem::take(&mut state.handles)
        };
        for handle in &handles {
            handle.abort();
        }
        for handle in handles {
            let _ = handle.await;
        }
    }
}

impl Drop for TaskGroup {
    fn drop(&mut self) {
        for handle in &self.0.get_mut().unwrap_or_else(|p| p.into_inner()).handles {
            handle.abort();
        }
    }
}

async fn catch_unwind<T>(name: &'static str, future: impl Future<Output = T>) -> Result<T> {
    AssertUnwindSafe(future)
        .catch_unwind()
        .await
        .map_err(|payload| {
            let panic = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("non-string panic");
            anyhow!("{name} panicked: {panic}")
        })
}

pub(crate) async fn catch<T>(
    name: &'static str,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    catch_unwind(name, future).await?
}

pub(crate) fn spawn(
    name: &'static str,
    future: impl Future<Output = ()> + Send + 'static,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        tracing::trace!(task = name, "background task started");
        match catch_unwind(name, future).await {
            Ok(()) => tracing::trace!(task = name, "background task ended"),
            Err(error) => tracing::error!(task = name, error = %error, "background task panicked"),
        }
    })
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn device_gates_serialize_same_device_only() {
        let first = super::device_gate("a");
        let same = super::device_gate("a");
        let other = super::device_gate("b");
        let held = first.lock().await;
        assert!(same.try_lock().is_err());
        assert!(other.try_lock().is_ok());
        drop(held);
        assert!(same.try_lock().is_ok());
    }

    #[tokio::test]
    async fn shutdown_joins_workers_and_prevents_new_spawns() {
        let tasks = super::TaskGroup::default();
        let marker = std::sync::Arc::new(());
        let held = marker.clone();
        tasks.spawn("test", async move {
            let _held = held;
            std::future::pending::<()>().await;
        });
        tasks.shutdown().await;
        assert_eq!(std::sync::Arc::strong_count(&marker), 1);
        let held = marker.clone();
        tasks.spawn("closed", async move {
            let _held = held;
            std::future::pending::<()>().await;
        });
        assert_eq!(std::sync::Arc::strong_count(&marker), 1);
        tasks.shutdown().await;
    }
}
