use std::{future::Future, panic::AssertUnwindSafe};

use anyhow::{Result, anyhow};
use futures::FutureExt;
use tokio::task::JoinHandle;

pub(crate) async fn catch<T>(name: &'static str, future: impl Future<Output = T>) -> Result<T> {
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

pub(crate) fn spawn(
    name: &'static str,
    future: impl Future<Output = ()> + Send + 'static,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        tracing::trace!(task = name, "background task started");
        match catch(name, future).await {
            Ok(()) => tracing::trace!(task = name, "background task ended"),
            Err(error) => tracing::error!(task = name, error = %error, "background task panicked"),
        }
    })
}
