//! Drive one replacement future at a time, without blocking owner observation.
use anyhow::{Result, bail};
use futures::{Stream, StreamExt};
use std::future::Future;

type Change = (String, String, String);

pub(super) async fn drive<S, F, R>(events: S, mut replace: F) -> Result<()>
where
    S: Stream<Item = Result<Change>>,
    F: FnMut(String) -> R,
    R: Future<Output = ()>,
{
    futures::pin_mut!(events);
    let mut replacement: Option<std::pin::Pin<Box<R>>> = None;
    loop {
        tokio::select! {
            biased;
            event = events.next() => {
                let Some(event) = event else { bail!("BlueZ recovery owner stream ended"); };
                let (name, old, new) = event?;
                if name != "org.bluez" || old == new { continue; }
                // Dropping a stale replacement must release its candidate workers.
                drop(replacement.take());
                replacement = Some(Box::pin(replace(new)));
            }
            _ = async {
                match &mut replacement {
                    Some(future) => future.as_mut().await,
                    None => std::future::pending().await,
                }
            } => { replacement = None; }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::drive;
    use futures::channel::mpsc;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    struct Candidate(Arc<Mutex<Vec<String>>>, String);
    impl Drop for Candidate {
        fn drop(&mut self) {
            self.0.lock().unwrap().push(format!("drop:{}", self.1));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn owner_loss_cancels_a_retrying_candidate_and_only_latest_owner_is_installed() {
        let (events, stream) = mpsc::unbounded();
        let log = Arc::new(Mutex::new(Vec::new()));
        let changes = log.clone();
        let worker = tokio::spawn(drive(stream, move |owner| {
            let log = changes.clone();
            async move {
                if owner.is_empty() {
                    log.lock().unwrap().push("unavailable".into());
                    return;
                }
                let _candidate = Candidate(log.clone(), owner.clone());
                log.lock().unwrap().push(format!("start:{owner}"));
                tokio::time::sleep(Duration::from_secs(10)).await;
                log.lock().unwrap().push(format!("install:{owner}"));
            }
        }));
        let emit = |name: &str, old: &str, new: &str| {
            events
                .unbounded_send(Ok((name.into(), old.into(), new.into())))
                .unwrap()
        };
        emit("org.bluez", "", "old");
        tokio::task::yield_now().await;
        emit("unrelated", "", "ignored");
        emit("org.bluez", "old", "old");
        tokio::task::yield_now().await;
        assert_eq!(*log.lock().unwrap(), ["start:old"]);
        emit("org.bluez", "old", "");
        tokio::task::yield_now().await;
        assert_eq!(
            *log.lock().unwrap(),
            ["start:old", "drop:old", "unavailable"]
        );
        emit("org.bluez", "", "new");
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            *log.lock().unwrap(),
            [
                "start:old",
                "drop:old",
                "unavailable",
                "start:new",
                "install:new",
                "drop:new"
            ]
        );
        drop(events);
        assert!(worker.await.unwrap().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn stream_failure_drops_inflight_replacement() {
        let (events, stream) = mpsc::unbounded();
        let marker = Arc::new(());
        let held = marker.clone();
        let worker = tokio::spawn(drive(stream, move |_| {
            let guard = held.clone();
            async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            }
        }));
        events
            .unbounded_send(Ok(("org.bluez".into(), "".into(), "owner".into())))
            .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(Arc::strong_count(&marker), 3);
        events
            .unbounded_send(Err(anyhow::anyhow!("bus lost")))
            .unwrap();
        assert_eq!(worker.await.unwrap().unwrap_err().to_string(), "bus lost");
        assert_eq!(Arc::strong_count(&marker), 1);
    }
}
