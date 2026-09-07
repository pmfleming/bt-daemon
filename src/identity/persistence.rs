//! Serialize/batch presentation updates off Tokio's runtime threads.
use super::RegistryFile;
use anyhow::{Context, Result};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex, mpsc},
    thread,
    time::Duration,
};

pub(super) struct Writer {
    pending: Arc<Mutex<Option<RegistryFile>>>,
    wake: Option<mpsc::SyncSender<Message>>,
    thread: Option<thread::JoinHandle<()>>,
}
enum Message {
    Changed,
    Flush(mpsc::Sender<std::result::Result<(), String>>),
}

impl Writer {
    pub fn new(path: PathBuf) -> Result<Self> {
        let pending = Arc::new(Mutex::new(None::<RegistryFile>));
        let updates = pending.clone();
        let (wake, messages) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("bt-identity-writer".into())
            .spawn(move || {
                let mut last_error: Option<String> = None;
                while let Ok(message) = messages.recv() {
                    let mut barriers = Vec::new();
                    match message {
                        Message::Changed => thread::sleep(Duration::from_millis(150)),
                        Message::Flush(reply) => barriers.push(reply),
                    }
                    // A single latest-value slot bounds memory and coalesces bursts.
                    while let Ok(message) = messages.try_recv() {
                        if let Message::Flush(reply) = message {
                            barriers.push(reply);
                        }
                    }
                    let latest = updates.lock().unwrap_or_else(|p| p.into_inner()).take();
                    if let Some(state) = latest {
                        last_error = crate::state::write_json(&path, &state, "identity registry")
                            .err()
                            .map(|error| format!("{error:#}"));
                        if let Some(error) = &last_error {
                            tracing::warn!(%error, "could not persist Bluetooth identity registry");
                        }
                    }
                    for reply in barriers {
                        let _ = reply.send(last_error.clone().map_or(Ok(()), Err));
                    }
                }
            })
            .context("start Bluetooth identity writer")?;
        Ok(Self {
            pending,
            wake: Some(wake),
            thread: Some(thread),
        })
    }
    pub fn schedule(&self, state: RegistryFile) {
        *self.pending.lock().unwrap_or_else(|p| p.into_inner()) = Some(state);
        if let Some(wake) = &self.wake {
            let _ = wake.try_send(Message::Changed);
        }
    }
    pub fn flush(&self) -> Result<()> {
        let (reply, completed) = mpsc::channel();
        self.wake
            .as_ref()
            .context("identity writer closed")?
            .send(Message::Flush(reply))
            .context("identity writer stopped")?;
        completed
            .recv()
            .context("identity flush cancelled")?
            .map_err(anyhow::Error::msg)
    }
}
impl Drop for Writer {
    fn drop(&mut self) {
        // Closing the channel lets the last queued update flush before shutdown.
        self.wake.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
