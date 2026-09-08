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
            .spawn(move || run_writer(path, updates, messages))
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
fn run_writer(
    path: PathBuf,
    updates: Arc<Mutex<Option<RegistryFile>>>,
    messages: mpsc::Receiver<Message>,
) {
    let mut last_error = None;
    while let Ok(message) = messages.recv() {
        let barriers = collect_barriers(message, &messages);
        let latest = updates.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(state) = latest {
            last_error = persist(&path, &state);
        }
        for reply in barriers {
            let _ = reply.send(last_error.clone().map_or(Ok(()), Err));
        }
    }
}

fn collect_barriers(
    first: Message,
    messages: &mpsc::Receiver<Message>,
) -> Vec<mpsc::Sender<std::result::Result<(), String>>> {
    let mut barriers = Vec::new();
    match first {
        Message::Changed => thread::sleep(Duration::from_millis(150)),
        Message::Flush(reply) => barriers.push(reply),
    }
    // The latest-value slot bounds memory; all pending flush callers see its result.
    for message in messages.try_iter() {
        if let Message::Flush(reply) = message {
            barriers.push(reply);
        }
    }
    barriers
}

fn persist(path: &std::path::Path, state: &RegistryFile) -> Option<String> {
    let error = crate::state::write_json(path, state, "identity registry")
        .err()
        .map(|error| format!("{error:#}"));
    if let Some(error) = &error {
        tracing::warn!(%error, "could not persist Bluetooth identity registry");
    }
    error
}

#[cfg(test)]
mod tests;

impl Drop for Writer {
    fn drop(&mut self) {
        // Closing the channel lets the last queued update flush before shutdown.
        self.wake.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
