//! Persistent BlueZ subscriptions: property updates never tear down other watches.
use super::{BluezBackend, BluezResultExt};
use anyhow::{Context, Result};
use bluer::{AdapterEvent, Address, DeviceEvent, DeviceProperty, SessionEvent};
use futures::{
    StreamExt,
    stream::{AbortHandle, Abortable, BoxStream, SelectAll},
};
use std::collections::HashMap;

enum Event {
    Session(SessionEvent),
    Adapter(String, AdapterEvent),
    Device(String, Address, DeviceEvent),
    Ended(String),
}

#[derive(Default)]
struct Watches {
    streams: SelectAll<BoxStream<'static, Event>>,
    handles: HashMap<String, AbortHandle>,
}

impl Watches {
    fn insert(&mut self, key: String, stream: BoxStream<'static, Event>) {
        if self.handles.contains_key(&key) {
            return;
        }
        let ended = key.clone();
        let (handle, registration) = AbortHandle::new_pair();
        self.handles.insert(key, handle);
        let stream = stream.chain(futures::stream::once(async move { Event::Ended(ended) }));
        self.streams
            .push(Abortable::new(stream, registration).boxed());
    }
    fn remove(&mut self, key: &str) {
        if let Some(handle) = self.handles.remove(key) {
            handle.abort();
        }
    }
    fn remove_adapter(&mut self, name: &str) {
        let prefix = format!("{name}/");
        let keys: Vec<_> = self
            .handles
            .keys()
            .filter(|key| *key == name || key.starts_with(&prefix))
            .cloned()
            .collect();
        for key in keys {
            self.remove(&key);
        }
    }
    async fn device(
        &mut self,
        backend: &BluezBackend,
        adapter: &str,
        address: Address,
    ) -> Result<()> {
        let key = format!("{adapter}/{address}");
        if self.handles.contains_key(&key) {
            return Ok(());
        }
        let device = backend.session.adapter(adapter)?.device(address)?;
        let name = adapter.to_owned();
        let stream = device
            .events()
            .await
            .backend_context("watch BlueZ device")?;
        self.insert(
            key,
            stream
                .map(move |event| Event::Device(name.clone(), address, event))
                .boxed(),
        );
        Ok(())
    }
    async fn adapter(&mut self, backend: &BluezBackend, name: &str) -> Result<()> {
        if self.handles.contains_key(name) {
            return Ok(());
        }
        let adapter = backend.session.adapter(name)?;
        let owned = name.to_owned();
        let stream = adapter
            .events()
            .await
            .backend_context("watch BlueZ adapter")?;
        self.insert(
            name.to_owned(),
            stream
                .map(move |event| Event::Adapter(owned.clone(), event))
                .boxed(),
        );
        for address in adapter.device_addresses().await? {
            if let Err(error) = self.device(backend, name, address).await {
                tracing::debug!(%error, "device disappeared during monitor initialization");
            }
        }
        Ok(())
    }
}

pub(super) async fn run(backend: &BluezBackend) -> Result<()> {
    let mut watches = Watches::default();
    watches.insert(
        "session".into(),
        backend.session.events().await?.map(Event::Session).boxed(),
    );
    for name in backend.session.adapter_names().await? {
        watches.adapter(backend, &name).await?;
    }
    loop {
        let event = watches
            .streams
            .next()
            .await
            .context("BlueZ event streams ended")?;
        match event {
            Event::Session(SessionEvent::AdapterAdded(name)) => {
                watches.adapter(backend, &name).await?
            }
            Event::Session(SessionEvent::AdapterRemoved(name)) => watches.remove_adapter(&name),
            Event::Adapter(name, AdapterEvent::DeviceAdded(address)) => {
                if let Err(error) = watches.device(backend, &name, address).await {
                    tracing::debug!(%error, "new device disappeared before monitoring");
                }
            }
            Event::Adapter(name, AdapterEvent::DeviceRemoved(address)) => {
                watches.remove(&format!("{name}/{address}"))
            }
            Event::Device(adapter, address, DeviceEvent::PropertyChanged(property)) => {
                // Preserve useful connection chronology without dumping arbitrary
                // device properties (names/manufacturer data may be sensitive).
                match property {
                    DeviceProperty::Connected(connected) => {
                        tracing::info!(%adapter, %address, connected, "BlueZ device connection changed")
                    }
                    DeviceProperty::ServicesResolved(resolved) => {
                        tracing::debug!(%adapter, %address, resolved, "BlueZ device services changed")
                    }
                    _ => {}
                }
            }
            Event::Ended(key) => {
                watches.remove(&key);
                // An unexpected stream ending must be recovered, unlike ordinary
                // property events. Rebuilding here is exceptional, not per-event.
                anyhow::bail!("BlueZ subscription ended: {key}");
            }
            _ => {}
        }
        let _ = backend.changes.send(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn removing_one_adapter_preserves_unrelated_subscriptions() {
        let mut watches = Watches::default();
        for key in ["hci0", "hci0/device", "hci1", "hci1/device"] {
            watches.insert(key.into(), futures::stream::pending().boxed());
        }
        watches.remove_adapter("hci0");
        assert_eq!(watches.handles.len(), 2);
        assert!(watches.handles.contains_key("hci1/device"));
    }
}
