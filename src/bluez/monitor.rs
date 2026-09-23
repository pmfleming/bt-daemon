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
        let expiry = backend.observations.lock().await.next_expiry();
        let expiration = async {
            if let Some(deadline) = expiry {
                tokio::time::sleep_until(deadline.into()).await;
            } else {
                futures::future::pending::<()>().await;
            }
        };
        let event = tokio::select! {
            event = watches.streams.next() => event.context("BlueZ event streams ended")?,
            _ = expiration => { let _ = backend.changes.send(()); continue; }
        };
        handle_event(backend, &mut watches, event).await?;
        let _ = backend.changes.send(());
    }
}

async fn handle_event(backend: &BluezBackend, watches: &mut Watches, event: Event) -> Result<()> {
    match event {
        Event::Session(SessionEvent::AdapterAdded(name)) => watches.adapter(backend, &name).await?,
        Event::Session(SessionEvent::AdapterRemoved(name)) => watches.remove_adapter(&name),
        Event::Adapter(name, AdapterEvent::DeviceAdded(address)) => {
            record_discovered_device(backend, &name, address).await;
            if let Err(error) = watches.device(backend, &name, address).await {
                tracing::debug!(%error, "new device disappeared before monitoring");
            }
        }
        Event::Adapter(name, AdapterEvent::DeviceRemoved(address)) => {
            watches.remove(&format!("{name}/{address}"));
        }
        Event::Device(adapter, address, DeviceEvent::PropertyChanged(property)) => {
            handle_property(backend, &adapter, address, property).await;
        }
        Event::Ended(key) => {
            watches.remove(&key);
            // Recover an unexpectedly ended subscription, not every property change.
            anyhow::bail!("BlueZ subscription ended: {key}");
        }
        _ => {}
    }
    Ok(())
}

async fn record_discovered_device(backend: &BluezBackend, name: &str, address: Address) {
    // InterfacesAdded is a real observation; discovery-start enumeration is not.
    let Ok(adapter) = backend.session.adapter(name) else {
        return;
    };
    if !adapter.is_discovering().await.unwrap_or(false) {
        return;
    }
    let rssi = match adapter.device(address) {
        Ok(device) => device.rssi().await.ok().flatten(),
        Err(_) => None,
    };
    backend
        .observations
        .lock()
        .await
        .record(name, address, rssi);
}

async fn handle_property(
    backend: &BluezBackend,
    adapter: &str,
    address: Address,
    property: DeviceProperty,
) {
    // Do not log arbitrary names/manufacturer data from device properties.
    match property {
        DeviceProperty::Rssi(rssi) => {
            backend
                .observations
                .lock()
                .await
                .record(adapter, address, Some(rssi))
        }
        DeviceProperty::Paired(paired) => {
            if let Some(provider) = &backend.fast_pair {
                provider.note_pairing(adapter, address, paired).await;
            }
        }
        DeviceProperty::Connected(connected) => {
            if let Some(provider) = &backend.fast_pair {
                provider.note_connection_change(address).await;
            }
            tracing::info!(%adapter, %address, connected, "BlueZ device connection changed");
        }
        DeviceProperty::ServicesResolved(resolved) => {
            tracing::debug!(%adapter, %address, resolved, "BlueZ device services changed");
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::{Event, Watches};
    use futures::{FutureExt, StreamExt, channel::mpsc};

    #[test]
    fn watches_preserve_live_peers_and_allow_replacement_after_removal() {
        let mut watches = Watches::default();
        let (events, stream) = mpsc::unbounded();
        watches.insert("hci1/device".into(), stream.boxed());
        watches.insert("hci1/device".into(), futures::stream::empty().boxed());
        for key in ["hci0", "hci0/device"] {
            watches.insert(key.into(), futures::stream::pending().boxed());
        }
        watches.remove_adapter("hci0");
        events
            .unbounded_send(Event::Adapter(
                "hci1".into(),
                bluer::AdapterEvent::DeviceAdded(bluer::Address::default()),
            ))
            .unwrap();
        assert!(matches!(watches.streams.next().now_or_never(),
            Some(Some(Event::Adapter(name, _))) if name == "hci1"));
        drop(events);
        assert!(matches!(watches.streams.next().now_or_never(),
            Some(Some(Event::Ended(key))) if key == "hci1/device"));
        watches.remove("hci1/device");
        watches.remove("missing");
        watches.insert("hci1/device".into(), futures::stream::empty().boxed());
        assert!(matches!(watches.streams.next().now_or_never(),
            Some(Some(Event::Ended(key))) if key == "hci1/device"));
        watches.remove_adapter("hci1");
        assert!(matches!(watches.streams.next().now_or_never(), Some(None)));
    }
}
