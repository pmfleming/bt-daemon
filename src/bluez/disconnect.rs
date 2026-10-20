//! Optional BlueZ Device1 diagnostics. No reconnection decisions or durable state.
use std::collections::HashMap;

use anyhow::{Context, Result};
use bluer::Address;
use futures::{StreamExt, stream::BoxStream};
use zbus::{Connection, MatchRule, MessageStream, message::Type};

use crate::model::{DeviceDisconnect, DisconnectReason};

pub(super) type Report = (String, Address, DeviceDisconnect);
const MAX_HISTORY: usize = 256;

#[derive(Default)]
pub(super) struct History(HashMap<(String, Address), DeviceDisconnect>);

impl History {
    pub fn record(&mut self, adapter: String, address: Address, report: DeviceDisconnect) {
        let key = (adapter, address);
        if !self.0.contains_key(&key) && self.0.len() >= MAX_HISTORY {
            let oldest = self
                .0
                .iter()
                .min_by_key(|(_, report)| report.observed_at_ms)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                self.0.remove(&oldest);
            }
        }
        self.0.insert(key, report);
    }

    pub fn get(&self, adapter: &str, address: Address) -> Option<DeviceDisconnect> {
        self.0.get(&(adapter.to_owned(), address)).cloned()
    }

    pub fn remove(&mut self, adapter: &str, address: Option<Address>) {
        self.0.retain(|(name, peer), _| {
            name != adapter || address.is_some_and(|address| address != *peer)
        });
    }
}

pub(super) async fn listen(connection: &Connection) -> Result<BoxStream<'static, Result<Report>>> {
    let bus = zbus::fdo::DBusProxy::new(connection).await?;
    let owner = bus
        .get_name_owner("org.bluez".try_into()?)
        .await
        .context("resolve BlueZ owner for disconnect diagnostics")?;
    // Pin to this backend generation's unique sender. A similarly named signal
    // from another process must not be allowed to influence our diagnostics.
    let rule = MatchRule::builder()
        .msg_type(Type::Signal)
        .sender(owner)?
        .interface("org.bluez.Device1")?
        .member("Disconnected")?
        .path_namespace("/org/bluez")?
        .build();
    let stream = MessageStream::for_match_rule(rule, connection, Some(64)).await?;
    Ok(stream
        .filter_map(|message| async move {
            match message {
                Err(error) => Some(Err(error.into())),
                Ok(message) => {
                    let header = message.header();
                    let (adapter, address) = device_path(header.path()?.as_str())?;
                    let body = message.body();
                    let (reason, text): (&str, &str) = match body.deserialize() {
                        Ok(value) => value,
                        Err(_) => {
                            tracing::warn!("ignored malformed BlueZ Disconnected signal");
                            return None;
                        }
                    };
                    Some(Ok((adapter, address, report(reason, text))))
                }
            }
        })
        .boxed())
}

fn device_path(path: &str) -> Option<(String, Address)> {
    let (adapter, peer) = path.strip_prefix("/org/bluez/")?.split_once('/')?;
    let index = adapter.strip_prefix("hci")?;
    if index.is_empty() || !index.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let peer = peer.strip_prefix("dev_")?;
    let bytes: Vec<_> = peer.split('_').collect();
    if bytes.len() != 6
        || bytes
            .iter()
            .any(|byte| byte.len() != 2 || !byte.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        return None;
    }
    Some((adapter.to_owned(), peer.replace('_', ":").parse().ok()?))
}

fn report(reason: &str, message: &str) -> DeviceDisconnect {
    let reason = match reason {
        "org.bluez.Reason.Timeout" => DisconnectReason::Timeout,
        "org.bluez.Reason.Local" => DisconnectReason::Local,
        "org.bluez.Reason.Remote" => DisconnectReason::Remote,
        "org.bluez.Reason.Authentication" => DisconnectReason::Authentication,
        "org.bluez.Reason.Suspend" => DisconnectReason::Suspend,
        _ => DisconnectReason::Unknown,
    };
    DeviceDisconnect {
        reason,
        message: message
            .chars()
            .filter(|c| {
                !c.is_control() && !matches!(*c, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
            })
            .take(256)
            .collect(),
        observed_at_ms: crate::time::unix_time_ms(),
    }
}

#[cfg(test)]
mod tests;
