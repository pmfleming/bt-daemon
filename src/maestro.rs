//! Pixel Buds component battery over Google's Maestro RFCOMM channel.
//!
//! Pixel Buds leave the case unknown in Fast Pair battery updates, even while
//! docked, but their Maestro runtime-info stream reports it whenever a bud is
//! in the case. The case has no radio, so the last docked reading is kept for
//! the rest of the session and marked as last-known.
use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use bluer::{Address, Session, rfcomm::Stream};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::{RwLock, broadcast},
};

use crate::{
    model::Battery,
    vendor_channel::{self, Channels},
};

mod protocol;
use protocol::{Batteries, FrameDecoder, Reading, Update};

const SOURCE: &str = "google-maestro";
/// The buds announce their RPC channel as soon as the RFCOMM link opens.
const ANNOUNCE_TIMEOUT: Duration = Duration::from_secs(5);

static SPEC: vendor_channel::Spec = vendor_channel::Spec {
    uuid: "25e97ff7-24ce-4c4c-8951-f764a708f7b5",
    profile_name: "Shelllist Pixel Buds Maestro",
    label: "Maestro",
    request_task: "maestro-requests",
    session_task: "maestro-session",
    reconciler_task: "maestro-reconciler",
    connect_task: "maestro-connect",
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Report {
    batteries: Batteries,
    /// False once the case reading is remembered from an earlier dock.
    case_live: bool,
}

impl Report {
    fn update(self, reported: Batteries) -> Self {
        if reported.case.is_some() {
            return Self {
                batteries: reported,
                case_live: true,
            };
        }
        let case = self.batteries.case.map(|case| Reading {
            charging: None,
            ..case
        });
        Self {
            batteries: Batteries { case, ..reported },
            case_live: false,
        }
    }

    fn batteries(&self) -> Vec<Battery> {
        let Batteries { left, right, case } = self.batteries;
        [
            ("maestro-left", "Left", "left", left, true),
            ("maestro-right", "Right", "right", right, true),
            ("maestro-case", "Case", "case", case, self.case_live),
        ]
        .into_iter()
        .filter_map(|(id, label, component, reading, live)| {
            reading.map(|reading| Battery {
                id: id.to_string(),
                label: label.to_string(),
                component: component.to_string(),
                percentage: reading.level,
                charging: reading.charging,
                source: SOURCE.to_string(),
                confidence: if live { "high" } else { "last-known" }.to_string(),
            })
        })
        .collect()
    }
}

/// Subscribes to runtime info on each connected Pixel Buds and exposes the
/// left, right and case readings.
pub struct MaestroBatteryProvider {
    links: Arc<Channels>,
    reports: RwLock<HashMap<Address, Report>>,
    changes: broadcast::Sender<()>,
}

#[async_trait]
impl vendor_channel::Handler for MaestroBatteryProvider {
    async fn run(&self, address: Address, stream: Stream) -> Result<()> {
        subscribe(address, stream, |batteries| self.store(address, batteries)).await
    }

    async fn ended(&self, address: Address) {
        if self.reports.write().await.remove(&address).is_some() {
            let _ = self.changes.send(());
        }
    }
}

impl MaestroBatteryProvider {
    pub async fn start(session: Session, changes: broadcast::Sender<()>) -> Result<Arc<Self>> {
        Channels::start(session, &SPEC, changes.subscribe(), |links| Self {
            links,
            reports: RwLock::default(),
            changes,
        })
        .await
    }

    pub(crate) fn abort(&self) {
        self.links.abort();
    }

    pub async fn shutdown(&self) {
        self.links.shutdown().await;
        self.reports.write().await.clear();
    }

    pub async fn batteries(&self, address: Address) -> Vec<Battery> {
        self.reports
            .read()
            .await
            .get(&address)
            .map(Report::batteries)
            .unwrap_or_default()
    }

    async fn store(&self, address: Address, reported: Batteries) {
        let report = {
            let mut reports = self.reports.write().await;
            let previous = reports.get(&address).copied();
            let report = previous.unwrap_or_default().update(reported);
            if previous == Some(report) {
                return;
            }
            reports.insert(address, report);
            report
        };
        let level = |reading: Option<Reading>| reading.map(|reading| reading.level);
        tracing::debug!(
            %address,
            left = ?level(report.batteries.left),
            right = ?level(report.batteries.right),
            case = ?level(report.batteries.case),
            case_live = report.case_live,
            "Maestro component battery updated"
        );
        let _ = self.changes.send(());
    }
}

/// Waits for the buds to announce their RPC channel, subscribes to runtime
/// info and hands every battery report to `store` until the channel closes.
async fn subscribe<S, F, Fut>(address: Address, stream: S, mut store: F) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnMut(Batteries) -> Fut,
    Fut: Future<Output = ()>,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut decoder = FrameDecoder::default();
    let mut packets = crate::framing::Reader::new(
        reader,
        move |bytes| decoder.push(bytes),
        |error| tracing::debug!(%address, %error, "ignored invalid Maestro frame"),
    );
    let channel = tokio::time::timeout(ANNOUNCE_TIMEOUT, async {
        loop {
            let Some(packet) = packets.next().await.context("read Maestro channel")? else {
                bail!("Maestro channel closed before announcing an RPC channel");
            };
            if let Some(channel) = protocol::announced_channel(&packet) {
                return Ok(channel);
            }
        }
    })
    .await
    .context("Pixel Buds did not announce a Maestro RPC channel")??;
    writer
        .write_all(&protocol::subscribe_runtime_info(channel))
        .await
        .context("write Maestro runtime-info subscription")?;
    writer.flush().await.context("flush Maestro subscription")?;
    tracing::debug!(%address, channel = channel.id, "subscribed to Maestro runtime info");
    while let Some(packet) = packets.next().await.context("read Maestro channel")? {
        match protocol::subscription_update(&packet, channel) {
            Ok(Update::Batteries(batteries)) => store(batteries).await,
            Ok(Update::Ended(status)) => {
                bail!("Pixel Buds ended the Maestro runtime-info stream with status {status}")
            }
            Ok(Update::Ignored) => {}
            Err(error) => tracing::debug!(%address, %error, "ignored Maestro runtime info"),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        Batteries, Reading, Report,
        protocol::{self, fixtures},
        subscribe,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn reading(level: u8, charging: bool) -> Option<Reading> {
        Some(Reading {
            level,
            charging: Some(charging),
        })
    }

    fn summary(report: &Report) -> Vec<(String, u8, Option<bool>, String)> {
        report
            .batteries()
            .into_iter()
            .map(|b| (b.component, b.percentage, b.charging, b.confidence))
            .collect()
    }

    #[test]
    fn case_reading_survives_undocking_as_last_known() {
        let docked = Report::default().update(Batteries {
            left: reading(100, true),
            right: reading(99, true),
            case: reading(43, false),
        });
        assert_eq!(
            summary(&docked),
            [
                ("left".into(), 100, Some(true), "high".into()),
                ("right".into(), 99, Some(true), "high".into()),
                ("case".into(), 43, Some(false), "high".into()),
            ]
        );

        let worn = docked.update(Batteries {
            left: reading(98, false),
            right: None,
            case: None,
        });
        assert_eq!(
            summary(&worn),
            [
                ("left".into(), 98, Some(false), "high".into()),
                ("case".into(), 43, None, "last-known".into()),
            ]
        );
        assert_eq!(worn.update(Batteries::default()).batteries().len(), 1);

        let redocked = worn.update(Batteries {
            case: reading(41, true),
            ..Batteries::default()
        });
        assert!(redocked.case_live);
        assert_eq!(redocked.batteries.case, reading(41, true));
        assert!(
            Report::default()
                .update(Batteries::default())
                .batteries()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn session_requires_an_announcement_and_drains_reports_before_eof() {
        let (local, remote) = tokio::io::duplex(64);
        drop(remote);
        let error = subscribe(bluer::Address::default(), local, |_| async {})
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("before announcing"));

        let (local, mut remote) = tokio::io::duplex(1024);
        let (stored_tx, mut stored) = tokio::sync::mpsc::unbounded_channel();
        let session = tokio::spawn(subscribe(
            bluer::Address::default(),
            local,
            move |batteries| {
                let _ = stored_tx.send(batteries);
                std::future::ready(())
            },
        ));

        // One read can contain corrupt/unrelated frames, the announcement, and
        // a report. The reader must retain the report across the subscription write.
        let announcement = fixtures::announcement();
        let docked = fixtures::runtime_info(&[(1, fixtures::reading(43, 1))]);
        let mut traffic = vec![0x7e, 0x01, 0x7e];
        for packet in [
            fixtures::runtime_stream(Vec::new()),
            announcement,
            fixtures::runtime_stream(docked),
        ] {
            traffic.extend(fixtures::frame(&packet));
        }
        remote.write_all(&traffic).await.unwrap();
        let mut request = [0_u8; 64];
        let count = remote.read(&mut request).await.unwrap();
        let channel = protocol::announced_channel(&fixtures::announcement()).unwrap();
        assert_eq!(request[..count], protocol::subscribe_runtime_info(channel));

        drop(remote);
        session.await.unwrap().unwrap();
        assert_eq!(stored.recv().await.unwrap().case, reading(43, false));
        assert!(
            stored.recv().await.is_none(),
            "exactly one report before EOF"
        );
    }
}
