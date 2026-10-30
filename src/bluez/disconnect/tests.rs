use super::{DisconnectReason, listen};
use crate::test_bus::Bus;
use futures::StreamExt;
use std::time::Duration;

async fn emit(connection: &zbus::Connection, path: &str, reason: &str, message: &str) {
    connection
        .emit_signal(
            None::<&str>,
            path,
            "org.bluez.Device1",
            "Disconnected",
            &(reason, message),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn private_bus_accepts_only_bluez_device_signals_and_recovers_with_a_new_listener() {
    let bus = Bus::new();
    let bluez = bus
        .builder()
        .name("org.bluez")
        .unwrap()
        .build()
        .await
        .unwrap();
    let rogue = bus.connection().await;
    let client = bus.connection().await;
    let mut events = listen(&client).await.unwrap();
    let path = "/org/bluez/hci1/dev_AA_BB_CC_DD_EE_FF";
    emit(&rogue, path, "org.bluez.Reason.Local", "forged").await;
    emit(
        &bluez,
        "/org/bluez/hci1",
        "org.bluez.Reason.Local",
        "bad path",
    )
    .await;
    bluez
        .emit_signal(
            None::<&str>,
            path,
            "org.bluez.Device1",
            "Disconnected",
            &(42u32,),
        )
        .await
        .unwrap();
    for (message, expected) in [
        (
            "\n\u{202e}link timeout\u{2066}".into(),
            "link timeout".into(),
        ),
        ("é".repeat(300), "é".repeat(256)),
    ] {
        emit(&bluez, path, "org.bluez.Reason.Timeout", &message).await;
        let (adapter, _, report) = tokio::time::timeout(Duration::from_secs(2), events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(adapter, "hci1");
        assert_eq!(report.reason, DisconnectReason::Timeout);
        assert_eq!(report.message, expected);
    }
    drop(events);
    bluez.release_name("org.bluez").await.unwrap();
    let replacement = bus
        .builder()
        .name("org.bluez")
        .unwrap()
        .build()
        .await
        .unwrap();
    let mut events = listen(&client).await.unwrap();
    emit(&bluez, path, "org.bluez.Reason.Local", "old owner").await;
    emit(&replacement, path, "org.bluez.Reason.Suspend", "suspend").await;
    let (_, _, report) = tokio::time::timeout(Duration::from_secs(2), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(report.reason, DisconnectReason::Suspend);
}
