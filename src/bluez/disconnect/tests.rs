use super::*;
use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    time::Duration,
};

struct Bus(Child);
impl Drop for Bus {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn private_bus_accepts_only_bluez_device_signals_and_recovers_with_a_new_listener() {
    // Nix sandboxes have no /etc/dbus-1/session.conf. Use a private policy,
    // independent of the host's session bus and without changing process env.
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("bus.conf");
    std::fs::write(&config, r#"<busconfig><type>session</type><listen>unix:tmpdir=/tmp</listen><auth>EXTERNAL</auth><policy context="default"><allow send_destination="*"/><allow receive_sender="*"/><allow own="*"/></policy></busconfig>"#).unwrap();
    let child = Command::new("dbus-daemon")
        .arg("--config-file")
        .arg(config)
        .args(["--nofork", "--print-address=1"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut bus = Bus(child);
    let mut address = String::new();
    BufReader::new(bus.0.stdout.take().unwrap())
        .read_line(&mut address)
        .unwrap();
    let connect = || zbus::connection::Builder::address(address.trim()).unwrap();
    let bluez = connect().name("org.bluez").unwrap().build().await.unwrap();
    let rogue = connect().build().await.unwrap();
    let client = connect().build().await.unwrap();
    let mut events = listen(&client).await.unwrap();
    let path = "/org/bluez/hci1/dev_AA_BB_CC_DD_EE_FF";
    rogue
        .emit_signal(
            None::<&str>,
            path,
            "org.bluez.Device1",
            "Disconnected",
            &("org.bluez.Reason.Local", "forged"),
        )
        .await
        .unwrap();
    bluez
        .emit_signal(
            None::<&str>,
            "/org/bluez/hci1",
            "org.bluez.Device1",
            "Disconnected",
            &("org.bluez.Reason.Local", "bad path"),
        )
        .await
        .unwrap();
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
    bluez
        .emit_signal(
            None::<&str>,
            path,
            "org.bluez.Device1",
            "Disconnected",
            &("org.bluez.Reason.Timeout", "link timeout"),
        )
        .await
        .unwrap();
    let (adapter, _, report) = tokio::time::timeout(Duration::from_secs(2), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(adapter, "hci1");
    assert_eq!(report.reason, DisconnectReason::Timeout);
    assert_eq!(report.message, "link timeout");
    drop(events);
    bluez.release_name("org.bluez").await.unwrap();
    let replacement = connect().name("org.bluez").unwrap().build().await.unwrap();
    let mut events = listen(&client).await.unwrap();
    bluez
        .emit_signal(
            None::<&str>,
            path,
            "org.bluez.Device1",
            "Disconnected",
            &("org.bluez.Reason.Local", "old owner"),
        )
        .await
        .unwrap();
    replacement
        .emit_signal(
            None::<&str>,
            path,
            "org.bluez.Device1",
            "Disconnected",
            &("org.bluez.Reason.Suspend", "suspend"),
        )
        .await
        .unwrap();
    let (_, _, report) = tokio::time::timeout(Duration::from_secs(2), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(report.reason, DisconnectReason::Suspend);
}
