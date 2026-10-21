//! Isolated D-Bus fixtures shared by unit and process-level integration tests.
use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
};

pub struct Process(pub Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

pub struct Bus {
    _process: Process,
    pub address: String,
    _directory: tempfile::TempDir,
}

impl Bus {
    pub fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let config = directory.path().join("bus.conf");
        // No host service directories/auto-activation, or session.conf dependency
        // (unavailable in Nix sandboxes). Never modify the parent process env.
        std::fs::write(&config, r#"<busconfig><type>session</type><listen>unix:tmpdir=/tmp</listen><auth>EXTERNAL</auth><policy context="default"><allow send_destination="*"/><allow receive_sender="*"/><allow own="*"/></policy></busconfig>"#).unwrap();
        let mut process = Process(
            Command::new("dbus-daemon")
                .arg("--config-file")
                .arg(config)
                .args(["--nofork", "--print-address=1"])
                .stdout(Stdio::piped())
                .spawn()
                .expect("dbus-daemon is required for this test"),
        );
        let mut address = String::new();
        BufReader::new(process.0.stdout.take().unwrap())
            .read_line(&mut address)
            .unwrap();
        Self {
            _process: process,
            address: address.trim().into(),
            _directory: directory,
        }
    }

    pub fn builder(&self) -> zbus::connection::Builder<'_> {
        zbus::connection::Builder::address(self.address.as_str()).unwrap()
    }

    pub async fn connection(&self) -> zbus::Connection {
        self.builder().build().await.unwrap()
    }
}
