//! Explicit, isolated acceptance experiment. Never starts a real Bluetooth backend.
use super::*;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io::Read, path::Path, process::Stdio, time::Instant};
use tokio::{io::AsyncReadExt, net::UnixListener, process::Command};

// The leader remains owned/unreaped while armed. Never signal a process group
// after wait has reaped its leader: a subsequently reused PID is not ours.
struct ProbeGroup(Option<u32>);
impl ProbeGroup {
    fn terminate(&mut self) {
        if let Some(pid) = self.0.take() {
            // SAFETY: Command::process_group(0) created this child's private
            // group; it contains only this fixture and its descendants.
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
        }
    }
}
impl Drop for ProbeGroup {
    fn drop(&mut self) {
        self.terminate();
    }
}

fn hash(path: &Path) -> String {
    let mut file = std::fs::File::open(path).unwrap();
    let mut hash = Sha256::new();
    let mut bytes = [0u8; 65536];
    loop {
        let n = file.read(&mut bytes).unwrap();
        if n == 0 {
            break;
        }
        hash.update(&bytes[..n]);
    }
    format!("{hash:x}", hash = hash.finalize())
}
fn memory(pid: u32) -> BTreeMap<String, u64> {
    std::fs::read_to_string(format!("/proc/{pid}/smaps_rollup"))
        .unwrap()
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let key = fields.next()?.trim_end_matches(':');
            matches!(key, "Rss" | "Pss" | "Private_Clean" | "Private_Dirty").then(|| {
                (
                    key.to_owned(),
                    fields.next().unwrap().parse::<u64>().unwrap() * 1024,
                )
            })
        })
        .collect()
}
fn process_group(pid: u32) -> Value {
    let mut pending = vec![pid];
    let mut processes = Vec::new();
    let mut total = BTreeMap::<String, u64>::new();
    while let Some(pid) = pending.pop() {
        if let Ok(children) = std::fs::read_to_string(format!("/proc/{pid}/task/{pid}/children")) {
            pending.extend(
                children
                    .split_whitespace()
                    .map(|pid| pid.parse::<u32>().unwrap()),
            );
        }
        let values = memory(pid);
        for (key, value) in &values {
            *total.entry(key.clone()).or_default() += value;
        }
        processes.push(json!({"pid":pid,"comm":std::fs::read_to_string(format!("/proc/{pid}/comm")).unwrap().trim(),"bytes":values}));
    }
    json!({"processes":processes,"total_bytes":total})
}
fn percentiles(mut samples: Vec<u128>) -> Value {
    samples.sort_unstable();
    json!({"n":samples.len(),"p50_ns":samples[samples.len()/2],"p99_ns":samples[(samples.len()*99/100).min(samples.len()-1)]})
}
async fn bounded_output(reader: impl tokio::io::AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.take(1024 * 1024 + 1).read_to_end(&mut bytes).await?;
    if bytes.len() > 1024 * 1024 {
        return Err(std::io::Error::other("probe log limit exceeded"));
    }
    Ok(bytes)
}
fn quickshell_binary() -> std::path::PathBuf {
    let name = std::env::var_os("QUICKSHELL_BIN").unwrap_or_else(|| "quickshell".into());
    let path = std::path::PathBuf::from(&name);
    if path.components().count() > 1 {
        return path.canonicalize().unwrap();
    }
    std::env::split_paths(&std::env::var_os("PATH").unwrap())
        .map(|path| path.join(&name))
        .find(|path| path.is_file())
        .expect("Quickshell executable on PATH")
        .canonicalize()
        .unwrap()
}
async fn quickshell(root: &Path, runtime: &Path, bus: &str, mode: &str, path: &Path) -> Value {
    let executable = quickshell_binary();
    let binary = std::path::PathBuf::from(std::env::var_os("BT_DAEMON_BIN").unwrap());
    let paths = std::iter::once(binary.parent().unwrap().to_owned())
        .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap()))
        .collect::<Vec<_>>();
    let mut child = Command::new(&executable)
        .process_group(0)
        .env("PATH", std::env::join_paths(paths).unwrap())
        .env("RUST_LOG", "off")
        .args(["--no-color", "--path"])
        .arg(root.join("tests/e2e/unix-jsonl.qml"))
        .env("QML_IMPORT_PATH", root.join("qml"))
        .env("QML2_IMPORT_PATH", root.join("qml"))
        .env("QT_QPA_PLATFORM", "offscreen")
        .env("QT_QUICK_BACKEND", "software")
        .env("QSG_RHI_BACKEND", "software")
        .env_remove("WAYLAND_DISPLAY")
        .env_remove("DISPLAY")
        .env("QT_QPA_PLATFORMTHEME", "")
        .env("XDG_RUNTIME_DIR", runtime)
        .env("XDG_CACHE_HOME", runtime.join("cache"))
        .env("XDG_CONFIG_HOME", runtime.join("config"))
        .env("DBUS_SESSION_BUS_ADDRESS", bus)
        .env("SOCKET_PROBE_CONTROL", runtime.join("control.sock"))
        .env("SOCKET_PROBE_MODE", mode)
        .env("SOCKET_PROBE_PATH", path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut group = ProbeGroup(child.id());
    let stdout = tokio::spawn(bounded_output(child.stdout.take().unwrap()));
    let stderr = tokio::spawn(bounded_output(child.stderr.take().unwrap()));
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait()).await;
    if status.is_err() {
        group.terminate();
        let _ = child.kill().await;
    } else {
        group.0 = None;
    }
    let stdout = String::from_utf8_lossy(&stdout.await.unwrap().unwrap()).into_owned();
    let stderr = String::from_utf8_lossy(&stderr.await.unwrap().unwrap()).into_owned();
    let pass = status.is_ok_and(|s| s.is_ok_and(|s| s.success()))
        && format!("{stdout}{stderr}").contains(&format!("SOCKET_PROBE_PASS {mode}"));
    json!({"passed":pass,"stdout":stdout,"stderr":stderr})
}
async fn native_qt_cases(root: &Path, rig: &mut Rig) -> Value {
    let runtime = rig.directory.path().to_owned();
    let bus = rig.bus.address.clone();
    let mut results = serde_json::Map::new();
    for mode in ["fragmented", "large", "blocked"] {
        let path = rig.directory.path().join(format!("probe-{mode}.sock"));
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            if mode == "blocked" {
                // Do not read the first connection at all. Qt's write buffer fills.
                let (replacement, _) = listener.accept().await.unwrap();
                let mut replacement = BufReader::new(replacement);
                let mut line = String::new();
                replacement.read_line(&mut line).await.unwrap();
                let request: Value = serde_json::from_str(&line).unwrap();
                assert_eq!(
                    request["id"], "reconnected",
                    "old buffered effects must never reach a new connection"
                );
                replacement.get_mut().write_all(format!("{}\n",json!({"kind":"response","id":"reconnected","ok":true,"response":{"ok":true,"data":{}}})).as_bytes()).await.unwrap();
                // Retain the first unread socket until after the replacement reply.
                tokio::time::sleep(Duration::from_millis(100)).await;
                drop(socket);
            } else {
                let mut reader = BufReader::new(socket);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                socket = reader.into_inner();
                let text = if mode == "large" {
                    "🦀".repeat(2 * 1024 * 1024)
                } else {
                    "你好 🦀 café".to_owned()
                };
                let response = format!(
                    "{}\n",
                    json!({"kind":"response","id":"probe","ok":true,"response":{"ok":true,"data":{"text":text}}})
                );
                if mode == "fragmented" {
                    for byte in response.as_bytes() {
                        socket.write_all(&[*byte]).await.unwrap();
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                } else {
                    socket.write_all(response.as_bytes()).await.unwrap();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
        let result = quickshell(root, &runtime, &bus, mode, &path).await;
        if tokio::time::timeout(Duration::from_secs(1), async {
            while !server.is_finished() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_err()
        {
            server.abort();
        }
        let server_ok = server.await.is_ok();
        let mut result = result;
        result["server_completed"] = json!(server_ok);
        results.insert(mode.into(), result);
    }
    results.insert(
        "bluetooth".into(),
        quickshell(root, &runtime, &bus, "bluetooth", &rig.path()).await,
    );
    let control = UnixListener::bind(runtime.join("control.sock")).unwrap();
    let path = rig.path();
    let frontend = quickshell(root, &runtime, &bus, "restart", &path);
    let restart = async {
        let (ready, _) = control.accept().await.unwrap();
        let mut line = String::new();
        BufReader::new(ready).read_line(&mut line).await.unwrap();
        assert_eq!(serde_json::from_str::<Value>(&line).unwrap()["ready"], true);
        rig.stop().await;
        rig.connection
            .release_name(api_daemon::BUS_NAME)
            .await
            .unwrap();
        rig.connection.clone().close().await.unwrap();
        let (fresh, _, _) = daemon(false);
        fresh
            .snapshots
            .send_replace(crate::daemon::subscription::SharedSnapshot::Available(
                Arc::new(crate::model::Snapshot {
                    adapters: vec![super::super::test_adapter("restarted-generation")],
                    ..Default::default()
                }),
            ));
        rig.connection = rig
            .bus
            .builder()
            .name(api_daemon::BUS_NAME)
            .unwrap()
            .serve_at(api_daemon::OBJECT_PATH, fresh)
            .unwrap()
            .build()
            .await
            .unwrap();
        rig.start().await;
    };
    let (result, restarted) = tokio::join!(
        frontend,
        tokio::time::timeout(Duration::from_secs(15), restart)
    );
    let mut result = result;
    result["service_recreated"] = json!(restarted.is_ok());
    results.insert("restart".into(), result);
    for mode in ["memory-direct", "memory-bridge"] {
        let frontend = quickshell(root, &runtime, &bus, mode, &path);
        let measure = async {
            let (ready, _) = control.accept().await.unwrap();
            let mut ready = BufReader::new(ready);
            let mut line = String::new();
            ready.read_line(&mut line).await.unwrap();
            let marker: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(marker["mode"], mode);
            let group = process_group(marker["pid"].as_u64().unwrap() as u32);
            ready.get_mut().write_all(b"release\n").await.unwrap();
            group
        };
        let (mut result, measured) = tokio::join!(
            frontend,
            tokio::time::timeout(Duration::from_secs(15), measure)
        );
        result["process_group"] = measured.unwrap();
        results.insert(mode.into(), result);
    }
    results.into()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit isolated benchmark: requires matching bt-daemon binary, Quickshell and Shelllist source"]
async fn unix_socket_acceptance_probe() {
    let root =
        std::path::PathBuf::from(std::env::var_os("SHELLLIST_SOURCE").expect("SHELLLIST_SOURCE"));
    let binary =
        std::path::PathBuf::from(std::env::var_os("BT_DAEMON_BIN").expect("BT_DAEMON_BIN"));
    let report_path = std::env::var_os("SOCKET_PROBE_REPORT").expect("SOCKET_PROBE_REPORT");
    let mut rig = Rig::new().await;
    let idle_memory = memory(std::process::id());
    let mut socket = rig.client().await;
    let mut bridge = Command::new(&binary)
        .arg("client")
        .env("DBUS_SESSION_BUS_ADDRESS", &rig.bus.address)
        .env("TOKIO_WORKER_THREADS", "1")
        .env("RUST_LOG", "off")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = bridge.stdin.take().unwrap();
    let mut output = BufReader::new(bridge.stdout.take().unwrap());
    let mut direct = Vec::new();
    let mut bridged = Vec::new();
    // Alternate paths to reduce systematic warmup/order bias; no pipelining.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    for index in 0..2100 {
        let message = format!(
            "{}\n",
            json!({"op":"call","id":"latency","method":"bluetooth.snapshot","params":{}})
        );
        for mode in if index % 2 == 0 {
            [true, false]
        } else {
            [false, true]
        } {
            let start = Instant::now();
            let mut line = String::new();
            tokio::time::timeout_at(deadline, async {
                if mode {
                    socket
                        .get_mut()
                        .write_all(message.as_bytes())
                        .await
                        .unwrap();
                    socket.read_line(&mut line).await.unwrap();
                } else {
                    input.write_all(message.as_bytes()).await.unwrap();
                    output.read_line(&mut line).await.unwrap();
                }
            })
            .await
            .expect("bounded latency experiment");
            let value: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(value["id"], "latency");
            assert_eq!(value["response"]["ok"], true);
            let elapsed = start.elapsed().as_nanos();
            if index >= 100 {
                if mode {
                    direct.push(elapsed);
                } else {
                    bridged.push(elapsed);
                }
            }
        }
    }
    let bridge_memory = memory(bridge.id().unwrap());
    let harness_memory = memory(std::process::id());
    drop(input);
    tokio::time::timeout(Duration::from_secs(10), bridge.wait())
        .await
        .unwrap()
        .unwrap();
    drop(socket);
    let qt = native_qt_cases(&root, &mut rig).await;
    let executable = quickshell_binary();
    let version = Command::new(&executable)
        .arg("--version")
        .output()
        .await
        .unwrap();
    assert!(version.status.success());
    let report = json!({"quickshell_version":String::from_utf8_lossy(&version.stdout),
        "quickshell_executable":executable,"bridge_executable":binary,"driver_debug_assertions":cfg!(debug_assertions),
        "scope":"isolated fake Bluetooth backend; real shared dispatcher, socket, D-Bus and bridge; sequential cached empty-snapshot requests; latency uses same Rust driver, not UI-frame latency",
        "restart_scope":"service object, listener and D-Bus connection recreated inside the same test process; not an OS-process kill or live service restart",
        "memory_scope":"sequential Quickshell probe process groups after 50 snapshot replies with one fake adapter; excludes daemon harness; summed RSS is not unique physical memory",
        "warmup_per_path":100,"socket":percentiles(direct),"bridge":percentiles(bridged),
        "memory_bytes":{"bridge_process":bridge_memory,"harness_before_clients":idle_memory,"harness_after_both_paths":harness_memory},
        "quickshell":qt,
        "sha256":{"quickshell_launcher":hash(&executable),"bridge_binary":hash(&binary),"test_executable":hash(&std::env::current_exe().unwrap()),"socket_adapter":hash(&root.join("qml/Shelllist/Io/process/JsonlSocketTransport.qml")),"shared_client":hash(&root.join("qml/Shelllist/Io/process/JsonlDaemonClient.qml")),"qml_probe":hash(&root.join("tests/e2e/unix-jsonl.qml"))}});
    std::fs::write(report_path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    rig.stop().await;
    for case in [
        "fragmented",
        "large",
        "blocked",
        "bluetooth",
        "restart",
        "memory-direct",
        "memory-bridge",
    ] {
        for witness in ["server_completed", "service_recreated"] {
            assert!(
                report["quickshell"][case][witness]
                    .as_bool()
                    .unwrap_or(true),
                "{case}: {witness}"
            );
        }
        assert_eq!(
            report["quickshell"][case]["passed"], true,
            "native Qt acceptance failed: {case}; see report"
        );
    }
}
