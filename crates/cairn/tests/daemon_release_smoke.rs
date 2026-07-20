#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn request(socket: &std::path::Path, method: &str) -> serde_json::Value {
    let mut stream = UnixStream::connect(socket).unwrap();
    writeln!(
        stream,
        "{}",
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": null
        })
    )
    .unwrap();
    stream.flush().unwrap();
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).unwrap();
    serde_json::from_str(line.trim()).unwrap()
}

#[test]
#[ignore = "release dogfood smoke; holds a real daemon idle for 30 seconds"]
fn idle_daemon_survives_thirty_seconds_and_shutdown_remains_responsive() {
    let runtime = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();
    std::fs::set_permissions(runtime.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_cairn"))
            .arg("daemon")
            .arg("--runtime-dir")
            .arg(runtime.path())
            .arg("--data-dir")
            .arg(data.path())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let control = runtime.path().join("control.sock");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !control.exists() {
        assert!(
            Instant::now() < deadline,
            "daemon control socket did not appear"
        );
        if let Some(status) = child.0.try_wait().unwrap() {
            let mut stderr = String::new();
            child
                .0
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            panic!("daemon exited before binding its socket: {status}; stderr={stderr}");
        }
        thread::sleep(Duration::from_millis(20));
    }

    thread::sleep(Duration::from_secs(30));
    assert!(
        child.0.try_wait().unwrap().is_none(),
        "idle daemon exited during the 30-second smoke window"
    );
    let status = request(&control, "status");
    assert_eq!(status["result"]["initialization"]["state"], "ready");
    let shutdown = request(&control, "shutdown");
    assert_eq!(shutdown["result"]["ok"], true);

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success(), "daemon exited unsuccessfully: {status}");
            break;
        }
        assert!(Instant::now() < deadline, "daemon did not stop after ACK");
        thread::sleep(Duration::from_millis(20));
    }
}
