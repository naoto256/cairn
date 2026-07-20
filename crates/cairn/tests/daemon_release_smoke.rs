#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

struct ChildGuard {
    child: Child,
    stderr_reader: Option<JoinHandle<String>>,
}

impl ChildGuard {
    fn new(mut child: Child) -> Self {
        let mut stderr = child.stderr.take().unwrap();
        let stderr_reader = thread::spawn(move || {
            let mut output = String::new();
            stderr.read_to_string(&mut output).unwrap();
            output
        });
        Self {
            child,
            stderr_reader: Some(stderr_reader),
        }
    }

    fn assert_running(&mut self, context: &str) {
        if let Some(status) = self.child.try_wait().unwrap() {
            let stderr = self.join_stderr();
            panic!("{context}: {status}; stderr={stderr}");
        }
    }

    fn join_stderr(&mut self) -> String {
        self.stderr_reader
            .take()
            .map(|reader| reader.join().unwrap())
            .unwrap_or_default()
    }

    fn try_wait(&mut self) -> Option<ExitStatus> {
        self.child.try_wait().unwrap()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        let _ = self.stderr_reader.take().map(JoinHandle::join);
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
    let mut child = ChildGuard::new(
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
        child.assert_running("daemon exited before binding its socket");
        thread::sleep(Duration::from_millis(20));
    }

    thread::sleep(Duration::from_secs(30));
    child.assert_running("idle daemon exited during the 30-second smoke window");
    let status = request(&control, "status");
    assert_eq!(status["result"]["initialization"]["state"], "ready");
    let shutdown = request(&control, "shutdown");
    assert_eq!(shutdown["result"]["ok"], true);

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait() {
            let stderr = child.join_stderr();
            assert!(
                status.success(),
                "daemon exited unsuccessfully: {status}; stderr={stderr}"
            );
            break;
        }
        assert!(Instant::now() < deadline, "daemon did not stop after ACK");
        thread::sleep(Duration::from_millis(20));
    }
}
