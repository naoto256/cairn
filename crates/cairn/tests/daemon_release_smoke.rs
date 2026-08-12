#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
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

struct FakeMcpDaemonGuard {
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
}

impl FakeMcpDaemonGuard {
    fn start(
        runtime: &std::path::Path,
        data_entered: mpsc::Sender<()>,
        data_eof: mpsc::Sender<()>,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let control = UnixListener::bind(runtime.join("control.sock")).unwrap();
        let data = UnixListener::bind(runtime.join("cairn.sock")).unwrap();
        control.set_nonblocking(true).unwrap();
        data.set_nonblocking(true).unwrap();

        let control_stop = stop.clone();
        let control_thread = thread::spawn(move || {
            loop {
                match control.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_read_timeout(Some(Duration::from_millis(100)))
                            .unwrap();
                        let mut request = String::new();
                        BufReader::new(stream.try_clone().unwrap())
                            .read_line(&mut request)
                            .unwrap();
                        assert!(request.contains("\"method\":\"status\""));
                        writeln!(
                            stream,
                            "{}",
                            serde_json::json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": {
                                    "daemon_version": env!("CARGO_PKG_VERSION"),
                                    "uptime_secs": 1,
                                    "repos": []
                                }
                            })
                        )
                        .unwrap();
                        stream.flush().unwrap();
                        return;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if control_stop.load(Ordering::Acquire) {
                            return;
                        }
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("fake control accept failed: {error}"),
                }
            }
        });

        let data_stop = stop.clone();
        let data_thread = thread::spawn(move || {
            loop {
                match data.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_read_timeout(Some(Duration::from_millis(100)))
                            .unwrap();
                        let mut request = String::new();
                        BufReader::new(stream.try_clone().unwrap())
                            .read_line(&mut request)
                            .unwrap();
                        assert!(request.contains("\"method\":\"list_repos\""));
                        data_entered.send(()).unwrap();
                        let mut byte = [0_u8; 1];
                        loop {
                            match stream.read(&mut byte) {
                                Ok(0) => {
                                    data_eof.send(()).unwrap();
                                    return;
                                }
                                Ok(_) => panic!("unexpected pipelined daemon bytes"),
                                Err(error)
                                    if matches!(
                                        error.kind(),
                                        std::io::ErrorKind::WouldBlock
                                            | std::io::ErrorKind::TimedOut
                                    ) =>
                                {
                                    if data_stop.load(Ordering::Acquire) {
                                        return;
                                    }
                                }
                                Err(error) => panic!("fake data read failed: {error}"),
                            }
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if data_stop.load(Ordering::Acquire) {
                            return;
                        }
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("fake data accept failed: {error}"),
                }
            }
        });

        Self {
            stop,
            threads: vec![control_thread, data_thread],
        }
    }
}

impl Drop for FakeMcpDaemonGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

#[test]
fn mcp_queue_overflow_exits_with_stdin_pipe_still_open() {
    let runtime = tempfile::tempdir().unwrap();
    std::fs::set_permissions(runtime.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (eof_tx, eof_rx) = mpsc::channel();
    let _daemon = FakeMcpDaemonGuard::start(runtime.path(), entered_tx, eof_tx);

    let mut child = Command::new(env!("CARGO_BIN_EXE_cairn"))
        .arg("mcp")
        .arg("--runtime-dir")
        .arg(runtime.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let (stdout_tx, stdout_rx) = mpsc::channel();
    let stdout_reader = thread::spawn(move || {
        let mut output = String::new();
        stdout.read_to_string(&mut output).unwrap();
        stdout_tx.send(output).unwrap();
    });
    let mut child = ChildGuard::new(child);

    let call = |id| {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {"name": "list_repos", "arguments": {}}
        })
        .to_string()
    };
    writeln!(stdin, "{}", call(1)).unwrap();
    stdin.flush().unwrap();
    entered_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("active daemon request did not enter");
    for id in 2..=66 {
        writeln!(stdin, "{}", call(id)).unwrap();
    }
    stdin.flush().unwrap();

    eof_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("queue overflow did not close the active daemon socket");
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.try_wait() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "cairn mcp did not exit while stdin remained open"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert!(
        status.success(),
        "cairn mcp failed: {}",
        child.join_stderr()
    );
    let output = stdout_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("stdout reader did not reach EOF");
    stdout_reader.join().unwrap();
    let responses = output.lines().collect::<Vec<_>>();
    assert_eq!(responses.len(), 1, "unexpected stdout: {output}");
    let response: serde_json::Value = serde_json::from_str(responses[0]).unwrap();
    assert_eq!(response["id"], 66);
    assert_eq!(response["error"]["code"], -32603);
    drop(stdin);
}

fn assert_mcp_disconnect_closes_active_daemon_socket(via_cancel_notification: bool) {
    let runtime = tempfile::tempdir().unwrap();
    std::fs::set_permissions(runtime.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (eof_tx, eof_rx) = mpsc::channel();
    let _daemon = FakeMcpDaemonGuard::start(runtime.path(), entered_tx, eof_tx);

    let mut child = Command::new(env!("CARGO_BIN_EXE_cairn"))
        .arg("mcp")
        .arg("--runtime-dir")
        .arg(runtime.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = Some(child.stdin.take().unwrap());
    let mut stdout = child.stdout.take().unwrap();
    let (stdout_tx, stdout_rx) = mpsc::channel();
    let stdout_reader = thread::spawn(move || {
        let mut output = String::new();
        stdout.read_to_string(&mut output).unwrap();
        stdout_tx.send(output).unwrap();
    });
    let mut child = ChildGuard::new(child);

    writeln!(
        stdin.as_mut().unwrap(),
        "{}",
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "list_repos", "arguments": {}}
        })
    )
    .unwrap();
    stdin.as_mut().unwrap().flush().unwrap();
    entered_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("active daemon request did not enter");

    if via_cancel_notification {
        writeln!(
            stdin.as_mut().unwrap(),
            "{}",
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "notifications/cancelled",
                "params": {"requestId": 1, "reason": "test cancellation"}
            })
        )
        .unwrap();
        stdin.as_mut().unwrap().flush().unwrap();
    } else {
        drop(stdin.take());
    }

    eof_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("MCP disconnect did not close the active daemon socket");
    if via_cancel_notification {
        child.assert_running("matching cancel unexpectedly terminated the MCP session");
        drop(stdin.take());
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.try_wait() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "cairn mcp did not exit after EOF"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert!(
        status.success(),
        "cairn mcp failed: {}",
        child.join_stderr()
    );
    let output = stdout_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("stdout reader did not reach EOF");
    stdout_reader.join().unwrap();
    assert!(
        output.is_empty(),
        "cancelled request wrote stdout: {output}"
    );
}

#[test]
fn mcp_matching_cancel_closes_active_daemon_socket_without_a_response() {
    assert_mcp_disconnect_closes_active_daemon_socket(true);
}

#[test]
fn mcp_stdin_eof_closes_active_daemon_socket_without_a_response() {
    assert_mcp_disconnect_closes_active_daemon_socket(false);
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
