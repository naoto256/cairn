//! Daemon main loop.
//!
//! Owns the two UDS listeners (`cairn.sock` and `control.sock`) and a
//! pluggable [`LineHandler`] pair — one for the read-only data RPC,
//! one for the control protocol. Concrete handlers live in
//! [`crate::data_rpc`] and [`crate::ctl`]; this module owns the
//! framing, the accept loops, and the shared shutdown signal.
//!
//! `cairn.sock` speaks plain JSON-RPC 2.0, not MCP. MCP framing is
//! the job of `cairn mcp` (the stdio front-end in `cairn-cli`),
//! which translates each MCP `tools/call` into either a data RPC
//! (`get_outline` / `find_symbols` / `list_repos`) or a control message
//! (`register_repo` / `reindex_repo`) and wraps the response back into
//! the MCP shape. Out-of-tree consumers (cairn-graph, cairn-audit,
//! IDE plugins) hit the daemon directly over the JSON-RPC surface
//! without speaking MCP at all.

use std::sync::Arc;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use crate::Result;
use crate::sockets::SocketPaths;

/// Implementations receive one newline-delimited request line at a
/// time and return one response line.
#[async_trait::async_trait]
pub trait LineHandler: Send + Sync {
    /// Process one request line. Returning `None` ends the connection
    /// (the server closes the stream cleanly).
    async fn handle(&self, line: &str) -> Option<String>;
}

/// Hand-off bundle used to start the daemon. The two handlers are
/// usually different concrete types (data RPC and control protocol),
/// but they share a uniform line-in / line-out shape over UDS.
pub struct Daemon {
    pub paths: SocketPaths,
    pub data_handler: Arc<dyn LineHandler>,
    pub control_handler: Arc<dyn LineHandler>,
    pub shutdown: Arc<Notify>,
}

impl Daemon {
    /// Bind both sockets, run accept loops until `shutdown` is
    /// notified, then drop the listeners (and unlink the socket
    /// files as a side effect on Unix).
    ///
    /// # Errors
    /// Bind / accept failures propagate.
    pub async fn run(self) -> Result<()> {
        self.paths.ensure()?;
        let cairn = UnixListener::bind(&self.paths.cairn)?;
        let ctrl = UnixListener::bind(&self.paths.control)?;
        info!(cairn = %self.paths.cairn.display(), control = %self.paths.control.display(), "daemon listening");

        let cairn_task = spawn_accept_loop(
            "cairn",
            cairn,
            self.data_handler.clone(),
            self.shutdown.clone(),
        );
        let ctrl_task = spawn_accept_loop(
            "control",
            ctrl,
            self.control_handler.clone(),
            self.shutdown.clone(),
        );

        // Wait for either accept loop to complete (which happens when
        // shutdown fires) and then drop everything.
        let _ = tokio::join!(cairn_task, ctrl_task);
        if let Err(err) = crate::lsp::pool::shutdown_global_if_initialized().await {
            warn!(error = %err, "lsp pool shutdown failed");
        }
        info!("daemon stopped");

        // Best-effort cleanup of socket files; the OS leaves them
        // behind after the listener is dropped.
        let _ = std::fs::remove_file(&self.paths.cairn);
        let _ = std::fs::remove_file(&self.paths.control);
        Ok(())
    }
}

fn spawn_accept_loop(
    name: &'static str,
    listener: UnixListener,
    handler: Arc<dyn LineHandler>,
    shutdown: Arc<Notify>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = shutdown.notified() => {
                    debug!(socket = name, "accept loop received shutdown");
                    break;
                }
                accepted = listener.accept() => match accepted {
                    Ok((stream, _addr)) => {
                        let h = handler.clone();
                        tokio::spawn(async move {
                            if let Err(e) = serve_one(stream, h).await {
                                warn!(error = %e, "{name} connection ended with error", name = name);
                            }
                        });
                    }
                    Err(e) => {
                        error!(?e, socket = name, "accept failed");
                        // Brief backoff to avoid spinning on a persistent error.
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                }
            }
        }
    })
}

/// Per-line byte cap on the UDS framing. JSON-RPC requests in practice
/// stay well under 1 MiB; the cap is a guard against a misbehaving (or
/// hostile) peer streaming an unbounded line and pinning the daemon's
/// memory. Apply per connection-side; the trust boundary is still
/// "0700 socket dir on the owning UID", but cheap defense in depth.
const MAX_LINE_BYTES: usize = 16 * 1024 * 1024;

/// Like [`AsyncBufReadExt::read_line`] but enforces [`MAX_LINE_BYTES`]
/// and returns `InvalidData` if a single line exceeds the cap. Uses
/// `Vec<u8>` so we don't pay UTF-8 validation on the hot path; the
/// handler does its own JSON parse downstream.
async fn read_line_capped<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    max: usize,
) -> std::io::Result<usize> {
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(buf.len());
        }
        let (done, n) = match available.iter().position(|&b| b == b'\n') {
            Some(i) => (true, i + 1),
            None => (false, available.len()),
        };
        if buf.len() + n > max {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("line exceeds {max} bytes"),
            ));
        }
        buf.extend_from_slice(&available[..n]);
        reader.consume(n);
        if done {
            return Ok(buf.len());
        }
    }
}

async fn serve_one(stream: UnixStream, handler: Arc<dyn LineHandler>) -> std::io::Result<()> {
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let mut buf: Vec<u8> = Vec::new();
    loop {
        buf.clear();
        let n = read_line_capped(&mut reader, &mut buf, MAX_LINE_BYTES).await?;
        if n == 0 {
            return Ok(()); // peer closed
        }
        let line = match std::str::from_utf8(&buf) {
            Ok(s) => s,
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "non-UTF-8 request line",
                ));
            }
        };
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if trimmed.is_empty() {
            continue;
        }
        match handler.handle(trimmed).await {
            Some(mut resp) => {
                if !resp.ends_with('\n') {
                    resp.push('\n');
                }
                write.write_all(resp.as_bytes()).await?;
                write.flush().await?;
            }
            None => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    struct EchoHandler;

    #[async_trait::async_trait]
    impl LineHandler for EchoHandler {
        async fn handle(&self, line: &str) -> Option<String> {
            Some(format!("echo: {line}"))
        }
    }

    #[tokio::test]
    async fn round_trip_one_request() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(tmp.path().to_path_buf());
        let shutdown = Arc::new(Notify::new());

        let daemon_task = tokio::spawn({
            let paths = paths.clone();
            let shutdown = shutdown.clone();
            async move {
                let daemon = Daemon {
                    paths,
                    data_handler: Arc::new(EchoHandler),
                    control_handler: Arc::new(EchoHandler),
                    shutdown,
                };
                daemon.run().await.unwrap();
            }
        });

        // Give the daemon a moment to bind.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut conn = UnixStream::connect(&paths.cairn).await.unwrap();
        conn.write_all(b"hello\n").await.unwrap();
        conn.flush().await.unwrap();

        let mut buf = vec![0u8; 64];
        let n = conn.read(&mut buf).await.unwrap();
        let resp = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(resp.contains("echo: hello"), "got: {resp:?}");

        shutdown.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(1), daemon_task).await;
    }

    #[tokio::test]
    async fn read_line_capped_rejects_oversized_line() {
        // Stream a payload that exceeds the cap with no newline. The
        // helper must return InvalidData rather than buffer unboundedly.
        let cap = 64usize;
        let payload = vec![b'x'; cap * 4];
        let mut reader = BufReader::new(&payload[..]);
        let mut buf = Vec::new();
        let err = read_line_capped(&mut reader, &mut buf, cap)
            .await
            .expect_err("expected line-too-long error");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn read_line_capped_accepts_line_at_limit() {
        // A line whose total length (including newline) is exactly the
        // cap should succeed.
        let cap = 64usize;
        let mut payload = vec![b'a'; cap - 1];
        payload.push(b'\n');
        let mut reader = BufReader::new(&payload[..]);
        let mut buf = Vec::new();
        let n = read_line_capped(&mut reader, &mut buf, cap).await.unwrap();
        assert_eq!(n, cap);
    }
}
