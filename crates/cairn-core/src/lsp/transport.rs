//! LSP base-protocol framing over arbitrary async byte streams.
//!
//! Implements the header/content framing defined by the LSP base
//! protocol: HTTP-style header fields terminated by `\r\n\r\n`,
//! followed by a `Content-Length`-sized JSON-RPC body. This module is
//! transport-only; it parses bodies as JSON but never interprets
//! their contents.
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::error::{Error, Result};

/// Cap on a single LSP message body. rust-analyzer's largest legitimate
/// responses (workspace symbols on huge crates) stay well under this; a
/// `Content-Length` above the cap is treated as a malicious or runaway
/// subprocess and refused before allocation, preventing local DoS.
pub(super) const MAX_BODY_SIZE: usize = 16 * 1024 * 1024;

/// Read one framed LSP message from `reader`.
///
/// Returns `Ok(None)` only on a clean EOF, i.e. the stream ends
/// before the first header byte of the next message — the normal
/// shape when the server process exits between messages. EOF in the
/// middle of a header or body is an `Error::Protocol`, as are a
/// missing or unparseable `Content-Length`, an oversized body, and
/// invalid JSON; header lines without a colon are skipped, not
/// rejected.
///
/// The header is read one byte at a time so no bytes past the
/// current message are ever consumed; this keeps `reader` correctly
/// positioned for the next call without a buffering layer.
pub(super) async fn read_lsp_message<R>(reader: &mut R) -> Result<Option<Value>>
where
    R: AsyncRead + Unpin,
{
    let mut header = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        match reader.read_exact(&mut byte).await {
            Ok(_) => {
                header.push(byte[0]);
                // The base protocol terminates the header part with
                // `\r\n\r\n`; accepting a bare `\n\n` is a leniency
                // of this implementation for servers that emit Unix
                // line endings.
                if header.ends_with(b"\r\n\r\n") || header.ends_with(b"\n\n") {
                    break;
                }
                // Implementation-chosen cap (not from the spec):
                // legitimate LSP headers are tens of bytes, and
                // without a bound a stream that never produces a
                // terminator would grow the buffer indefinitely.
                if header.len() > 8192 {
                    return Err(Error::Protocol("LSP header too large".into()));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof && header.is_empty() => {
                return Ok(None);
            }
            Err(e) => return Err(Error::Protocol(format!("read header: {e}"))),
        }
    }

    let header_text =
        std::str::from_utf8(&header).map_err(|e| Error::Protocol(format!("header utf8: {e}")))?;
    let len = content_length(header_text)?;
    if len > MAX_BODY_SIZE {
        return Err(Error::Protocol(format!(
            "LSP message body exceeds {MAX_BODY_SIZE} bytes ({len})"
        )));
    }
    let mut body = vec![0_u8; len];
    reader
        .read_exact(&mut body)
        .await
        .map_err(|e| Error::Protocol(format!("read body: {e}")))?;
    serde_json::from_slice(&body).map_err(|e| Error::Protocol(format!("body json: {e}")))
}

/// Extract the mandatory `Content-Length` value (body size in bytes)
/// from the header block.
///
/// Field names are matched case-insensitively; other fields (e.g.
/// `Content-Type`) and lines without a `:` are skipped. `Err` means
/// no parseable `Content-Length` was present, which the base
/// protocol requires.
fn content_length(header: &str) -> Result<usize> {
    for line in header.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            return value
                .trim()
                .parse::<usize>()
                .map_err(|e| Error::Protocol(format!("invalid content-length: {e}")));
        }
    }
    Err(Error::Protocol("missing content-length".into()))
}

/// Serialize `message` and write it with base-protocol framing.
///
/// Only the mandatory `Content-Length` field is emitted; the content
/// type is left at the protocol default (utf-8 JSON-RPC). The writer
/// is flushed before returning, so on `Ok` the whole message has
/// been handed to the underlying stream. Serialization and I/O
/// failures are folded into `Error::Protocol`.
pub(super) async fn write_lsp_message<W>(writer: &mut W, message: &Value) -> Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    let body = serde_json::to_vec(message).map_err(|e| Error::Protocol(e.to_string()))?;
    let header = format!("Content-Length: {}\r\n\r\n", body.len());
    writer
        .write_all(header.as_bytes())
        .await
        .map_err(|e| Error::Protocol(format!("write header: {e}")))?;
    writer
        .write_all(&body)
        .await
        .map_err(|e| Error::Protocol(format!("write body: {e}")))?;
    writer
        .flush()
        .await
        .map_err(|e| Error::Protocol(format!("flush: {e}")))?;
    Ok(())
}
