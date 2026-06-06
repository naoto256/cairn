use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::error::{Error, Result};

/// Cap on a single LSP message body. rust-analyzer's largest legitimate
/// responses (workspace symbols on huge crates) stay well under this; a
/// `Content-Length` above the cap is treated as a malicious or runaway
/// subprocess and refused before allocation, preventing local DoS.
pub(super) const MAX_BODY_SIZE: usize = 16 * 1024 * 1024;

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
                if header.ends_with(b"\r\n\r\n") || header.ends_with(b"\n\n") {
                    break;
                }
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
