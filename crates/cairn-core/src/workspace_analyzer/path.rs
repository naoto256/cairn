pub(super) fn file_uri_to_manifest_path(uri: &str) -> Option<String> {
    let path = uri.strip_prefix("file://")?;
    let decoded = percent_decode(path)?;
    let marker = "/src/";
    if let Some(idx) = decoded.find(marker) {
        return Some(decoded[idx + 1..].to_string());
    }
    decoded.rsplit_once('/').map(|(_, name)| name.to_string())
}

fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = *bytes.get(i + 1)?;
            let lo = *bytes.get(i + 2)?;
            out.push(hex_value(hi)? * 16 + hex_value(lo)?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
