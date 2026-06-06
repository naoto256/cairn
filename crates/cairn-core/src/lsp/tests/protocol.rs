use super::*;

#[test]
fn file_url_percent_encodes_spaces() {
    let url = Url::from_file_path(Path::new("/tmp/cairn fake/src/lib.rs")).unwrap();
    assert_eq!(url.as_str(), "file:///tmp/cairn%20fake/src/lib.rs");
}

#[test]
fn parses_location_link_definition_result() {
    let value = json!([
        {
            "targetUri": "file:///tmp/lib.rs",
            "targetRange": {
                "start": { "line": 1, "character": 0 },
                "end": { "line": 1, "character": 8 }
            },
            "targetSelectionRange": {
                "start": { "line": 1, "character": 4 },
                "end": { "line": 1, "character": 8 }
            }
        }
    ]);

    let locations = parse_definition_result(value).unwrap();
    assert_eq!(locations[0].range.start.character, 4);
}

#[tokio::test]
async fn oversized_content_length_is_rejected_before_allocation() {
    // A `Content-Length` above MAX_BODY_SIZE must be refused before
    // any allocation — a buggy or malicious subprocess must not be
    // able to force arbitrary-sized buffer allocation. Build a
    // header by hand against a duplex pipe and confirm
    // `read_lsp_message` errors out without ever reading the body.
    let (mut a, mut b) = tokio::io::duplex(256);
    let oversized = MAX_BODY_SIZE + 1;
    let header = format!("Content-Length: {oversized}\r\n\r\n");
    a.write_all(header.as_bytes()).await.unwrap();
    // Intentionally do NOT supply the body — a buggy implementation
    // would block here trying to read `oversized` bytes; the fixed
    // version must return Error::Protocol from the header check.
    let err = read_lsp_message(&mut b).await.unwrap_err();
    assert!(
        matches!(err, Error::Protocol(ref msg) if msg.contains("body exceeds")),
        "unexpected: {err:?}"
    );
}
