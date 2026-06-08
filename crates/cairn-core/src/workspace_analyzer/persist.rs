use cairn_proto::RefKind;
use rusqlite::{Connection, OptionalExtension, params};

use crate::Result;
use crate::cas::kind_conv::ref_kind_to_str;
use crate::lsp::Location;
use crate::manifest::ManifestId;

use super::WorkspaceFacts;
use super::path::file_uri_to_manifest_path;

pub(super) fn persist_resolved_refs(
    conn: &mut Connection,
    manifest_id: ManifestId,
    analyzer_id: &str,
    parser_id: &str,
    facts: &WorkspaceFacts,
) -> Result<usize> {
    let tx = conn.transaction()?;
    tx.execute(
        "DELETE FROM refs
          WHERE source = ?1
            AND blob_sha IN (
                SELECT blob_sha FROM manifest_entries WHERE manifest_id = ?2
            )",
        params![ref_source(analyzer_id), manifest_id.0],
    )?;

    let mut inserted = 0;
    for r in &facts.resolved_refs {
        let Some(source_blob) = blob_for_path(&tx, manifest_id, &r.source_path)? else {
            continue;
        };
        let Some(parser_id) = parser_for_blob(&tx, &source_blob, parser_id)? else {
            continue;
        };
        let Some((target_qualified, target_name)) = target_symbol_for_location(
            &tx,
            manifest_id,
            &parser_id,
            r.target_path.as_deref(),
            &r.target,
        )?
        else {
            continue;
        };
        let enclosing_id = enclosing_symbol_for_ref(
            &tx,
            &source_blob,
            &parser_id,
            r.source_byte_range.start,
            r.source_byte_range.end,
        )?;
        let byte_start = i64::try_from(r.source_byte_range.start).unwrap_or(i64::MAX);
        let byte_end = i64::try_from(r.source_byte_range.end).unwrap_or(i64::MAX);
        let line = i64::from(r.source_position.line.saturating_add(1));
        tx.execute(
            "INSERT INTO refs
               (blob_sha, parser_id, enclosing_id, target_name, target_qualified,
                kind, type_role, byte_start, byte_end, line, source)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, ?8, ?9, ?10)",
            params![
                source_blob,
                parser_id,
                enclosing_id,
                target_name,
                target_qualified,
                ref_kind_to_str(RefKind::Call),
                byte_start,
                byte_end,
                line,
                ref_source(analyzer_id),
            ],
        )?;
        inserted += 1;
    }

    tx.commit()?;
    Ok(inserted)
}

fn ref_source(analyzer_id: &str) -> String {
    if analyzer_id == "rust-analyzer-lsp" {
        return "tier3-rust-analyzer".to_string();
    }
    format!("tier3-{analyzer_id}")
}

fn blob_for_path(conn: &Connection, manifest_id: ManifestId, path: &str) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT blob_sha FROM manifest_entries
             WHERE manifest_id = ?1 AND path = ?2",
            params![manifest_id.0, path],
            |r| r.get(0),
        )
        .optional()?)
}

fn parser_for_blob(conn: &Connection, blob_sha: &str, parser_id: &str) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT parser_id FROM blobs
             WHERE blob_sha = ?1 AND parser_id = ?2
             LIMIT 1",
            params![blob_sha, parser_id],
            |r| r.get(0),
        )
        .optional()?)
}

fn target_symbol_for_location(
    conn: &Connection,
    manifest_id: ManifestId,
    parser_id: &str,
    target_path: Option<&str>,
    location: &Location,
) -> Result<Option<(String, String)>> {
    let Some(path) = target_path
        .map(str::to_string)
        .or_else(|| file_uri_to_manifest_path(location.uri.as_str()))
    else {
        return Ok(None);
    };
    let Some(blob_sha) = blob_for_path(conn, manifest_id, &path)? else {
        return Ok(None);
    };
    let line = i64::from(location.range.start.line.saturating_add(1));
    Ok(conn
        .query_row(
            "SELECT qualified, name FROM symbols
             WHERE blob_sha = ?1
               AND parser_id = ?2
               AND line_start <= ?3 AND line_end >= ?3
               AND kind IN ('function', 'method', 'test')
             ORDER BY (line_end - line_start) ASC, line_start DESC
             LIMIT 1",
            params![blob_sha, parser_id, line],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?)
}

fn enclosing_symbol_for_ref(
    conn: &Connection,
    blob_sha: &str,
    parser_id: &str,
    byte_start: usize,
    byte_end: usize,
) -> Result<Option<i64>> {
    let start = i64::try_from(byte_start).unwrap_or(i64::MAX);
    let end = i64::try_from(byte_end).unwrap_or(i64::MAX);
    Ok(conn
        .query_row(
            "SELECT id FROM symbols
             WHERE blob_sha = ?1
               AND parser_id = ?2
               AND byte_start <= ?3 AND byte_end >= ?4
               AND kind IN ('function', 'method', 'test')
             ORDER BY (byte_end - byte_start) ASC
             LIMIT 1",
            params![blob_sha, parser_id, start, end],
            |r| r.get(0),
        )
        .optional()?)
}
