use cairn_proto::common::SymbolKind;
use rusqlite::{Connection, ToSql};

use crate::Result;
use crate::anchor::{self, AnchorName};
use crate::cas::kind_conv::symbol_kind_from_str;

/// Row data needed to render a `get_symbol_source` response. The
/// caller pulls the actual bytes from disk or git based on
/// `blob_sha` + `byte_start..byte_end`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolSourceRow {
    /// Canonical metadata row selected after physical-declaration dedup.
    pub symbol_id: i64,
    pub qualified: String,
    pub name: String,
    pub kind: SymbolKind,
    pub signature: Option<String>,
    pub doc: Option<String>,
    pub path: String,
    pub blob_sha: String,
    pub byte_start: usize,
    pub byte_end: usize,
    pub line_start: u32,
    pub line_end: u32,
    pub parser_id: String,
}

/// Look up physical declarations by qualified name in the manifest at
/// `anchor` and return the metadata needed to materialise their source spans.
/// `file_filter` constrains the search to one path; `line_filter` matches the
/// declaration's 1-indexed `line_start`.
///
/// Multiple parser rows can describe the same physical declaration. Those
/// rows collapse by `(path, blob_sha, byte_start, byte_end)`, with metadata
/// chosen deterministically by `parser_id COLLATE BINARY ASC, symbols.id ASC`.
/// The repository and manifest portions of the physical identity are fixed by
/// the caller and resolved anchor respectively.
///
/// # Errors
/// `Error::AnchorNotFound` when the anchor doesn't resolve; SQLite
/// errors otherwise.
pub fn get_symbol_source_rows(
    conn: &Connection,
    anchor: &AnchorName,
    qualified: &str,
    file_filter: Option<&str>,
    line_filter: Option<u32>,
    limit: usize,
) -> Result<Vec<SymbolSourceRow>> {
    let manifest_id =
        anchor::resolve(conn, anchor)?.ok_or_else(|| crate::Error::AnchorNotFound {
            name: anchor.as_str().to_string(),
        })?;

    let mut sql = String::from(
        "WITH ranked AS (
             SELECT s.id AS symbol_id, s.name, s.kind, s.signature, s.doc,
                    s.byte_start, s.byte_end, s.line_start, s.line_end,
                    me.path, s.blob_sha, s.parser_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY me.path, s.blob_sha,
                                     s.byte_start, s.byte_end
                        ORDER BY s.parser_id COLLATE BINARY ASC, s.id ASC
                    ) AS physical_rank
               FROM symbols s
               JOIN manifest_entries me
                 ON me.manifest_id = ?1
                AND me.blob_sha = s.blob_sha
              WHERE s.qualified = ?2",
    );
    let mut bound: Vec<Box<dyn ToSql>> =
        vec![Box::new(manifest_id.0), Box::new(qualified.to_string())];
    if let Some(f) = file_filter {
        sql.push_str(" AND me.path = ?");
        bound.push(Box::new(f.to_string()));
    }
    if let Some(line) = line_filter {
        sql.push_str(" AND s.line_start = ?");
        bound.push(Box::new(i64::from(line)));
    }
    sql.push_str(
        ")
         SELECT symbol_id, name, kind, signature, doc,
                byte_start, byte_end, line_start, line_end,
                path, blob_sha, parser_id
           FROM ranked
          WHERE physical_rank = 1
          ORDER BY path COLLATE BINARY ASC, line_start ASC,
                   byte_start ASC, byte_end ASC, blob_sha COLLATE BINARY ASC",
    );
    sql.push_str(&format!(" LIMIT {}", limit.max(1)));

    let mut stmt = conn.prepare(&sql)?;
    let param_refs: Vec<&dyn ToSql> = bound.iter().map(|b| b.as_ref()).collect();
    let rows = stmt
        .query_map(param_refs.as_slice(), |r| {
            Ok(SymbolSourceRow {
                symbol_id: r.get(0)?,
                qualified: qualified.to_string(),
                name: r.get(1)?,
                kind: symbol_kind_from_str(&r.get::<_, String>(2)?),
                signature: r.get(3)?,
                doc: r.get(4)?,
                byte_start: usize::try_from(r.get::<_, i64>(5)?).unwrap_or(0),
                byte_end: usize::try_from(r.get::<_, i64>(6)?).unwrap_or(0),
                line_start: u32::try_from(r.get::<_, i64>(7)?).unwrap_or(0),
                line_end: u32::try_from(r.get::<_, i64>(8)?).unwrap_or(0),
                path: r.get(9)?,
                blob_sha: r.get(10)?,
                parser_id: r.get(11)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Backward-compatible single-row lookup for callers that have not yet
/// adopted physical-declaration cardinality handling.
pub fn get_symbol_source_row(
    conn: &Connection,
    anchor: &AnchorName,
    qualified: &str,
    file_filter: Option<&str>,
) -> Result<Option<SymbolSourceRow>> {
    Ok(
        get_symbol_source_rows(conn, anchor, qualified, file_filter, None, 1)?
            .into_iter()
            .next(),
    )
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::*;
    use crate::cas::store;

    fn physical_declaration_fixture() -> (tempfile::TempDir, Connection) {
        let tmp = tempfile::tempdir().unwrap();
        let conn = store::open(&tmp.path().join("store.db")).unwrap();
        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO anchors (anchor_name, manifest_id, last_updated_ns)
             VALUES ('HEAD', 1, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (1, 'src/lib.rs', 'sha-source')",
            [],
        )
        .unwrap();
        conn.execute_batch(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES
               ('sha-source', 'z-parser', 1, 0),
               ('sha-source', 'a-parser', 1, 0);

             INSERT INTO symbols
               (blob_sha, parser_id, name, qualified, kind, signature,
                byte_start, byte_end, line_start, line_end, source, scope)
             VALUES
               ('sha-source', 'z-parser', 'same', 'crate::same', 'function',
                'z metadata', 0, 10, 1, 1, 'syntactic', 'top_level'),
               ('sha-source', 'a-parser', 'same', 'crate::same', 'function',
                'a metadata', 0, 10, 1, 1, 'syntactic', 'top_level'),
               ('sha-source', 'a-parser', 'same', 'crate::same', 'function',
                'second declaration', 20, 30, 3, 3, 'syntactic', 'top_level');",
        )
        .unwrap();
        (tmp, conn)
    }

    #[test]
    fn physical_duplicates_choose_binary_parser_then_numeric_symbol_id() {
        let (_tmp, conn) = physical_declaration_fixture();
        let rows =
            get_symbol_source_rows(&conn, &AnchorName::head(), "crate::same", None, None, 10)
                .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].parser_id, "a-parser");
        assert_eq!(rows[0].symbol_id, 2);
        assert_eq!(rows[0].signature.as_deref(), Some("a metadata"));
        assert_eq!(rows[1].byte_start, 20);
    }

    #[test]
    fn file_and_one_indexed_line_selectors_filter_physical_declarations() {
        let (_tmp, conn) = physical_declaration_fixture();
        let rows = get_symbol_source_rows(
            &conn,
            &AnchorName::head(),
            "crate::same",
            Some("src/lib.rs"),
            Some(1),
            10,
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].line_start, 1);
        assert_eq!(rows[0].parser_id, "a-parser");

        let missing = get_symbol_source_rows(
            &conn,
            &AnchorName::head(),
            "crate::same",
            Some("src/other.rs"),
            Some(1),
            10,
        )
        .unwrap();
        assert!(missing.is_empty());
    }
}
