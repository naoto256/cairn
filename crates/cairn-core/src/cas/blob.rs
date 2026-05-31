//! Blob-level CAS operations: insert, lookup, reuse, delete.
//!
//! A "blob" here is the unit of dedup: one row in `blobs` per
//! `(blob_sha, parser_id)` plus the per-blob rows in `symbols` /
//! `refs` / `imports`. Implementations (impl-block edges) and FTS
//! sync are handled by other layers.
//!
//! Insertion is atomic via the caller's transaction. The caller picks
//! `parsed_at_ns` (so that fixture tests can pin a stable timestamp).

use std::collections::HashMap;

use cairn_lang_api::{ImplFact, ImportFact, RefFact, SemanticFacts, SymbolFact, SyntacticFacts};
use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::Result;
use crate::cas::kind_conv::{
    ref_kind_to_str, symbol_kind_to_str, type_role_to_str, visibility_to_str,
};

/// Everything one parser produces for one blob.
#[derive(Debug, Clone, Default)]
pub struct ParsedData {
    pub syntactic: SyntacticFacts,
    pub semantic: Option<SemanticFacts>,
}

/// Metadata returned by [`lookup`] when a `(blob_sha, parser_id)`
/// entry exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobMeta {
    pub parser_revision: u32,
    pub parsed_at_ns: i64,
}

/// Insert `data` for the given `(blob_sha, parser_id)`. Writes the
/// `blobs` row, the symbols, refs (syntactic + semantic merged),
/// imports (likewise), and applies any `doc_overrides`. Caller owns
/// the transaction; this function does not commit.
///
/// # Errors
/// Any SQLite error encountered while writing, or row-id conversion
/// out of range (shouldn't happen for realistic source sizes).
pub fn insert(
    tx: &Transaction<'_>,
    blob_sha: &str,
    parser_id: &str,
    parser_revision: u32,
    parsed_at_ns: i64,
    data: &ParsedData,
) -> Result<()> {
    tx.execute(
        "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
         VALUES (?1, ?2, ?3, ?4)",
        params![blob_sha, parser_id, parser_revision, parsed_at_ns],
    )?;

    // Symbols first — refs/imports rely on the within-blob symbol IDs
    // to populate `enclosing_id` and (later) `type_id`.
    let n = data.syntactic.symbols.len();
    let mut symbol_ids: Vec<i64> = Vec::with_capacity(n);
    // First-writer-wins: matches the original indexer's resolution
    // when the same qualified name appears more than once in a blob
    // (which only happens for overloads / partial-class scenarios).
    let mut by_qualified: HashMap<&str, i64> = HashMap::with_capacity(n);

    for sym in &data.syntactic.symbols {
        let parent_id = sym.parent_idx.and_then(|i| symbol_ids.get(i).copied());
        let id = insert_symbol(tx, blob_sha, parser_id, parent_id, sym)?;
        symbol_ids.push(id);
        by_qualified.entry(sym.qualified.as_str()).or_insert(id);
    }

    // doc_overrides: the syntactic pass often misses richer doc forms
    // (attribute clusters etc.); the analyzer patches them in.
    if let Some(sem) = &data.semantic {
        for d in &sem.doc_overrides {
            tx.execute(
                "UPDATE symbols SET doc = ?1
                 WHERE blob_sha = ?2 AND parser_id = ?3 AND qualified = ?4",
                params![d.doc, blob_sha, parser_id, d.target_qualified],
            )?;
        }
    }

    // refs (syntactic + semantic). Semantic refs identify their
    // caller by `enclosing_qualified` (no shared index space); fall
    // back to the by-qualified map.
    for r in &data.syntactic.refs {
        let enc = resolve_enclosing(r, &symbol_ids, &by_qualified);
        insert_ref(tx, blob_sha, parser_id, enc, r, "syntactic")?;
    }
    if let Some(sem) = &data.semantic {
        for r in &sem.refs {
            let enc = resolve_enclosing(r, &symbol_ids, &by_qualified);
            insert_ref(tx, blob_sha, parser_id, enc, r, "semantic")?;
        }
    }

    // imports (syntactic + semantic). The schema doesn't distinguish
    // source for imports; the existing query layer doesn't need it.
    for im in &data.syntactic.imports {
        insert_import(tx, blob_sha, parser_id, im)?;
    }
    if let Some(sem) = &data.semantic {
        for im in &sem.imports {
            insert_import(tx, blob_sha, parser_id, im)?;
        }
    }

    // impls (semantic only — syntactic-only backends don't emit
    // impl edges).
    if let Some(sem) = &data.semantic {
        for imp in &sem.impls {
            insert_impl(tx, blob_sha, parser_id, imp)?;
        }
    }

    Ok(())
}

/// Look up `(blob_sha, parser_id)`'s blob metadata. Returns `Ok(None)`
/// if no row exists.
///
/// # Errors
/// SQLite errors other than `QueryReturnedNoRows`.
pub fn lookup(conn: &Connection, blob_sha: &str, parser_id: &str) -> Result<Option<BlobMeta>> {
    let row = conn
        .query_row(
            "SELECT parser_revision, parsed_at_ns FROM blobs
             WHERE blob_sha = ?1 AND parser_id = ?2",
            params![blob_sha, parser_id],
            |r| {
                Ok(BlobMeta {
                    parser_revision: r.get::<_, u32>(0)?,
                    parsed_at_ns: r.get::<_, i64>(1)?,
                })
            },
        )
        .optional()?;
    Ok(row)
}

/// Delete a blob and all its dependent rows (symbols / refs / imports
/// / impls). Caller owns the transaction; the FK CASCADE handles the
/// cleanup once the blob row is gone.
///
/// # Errors
/// SQLite errors.
pub fn delete(tx: &Transaction<'_>, blob_sha: &str, parser_id: &str) -> Result<usize> {
    let n = tx.execute(
        "DELETE FROM blobs WHERE blob_sha = ?1 AND parser_id = ?2",
        params![blob_sha, parser_id],
    )?;
    Ok(n)
}

/// Reuse an existing parse if one is on disk for this
/// `(blob_sha, parser_id)` at the expected revision, else compute and
/// insert. Returns `true` if the inserted-or-reused entry is fresh
/// (= `compute` ran), `false` if reused.
///
/// The caller is responsible for `BEGIN`/`COMMIT`: pass a connection
/// that is not mid-transaction. The function uses an immediate
/// transaction internally so a concurrent writer can't interleave.
///
/// `compute` is called lazily — only when a fresh parse is needed.
///
/// # Errors
/// `compute` errors are propagated; SQLite errors otherwise.
pub fn reuse_or_compute<F>(
    conn: &mut Connection,
    blob_sha: &str,
    parser_id: &str,
    expected_revision: u32,
    parsed_at_ns: i64,
    compute: F,
) -> Result<bool>
where
    F: FnOnce() -> Result<ParsedData>,
{
    if let Some(meta) = lookup(conn, blob_sha, parser_id)?
        && meta.parser_revision == expected_revision
    {
        return Ok(false);
    }

    let data = compute()?;
    let tx = conn.transaction()?;
    // If a stale row exists (different revision), drop it first.
    delete(&tx, blob_sha, parser_id)?;
    insert(
        &tx,
        blob_sha,
        parser_id,
        expected_revision,
        parsed_at_ns,
        &data,
    )?;
    tx.commit()?;
    Ok(true)
}

// ─── helpers ───────────────────────────────────────────────────────────────

fn insert_symbol(
    tx: &Transaction<'_>,
    blob_sha: &str,
    parser_id: &str,
    parent_id: Option<i64>,
    sym: &SymbolFact,
) -> Result<i64> {
    let kind_str = symbol_kind_to_str(&sym.kind);
    let visibility_str = sym.visibility.map(visibility_to_str);
    let line_start = i64::from(sym.line_range.start);
    let line_end = i64::from(sym.line_range.end);
    let byte_start = i64::try_from(sym.byte_range.start).unwrap_or(i64::MAX);
    let byte_end = i64::try_from(sym.byte_range.end).unwrap_or(i64::MAX);
    let body_start = sym.body_start.map(|b| i64::try_from(b).unwrap_or(i64::MAX));

    tx.execute(
        "INSERT INTO symbols
           (blob_sha, parser_id, parent_id, name, qualified, kind,
            signature, visibility, doc, byte_start, byte_end,
            line_start, line_end, body_start, source)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        params![
            blob_sha,
            parser_id,
            parent_id,
            sym.name,
            sym.qualified,
            kind_str,
            sym.signature,
            visibility_str,
            sym.doc,
            byte_start,
            byte_end,
            line_start,
            line_end,
            body_start,
            "syntactic",
        ],
    )?;
    Ok(tx.last_insert_rowid())
}

fn insert_ref(
    tx: &Transaction<'_>,
    blob_sha: &str,
    parser_id: &str,
    enclosing_id: Option<i64>,
    r: &RefFact,
    source: &str,
) -> Result<()> {
    let kind_str = ref_kind_to_str(r.kind);
    let type_role_str = r.type_role.map(type_role_to_str);
    let byte_start = i64::try_from(r.byte_range.start).unwrap_or(i64::MAX);
    let byte_end = i64::try_from(r.byte_range.end).unwrap_or(i64::MAX);
    let line = i64::from(r.line);

    tx.execute(
        "INSERT INTO refs
           (blob_sha, parser_id, enclosing_id, target_name, target_qualified,
            kind, type_role, byte_start, byte_end, line, source)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            blob_sha,
            parser_id,
            enclosing_id,
            r.target_name,
            r.target_qualified,
            kind_str,
            type_role_str,
            byte_start,
            byte_end,
            line,
            source,
        ],
    )?;
    Ok(())
}

fn insert_impl(
    tx: &Transaction<'_>,
    blob_sha: &str,
    parser_id: &str,
    imp: &ImplFact,
) -> Result<()> {
    let line = i64::from(imp.line);
    tx.execute(
        "INSERT INTO implementations
           (blob_sha, parser_id, type_qualified, interface_qualified, kind, line)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            blob_sha,
            parser_id,
            imp.type_qualified,
            imp.interface_qualified,
            imp.kind,
            line,
        ],
    )?;
    Ok(())
}

fn insert_import(
    tx: &Transaction<'_>,
    blob_sha: &str,
    parser_id: &str,
    im: &ImportFact,
) -> Result<()> {
    let line = i64::from(im.line);
    let reexport = i64::from(im.is_reexport);
    tx.execute(
        "INSERT INTO imports
           (blob_sha, parser_id, to_module, imported, alias, is_reexport, line)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            blob_sha,
            parser_id,
            im.to_module,
            im.imported,
            im.alias,
            reexport,
            line,
        ],
    )?;
    Ok(())
}

fn resolve_enclosing(
    r: &RefFact,
    symbol_ids: &[i64],
    by_qualified: &HashMap<&str, i64>,
) -> Option<i64> {
    r.enclosing_idx
        .and_then(|i| symbol_ids.get(i).copied())
        .or_else(|| {
            r.enclosing_qualified
                .as_deref()
                .and_then(|q| by_qualified.get(q).copied())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::store;
    use cairn_lang_api::{DocOverride, ImportFact, RefFact, RefKind, SymbolFact, SymbolKind};

    fn fresh() -> (tempfile::TempDir, Connection) {
        let tmp = tempfile::tempdir().unwrap();
        let conn = store::open(&tmp.path().join("store.db")).unwrap();
        (tmp, conn)
    }

    fn sym(name: &str, qualified: &str, parent_idx: Option<usize>) -> SymbolFact {
        SymbolFact {
            name: name.into(),
            qualified: qualified.into(),
            kind: SymbolKind::Function,
            signature: None,
            doc: None,
            visibility: None,
            byte_range: 0..1,
            line_range: 1..1,
            body_start: None,
            parent_idx,
        }
    }

    #[test]
    fn insert_then_lookup_returns_meta() {
        let (_tmp, mut c) = fresh();
        let data = ParsedData::default();
        let tx = c.transaction().unwrap();
        insert(&tx, "shaA", "rust", 7, 1234, &data).unwrap();
        tx.commit().unwrap();

        let meta = lookup(&c, "shaA", "rust").unwrap().unwrap();
        assert_eq!(meta.parser_revision, 7);
        assert_eq!(meta.parsed_at_ns, 1234);
    }

    #[test]
    fn lookup_returns_none_for_missing() {
        let (_tmp, c) = fresh();
        assert!(lookup(&c, "nope", "rust").unwrap().is_none());
    }

    #[test]
    fn insert_persists_symbols_with_parent_chain() {
        let (_tmp, mut c) = fresh();
        let data = ParsedData {
            syntactic: SyntacticFacts {
                symbols: vec![
                    sym("Foo", "m::Foo", None),
                    sym("bar", "m::Foo::bar", Some(0)),
                    sym("baz", "m::Foo::baz", Some(0)),
                ],
                ..Default::default()
            },
            semantic: None,
        };
        let tx = c.transaction().unwrap();
        insert(&tx, "shaB", "rust", 1, 0, &data).unwrap();
        tx.commit().unwrap();

        let count: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM symbols WHERE blob_sha = 'shaB'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 3);

        // Parent chain: 'bar' and 'baz' both reference 'Foo'.
        let foo_id: i64 = c
            .query_row(
                "SELECT id FROM symbols WHERE qualified = 'm::Foo'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let children: Vec<i64> = c
            .prepare("SELECT id FROM symbols WHERE parent_id = ?1 ORDER BY name")
            .unwrap()
            .query_map([foo_id], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn refs_resolve_enclosing_via_idx_or_qualified() {
        let (_tmp, mut c) = fresh();
        let data = ParsedData {
            syntactic: SyntacticFacts {
                symbols: vec![sym("caller", "m::caller", None)],
                refs: vec![RefFact {
                    target_name: "callee".into(),
                    target_qualified: Some("other::callee".into()),
                    kind: RefKind::Call,
                    type_role: None,
                    enclosing_idx: Some(0), // direct index
                    enclosing_qualified: None,
                    byte_range: 10..20,
                    line: 5,
                }],
                ..Default::default()
            },
            semantic: Some(SemanticFacts {
                refs: vec![RefFact {
                    target_name: "callee2".into(),
                    target_qualified: None,
                    kind: RefKind::Call,
                    type_role: None,
                    enclosing_idx: None, // semantic ref → resolve via qualified
                    enclosing_qualified: Some("m::caller".into()),
                    byte_range: 30..40,
                    line: 7,
                }],
                ..Default::default()
            }),
        };
        let tx = c.transaction().unwrap();
        insert(&tx, "shaC", "rust", 1, 0, &data).unwrap();
        tx.commit().unwrap();

        let resolved: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM refs WHERE enclosing_id IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(resolved, 2);
        let syntactic_first: String = c
            .query_row(
                "SELECT source FROM refs ORDER BY line ASC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(syntactic_first, "syntactic");
    }

    #[test]
    fn doc_overrides_patch_symbol() {
        let (_tmp, mut c) = fresh();
        let data = ParsedData {
            syntactic: SyntacticFacts {
                symbols: vec![SymbolFact {
                    name: "foo".into(),
                    qualified: "m::foo".into(),
                    kind: SymbolKind::Function,
                    signature: None,
                    doc: Some("old doc".into()),
                    visibility: None,
                    byte_range: 0..1,
                    line_range: 1..1,
                    body_start: None,
                    parent_idx: None,
                }],
                ..Default::default()
            },
            semantic: Some(SemanticFacts {
                doc_overrides: vec![DocOverride {
                    target_qualified: "m::foo".into(),
                    doc: "new richer doc".into(),
                }],
                ..Default::default()
            }),
        };
        let tx = c.transaction().unwrap();
        insert(&tx, "shaD", "rust", 1, 0, &data).unwrap();
        tx.commit().unwrap();

        let doc: String = c
            .query_row(
                "SELECT doc FROM symbols WHERE qualified = 'm::foo'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(doc, "new richer doc");
    }

    #[test]
    fn imports_merged_from_both_layers() {
        let (_tmp, mut c) = fresh();
        let data = ParsedData {
            syntactic: SyntacticFacts {
                imports: vec![ImportFact {
                    to_module: "std::io".into(),
                    imported: Some("Read".into()),
                    alias: None,
                    is_reexport: false,
                    line: 1,
                }],
                ..Default::default()
            },
            semantic: Some(SemanticFacts {
                imports: vec![ImportFact {
                    to_module: "other::mod".into(),
                    imported: None,
                    alias: Some("mod_alias".into()),
                    is_reexport: true,
                    line: 2,
                }],
                ..Default::default()
            }),
        };
        let tx = c.transaction().unwrap();
        insert(&tx, "shaE", "rust", 1, 0, &data).unwrap();
        tx.commit().unwrap();

        let count: i64 = c
            .query_row("SELECT COUNT(*) FROM imports", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
        let reexport: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM imports WHERE is_reexport = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(reexport, 1);
    }

    #[test]
    fn delete_cascades_to_dependent_rows() {
        let (_tmp, mut c) = fresh();
        let data = ParsedData {
            syntactic: SyntacticFacts {
                symbols: vec![sym("foo", "foo", None)],
                refs: vec![RefFact {
                    target_name: "bar".into(),
                    target_qualified: None,
                    kind: RefKind::Call,
                    type_role: None,
                    enclosing_idx: Some(0),
                    enclosing_qualified: None,
                    byte_range: 0..1,
                    line: 1,
                }],
                imports: vec![ImportFact {
                    to_module: "x".into(),
                    imported: None,
                    alias: None,
                    is_reexport: false,
                    line: 1,
                }],
            },
            semantic: None,
        };
        let tx = c.transaction().unwrap();
        insert(&tx, "shaF", "rust", 1, 0, &data).unwrap();
        let removed = delete(&tx, "shaF", "rust").unwrap();
        tx.commit().unwrap();
        assert_eq!(removed, 1);

        for t in ["symbols", "refs", "imports"] {
            let n: i64 = c
                .query_row(&format!("SELECT COUNT(*) FROM {t}"), [], |r| r.get(0))
                .unwrap();
            assert_eq!(n, 0, "{t} not cascaded");
        }
    }

    #[test]
    fn reuse_or_compute_skips_when_revision_matches() {
        let (_tmp, mut c) = fresh();
        let counter = std::cell::RefCell::new(0u32);
        let bump = || {
            *counter.borrow_mut() += 1;
            Ok(ParsedData::default())
        };

        let fresh1 = reuse_or_compute(&mut c, "shaG", "rust", 3, 0, bump).unwrap();
        assert!(fresh1);
        assert_eq!(*counter.borrow(), 1);

        let fresh2 = reuse_or_compute(&mut c, "shaG", "rust", 3, 0, bump).unwrap();
        assert!(!fresh2);
        assert_eq!(*counter.borrow(), 1, "compute should not run on hit");
    }

    #[test]
    fn reuse_or_compute_reparses_on_revision_bump() {
        let (_tmp, mut c) = fresh();
        let counter = std::cell::RefCell::new(0u32);
        let bump = || {
            *counter.borrow_mut() += 1;
            Ok(ParsedData::default())
        };

        reuse_or_compute(&mut c, "shaH", "rust", 1, 0, bump).unwrap();
        let fresh = reuse_or_compute(&mut c, "shaH", "rust", 2, 0, bump).unwrap();
        assert!(fresh);
        assert_eq!(*counter.borrow(), 2);

        let meta = lookup(&c, "shaH", "rust").unwrap().unwrap();
        assert_eq!(meta.parser_revision, 2);
    }
}
