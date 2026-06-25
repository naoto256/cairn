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

use cairn_lang_api::{
    ImplFact, ImportFact, RefFact, SemanticFacts, SymbolFact, SyntacticFacts, SyntacticKind,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use crate::Result;
use crate::cas::kind_conv::{
    ref_kind_to_str, symbol_kind_to_str, type_role_to_str, visibility_to_str,
};
use crate::resolution::{ResolutionKind, SemanticKind};

/// Everything one parser produces for one blob.
#[derive(Debug, Clone, Default)]
pub struct ParsedData {
    pub syntactic: SyntacticFacts,
    /// `None` means there is no semantic layer for this parse. Callers
    /// that need to distinguish "not available" from "failed" must
    /// carry the extraction error outside this storage DTO.
    pub semantic: Option<SemanticFacts>,
}

/// Metadata returned by [`lookup`] when a `(blob_sha, parser_id)`
/// entry exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobMeta {
    pub parser_revision: u32,
    pub parsed_at_ns: i64,
    pub analyzer_id: Option<String>,
    pub analyzer_revision: Option<u32>,
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
    analyzer: Option<(&str, u32)>,
    data: &ParsedData,
) -> Result<()> {
    let (analyzer_id, analyzer_revision) = match analyzer {
        Some((id, revision)) => (Some(id), Some(revision)),
        None => (None, None),
    };
    tx.execute(
        "INSERT INTO blobs
           (blob_sha, parser_id, parser_revision, parsed_at_ns, analyzer_id, analyzer_revision)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            blob_sha,
            parser_id,
            parser_revision,
            parsed_at_ns,
            analyzer_id,
            analyzer_revision,
        ],
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
    // (attribute clusters etc.); the analyzer patches them in. We
    // scope the UPDATE by `(qualified, kind)` because Rust admits
    // multiple symbol rows for the same qualified name (a `struct
    // Foo` next to `impl Foo` and `impl Trait for Foo`), and the
    // analyzer only sourced the doc from one of them.
    if let Some(sem) = &data.semantic {
        for d in &sem.doc_overrides {
            let kind = crate::cas::kind_conv::symbol_kind_to_str(&d.target_kind);
            tx.execute(
                "UPDATE symbols SET doc = ?1
                 WHERE blob_sha = ?2 AND parser_id = ?3
                   AND qualified = ?4 AND kind = ?5",
                params![d.doc, blob_sha, parser_id, d.target_qualified, kind],
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
    // impl edges). For each impl we additionally emit a Tier-2
    // direct-translation Resolution row when the grammar shape maps
    // unambiguously to a semantic_kind (see
    // `tier2_direct_resolution`). The two writes share this
    // transaction so a Resolution can never outlive its fact.
    if let Some(sem) = &data.semantic {
        for imp in &sem.impls {
            insert_impl(tx, blob_sha, parser_id, imp)?;
            insert_direct_resolution(tx, blob_sha, parser_id, imp)?;
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
            "SELECT parser_revision, parsed_at_ns, analyzer_id, analyzer_revision FROM blobs
             WHERE blob_sha = ?1 AND parser_id = ?2",
            params![blob_sha, parser_id],
            |r| {
                Ok(BlobMeta {
                    parser_revision: r.get::<_, u32>(0)?,
                    parsed_at_ns: r.get::<_, i64>(1)?,
                    analyzer_id: r.get::<_, Option<String>>(2)?,
                    analyzer_revision: r.get::<_, Option<u32>>(3)?,
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
    expected_analyzer: Option<(&str, u32)>,
    parsed_at_ns: i64,
    compute: F,
) -> Result<bool>
where
    F: FnOnce() -> Result<ParsedData>,
{
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    // Hold the write reservation across the re-check and compute so
    // two writers cannot both observe a miss and race into the UNIQUE
    // key on `(blob_sha, parser_id)`.
    if let Some(meta) = lookup(&tx, blob_sha, parser_id)?
        && meta.parser_revision == expected_revision
        && analyzer_matches(&meta, expected_analyzer)
    {
        tx.commit()?;
        return Ok(false);
    }

    let data = compute()?;
    // If a stale row exists (different revision), drop it first.
    delete(&tx, blob_sha, parser_id)?;
    insert(
        &tx,
        blob_sha,
        parser_id,
        expected_revision,
        parsed_at_ns,
        expected_analyzer,
        &data,
    )?;
    tx.commit()?;
    Ok(true)
}

fn analyzer_matches(meta: &BlobMeta, expected: Option<(&str, u32)>) -> bool {
    match expected {
        Some((id, revision)) => {
            meta.analyzer_id.as_deref() == Some(id) && meta.analyzer_revision == Some(revision)
        }
        None => meta.analyzer_id.is_none() && meta.analyzer_revision.is_none(),
    }
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
    // `syntactic_kind` (added in schema v7) carries the grammar-direct
    // shape — serialized via the enum's snake_case serde rename so the
    // string in the DB matches the lang-api `SyntacticKind` mapping
    // (`extends`, `implements`, `colon`, ...). The resolution layer
    // will read it in Phase 3; query paths do not touch it yet.
    let syntactic_kind = imp
        .syntactic_kind
        .as_ref()
        .and_then(|k| serde_json::to_value(k).ok())
        .and_then(|v| v.as_str().map(str::to_owned));
    // `interface_byte_range` (added to `ImplFact` in Phase 2) is mirrored
    // into the row in Phase 4 so the query layer can JOIN against the
    // `resolutions` table on the same `(blob, byte_range)` tuple the
    // resolution writer uses. Backends that ship no range write NULL.
    let (iface_start, iface_end) = imp.interface_byte_range.map_or((None, None), |(s, e)| {
        (Some(i64::from(s)), Some(i64::from(e)))
    });
    tx.execute(
        "INSERT INTO implementations
           (blob_sha, parser_id, type_qualified, interface_qualified, kind,
            syntactic_kind, line, interface_byte_start, interface_byte_end)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            blob_sha,
            parser_id,
            imp.type_qualified,
            imp.interface_qualified,
            imp.kind,
            syntactic_kind,
            line,
            iface_start,
            iface_end,
        ],
    )?;
    Ok(())
}

/// Map (parser_id, syntactic_kind) to a Tier-2 direct-translation
/// `(SemanticKind, source)` pair, or `None` when no direct
/// translation is defined.
///
/// "Direct translation" means the grammar shape determines the
/// semantic kind unambiguously without Tier-2.5 / Tier-3 context.
/// Ambiguous cases (Python `BaseArg`, Kotlin / Swift class / C# `Colon`)
/// are deliberately omitted — they will be resolved by Tier-2.5.
///
/// `<lang>` in the source string follows the backend crate identifier
/// convention used by `WORKSPACE_TIER_PREFIXES`. `tree-sitter-tsx` is
/// mapped to `typescript` because the TSX backend shares the
/// TypeScript analyzer and its impl edges are TS-shaped; the `.tsx`
/// dialect difference is purely about JSX-in-expression positions.
fn tier2_direct_resolution(
    parser_id: &str,
    syntactic_kind: SyntacticKind,
) -> Option<(SemanticKind, &'static str)> {
    let lang = match parser_id {
        "tree-sitter-java" => "java",
        "tree-sitter-typescript" | "tree-sitter-tsx" => "typescript",
        "tree-sitter-javascript" => "javascript",
        "tree-sitter-php" => "php",
        "tree-sitter-ruby" => "ruby",
        "tree-sitter-rust" => "rust",
        "tree-sitter-cpp" => "cpp",
        "tree-sitter-objc" => "objc",
        "tree-sitter-swift" => "swift",
        _ => return None,
    };
    let sem = match (lang, syntactic_kind) {
        // extends → inherit for Java / TS / JS / PHP.
        ("java" | "typescript" | "javascript" | "php", SyntacticKind::Extends) => {
            SemanticKind::Inherit
        }
        // implements → implement for Java / TS / PHP.
        ("java" | "typescript" | "php", SyntacticKind::Implements) => SemanticKind::Implement,
        // Ruby class < Base.
        ("ruby", SyntacticKind::LessThan) => SemanticKind::Inherit,
        // Ruby include / extend / prepend → mixin.
        ("ruby", SyntacticKind::Include | SyntacticKind::ExtendKw | SyntacticKind::Prepend) => {
            SemanticKind::Mixin
        }
        // PHP `use Trait;` inside a class body.
        ("php", SyntacticKind::TraitUse) => SemanticKind::Mixin,
        // Rust impl block. (Rust currently ships no byte range, so the
        // caller will skip emission — keeping the entry here documents
        // intent and makes it cheap to wire once syn spans are bridged
        // to byte offsets.)
        ("rust", SyntacticKind::ImplFor) => SemanticKind::Implement,
        // C++ public/private/protected base.
        (
            "cpp",
            SyntacticKind::PublicBase | SyntacticKind::PrivateBase | SyntacticKind::ProtectedBase,
        ) => SemanticKind::Inherit,
        // Objective-C `: Super`, `<Protocol>`, `(Category)`.
        ("objc", SyntacticKind::InterfaceColon) => SemanticKind::Inherit,
        ("objc", SyntacticKind::ProtocolList) => SemanticKind::Implement,
        ("objc", SyntacticKind::Category) => SemanticKind::Extension,
        // Swift `extension Foo { ... }` self-edge.
        ("swift", SyntacticKind::Extension) => SemanticKind::Extension,
        // Everything else — including the ambiguous cases (BaseArg,
        // Colon, Supertrait, Embed) — is left to Tier-2.5+.
        _ => return None,
    };
    let source = match lang {
        "java" => "tier2-direct-java",
        "typescript" => "tier2-direct-typescript",
        "javascript" => "tier2-direct-javascript",
        "php" => "tier2-direct-php",
        "ruby" => "tier2-direct-ruby",
        "rust" => "tier2-direct-rust",
        "cpp" => "tier2-direct-cpp",
        "objc" => "tier2-direct-objc",
        "swift" => "tier2-direct-swift",
        _ => return None,
    };
    Some((sem, source))
}

/// Emit a Tier-2 direct-translation `resolutions` row for `imp` when
/// the (parser_id, syntactic_kind) pair has an unambiguous mapping
/// *and* the backend supplied a site byte range. The row carries
/// `target_symbol_id = NULL` — Tier-2 does no cross-file resolution.
fn insert_direct_resolution(
    tx: &Transaction<'_>,
    blob_sha: &str,
    parser_id: &str,
    imp: &ImplFact,
) -> Result<()> {
    let Some(syntactic) = imp.syntactic_kind else {
        return Ok(());
    };
    let Some((sem, source)) = tier2_direct_resolution(parser_id, syntactic) else {
        return Ok(());
    };
    let Some((start, end)) = imp.interface_byte_range else {
        return Ok(());
    };
    // Tier-2 direct does no cross-file resolution: both `target_symbol_id`
    // and `target_path` are NULL by construction. The column list is spelled
    // out so a future schema column with a different default does not
    // silently slip through this writer.
    tx.execute(
        "INSERT INTO resolutions
           (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
            kind, semantic_kind, target_symbol_id, target_path, source)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, ?7)",
        params![
            blob_sha,
            parser_id,
            i64::from(start),
            i64::from(end),
            ResolutionKind::Type.as_str(),
            sem.as_str(),
            source,
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
    // Schema v9: byte_range is Option<(u32, u32)> on the wire and
    // NULL on disk when absent. Backends that don't yet ship a range
    // (everything but Ruby `require` / `require_relative` today) get
    // NULL stored, which the `find_imports` LEFT JOIN treats as
    // "no resolution available".
    let (byte_start, byte_end) = match im.byte_range {
        Some((s, e)) => (Some(i64::from(s)), Some(i64::from(e))),
        None => (None, None),
    };
    tx.execute(
        "INSERT INTO imports
           (blob_sha, parser_id, to_module, imported, alias, is_reexport, line,
            byte_start, byte_end)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            blob_sha,
            parser_id,
            im.to_module,
            im.imported,
            im.alias,
            reexport,
            line,
            byte_start,
            byte_end,
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
        insert(&tx, "shaA", "rust", 7, 1234, None, &data).unwrap();
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
        insert(&tx, "shaB", "rust", 1, 0, None, &data).unwrap();
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
        insert(&tx, "shaC", "rust", 1, 0, None, &data).unwrap();
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
                    target_kind: SymbolKind::Function,
                    doc: "new richer doc".into(),
                }],
                ..Default::default()
            }),
        };
        let tx = c.transaction().unwrap();
        insert(&tx, "shaD", "rust", 1, 0, None, &data).unwrap();
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
    fn doc_override_does_not_leak_to_sibling_kinds() {
        // Regression: a `struct Foo` and its sibling `impl Foo` /
        // `impl Trait for Foo` share `qualified="Foo"`. The
        // analyzer's doc_override (sourced from the struct) used to
        // UPDATE every row matching qualified, leaking the struct
        // doc onto the impl rows. The fix scopes UPDATE by kind.
        let (_tmp, mut c) = fresh();
        let data = ParsedData {
            syntactic: SyntacticFacts {
                symbols: vec![
                    SymbolFact {
                        name: "Foo".into(),
                        qualified: "Foo".into(),
                        kind: SymbolKind::Struct,
                        signature: Some("pub struct Foo".into()),
                        doc: None,
                        visibility: None,
                        byte_range: 0..1,
                        line_range: 1..1,
                        body_start: None,
                        parent_idx: None,
                    },
                    SymbolFact {
                        name: "Foo".into(),
                        qualified: "Foo".into(),
                        kind: SymbolKind::Impl,
                        signature: Some("impl Foo".into()),
                        doc: None,
                        visibility: None,
                        byte_range: 2..3,
                        line_range: 5..5,
                        body_start: None,
                        parent_idx: None,
                    },
                    SymbolFact {
                        name: "Foo".into(),
                        qualified: "Foo".into(),
                        kind: SymbolKind::Impl,
                        signature: Some("impl Display for Foo".into()),
                        doc: None,
                        visibility: None,
                        byte_range: 4..5,
                        line_range: 10..10,
                        body_start: None,
                        parent_idx: None,
                    },
                ],
                ..Default::default()
            },
            semantic: Some(SemanticFacts {
                doc_overrides: vec![DocOverride {
                    target_qualified: "Foo".into(),
                    target_kind: SymbolKind::Struct,
                    doc: "Struct-only doc".into(),
                }],
                ..Default::default()
            }),
        };
        let tx = c.transaction().unwrap();
        insert(&tx, "shaG", "rust", 1, 0, None, &data).unwrap();
        tx.commit().unwrap();

        let mut stmt = c
            .prepare("SELECT kind, doc FROM symbols WHERE qualified = 'Foo' ORDER BY line_start")
            .unwrap();
        let rows: Vec<(String, Option<String>)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(std::result::Result::unwrap)
            .collect();
        assert_eq!(
            rows,
            vec![
                ("struct".into(), Some("Struct-only doc".into())),
                ("impl".into(), None),
                ("impl".into(), None),
            ],
            "doc override must patch the struct only, not its sibling impl rows"
        );
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

                    byte_range: None,
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

                    byte_range: None,
                }],
                ..Default::default()
            }),
        };
        let tx = c.transaction().unwrap();
        insert(&tx, "shaE", "rust", 1, 0, None, &data).unwrap();
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

                    byte_range: None,
                }],
            },
            semantic: None,
        };
        let tx = c.transaction().unwrap();
        insert(&tx, "shaF", "rust", 1, 0, None, &data).unwrap();
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

        let fresh1 = reuse_or_compute(&mut c, "shaG", "rust", 3, None, 0, bump).unwrap();
        assert!(fresh1);
        assert_eq!(*counter.borrow(), 1);

        let fresh2 = reuse_or_compute(&mut c, "shaG", "rust", 3, None, 0, bump).unwrap();
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

        reuse_or_compute(&mut c, "shaH", "rust", 1, None, 0, bump).unwrap();
        let fresh = reuse_or_compute(&mut c, "shaH", "rust", 2, None, 0, bump).unwrap();
        assert!(fresh);
        assert_eq!(*counter.borrow(), 2);

        let meta = lookup(&c, "shaH", "rust").unwrap().unwrap();
        assert_eq!(meta.parser_revision, 2);
    }

    #[test]
    fn reuse_or_compute_reparses_on_analyzer_revision_bump() {
        let (_tmp, mut c) = fresh();
        let counter = std::cell::RefCell::new(0u32);
        let bump = || {
            *counter.borrow_mut() += 1;
            Ok(ParsedData::default())
        };

        reuse_or_compute(&mut c, "shaI", "rust", 1, Some(("rust-syn", 1)), 0, bump).unwrap();
        let fresh =
            reuse_or_compute(&mut c, "shaI", "rust", 1, Some(("rust-syn", 2)), 0, bump).unwrap();
        assert!(fresh);
        assert_eq!(*counter.borrow(), 2);

        let meta = lookup(&c, "shaI", "rust").unwrap().unwrap();
        assert_eq!(meta.parser_revision, 1);
        assert_eq!(meta.analyzer_id.as_deref(), Some("rust-syn"));
        assert_eq!(meta.analyzer_revision, Some(2));
    }

    #[test]
    fn reuse_or_compute_reparses_when_analyzer_disappears() {
        let (_tmp, mut c) = fresh();
        let counter = std::cell::RefCell::new(0u32);
        let bump = || {
            *counter.borrow_mut() += 1;
            Ok(ParsedData::default())
        };

        reuse_or_compute(&mut c, "shaJ", "rust", 1, Some(("rust-syn", 1)), 0, bump).unwrap();
        let fresh = reuse_or_compute(&mut c, "shaJ", "rust", 1, None, 0, bump).unwrap();
        assert!(fresh);
        assert_eq!(*counter.borrow(), 2);

        let meta = lookup(&c, "shaJ", "rust").unwrap().unwrap();
        assert_eq!(meta.analyzer_id, None);
        assert_eq!(meta.analyzer_revision, None);
    }

    #[test]
    fn reuse_or_compute_serializes_concurrent_writers() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Barrier};
        use std::time::Duration;

        let tmp = tempfile::tempdir().unwrap();
        let db = tmp.path().join("store.db");
        let mut first_conn = store::open(&db).unwrap();
        first_conn.busy_timeout(Duration::from_secs(5)).unwrap();

        let barrier = Arc::new(Barrier::new(2));
        let compute_count = Arc::new(AtomicUsize::new(0));
        let second_barrier = Arc::clone(&barrier);
        let second_count = Arc::clone(&compute_count);
        let second_db = db.clone();

        let second = std::thread::spawn(move || {
            second_barrier.wait();
            let mut conn = store::open(&second_db).unwrap();
            conn.busy_timeout(Duration::from_secs(5)).unwrap();
            reuse_or_compute(&mut conn, "shaK", "rust", 1, None, 20, || {
                second_count.fetch_add(1, Ordering::SeqCst);
                Ok(ParsedData::default())
            })
            .unwrap()
        });

        let first_fresh = reuse_or_compute(&mut first_conn, "shaK", "rust", 1, None, 10, || {
            compute_count.fetch_add(1, Ordering::SeqCst);
            barrier.wait();
            std::thread::sleep(Duration::from_millis(100));
            Ok(ParsedData::default())
        })
        .unwrap();
        let second_fresh = second.join().unwrap();

        assert!(first_fresh);
        assert!(!second_fresh);
        assert_eq!(compute_count.load(Ordering::SeqCst), 1);
    }

    // ─── Tier-2 direct-translation resolution emission (Phase 3) ─────────

    fn impl_fact(
        type_q: &str,
        iface: Option<&str>,
        kind: &str,
        syntactic: SyntacticKind,
        byte_range: Option<(u32, u32)>,
    ) -> ImplFact {
        ImplFact {
            type_qualified: type_q.into(),
            interface_qualified: iface.map(str::to_string),
            kind: kind.into(),
            syntactic_kind: Some(syntactic),
            line: 1,
            interface_byte_range: byte_range,
        }
    }

    fn insert_one_impl(c: &mut Connection, sha: &str, parser_id: &str, imp: ImplFact) {
        let data = ParsedData {
            syntactic: SyntacticFacts::default(),
            semantic: Some(SemanticFacts {
                impls: vec![imp],
                ..Default::default()
            }),
        };
        let tx = c.transaction().unwrap();
        insert(&tx, sha, parser_id, 1, 0, None, &data).unwrap();
        tx.commit().unwrap();
    }

    fn resolutions_for(
        c: &Connection,
        sha: &str,
    ) -> Vec<(String, Option<String>, String, i64, i64)> {
        c.prepare(
            "SELECT kind, semantic_kind, source, site_byte_start, site_byte_end
             FROM resolutions WHERE site_blob_sha = ?1 ORDER BY site_byte_start",
        )
        .unwrap()
        .query_map([sha], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, i64>(4)?,
            ))
        })
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap()
    }

    #[test]
    fn tier2_direct_emits_java_extends_implements() {
        let (_tmp, mut c) = fresh();
        insert_one_impl(
            &mut c,
            "j1",
            "tree-sitter-java",
            impl_fact(
                "p.Dog",
                Some("p.Animal"),
                "inherit",
                SyntacticKind::Extends,
                Some((10, 16)),
            ),
        );
        // reuse same connection for subsequent shas
        insert_one_impl(
            &mut c,
            "j2",
            "tree-sitter-java",
            impl_fact(
                "p.Dog",
                Some("p.Walker"),
                "implement",
                SyntacticKind::Implements,
                Some((20, 26)),
            ),
        );
        let r1 = resolutions_for(&c, "j1");
        assert_eq!(r1.len(), 1);
        assert_eq!(r1[0].0, "type");
        assert_eq!(r1[0].1.as_deref(), Some("inherit"));
        assert_eq!(r1[0].2, "tier2-direct-java");
        assert_eq!((r1[0].3, r1[0].4), (10, 16));

        let r2 = resolutions_for(&c, "j2");
        assert_eq!(r2.len(), 1);
        assert_eq!(r2[0].1.as_deref(), Some("implement"));
        assert_eq!(r2[0].2, "tier2-direct-java");
    }

    #[test]
    fn tier2_direct_emits_ruby_less_than_and_mixins() {
        let (_tmp, mut c) = fresh();
        insert_one_impl(
            &mut c,
            "rb1",
            "tree-sitter-ruby",
            impl_fact(
                "Dog",
                Some("Animal"),
                "inherit",
                SyntacticKind::LessThan,
                Some((5, 11)),
            ),
        );
        // reuse same connection for subsequent shas
        insert_one_impl(
            &mut c,
            "rb2",
            "tree-sitter-ruby",
            impl_fact(
                "Dog",
                Some("Walkable"),
                "include",
                SyntacticKind::Include,
                Some((30, 38)),
            ),
        );
        // reuse
        insert_one_impl(
            &mut c,
            "rb3",
            "tree-sitter-ruby",
            impl_fact(
                "Dog",
                Some("Hooks"),
                "extend",
                SyntacticKind::ExtendKw,
                Some((40, 45)),
            ),
        );
        // reuse
        insert_one_impl(
            &mut c,
            "rb4",
            "tree-sitter-ruby",
            impl_fact(
                "Dog",
                Some("Tracer"),
                "prepend",
                SyntacticKind::Prepend,
                Some((50, 56)),
            ),
        );

        let r1 = resolutions_for(&c, "rb1");
        assert_eq!(r1[0].1.as_deref(), Some("inherit"));
        assert_eq!(r1[0].2, "tier2-direct-ruby");
        for sha in ["rb2", "rb3", "rb4"] {
            let r = resolutions_for(&c, sha);
            assert_eq!(r.len(), 1, "sha={sha}");
            assert_eq!(r[0].1.as_deref(), Some("mixin"), "sha={sha}");
            assert_eq!(r[0].2, "tier2-direct-ruby");
        }
    }

    #[test]
    fn tier2_direct_emits_php_extends_implements_trait_use() {
        let (_tmp, mut c) = fresh();
        insert_one_impl(
            &mut c,
            "p1",
            "tree-sitter-php",
            impl_fact(
                "Dog",
                Some("Animal"),
                "inherit",
                SyntacticKind::Extends,
                Some((10, 16)),
            ),
        );
        // reuse same connection for subsequent shas
        insert_one_impl(
            &mut c,
            "p2",
            "tree-sitter-php",
            impl_fact(
                "Dog",
                Some("I"),
                "implement",
                SyntacticKind::Implements,
                Some((20, 21)),
            ),
        );
        // reuse
        insert_one_impl(
            &mut c,
            "p3",
            "tree-sitter-php",
            impl_fact(
                "Dog",
                Some("T"),
                "mixin",
                SyntacticKind::TraitUse,
                Some((30, 31)),
            ),
        );

        assert_eq!(resolutions_for(&c, "p1")[0].1.as_deref(), Some("inherit"));
        assert_eq!(resolutions_for(&c, "p2")[0].1.as_deref(), Some("implement"));
        let r3 = resolutions_for(&c, "p3");
        assert_eq!(r3[0].1.as_deref(), Some("mixin"));
        assert_eq!(r3[0].2, "tier2-direct-php");
    }

    #[test]
    fn tier2_direct_emits_objc_super_protocol_category() {
        let (_tmp, mut c) = fresh();
        insert_one_impl(
            &mut c,
            "o1",
            "tree-sitter-objc",
            impl_fact(
                "Dog",
                Some("Animal"),
                "inherit",
                SyntacticKind::InterfaceColon,
                Some((10, 16)),
            ),
        );
        // reuse same connection for subsequent shas
        insert_one_impl(
            &mut c,
            "o2",
            "tree-sitter-objc",
            impl_fact(
                "Dog",
                Some("P"),
                "implement",
                SyntacticKind::ProtocolList,
                Some((20, 21)),
            ),
        );
        // reuse
        insert_one_impl(
            &mut c,
            "o3",
            "tree-sitter-objc",
            impl_fact(
                "Dog",
                None,
                "extension",
                SyntacticKind::Category,
                Some((30, 40)),
            ),
        );

        assert_eq!(resolutions_for(&c, "o1")[0].1.as_deref(), Some("inherit"));
        assert_eq!(resolutions_for(&c, "o2")[0].1.as_deref(), Some("implement"));
        let r3 = resolutions_for(&c, "o3");
        assert_eq!(r3[0].1.as_deref(), Some("extension"));
        assert_eq!(r3[0].2, "tier2-direct-objc");
    }

    #[test]
    fn tier2_direct_skips_ambiguous_python_basearg() {
        // Python `class Dog(Animal):` is BaseArg — ambiguous between
        // inherit and mixin in the multi-base case; Tier-2 must not
        // write a resolution.
        let (_tmp, mut c) = fresh();
        insert_one_impl(
            &mut c,
            "py1",
            "tree-sitter-python",
            impl_fact(
                "Dog",
                Some("Animal"),
                "inherit",
                SyntacticKind::BaseArg,
                Some((10, 16)),
            ),
        );
        assert!(resolutions_for(&c, "py1").is_empty());
    }

    #[test]
    fn tier2_direct_skips_ambiguous_kotlin_csharp_swift_colon() {
        for parser in [
            "tree-sitter-kotlin",
            "tree-sitter-c-sharp",
            "tree-sitter-swift",
        ] {
            let (_tmp, mut c) = fresh();
            insert_one_impl(
                &mut c,
                "k1",
                parser,
                impl_fact(
                    "Dog",
                    Some("Animal"),
                    "inherit",
                    SyntacticKind::Colon,
                    Some((5, 11)),
                ),
            );
            assert!(
                resolutions_for(&c, "k1").is_empty(),
                "parser={parser} should not emit direct resolution for Colon"
            );
        }
    }

    #[test]
    fn tier2_direct_skips_rust_when_byte_range_missing() {
        // Rust analyzer ships `None` for interface_byte_range; the
        // mapping exists but persistence skips emission. Smoke this
        // so the "Rust direct row absent today" contract is explicit.
        let (_tmp, mut c) = fresh();
        insert_one_impl(
            &mut c,
            "rs1",
            "tree-sitter-rust",
            impl_fact("Dog", Some("Animal"), "trait", SyntacticKind::ImplFor, None),
        );
        assert!(resolutions_for(&c, "rs1").is_empty());
    }

    #[test]
    fn tier2_direct_emits_typescript_tsx_javascript() {
        let (_tmp, mut c) = fresh();
        // tsx maps to typescript source.
        insert_one_impl(
            &mut c,
            "tsx1",
            "tree-sitter-tsx",
            impl_fact(
                "Dog",
                Some("Animal"),
                "inherit",
                SyntacticKind::Extends,
                Some((5, 11)),
            ),
        );
        // reuse same connection for subsequent shas
        insert_one_impl(
            &mut c,
            "js1",
            "tree-sitter-javascript",
            impl_fact(
                "Dog",
                Some("Animal"),
                "inherit",
                SyntacticKind::Extends,
                Some((5, 11)),
            ),
        );
        assert_eq!(resolutions_for(&c, "tsx1")[0].2, "tier2-direct-typescript");
        assert_eq!(resolutions_for(&c, "js1")[0].2, "tier2-direct-javascript");
    }

    #[test]
    fn tier2_direct_emits_cpp_swift_extension() {
        let (_tmp, mut c) = fresh();
        insert_one_impl(
            &mut c,
            "cpp1",
            "tree-sitter-cpp",
            impl_fact(
                "Dog",
                Some("Animal"),
                "inherit",
                SyntacticKind::PublicBase,
                Some((5, 11)),
            ),
        );
        // reuse same connection for subsequent shas
        insert_one_impl(
            &mut c,
            "sw1",
            "tree-sitter-swift",
            impl_fact(
                "Dog",
                None,
                "extension",
                SyntacticKind::Extension,
                Some((0, 30)),
            ),
        );
        assert_eq!(resolutions_for(&c, "cpp1")[0].1.as_deref(), Some("inherit"));
        assert_eq!(resolutions_for(&c, "cpp1")[0].2, "tier2-direct-cpp");
        let sw = resolutions_for(&c, "sw1");
        assert_eq!(sw[0].1.as_deref(), Some("extension"));
        assert_eq!(sw[0].2, "tier2-direct-swift");
    }
}
