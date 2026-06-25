use std::path::Path;

use cairn_proto::RefKind;
use rusqlite::{Connection, OptionalExtension, params};

use crate::Result;
use crate::cas::kind_conv::ref_kind_to_str;
use crate::lsp::Location;
use crate::manifest::ManifestId;

use super::{ResolutionKind, WorkspaceFacts, WorkspaceResolution};

/// 0.1.x persisted rust-analyzer refs under this alias instead of the
/// uniform `<tier_prefix>-<analyzer_id>` scheme. Cleared alongside the
/// current source so reindexing does not leave duplicate rows behind.
/// Remove this compatibility path after the 0.5.0 migration window closes.
const LEGACY_RUST_REF_SOURCE: &str = "tier3-rust-analyzer";

pub(super) fn persist_resolved_refs(
    conn: &mut Connection,
    manifest_id: ManifestId,
    analyzer_id: &str,
    tier_prefix: &str,
    parser_id: &str,
    facts: &WorkspaceFacts,
) -> Result<usize> {
    let tx = conn.transaction()?;
    for source in ref_sources_to_clear(analyzer_id, tier_prefix) {
        tx.execute(
            "DELETE FROM refs
              WHERE source = ?1
                AND blob_sha IN (
                    SELECT blob_sha FROM manifest_entries WHERE manifest_id = ?2
                )",
            params![source, manifest_id.0],
        )?;
    }

    let mut inserted = 0;
    for r in &facts.resolved_refs {
        let Some(source_blob) = blob_for_path(&tx, manifest_id, &r.source_path)? else {
            continue;
        };
        let Some(parser_id) = parser_for_blob(&tx, &source_blob, parser_id)? else {
            continue;
        };
        // A `None` target_path means the definition resolved outside
        // the repo root (dependency or stdlib); there is no manifest
        // row to attach it to, so the ref is skipped.
        let Some(target_path) = r.target_path.as_deref() else {
            continue;
        };
        let target =
            target_symbol_for_location(&tx, manifest_id, &parser_id, target_path, &r.target)?;
        // Import refs (e.g. C/C++/ObjC `#include`) commonly resolve to a
        // file location that sits outside any symbol's byte range — the
        // top of the header itself, before any declaration. Falling
        // through to `continue` would drop those edges entirely; instead
        // synthesize a header-level target from the file path so
        // `find_imports` and `get_outline` still surface the edge. Other
        // ref kinds (Call, Type, ...) keep the original "no symbol →
        // skip" behaviour because a callee with no enclosing definition
        // is genuinely unresolved.
        let (target_qualified, target_name) = match target {
            Some(target) => target,
            None if r.kind == RefKind::Import => {
                let name = Path::new(target_path)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or(target_path)
                    .to_string();
                (target_path.to_string(), name)
            }
            None => continue,
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
                ref_kind_to_str(r.kind),
                byte_start,
                byte_end,
                line,
                ref_source(tier_prefix, analyzer_id),
            ],
        )?;
        inserted += 1;
    }

    tx.commit()?;
    Ok(inserted)
}

fn ref_source(tier_prefix: &str, analyzer_id: &str) -> String {
    format!("{tier_prefix}-{analyzer_id}")
}

fn ref_sources_to_clear(analyzer_id: &str, tier_prefix: &str) -> Vec<String> {
    let mut sources = vec![ref_source(tier_prefix, analyzer_id)];
    if analyzer_id == "rust-analyzer-lsp" {
        sources.push(LEGACY_RUST_REF_SOURCE.to_string());
    }
    sources
}

/// Three-way classification of the analyzer-emitted target_path after
/// the manifest-existence check. The `PhantomDropped` arm is what
/// guarantees an analyzer bug never escapes via the qualified-only
/// symbol fallback below: we strip the path but block the fallback
/// from inventing a different one.
enum PathOrigin {
    /// Analyzer did not emit a path (e.g., cross-parser type/call
    /// where the resolver could not identify the file).
    None,
    /// Analyzer emitted a path and it exists in the manifest. The
    /// String is the same path, ready to land in the row.
    Valid(String),
    /// Analyzer emitted a path that is not in the manifest. The path
    /// is dropped to NULL on the row and the qualified-only fallback
    /// is skipped so a coincidentally-matching sibling symbol cannot
    /// hide the analyzer bug.
    PhantomDropped,
}

/// Persist resolution-layer rows emitted by a workspace analyzer.
///
/// Mirrors [`persist_resolved_refs`]'s delete-then-insert pattern: any
/// existing rows whose `source` matches `<tier_prefix>-<analyzer_id>` and
/// whose blob belongs to this manifest are cleared first, then the new rows
/// are inserted.
///
/// Two target axes are persisted independently (v10+):
///
/// - `target_path` is the source of truth for "which workspace file" and is
///   sanitized against `manifest_entries` here. Analyzer bugs that emit a
///   phantom path get a `debug!` log and the column drops to NULL — the row
///   itself is preserved so the site-presence signal is not lost, and the
///   qualified-only symbol fallback is **skipped** so the bug cannot hide
///   behind a coincidentally-matching sibling symbol.
/// - `target_symbol_id` is the source of truth for "which symbol" and is
///   resolved best-effort by `resolve_resolution_target`. Failure to find a
///   matching symbol does not affect `target_path` persistence.
pub(super) fn persist_resolutions(
    conn: &mut Connection,
    manifest_id: ManifestId,
    analyzer_id: &str,
    tier_prefix: &str,
    parser_id: &str,
    facts: &WorkspaceFacts,
) -> Result<usize> {
    let source = format!("{tier_prefix}-{analyzer_id}");
    let tx = conn.transaction()?;
    tx.execute(
        "DELETE FROM resolutions
          WHERE source = ?1
            AND site_blob_sha IN (
                SELECT blob_sha FROM manifest_entries WHERE manifest_id = ?2
            )",
        params![source, manifest_id.0],
    )?;

    let mut inserted = 0;
    for r in &facts.resolutions {
        let Some(site_blob) = blob_for_path(&tx, manifest_id, &r.source_path)? else {
            continue;
        };
        let Some(site_parser) = parser_for_blob(&tx, &site_blob, parser_id)? else {
            continue;
        };

        // Sanitize `target_path` against the current manifest. Import
        // edges and cross-parser type/call edges set
        // `target_qualified = None`, so they bypass
        // `resolve_resolution_target`'s path check; this loop is the
        // only place that guarantees no phantom path reaches the wire
        // surface. Three states tracked:
        //
        //   PathOrigin::None          — analyzer did not emit a path
        //   PathOrigin::Valid(path)   — analyzer emitted, exists in
        //                                manifest
        //   PathOrigin::PhantomDropped — analyzer emitted, but path
        //                                did NOT exist; dropped here
        //
        // Distinguishing PhantomDropped from None matters for the
        // qualified-only fallback below: phantom-path resolutions must
        // never be silently re-pointed to a different file via the
        // qualified-only path. They stay `target_path NULL` /
        // `target_symbol_id NULL` so analyzer bugs surface rather than
        // hide behind a coincidentally-matching sibling symbol.
        let path_origin = match r.target_path.as_deref() {
            None => PathOrigin::None,
            Some(path) => match blob_for_path(&tx, manifest_id, path)? {
                Some(_) => PathOrigin::Valid(path.to_string()),
                None => {
                    tracing::debug!(
                        target: "cairn_core::persist",
                        source = %source,
                        source_path = %r.source_path,
                        site_byte_start = r.site_byte_range.start,
                        site_byte_end = r.site_byte_range.end,
                        target_path = %path,
                        "persist_resolutions: target_path not in manifest, dropping to NULL"
                    );
                    PathOrigin::PhantomDropped
                }
            },
        };

        // PathOrigin determines whether the symbol lookup is allowed
        // to run at all:
        //   Valid    → path-scoped lookup, then cross-parser fallback
        //   None     → manifest-wide qualified-only fallback (cross-
        //              parser type/call where the resolver could not
        //              identify the target file itself)
        //   Phantom  → no lookup; the row keeps target_symbol_id NULL
        //              so the analyzer-bug signal is preserved
        let (mut sanitized_target_path, mut target_symbol_id) = match &path_origin {
            PathOrigin::PhantomDropped => (None, None),
            PathOrigin::Valid(path) => {
                let id = resolve_resolution_target(&tx, manifest_id, parser_id, r, Some(path))?;
                (Some(path.clone()), id)
            }
            PathOrigin::None => {
                let id = resolve_resolution_target(&tx, manifest_id, parser_id, r, None)?;
                (None, id)
            }
        };

        // Symbol-id-derived target_path: only legal when the analyzer
        // itself did not emit a path AND a qualified-only fallback
        // adopted a unique sibling-parser symbol. Never run for the
        // PhantomDropped case (above) — that path is preserved as
        // analyzer-bug signal — and never overwrite an
        // analyzer-emitted Valid path.
        if matches!(path_origin, PathOrigin::None) && sanitized_target_path.is_none() {
            if let Some(id) = target_symbol_id {
                match path_for_symbol_id(&tx, manifest_id, id)? {
                    Some(derived) => sanitized_target_path = Some(derived),
                    None => {
                        // 3-state invariant: if we can't recover the
                        // file the resolved symbol lives in, drop the
                        // symbol id too so the row stays in one of the
                        // documented `(target_path, target_symbol_id)`
                        // shapes: `(Some, Some)`, `(Some, None)`, or
                        // `(None, None)`. The fourth combination
                        // `(None, Some)` would be a documented-but-
                        // unsurfaced inconsistency — the wire layer
                        // exposes `target_path` directly and there is
                        // no consumer today that walks
                        // `target_symbol_id` back to a path, so a row
                        // with only the symbol id would carry
                        // `kind_source = tier25-…` without any
                        // user-visible workspace target, which reads
                        // as "resolved but invisible". `warn!` so the
                        // race (manifest GC vs persist tx) shows up
                        // in operator logs even though it does not
                        // break correctness.
                        tracing::warn!(
                            target: "cairn_core::persist",
                            source = %source,
                            source_path = %r.source_path,
                            site_byte_start = r.site_byte_range.start,
                            site_byte_end = r.site_byte_range.end,
                            target_symbol_id = id,
                            "persist_resolutions: path_for_symbol_id returned None; \
                             dropping target_symbol_id to preserve 3-state invariant"
                        );
                        target_symbol_id = None;
                    }
                }
            }
        }
        tx.execute(
            "INSERT INTO resolutions
               (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                kind, semantic_kind, target_symbol_id, target_path, source)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                site_blob,
                site_parser,
                i64::from(r.site_byte_range.start),
                i64::from(r.site_byte_range.end),
                r.kind.as_str(),
                r.semantic_kind.map(|s| s.as_str()),
                target_symbol_id,
                sanitized_target_path,
                source,
            ],
        )?;
        inserted += 1;
    }

    tx.commit()?;
    Ok(inserted)
}

/// Best-effort symbol-id lookup. Returns `None` for import edges (which set
/// `target_qualified = None` by contract) and for any other resolution
/// whose `(blob_sha, qualified)` pair cannot be matched to a row in the
/// `symbols` table.
///
/// `target_path_hint` reflects the **sanitized** path the caller has
/// already validated against the manifest: `Some(path)` means "the
/// analyzer emitted this path and it exists in the manifest"; `None`
/// means either the analyzer did not emit one **or** it emitted a
/// phantom that the caller dropped (`PathOrigin::PhantomDropped`). The
/// caller therefore must call this function with `None` for the
/// phantom case so the manifest-wide qualified-only fallback below is
/// only allowed for genuine `target_path = None` analyzers (cross-
/// parser type/call) and never re-points a phantom path to a different
/// file. Two pathways are tried in order:
///
///   1. Path-scoped: same-parser exact match, then path-scoped cross-
///      parser uniqueness fallback (multiple hits → None).
///   2. Manifest-wide: only when `target_path_hint == None`, restricted
///      to symbols whose blob appears in this manifest, with the same
///      uniqueness check.
///
/// Today the cross-parser fallbacks primarily help cross-language
/// hierarchies (Kotlin extending a Java class, Swift importing an
/// Objective-C declaration). The manifest-wide step (#2) covers
/// resolvers that emit `target_qualified` but cannot identify the
/// file themselves — for example a future Python or Swift resolver
/// that walks `import x.y.Z` to an FQN without scanning sibling
/// backend files, or any qualified-name-first cross-language
/// resolution (Kotlin → Java is handled by step #1 today because
/// PR #213 made Kotlin symbols carry FQNs, but the same Kotlin
/// resolver hitting a generated-source file with no `target_path`
/// would land in step #2).
/// Future risk: a blob indexed by multiple
/// parsers (TS/JS overlap, generated files) — the uniqueness check
/// catches it.
fn resolve_resolution_target(
    conn: &Connection,
    manifest_id: ManifestId,
    parser_id: &str,
    r: &WorkspaceResolution,
    target_path_hint: Option<&str>,
) -> Result<Option<i64>> {
    let Some(qualified) = r.target_qualified.as_deref() else {
        return Ok(None);
    };

    // Path-scoped lookup is the fast path when the resolver already knew
    // which workspace file holds the target. Failing that we fall through
    // to qualified-only (manifest-wide) lookups so we can still pin the
    // symbol id for cross-parser cases where the resolver does not index
    // the sibling backend's files (e.g. Kotlin → Java).
    let target_blob_hint = match target_path_hint {
        Some(path) => blob_for_path(conn, manifest_id, path)?,
        None => None,
    };

    if let Some(target_blob) = target_blob_hint.as_deref() {
        if let Some(id) = conn
            .query_row(
                "SELECT id FROM symbols
                 WHERE blob_sha = ?1
                   AND parser_id = ?2
                   AND qualified = ?3
                 ORDER BY (byte_end - byte_start) ASC
                 LIMIT 1",
                params![target_blob, parser_id, qualified],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
        {
            return Ok(Some(id));
        }
        let mut stmt = conn.prepare(
            "SELECT id, parser_id FROM symbols
             WHERE blob_sha = ?1 AND qualified = ?2
             LIMIT 2",
        )?;
        let candidates: Vec<(i64, String)> = stmt
            .query_map(params![target_blob, qualified], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        match candidates.as_slice() {
            [(id, _)] => return Ok(Some(*id)),
            [] => {}
            many => {
                tracing::debug!(
                    target: "cairn_core::persist",
                    source_parser_id = parser_id,
                    target_qualified = %qualified,
                    target_path = r.target_path.as_deref().unwrap_or(""),
                    candidate_count = many.len(),
                    "resolve_resolution_target: cross-parser fallback ambiguous (path-scoped), returning None"
                );
                return Ok(None);
            }
        }
    }

    // Manifest-wide qualified-only lookup: covers cross-parser type/call
    // resolution where the resolver returned `(target_path = None,
    // target_qualified = Some)`. Only adopts unique matches restricted
    // to blobs that appear in this manifest, mirroring the strictness
    // of the path-scoped uniqueness check above.
    //
    // Gated on `target_path_hint == None`: this branch is *not* allowed
    // when the analyzer emitted a target_path that the caller dropped
    // (PathOrigin::PhantomDropped), so a phantom path cannot be
    // silently re-pointed to a different file by a coincidentally-
    // matching sibling-parser symbol. Phantom-path analyzer bugs
    // should surface as `target_symbol_id = NULL` /
    // `target_path = NULL`, not get hidden behind an unrelated symbol.
    if target_path_hint.is_some() {
        return Ok(None);
    }
    // Also gated on `kind != Import`: imports target a *file*, not a
    // symbol. Some backends (Kotlin / Swift / C# today, PHP / Python
    // in some shapes) emit `target_qualified = Some(b.fqn)` for
    // external imports they cannot resolve to a workspace file. If
    // that bare FQN happens to match a unique workspace symbol via
    // manifest-wide lookup, adopting it would silently re-point the
    // import edge to whatever file holds that symbol — turning a
    // "no single target file" import semantic into "specific symbol's
    // file". The caller (`persist_resolutions`) then back-derives
    // `target_path` from the adopted symbol id, completing the
    // semantic break. Gate at this branch so the manifest-wide rescue
    // only applies to type / call edges where the symbol is the
    // primary target identity.
    if matches!(r.kind, ResolutionKind::Import) {
        return Ok(None);
    }
    let mut stmt = conn.prepare(
        "SELECT s.id, s.parser_id FROM symbols s
         WHERE s.qualified = ?1
           AND EXISTS (
               SELECT 1 FROM manifest_entries me
                WHERE me.manifest_id = ?2
                  AND me.blob_sha = s.blob_sha
           )
         LIMIT 2",
    )?;
    let candidates: Vec<(i64, String)> = stmt
        .query_map(params![qualified, manifest_id.0], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    match candidates.as_slice() {
        [(id, _)] => Ok(Some(*id)),
        [] => Ok(None),
        many => {
            tracing::debug!(
                target: "cairn_core::persist",
                source_parser_id = parser_id,
                target_qualified = %qualified,
                candidate_count = many.len(),
                "resolve_resolution_target: qualified-only fallback ambiguous, returning None"
            );
            Ok(None)
        }
    }
}

/// Resolve a symbols row id back to its workspace file path in this
/// manifest. Returns `None` if the symbol's blob is not in the manifest
/// (which would normally not happen since `resolve_resolution_target`
/// already filters by manifest membership, but guards against drift).
fn path_for_symbol_id(
    conn: &Connection,
    manifest_id: ManifestId,
    symbol_id: i64,
) -> Result<Option<String>> {
    // `ORDER BY me.path` so the choice is deterministic when the same
    // blob is referenced from multiple paths in the manifest (rare,
    // but the manifest schema allows it). Lexicographic order is
    // arbitrary but stable across runs.
    Ok(conn
        .query_row(
            "SELECT me.path FROM symbols s
             JOIN manifest_entries me
               ON me.manifest_id = ?1
              AND me.blob_sha = s.blob_sha
             WHERE s.id = ?2
             ORDER BY me.path
             LIMIT 1",
            params![manifest_id.0, symbol_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?)
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
    target_path: &str,
    location: &Location,
) -> Result<Option<(String, String)>> {
    let Some(blob_sha) = blob_for_path(conn, manifest_id, target_path)? else {
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
