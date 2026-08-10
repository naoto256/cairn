//! `find_references` — either way: refs that target a symbol
//! (incoming, default) or refs inside a symbol's body (outgoing).
//! Reads the CAS `refs` table scoped by the resolved anchor.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cairn_proto::Completeness;
use cairn_proto::common::PartialReason;
use cairn_proto::methods::{
    FindReferenceHit, FindReferencesArgs, FindReferencesResult, ReferenceDirection,
};
use linkme::distributed_slice;
use serde_json::Value;
use tracing::debug;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::data_rpc::helpers::{
    EmissionContext, QueryArgsView, QueryToolKind, SnapshotQueryRequest,
    build_snapshot_aware_feedback, completeness_for_snapshot_scan, limit_with_probe,
    parser_id_filter, query_one_or_all_snapshots,
};
use crate::query::{self, FindReferencesArgs as QueryArgs, ReferenceHit};
use crate::register::load_blob_or_verified_worktree;
use crate::{Error, Result};

pub struct FindReferences;

#[async_trait::async_trait]
impl DataMethod for FindReferences {
    fn name(&self) -> &'static str {
        "find_references"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: FindReferencesArgs = parse_params(params)?;
        if args.symbol.trim().is_empty() {
            return Err(Error::InvalidArgument(
                "find_references: `symbol` must be non-empty".into(),
            ));
        }

        let effective_limit = args.pagination.limit.unwrap_or(100).max(1);
        let q = QueryArgs {
            symbol: args.symbol.clone(),
            direction: args.direction,
            kind: args.kind,
            include_noise: args.include_noise,
            limit: Some(limit_with_probe(effective_limit)),
        };
        let anchor_arg = args.scope.anchor.clone();
        let branch_arg = args.scope.branch.clone();
        let requested_repo = args.scope.repo.clone();
        let any_omitted_unresolved_calls = Arc::new(AtomicBool::new(false));
        let query_omitted_unresolved_calls = Arc::clone(&any_omitted_unresolved_calls);

        let execution = query_one_or_all_snapshots(
            ctx,
            SnapshotQueryRequest {
                requested_repo,
                anchor: anchor_arg,
                branch: branch_arg,
                method_name: "find_references",
                effective_limit,
                verbose_tier3: args.tier3.verbose_tier3,
                exact_file: None,
            },
            move |entry, conn, snapshot| {
                let anchor_label = snapshot.anchor.as_str().to_string();
                let worktree_root = PathBuf::from(&entry.root_path);
                let outcome = query::find_references_with_status(conn, &snapshot.anchor, &q)?;
                let qualified_fallback = outcome.qualified_fallback;
                let omitted_unresolved_calls = outcome.omitted_unresolved_calls;
                query_omitted_unresolved_calls
                    .fetch_or(omitted_unresolved_calls, Ordering::Relaxed);
                let mut snippets = SnippetCache::new(entry.alias.clone(), worktree_root);
                Ok(outcome
                    .hits
                    .into_iter()
                    .map(|h| {
                        let parser_id = h.parser_id.clone();
                        (
                            into_wire_hit(&entry.alias, &anchor_label, h, &mut snippets),
                            parser_id,
                            qualified_fallback,
                            omitted_unresolved_calls,
                        )
                    })
                    .collect())
            },
            |hits| parser_id_filter(hits.iter().map(|(_, parser_id, _, _)| parser_id.clone())),
            |_out: &mut Vec<(FindReferenceHit, String, bool, bool)>| {},
        )
        .await?;
        let qualified_fallback = execution
            .items
            .iter()
            .any(|(_, _, qualified_fallback, _)| *qualified_fallback);
        // Evidence follows surviving rows across cross-repo final trimming.
        // An unresolved-only result has no row to carry provenance, so retain
        // the aggregate bit only when the final result itself is empty.
        let omitted_unresolved_calls = execution.items.iter().any(|(_, _, _, omitted)| *omitted)
            || (execution.items.is_empty() && any_omitted_unresolved_calls.load(Ordering::Relaxed));
        let items: Vec<_> = execution
            .items
            .into_iter()
            .map(|(item, _, _, _)| item)
            .collect();
        let tier3_status = execution.tier3_status;
        let freshness_issues = execution.freshness_issues;
        let completeness = call_graph_completeness(
            qualified_reference_completeness(
                completeness_for_snapshot_scan(
                    execution.capped,
                    execution.skipped_unavailable,
                    &freshness_issues,
                ),
                qualified_fallback,
            ),
            omitted_unresolved_calls,
        );
        let emission_ctx = EmissionContext {
            tool: QueryToolKind::FindReferences,
            items_empty: items.is_empty(),
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                repo: args.scope.repo.as_deref(),
                // `fuzzy: true` suppresses the try-fuzzy empty-result
                // hint: `find_references` matches by symbol name
                // without a fuzzy toggle to flip. `direction` is
                // reported true only when the caller picked a
                // non-`Incoming` direction, which lets the shared
                // relax-filter hint suggest dropping it.
                fuzzy: true,
                kind: args.kind.is_some(),
                container: None,
                path: None,
                direction: args.direction != ReferenceDirection::Incoming,
                ..QueryArgsView::default()
            },
        };
        let (diagnostics, hints) =
            build_snapshot_aware_feedback(&emission_ctx, &freshness_issues, execution.capped);

        Ok(serde_json::to_value(FindReferencesResult {
            items,
            completeness,
            tier3_status,
            diagnostics,
            hints,
            timing: cairn_proto::Timing::default(),
        })
        .unwrap())
    }
}

pub(super) fn qualified_reference_completeness(
    completeness: Completeness,
    qualified_fallback: bool,
) -> Completeness {
    if !qualified_fallback {
        return completeness;
    }

    // Snapshot freshness and unavailable-repository evidence remain stronger
    // than query-local uncertainty. A cap is weaker: the returned rows came
    // from a bare-name fallback, so callers must not mistake them for exact
    // qualified matches. The independent cap hint still reports truncation.
    if matches!(completeness, Completeness::Complete)
        || matches!(
            completeness,
            Completeness::Partial {
                reason: Some(PartialReason::Cap),
                ..
            }
        )
    {
        Completeness::partial_semantic("qualified_fallback")
    } else {
        completeness
    }
}

pub(super) fn call_graph_completeness(
    completeness: Completeness,
    omitted_unresolved_calls: bool,
) -> Completeness {
    if !omitted_unresolved_calls {
        return completeness;
    }

    // Snapshot freshness and unavailable-repository evidence outrank local
    // semantic uncertainty. An actual omitted call outranks a cap; the cap
    // remains independently visible through its exactly-once hint.
    if matches!(completeness, Completeness::Complete)
        || matches!(
            completeness,
            Completeness::Partial {
                reason: Some(PartialReason::Cap),
                ..
            }
        )
    {
        Completeness::partial_semantic("call_graph_unresolved")
    } else {
        completeness
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(FindReferences);

fn into_wire_hit(
    repo: &str,
    anchor: &str,
    h: ReferenceHit,
    snippets: &mut SnippetCache,
) -> FindReferenceHit {
    let location = format!("{repo}:{anchor}:{}:{}", h.path, h.line);
    let snippet = snippets.line_for(&h.blob_sha, &h.path, h.line);
    FindReferenceHit {
        target_name: h.target_name,
        target_qualified: h.target_qualified,
        kind: h.kind,
        kind_source: h.kind_source,
        target_path: h.target_path,
        enclosing_qualified: h.enclosing_qualified,
        branch: anchor.to_string(),
        location,
        snippet,
    }
}

/// Lazily reads each touched blob at most once. Most `find_references`
/// result sets cluster hits onto a small number of files (one impl
/// block, one trait method), so the cache turns N hits into K blob
/// reads with K ≪ N.
pub(super) struct SnippetCache {
    repo: String,
    worktree_root: PathBuf,
    /// `blob_sha → file contents` once materialised. `None` means we
    /// already tried and the blob couldn't be loaded; we won't retry.
    blobs: HashMap<String, Option<Vec<u8>>>,
}

impl SnippetCache {
    pub(super) fn new(repo: String, worktree_root: PathBuf) -> Self {
        Self {
            repo,
            worktree_root,
            blobs: HashMap::new(),
        }
    }

    pub(super) fn line_for(&mut self, blob_sha: &str, path: &str, line: u32) -> Option<String> {
        let bytes = self.load(blob_sha, path);
        bytes
            .and_then(|b| extract_line(b, line))
            .map(|s| s.trim_end_matches(['\r', '\n']).to_string())
    }

    fn load(&mut self, blob_sha: &str, path: &str) -> Option<&[u8]> {
        if !self.blobs.contains_key(blob_sha) {
            let loaded = load_blob_or_verified_worktree(&self.worktree_root, blob_sha, path)
                .inspect_err(|_| {
                    debug!(
                        repo = self.repo,
                        path, "snippet omitted because indexed bytes are unavailable or changed"
                    );
                })
                .ok();
            self.blobs.insert(blob_sha.to_string(), loaded);
        }
        self.blobs.get(blob_sha).and_then(Option::as_deref)
    }
}

/// Returns the requested 1-indexed line as a UTF-8 lossy slice.
/// `None` when the file is shorter than the requested line.
fn extract_line(bytes: &[u8], line: u32) -> Option<String> {
    if line == 0 {
        return None;
    }
    let target = line as usize - 1;
    let mut current = 0;
    let mut start = 0;
    for (idx, &b) in bytes.iter().enumerate() {
        if current == target {
            // walk to the end of this line
            let end = bytes[idx..]
                .iter()
                .position(|&c| c == b'\n')
                .map_or(bytes.len(), |n| idx + n);
            return Some(String::from_utf8_lossy(&bytes[start..end]).into_owned());
        }
        if b == b'\n' {
            current += 1;
            start = idx + 1;
        }
    }
    if current == target && start <= bytes.len() {
        return Some(String::from_utf8_lossy(&bytes[start..]).into_owned());
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use cairn_proto::Completeness;
    use cairn_proto::common::PartialReason;
    use serde_json::json;

    use super::*;
    use crate::cas::{registry as cas_registry, store as cas_store};
    use crate::data_rpc::helpers::test_support::assert_limit_probe;
    use crate::data_rpc::methods::find_callees::FindCallees;
    use crate::data_rpc::methods::find_callers::FindCallers;
    use crate::paths::{CasDataDir, path_hash};
    use crate::register::register_repo;
    use crate::testutil::init_repo;

    #[test]
    fn qualified_fallback_only_replaces_complete_or_cap_reason() {
        assert_eq!(
            qualified_reference_completeness(Completeness::Complete, true),
            Completeness::partial_semantic("qualified_fallback")
        );
        assert_eq!(
            qualified_reference_completeness(
                Completeness::partial_truncated(PartialReason::Cap),
                true,
            ),
            Completeness::partial_semantic("qualified_fallback")
        );

        let freshness = Completeness::partial_truncated("file_not_indexed_or_snapshot_stale");
        assert_eq!(
            qualified_reference_completeness(freshness.clone(), true),
            freshness
        );
        let tier = Completeness::partial_semantic(PartialReason::Tier2Warming);
        assert_eq!(qualified_reference_completeness(tier.clone(), true), tier);
        assert_eq!(
            qualified_reference_completeness(Completeness::Complete, false),
            Completeness::Complete
        );
    }

    #[test]
    fn unresolved_call_only_replaces_complete_or_cap_reason() {
        assert_eq!(
            call_graph_completeness(Completeness::Complete, true),
            Completeness::partial_semantic("call_graph_unresolved")
        );
        assert_eq!(
            call_graph_completeness(Completeness::partial_truncated(PartialReason::Cap), true),
            Completeness::partial_semantic("call_graph_unresolved")
        );

        let freshness = Completeness::partial_truncated("file_not_indexed_or_snapshot_stale");
        assert_eq!(call_graph_completeness(freshness.clone(), true), freshness);
        let tier = Completeness::partial_semantic(PartialReason::Tier2Warming);
        assert_eq!(call_graph_completeness(tier.clone(), true), tier);
        assert_eq!(
            call_graph_completeness(Completeness::Complete, false),
            Completeness::Complete
        );
    }

    #[tokio::test]
    async fn exact_limit_is_complete_and_over_limit_is_partial() {
        assert_limit_probe(
            &FindReferences,
            json!({"repo": "demo", "symbol": "target", "kind": "call", "include_noise": true, "limit": 3}),
            json!({"repo": "demo", "symbol": "target", "kind": "call", "include_noise": true, "limit": 2}),
        )
        .await;
    }

    #[tokio::test]
    async fn repo_none_searches_all_registered_repos_and_caps_accumulated_total() {
        let fixture = cross_repo_fixture();

        let all = FindReferences
            .dispatch(
                &fixture.ctx,
                json!({"symbol": "target", "kind": "call", "include_noise": true, "limit": 10, "anchor": "HEAD"}),
            )
            .await
            .unwrap();
        let items = all["items"].as_array().unwrap();
        assert_eq!(items.len(), 3);
        assert!(
            items
                .iter()
                .any(|h| h["location"].as_str().unwrap().starts_with("alpha:HEAD:"))
        );
        assert!(
            items
                .iter()
                .any(|h| h["location"].as_str().unwrap().starts_with("beta:HEAD:"))
        );
        assert_eq!(
            serde_json::from_value::<Completeness>(all["completeness"].clone()).unwrap(),
            Completeness::Complete
        );

        let capped = FindReferences
            .dispatch(
                &fixture.ctx,
                json!({"symbol": "target", "kind": "call", "include_noise": true, "limit": 2, "anchor": "HEAD"}),
            )
            .await
            .unwrap();
        assert_eq!(capped["items"].as_array().unwrap().len(), 2);
        assert_eq!(
            serde_json::from_value::<Completeness>(capped["completeness"].clone()).unwrap(),
            Completeness::partial_truncated("cap")
        );
    }

    #[tokio::test]
    async fn capped_qualified_fallback_keeps_one_cap_hint() {
        let fixture = cross_repo_fixture();
        let capped = FindReferences
            .dispatch(
                &fixture.ctx,
                json!({
                    "symbol": "Unrelated::target",
                    "kind": "call",
                    "include_noise": true,
                    "limit": 2,
                    "anchor": "HEAD"
                }),
            )
            .await
            .unwrap();

        assert_eq!(capped["items"].as_array().unwrap().len(), 2);
        assert_eq!(
            serde_json::from_value::<Completeness>(capped["completeness"].clone()).unwrap(),
            Completeness::partial_semantic("qualified_fallback")
        );
        assert_eq!(
            capped["hints"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|hint| hint["code"] == "capped_increase_limit")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn final_trim_drops_fallback_status_with_the_fallback_row() {
        let fixture = qualified_final_trim_fixture();

        let references = FindReferences
            .dispatch(
                &fixture.ctx,
                json!({
                    "symbol": "Exact.target",
                    "kind": "call",
                    "limit": 1,
                    "anchor": "HEAD"
                }),
            )
            .await
            .unwrap();
        assert_exact_survivor_with_cap(&references);

        let callers = FindCallers
            .dispatch(
                &fixture.ctx,
                json!({
                    "name": "Exact.target",
                    "limit": 1,
                    "anchor": "HEAD"
                }),
            )
            .await
            .unwrap();
        assert_exact_survivor_with_cap(&callers);
    }

    #[tokio::test]
    async fn final_trim_drops_unresolved_status_with_the_unresolved_repo() {
        let fixture = call_graph_final_trim_fixture();

        let references = FindReferences
            .dispatch(
                &fixture.ctx,
                json!({
                    "symbol": "caller",
                    "direction": "outgoing",
                    "kind": "call",
                    "limit": 1,
                    "anchor": "HEAD"
                }),
            )
            .await
            .unwrap();
        assert_resolved_survivor_with_cap(&references);

        let callees = FindCallees
            .dispatch(
                &fixture.ctx,
                json!({"name": "caller", "limit": 1, "anchor": "HEAD"}),
            )
            .await
            .unwrap();
        assert_resolved_survivor_with_cap(&callees);
    }

    #[test]
    fn extract_line_returns_target_line_text() {
        let src = b"alpha\nbeta\ngamma\n";
        assert_eq!(extract_line(src, 1).as_deref(), Some("alpha"));
        assert_eq!(extract_line(src, 2).as_deref(), Some("beta"));
        assert_eq!(extract_line(src, 3).as_deref(), Some("gamma"));
    }

    #[test]
    fn extract_line_handles_trailing_no_newline() {
        let src = b"alpha\nbeta";
        assert_eq!(extract_line(src, 2).as_deref(), Some("beta"));
    }

    #[test]
    fn extract_line_returns_none_past_end() {
        let src = b"alpha\nbeta\n";
        assert_eq!(extract_line(src, 5), None);
        assert_eq!(extract_line(src, 0), None);
    }

    #[test]
    fn snippet_cache_omits_changed_worktree_fallback_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("source.rs"), b"changed\n").unwrap();
        let indexed_sha = crate::cas::hash::git_blob_sha(b"indexed\n");
        let mut cache = SnippetCache::new("demo".into(), tmp.path().to_path_buf());

        assert_eq!(cache.line_for(&indexed_sha, "source.rs", 1), None);
    }

    struct CrossRepoFixture {
        _repos: Vec<tempfile::TempDir>,
        _data: tempfile::TempDir,
        ctx: DataCtx,
    }

    fn cross_repo_fixture() -> CrossRepoFixture {
        let alpha = init_repo(&[(
            "src/alpha.rs",
            "pub fn target() {}\n\
             pub fn caller_a() { target(); }\n\
             pub fn caller_b() { target(); }\n",
        )])
        .0;
        let beta = init_repo(&[(
            "src/beta.rs",
            "pub fn target() {}\n\
             pub fn caller_c() { target(); }\n",
        )])
        .0;
        let data = tempfile::tempdir().unwrap();
        let cas = CasDataDir::with_root(data.path().to_path_buf());
        cas.ensure().unwrap();
        register_alias(&cas, "alpha", alpha.path());
        register_alias(&cas, "beta", beta.path());

        CrossRepoFixture {
            _repos: vec![alpha, beta],
            _data: data,
            ctx: DataCtx {
                cas_data_dir: Arc::new(cas),
                lifecycle: None,
            },
        }
    }

    fn qualified_final_trim_fixture() -> CrossRepoFixture {
        let alpha = init_repo(&[(
            "src/alpha.rs",
            "pub fn target() {}\npub fn alpha_caller() { target(); }\n",
        )])
        .0;
        let beta = init_repo(&[(
            "src/beta.rs",
            "pub fn target() {}\npub fn beta_caller() { target(); }\n",
        )])
        .0;
        let data = tempfile::tempdir().unwrap();
        let cas = CasDataDir::with_root(data.path().to_path_buf());
        cas.ensure().unwrap();
        register_alias(&cas, "alpha", alpha.path());
        register_alias(&cas, "beta", beta.path());
        set_target_qualified(&cas, alpha.path(), Some("Exact.target"));
        set_target_qualified(&cas, beta.path(), None);

        CrossRepoFixture {
            _repos: vec![alpha, beta],
            _data: data,
            ctx: DataCtx {
                cas_data_dir: Arc::new(cas),
                lifecycle: None,
            },
        }
    }

    fn call_graph_final_trim_fixture() -> CrossRepoFixture {
        let alpha = init_repo(&[(
            "src/alpha.rs",
            "pub fn first() {}\n\
             pub fn second() {}\n\
             pub fn caller() { first(); second(); }\n",
        )])
        .0;
        let beta = init_repo(&[(
            "src/beta.rs",
            "pub struct Widget;\n\
             impl Widget { pub fn render(&self) {} }\n\
             pub fn caller(arg: Widget) { arg.render(); }\n",
        )])
        .0;
        let data = tempfile::tempdir().unwrap();
        let cas = CasDataDir::with_root(data.path().to_path_buf());
        cas.ensure().unwrap();
        register_alias(&cas, "alpha", alpha.path());
        register_alias(&cas, "beta", beta.path());

        CrossRepoFixture {
            _repos: vec![alpha, beta],
            _data: data,
            ctx: DataCtx {
                cas_data_dir: Arc::new(cas),
                lifecycle: None,
            },
        }
    }

    fn set_target_qualified(cas: &CasDataDir, repo_path: &std::path::Path, value: Option<&str>) {
        let canonical = std::fs::canonicalize(repo_path).unwrap();
        let repo_hash = path_hash(&canonical);
        let conn = cas_store::open(&cas.store_db_path(&repo_hash)).unwrap();
        let changed = conn
            .execute(
                "UPDATE refs SET target_qualified = ?1
                  WHERE target_name = 'target' AND kind = 'call'",
                rusqlite::params![value],
            )
            .unwrap();
        assert_eq!(changed, 1, "fixture must contain exactly one target call");
    }

    fn assert_exact_survivor_with_cap(value: &Value) {
        let items = value["items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "{value:#}");
        assert!(
            items[0]["location"]
                .as_str()
                .unwrap()
                .starts_with("alpha:HEAD:"),
            "global final trim must retain the deterministic alpha exact row: {value:#}"
        );
        assert_eq!(items[0]["target_qualified"], "Exact.target");
        assert_eq!(
            serde_json::from_value::<Completeness>(value["completeness"].clone()).unwrap(),
            Completeness::partial_truncated(PartialReason::Cap)
        );
        let cap_hints = value["hints"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|hint| hint["code"] == "capped_increase_limit")
            .collect::<Vec<_>>();
        assert_eq!(cap_hints.len(), 1, "{value:#}");
        assert_eq!(cap_hints[0]["action"], "increase_limit");
    }

    fn assert_resolved_survivor_with_cap(value: &Value) {
        let items = value["items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "{value:#}");
        assert!(
            items[0]["location"]
                .as_str()
                .unwrap()
                .starts_with("alpha:HEAD:"),
            "global final trim must retain a deterministic resolved row: {value:#}"
        );
        assert_eq!(
            serde_json::from_value::<Completeness>(value["completeness"].clone()).unwrap(),
            Completeness::partial_truncated(PartialReason::Cap)
        );
        assert_eq!(
            value["hints"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|hint| hint["code"] == "capped_increase_limit")
                .count(),
            1,
            "{value:#}"
        );
    }

    fn register_alias(cas: &CasDataDir, alias: &str, repo_path: &std::path::Path) {
        let canonical = std::fs::canonicalize(repo_path).unwrap();
        let repo_hash = path_hash(&canonical);
        let store_path = cas.store_db_path(&repo_hash);
        let mut store = cas_store::open(&store_path).unwrap();
        let now_ns = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        )
        .unwrap_or(i64::MAX);
        register_repo(&mut store, &canonical, now_ns).unwrap();

        let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::upsert(&tx, alias, &canonical.to_string_lossy(), &repo_hash, now_ns).unwrap();
        tx.commit().unwrap();
    }
}
