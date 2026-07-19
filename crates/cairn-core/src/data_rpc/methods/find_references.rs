//! `find_references` — either way: refs that target a symbol
//! (incoming, default) or refs inside a symbol's body (outgoing).
//! Reads the CAS `refs` table scoped by the resolved anchor.

use std::collections::HashMap;
use std::path::PathBuf;

use cairn_proto::methods::{
    FindReferenceHit, FindReferencesArgs, FindReferencesResult, ReferenceDirection,
};
use linkme::distributed_slice;
use serde_json::Value;
use tracing::debug;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::data_rpc::helpers::{
    EmissionContext, QueryArgsView, QueryToolKind, build_diagnostics, build_hints,
    completeness_for_scan, limit_with_probe, parser_id_filter, tier_status_for_query,
    with_one_or_all_stores,
};
use crate::query::{self, FindReferencesArgs as QueryArgs, ReferenceHit};
use crate::register::load_blob_or_worktree;
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

        let (hits, capped, skipped_unavailable) = with_one_or_all_stores(
            ctx,
            requested_repo,
            "find_references",
            effective_limit,
            move |entry, conn| {
                let anchor = crate::anchor::resolve_explicit_or_default(
                    conn,
                    anchor_arg.as_deref(),
                    branch_arg.as_deref(),
                )?;
                let anchor_label = anchor.as_str().to_string();
                let worktree_root = PathBuf::from(&entry.root_path);
                let hits = query::find_references(conn, &anchor, &q)?;
                let mut snippets = SnippetCache::new(worktree_root);
                Ok(hits
                    .into_iter()
                    .map(|h| {
                        let parser_id = h.parser_id.clone();
                        (
                            into_wire_hit(&entry.alias, &anchor_label, h, &mut snippets),
                            parser_id,
                        )
                    })
                    .collect())
            },
            |_out: &mut Vec<(FindReferenceHit, String)>| {},
        )
        .await?;
        let parser_ids = parser_id_filter(hits.iter().map(|(_, parser_id)| parser_id.clone()));
        let items: Vec<_> = hits.into_iter().map(|(item, _)| item).collect();
        let tier3_status = tier_status_for_query(
            ctx,
            args.scope.repo.clone(),
            args.scope.anchor.clone(),
            args.scope.branch.clone(),
            parser_ids,
            args.tier3.verbose_tier3,
            "find_references",
        )
        .await?;
        let completeness = completeness_for_scan(capped, skipped_unavailable);
        let emission_ctx = EmissionContext {
            tool: QueryToolKind::FindReferences,
            items_empty: items.is_empty(),
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                repo: args.scope.repo.as_deref(),
                fuzzy: true,
                kind: args.kind.is_some(),
                container: None,
                path: None,
                direction: args.direction != ReferenceDirection::Incoming,
                ..QueryArgsView::default()
            },
        };
        let diagnostics = build_diagnostics(&emission_ctx);
        let hints = build_hints(&emission_ctx);

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
    worktree_root: PathBuf,
    /// `blob_sha → file contents` once materialised. `None` means we
    /// already tried and the blob couldn't be loaded; we won't retry.
    blobs: HashMap<String, Option<Vec<u8>>>,
}

impl SnippetCache {
    pub(super) fn new(worktree_root: PathBuf) -> Self {
        Self {
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
            let loaded = load_blob_or_worktree(&self.worktree_root, blob_sha, path)
                .inspect_err(|e| {
                    debug!(
                        blob_sha,
                        path,
                        error = %e,
                        "snippet: blob not in git and worktree unreadable"
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
    use serde_json::json;

    use super::*;
    use crate::cas::{registry as cas_registry, store as cas_store};
    use crate::data_rpc::helpers::test_support::assert_limit_probe;
    use crate::paths::{CasDataDir, path_hash};
    use crate::register::register_repo;
    use crate::testutil::init_repo;

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
