//! Daemon data RPC — plain JSON-RPC 2.0 over `cairn.sock`.
//!
//! This is the kernel API. Out-of-tree consumers (cairn's MCP front-end,
//! a future LSP front-end, cairn-graph, cairn-audit, IDE plugins) talk
//! to the daemon through this surface; no protocol-specific wrapping.
//!
//! Each method lives in its own module under [`methods`] and registers
//! itself into the [`DATA_METHODS`] distributed slice. Adding a new
//! method is a one-file change: write a `struct Foo; impl DataMethod
//! for Foo` and a `#[distributed_slice]` entry, and the dispatcher
//! picks it up automatically. The cross-cutting amenities the methods
//! share — a snapshot-target resolver for cross-branch queries and
//! the JSON-RPC envelope helpers — live here on [`DataCtx`] / in this
//! module.
//!
//! Admin verbs (`register_repo`, `reindex_repo`, `status`, `doctor`,
//! `shutdown`) live on a separate control socket so the data plane
//! stays read-only by construction. The MCP front-end translates
//! `register_repo` / `reindex_repo` tools into [`cairn_proto::control`]
//! messages on that other socket; the daemon never speaks MCP itself.
//!
//! Wire shape (one request per line, one response per line):
//!
//! ```text
//! → {"jsonrpc":"2.0","id":1,"method":"get_outline","params":{"repo":"demo","file":"src/lib.rs"}}
//! ← {"jsonrpc":"2.0","id":1,"result":{"items":[...]}}
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use cairn_proto::jsonrpc::{
    JsonRpcVersion, Request, RequestId, Response, ResponseError, error_code,
};
use linkme::distributed_slice;
use serde_json::Value;
use tracing::{debug, warn};

use crate::daemon::LineHandler;
use crate::indexer::Indexer;
use crate::storage::Storage;
use crate::{Error, Result, registry_db};

pub mod methods;

// ─── trait + registry ──────────────────────────────────────────────────────

/// One JSON-RPC method exposed on the data socket. Each implementer
/// lives in its own [`methods`] sub-module and registers a constructor
/// into [`DATA_METHODS`] via `#[distributed_slice]`. The constructor
/// indirection lets registration be `const`-evaluable while the
/// returned trait object owns whatever per-method state (none, today)
/// the implementation needs.
#[async_trait::async_trait]
pub trait DataMethod: Send + Sync {
    /// JSON-RPC method name advertised on the wire (e.g. `"get_outline"`).
    /// Must match the `method` field a client sends.
    fn name(&self) -> &'static str;

    /// Run the method. `params` is the request's `params` field (or
    /// `Value::Null` when omitted). On success the returned [`Value`]
    /// becomes the JSON-RPC `result`. On error the [`Error`] is
    /// translated into a JSON-RPC error by [`error_from`].
    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value>;
}

/// Linker-time registry of data-RPC methods. Mirrors the pattern used
/// by `cairn-lang-api::LANGUAGE_BACKENDS`: each method module contributes
/// one entry; the daemon collects them at startup and dispatches by
/// name.
#[allow(unsafe_code)]
#[distributed_slice]
pub static DATA_METHODS: [fn() -> Box<dyn DataMethod>] = [..];

/// Shared state each [`DataMethod`] gets at dispatch time. Holds the
/// [`Storage`] (registry queries + snapshot DB opens) and the
/// [`Indexer`] (`active_snapshot` / target-list helpers).
#[derive(Clone)]
pub struct DataCtx {
    pub storage: Arc<Storage>,
    pub indexer: Arc<Indexer>,
}

// ─── handler ───────────────────────────────────────────────────────────────

/// Plain-JSON-RPC handler bound to `cairn.sock`. One instance per
/// daemon. The dispatch table is materialised once from
/// [`DATA_METHODS`] at construction.
pub struct DataRpc {
    ctx: DataCtx,
    methods: HashMap<&'static str, Box<dyn DataMethod>>,
}

impl DataRpc {
    #[must_use]
    pub fn new(storage: Arc<Storage>, indexer: Arc<Indexer>) -> Self {
        let mut methods: HashMap<&'static str, Box<dyn DataMethod>> = HashMap::new();
        for ctor in DATA_METHODS {
            let method = ctor();
            methods.insert(method.name(), method);
        }
        Self {
            ctx: DataCtx { storage, indexer },
            methods,
        }
    }
}

#[async_trait::async_trait]
impl LineHandler for DataRpc {
    async fn handle(&self, line: &str) -> Option<String> {
        let req: Request = match serde_json::from_str(line) {
            Ok(r) => r,
            Err(e) => {
                let resp = error_resp(
                    RequestId::Number(0),
                    error_code::PARSE_ERROR,
                    format!("invalid JSON-RPC envelope: {e}"),
                );
                return Some(serialize(&resp));
            }
        };
        debug!(method = %req.method, "data RPC");
        let resp = self.dispatch(req).await;
        Some(serialize(&resp))
    }
}

impl DataRpc {
    async fn dispatch(&self, req: Request) -> Response {
        let id = req.id.clone();
        let Some(method) = self.methods.get(req.method.as_str()) else {
            return error_resp(
                id,
                error_code::METHOD_NOT_FOUND,
                format!("unknown method: {}", req.method),
            );
        };
        let params = req.params.clone().unwrap_or(Value::Null);
        match method.dispatch(&self.ctx, params).await {
            Ok(value) => ok_resp(id, value),
            Err(err) => error_from(id, &err),
        }
    }
}

// ─── helpers shared by method modules ─────────────────────────────────────

/// Decode `params` (the raw `Value` from the JSON-RPC envelope) into a
/// concrete args struct. Returns an `Error::InvalidArgument` (which
/// [`error_from`] maps to `error_code::INVALID_PARAMS`) on shape
/// mismatch.
pub(crate) fn parse_params<T: serde::de::DeserializeOwned>(params: Value) -> Result<T> {
    serde_json::from_value(params)
        .map_err(|e| Error::InvalidArgument(format!("invalid params: {e}")))
}

fn ok_resp(id: RequestId, result: Value) -> Response {
    Response {
        jsonrpc: JsonRpcVersion::V2,
        id,
        result: Some(result),
        error: None,
    }
}

fn error_resp(id: RequestId, code: i32, message: impl Into<String>) -> Response {
    Response {
        jsonrpc: JsonRpcVersion::V2,
        id,
        result: None,
        error: Some(ResponseError {
            code,
            message: message.into(),
            data: None,
        }),
    }
}

fn error_from(id: RequestId, err: &Error) -> Response {
    let msg = err.to_string();
    let code = match err {
        Error::InvalidArgument(s) if s.starts_with("invalid params") => error_code::INVALID_PARAMS,
        Error::InvalidArgument(s) if s.starts_with("no repo ") => error_code::REPO_NOT_FOUND,
        Error::InvalidArgument(s) if s.contains("has no snapshot") => {
            error_code::SNAPSHOT_NOT_READY
        }
        _ => error_code::INTERNAL_ERROR,
    };
    error_resp(id, code, msg)
}

fn serialize(resp: &Response) -> String {
    serde_json::to_string(resp).unwrap_or_else(|e| {
        warn!(error = %e, "data RPC response serialization failed");
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"serialization failed"}}"#
            .to_string()
    })
}

// ─── cross-method snapshot helpers ─────────────────────────────────────────

/// One snapshot to query in a cross-branch search. Carries the
/// originating repo alias and branch label so each method's hits can
/// be attributed correctly, even when the query spans every registered
/// repo (`alias = None` on the caller side).
pub(crate) struct SnapshotTarget {
    pub repo_alias: String,
    pub branch: String,
    pub db_path: PathBuf,
    /// Filesystem path of the worktree this snapshot was indexed
    /// from. Needed by methods that read file content at request
    /// time (e.g. `get_symbol_source`); other methods ignore it.
    pub worktree_root: PathBuf,
    /// The tier this snapshot reached at index time. Tier-2 methods
    /// (`find_impls` / `find_references` / `find_imports`) use it to
    /// decide between `Complete` and `Partial { missing_tiers:
    /// [Semantic] }`; Tier-1 methods ignore it (their results don't
    /// depend on the semantic layer).
    pub enrichment: cairn_proto::SourceTier,
}

impl DataCtx {
    /// Resolve the (data DB path, canonical alias) of the active
    /// snapshot for an alias — see `Indexer::active_snapshot` for the
    /// exact picker. Used by single-snapshot calls like `get_outline`.
    pub(crate) async fn resolve_active_snapshot(&self, alias: &str) -> Result<(PathBuf, String)> {
        let handle = self.indexer.active_snapshot(alias).await?;
        Ok((handle.snapshot_db_path, handle.alias))
    }

    /// Build the per-snapshot work list for a cross-branch query
    /// scoped to one repo. `branch = None` returns every snapshot
    /// owned by the alias.
    pub(crate) async fn snapshot_targets(
        &self,
        alias: &str,
        branch: Option<&str>,
    ) -> Result<Vec<SnapshotTarget>> {
        self.snapshot_targets_inner(Some(alias), branch).await
    }

    /// Build the per-snapshot work list for an unrestricted query.
    /// `repo = None` walks every registered repo. `branch` filters
    /// inside whatever repos are picked. Used by `find_symbols` to
    /// answer the "which repo has this symbol?" question.
    pub(crate) async fn snapshot_targets_any_repo(
        &self,
        repo: Option<&str>,
        branch: Option<&str>,
    ) -> Result<Vec<SnapshotTarget>> {
        self.snapshot_targets_inner(repo, branch).await
    }

    async fn snapshot_targets_inner(
        &self,
        repo: Option<&str>,
        branch: Option<&str>,
    ) -> Result<Vec<SnapshotTarget>> {
        let repo_owned = repo.map(str::to_string);
        let branch_owned = branch.map(str::to_string);
        let data_dir = self.storage.data_dir.clone();
        self.storage
            .with_registry(move |conn| {
                // Resolve the candidate repo list. `Some(alias)` →
                // exactly that one (404 if not registered);
                // `None` → every registered repo.
                let repos: Vec<registry_db::Repo> = match repo_owned.as_deref() {
                    Some(alias) => {
                        let r = registry_db::find_repo_by_alias(conn, alias)?
                            .ok_or_else(|| Error::InvalidArgument(format!("no repo `{alias}`")))?;
                        vec![r]
                    }
                    None => registry_db::list_repos(conn)?,
                };
                let mut targets = Vec::new();
                for repo in &repos {
                    for wt in registry_db::list_worktrees(conn, repo.id)? {
                        for snap in registry_db::list_snapshots(conn, wt.id)? {
                            if let Some(ref b) = branch_owned
                                && snap.branch != *b
                            {
                                continue;
                            }
                            let db_path = data_dir.snapshot_db_path(
                                &repo.repo_hash,
                                &wt.worktree_hash,
                                &snap.branch,
                            );
                            targets.push(SnapshotTarget {
                                repo_alias: repo.alias.clone(),
                                enrichment: snap.enrichment.into(),
                                branch: snap.branch,
                                db_path,
                                worktree_root: PathBuf::from(&wt.path),
                            });
                        }
                    }
                }
                Ok(targets)
            })
            .await
    }
}

/// Translate the DB's source-tier string into the wire enum. Method
/// modules use this when materialising hits from a snapshot DB.
pub(crate) fn parse_source_tier(s: &str) -> cairn_proto::SourceTier {
    match s {
        "semantic" => cairn_proto::SourceTier::Semantic,
        _ => cairn_proto::SourceTier::Syntactic,
    }
}

/// Aggregate per-snapshot enrichment levels into a single
/// [`Completeness`] for a Tier-2 query (`find_impls` /
/// `find_references` / `find_imports`).
///
/// A Tier-2 result is `Complete` only when every snapshot it touched
/// reached the semantic tier. If any snapshot is syntactic-only (e.g. a
/// language without a Tier-2 analyzer yet, or enrichment still pending),
/// the impl/ref/import edges from that snapshot may be missing, so the
/// whole response is `Partial { missing_tiers: [Semantic] }` with a
/// reason naming the syntactic-only branches.
///
/// The wire format is response-level, not hit-level, so this collapses
/// the per-snapshot picture into one verdict — the conservative choice
/// (any gap ⇒ partial) keeps the signal honest.
pub(crate) fn completeness_from_targets(targets: &[SnapshotTarget]) -> cairn_proto::Completeness {
    let syntactic: Vec<&str> = targets
        .iter()
        .filter(|t| t.enrichment == cairn_proto::SourceTier::Syntactic)
        .map(|t| t.branch.as_str())
        .collect();
    if syntactic.is_empty() {
        cairn_proto::Completeness::complete()
    } else {
        cairn_proto::Completeness::partial_semantic(format!(
            "branch(es) [{}] indexed at syntactic tier only; \
             impl/reference/import edges may be incomplete",
            syntactic.join(", ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::Indexer;
    use crate::paths::DataDir;
    use cairn_lang_api::LanguageBackend;
    use cairn_proto::methods::{
        FindReferencesResult, FindSymbolResult, GetSymbolSourceResult, ImplsResult, ImportsResult,
        OutlineResult,
    };

    async fn fixture() -> (tempfile::TempDir, Arc<Storage>, DataRpc) {
        let work = tempfile::tempdir().unwrap();
        let repo_root = work.path().join("repo");
        std::fs::create_dir_all(repo_root.join("src")).unwrap();
        std::fs::write(
            repo_root.join("src").join("lib.rs"),
            "/// Greet someone.\npub fn hello() {}\n\npub fn caller() { hello(); }\n\nstruct Foo;\nimpl Foo { pub fn bar(&self) {} }\n",
        )
        .unwrap();
        std::fs::create_dir(repo_root.join(".git")).unwrap();
        std::fs::write(
            repo_root.join(".git").join("HEAD"),
            "ref: refs/heads/main\n",
        )
        .unwrap();

        let data_dir = DataDir::with_root(work.path().join("cc"));
        let storage = Arc::new(Storage::open(data_dir).unwrap());
        let backends: Vec<Box<dyn LanguageBackend>> = vec![Box::new(cairn_lang_rust::RustBackend)];
        let indexer = Arc::new(Indexer::with_backends(storage.clone(), backends));
        indexer.register_repo("demo", &repo_root).await.unwrap();
        indexer.full_index("demo").await.unwrap();

        let server = DataRpc::new(storage.clone(), indexer);
        (work, storage, server)
    }

    #[tokio::test]
    async fn outline_returns_indexed_symbols() {
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":1,"method":"get_outline","params":{"repo":"demo","file":"src/lib.rs"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let outline: OutlineResult = serde_json::from_value(resp.result.unwrap()).unwrap();
        let names: Vec<&str> = outline.items.iter().map(|i| i.name.as_str()).collect();
        assert!(names.contains(&"hello"));
        assert!(names.contains(&"Foo"));
        assert!(names.contains(&"bar"));
        let hello = outline.items.iter().find(|i| i.name == "hello").unwrap();
        assert!(hello.doc.as_deref().unwrap().contains("Greet someone"));
    }

    #[tokio::test]
    async fn find_symbols_exact_match() {
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":2,"method":"find_symbols","params":{"query":"hello","repo":"demo"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let hits: FindSymbolResult = serde_json::from_value(resp.result.unwrap()).unwrap();
        assert!(!hits.items.is_empty());
        let h = &hits.items[0];
        assert_eq!(h.name, "hello");
        assert_eq!(h.branch, "main");
        assert!(h.location.starts_with("demo:main:src/lib.rs:"));
    }

    #[tokio::test]
    async fn find_symbols_fuzzy_finds_indexed_symbol_by_word() {
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":3,"method":"find_symbols","params":{"query":"hello","repo":"demo","fuzzy":true}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let hits: FindSymbolResult = serde_json::from_value(resp.result.unwrap()).unwrap();
        assert!(hits.items.iter().any(|h| h.name == "hello"));
    }

    #[tokio::test]
    async fn find_symbols_repo_optional_searches_every_registered_repo() {
        // 0.2.1: `repo` is optional. With only `query` set, the call
        // searches every registered repo (the fixture has just `demo`)
        // and returns the hit attributed to its originating repo.
        let (_w, _s, srv) = fixture().await;
        let line =
            r#"{"jsonrpc":"2.0","id":99,"method":"find_symbols","params":{"query":"hello"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let hits: FindSymbolResult = serde_json::from_value(resp.result.unwrap()).unwrap();
        let hit = hits
            .items
            .iter()
            .find(|h| h.name == "hello")
            .expect("hello not found via cross-repo query");
        assert_eq!(
            hit.repo, "demo",
            "repo attribution missing on cross-repo hit"
        );
    }

    #[tokio::test]
    async fn find_symbols_rejects_when_every_filter_omitted() {
        // 0.2.1: at least one of {query, kind, container, path} must
        // be supplied. Otherwise the call would dump the snapshot.
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":300,"method":"find_symbols","params":{"repo":"demo"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        assert!(
            resp.error.is_some(),
            "expected error when no structural filter is supplied"
        );
    }

    #[tokio::test]
    async fn find_symbols_kind_alone_enumerates_kind() {
        // `{kind: "function"}` lists every function — no query.
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":301,"method":"find_symbols","params":{"repo":"demo","kind":"function"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let r: FindSymbolResult = serde_json::from_value(resp.result.unwrap()).unwrap();
        let names: Vec<&str> = r.items.iter().map(|h| h.name.as_str()).collect();
        assert!(names.contains(&"hello"), "expected hello, got {names:?}");
        assert!(names.contains(&"caller"), "expected caller, got {names:?}");
    }

    #[tokio::test]
    async fn find_symbols_container_enumerates_members() {
        // `{container: "Foo"}` returns Foo's members (Foo::bar) and
        // nothing else.
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":302,"method":"find_symbols","params":{"repo":"demo","container":"Foo"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let r: FindSymbolResult = serde_json::from_value(resp.result.unwrap()).unwrap();
        assert!(
            r.items
                .iter()
                .any(|h| h.qualified == "Foo::bar" || h.qualified.starts_with("Foo::")),
            "expected a Foo:: member, got {:?}",
            r.items
        );
        // No top-level non-Foo symbols should leak in.
        assert!(
            !r.items.iter().any(|h| h.qualified == "hello"),
            "module-level hello shouldn't appear under container=Foo, got {:?}",
            r.items
        );
    }

    #[tokio::test]
    async fn find_symbols_path_prefix_filters_by_file() {
        // path="src/" matches; path="other/" matches nothing.
        let (_w, _s, srv) = fixture().await;
        let line_match = r#"{"jsonrpc":"2.0","id":303,"method":"find_symbols","params":{"repo":"demo","kind":"function","path":"src/"}}"#;
        let line_miss = r#"{"jsonrpc":"2.0","id":304,"method":"find_symbols","params":{"repo":"demo","kind":"function","path":"nowhere/"}}"#;
        let m: Response = serde_json::from_str(&srv.handle(line_match).await.unwrap()).unwrap();
        let s: FindSymbolResult = serde_json::from_value(m.result.unwrap()).unwrap();
        assert!(!s.items.is_empty(), "src/ should match");
        let miss: Response = serde_json::from_str(&srv.handle(line_miss).await.unwrap()).unwrap();
        let s2: FindSymbolResult = serde_json::from_value(miss.result.unwrap()).unwrap();
        assert!(s2.items.is_empty(), "nowhere/ should yield nothing");
    }

    #[tokio::test]
    async fn find_symbols_truncation_reported_as_partial() {
        // limit=1 against a multi-symbol kind triggers truncation.
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":305,"method":"find_symbols","params":{"repo":"demo","kind":"function","limit":1}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let r = resp.result.unwrap();
        let c = r["completeness"].clone();
        assert_eq!(c["status"], "partial", "expected partial, got {c:?}");
        assert!(
            c["reason"].as_str().unwrap().contains("limit"),
            "reason should mention the cap: {c:?}"
        );
    }

    #[tokio::test]
    async fn find_impls_finds_inherent_impl_by_type() {
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":50,"method":"find_impls","params":{"repo":"demo","type":"Foo"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let result: ImplsResult = serde_json::from_value(resp.result.unwrap()).unwrap();
        assert!(
            result.items.iter().any(|h| h.type_qualified == "Foo"
                && h.kind == "inherent"
                && h.interface_qualified.is_none()),
            "expected inherent impl Foo hit, got {:?}",
            result.items
        );
    }

    #[tokio::test]
    async fn find_impls_requires_trait_or_type() {
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":51,"method":"find_impls","params":{"repo":"demo"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        assert!(resp.error.is_some());
    }

    #[tokio::test]
    async fn find_imports_returns_empty_for_demo_fixture() {
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":52,"method":"find_imports","params":{"repo":"demo"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let result: ImportsResult = serde_json::from_value(resp.result.unwrap()).unwrap();
        assert!(result.items.is_empty());
    }

    #[tokio::test]
    async fn find_symbols_branch_filter_unknown_errors() {
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":100,"method":"find_symbols","params":{"query":"hello","repo":"demo","branch":"no-such-branch"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        assert!(resp.error.is_some(), "expected error for unknown branch");
    }

    #[tokio::test]
    async fn list_repos_includes_demo() {
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":4,"method":"list_repos"}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let v = resp.result.unwrap();
        let repos = v["repos"].as_array().unwrap();
        assert_eq!(repos.len(), 1);
        assert_eq!(repos[0]["alias"], "demo");
        assert!(
            repos[0]["languages"]
                .as_array()
                .unwrap()
                .iter()
                .any(|l| l == "rust")
        );
    }

    #[tokio::test]
    async fn unknown_method_returns_method_not_found() {
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":5,"method":"foo/bar"}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let err = resp.error.unwrap();
        assert_eq!(err.code, error_code::METHOD_NOT_FOUND);
    }

    #[tokio::test]
    async fn find_references_locates_caller_of_hello() {
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":200,"method":"find_references","params":{"repo":"demo","symbol":"hello"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let result: FindReferencesResult = serde_json::from_value(resp.result.unwrap()).unwrap();
        assert!(
            result
                .items
                .iter()
                .any(|h| h.target_name == "hello" && h.location.contains("src/lib.rs")),
            "expected a hello() call site, got {:?}",
            result.items
        );
    }

    #[tokio::test]
    async fn get_symbol_source_returns_function_body() {
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":201,"method":"get_symbol_source","params":{"repo":"demo","qualified":"hello"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let result: GetSymbolSourceResult = serde_json::from_value(resp.result.unwrap()).unwrap();
        assert_eq!(result.name, "hello");
        assert!(
            result.source.contains("fn hello"),
            "expected source to contain `fn hello`, got: {:?}",
            result.source
        );
    }

    #[tokio::test]
    async fn get_symbol_source_unknown_qualified_errors() {
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":202,"method":"get_symbol_source","params":{"repo":"demo","qualified":"definitely_not_there"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        assert!(resp.error.is_some());
    }

    #[tokio::test]
    async fn get_symbol_source_signature_only_skips_body() {
        // 0.2.1: signature_only=true returns sig + doc without paying
        // for the file read. `source` is empty; `signature` carries
        // the head.
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":403,"method":"get_symbol_source","params":{"repo":"demo","qualified":"hello","signature_only":true}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let r: GetSymbolSourceResult = serde_json::from_value(resp.result.unwrap()).unwrap();
        assert_eq!(r.name, "hello");
        assert_eq!(
            r.source, "",
            "signature_only must leave source empty; got {:?}",
            r.source
        );
        assert!(
            r.signature
                .as_deref()
                .unwrap_or_default()
                .contains("fn hello"),
            "signature should still carry the head, got {:?}",
            r.signature
        );
    }

    #[tokio::test]
    async fn find_references_outgoing_returns_callees() {
        // 0.2.1: `direction=outgoing` answers "what does X call?"
        // The fixture has `pub fn caller() { hello(); }`; outgoing on
        // `caller` should surface the `hello` call site.
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":404,"method":"find_references","params":{"repo":"demo","symbol":"caller","direction":"outgoing"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let r: FindReferencesResult = serde_json::from_value(resp.result.unwrap()).unwrap();
        assert!(
            r.items
                .iter()
                .any(|h| h.target_name == "hello"
                    && h.enclosing_qualified.as_deref() == Some("caller")),
            "expected outgoing callee `hello` from `caller`, got {:?}",
            r.items
        );
    }

    #[tokio::test]
    async fn find_references_direction_default_is_incoming() {
        // Default direction must remain `incoming` so existing 0.2.0
        // callers don't see a silent shift in semantics.
        let (_w, _s, srv) = fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":405,"method":"find_references","params":{"repo":"demo","symbol":"hello"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let r: FindReferencesResult = serde_json::from_value(resp.result.unwrap()).unwrap();
        assert!(
            r.items
                .iter()
                .any(|h| h.target_name == "hello"
                    && h.enclosing_qualified.as_deref() == Some("caller")),
            "expected incoming hit (target=hello, enclosing=caller), got {:?}",
            r.items
        );
    }

    /// Regression guard for the generic-impl enclosing-attribution
    /// bug. Pre-fix, indexing a fixture that contained
    /// `impl<T> Container<T> { fn push(...) { foo(); } }` produced
    /// a refs row for `foo` whose `enclosing_qualified` was
    /// `"Container::push"` (from the syn analyzer), but the
    /// symbols table had `qualified = "Container<T>::push"` (from
    /// the tree-sitter pass), so `apply_pending_refs` couldn't
    /// resolve enclosing_id. End-to-end assertion: the resolved
    /// enclosing must come back non-null.
    #[tokio::test]
    async fn generic_impl_method_enclosing_resolves_end_to_end() {
        let work = tempfile::tempdir().unwrap();
        let repo_root = work.path().join("repo");
        std::fs::create_dir_all(repo_root.join("src")).unwrap();
        std::fs::write(
            repo_root.join("src").join("lib.rs"),
            "fn helper() {}\n\
             pub struct Container<T> { v: Vec<T> }\n\
             impl<T> Container<T> {\n\
                 pub fn push(&mut self, item: T) { helper(); }\n\
             }\n",
        )
        .unwrap();
        std::fs::create_dir(repo_root.join(".git")).unwrap();
        std::fs::write(
            repo_root.join(".git").join("HEAD"),
            "ref: refs/heads/main\n",
        )
        .unwrap();
        let data_dir = DataDir::with_root(work.path().join("cc"));
        let storage = Arc::new(Storage::open(data_dir).unwrap());
        let backends: Vec<Box<dyn LanguageBackend>> = vec![Box::new(cairn_lang_rust::RustBackend)];
        let indexer = Arc::new(Indexer::with_backends(storage.clone(), backends));
        indexer.register_repo("g", &repo_root).await.unwrap();
        indexer.full_index("g").await.unwrap();

        let server = DataRpc::new(storage.clone(), indexer);
        let line = r#"{"jsonrpc":"2.0","id":300,"method":"find_references","params":{"repo":"g","symbol":"helper"}}"#;
        let resp: Response = serde_json::from_str(&server.handle(line).await.unwrap()).unwrap();
        let result: FindReferencesResult = serde_json::from_value(resp.result.unwrap()).unwrap();
        let hit = result
            .items
            .iter()
            .find(|h| h.target_name == "helper")
            .expect("helper call site missing");
        assert_eq!(
            hit.enclosing_qualified.as_deref(),
            Some("Container::push"),
            "generic-typed impl method's enclosing must resolve to the stripped qualified, got items: {:?}",
            result.items
        );
    }

    #[tokio::test]
    async fn malformed_json_yields_parse_error() {
        let (_w, _s, srv) = fixture().await;
        let resp_str = srv.handle("not-json").await.unwrap();
        let resp: Response = serde_json::from_str(&resp_str).unwrap();
        assert_eq!(resp.error.unwrap().code, error_code::PARSE_ERROR);
    }

    // ─── completeness wiring (partial-result) ──────────────────────────

    /// A markdown-only repo: tree-sitter syntactic extraction, no Tier-2
    /// analyzer at all, so the snapshot's enrichment stays Syntactic.
    /// Used to exercise the Syntactic → `Completeness::Partial` wiring
    /// end-to-end (Python is no longer syntactic-only — it has a Tier-2
    /// analyzer now).
    async fn markdown_fixture() -> (tempfile::TempDir, Arc<Storage>, DataRpc) {
        let work = tempfile::tempdir().unwrap();
        let repo_root = work.path().join("repo");
        std::fs::create_dir_all(&repo_root).unwrap();
        std::fs::write(
            repo_root.join("README.md"),
            "# Title\n\nSome prose.\n\n## Section\n\nMore prose.\n",
        )
        .unwrap();
        std::fs::create_dir(repo_root.join(".git")).unwrap();
        std::fs::write(
            repo_root.join(".git").join("HEAD"),
            "ref: refs/heads/main\n",
        )
        .unwrap();

        let data_dir = DataDir::with_root(work.path().join("cc"));
        let storage = Arc::new(Storage::open(data_dir).unwrap());
        let backends: Vec<Box<dyn LanguageBackend>> =
            vec![Box::new(cairn_lang_markdown::MarkdownBackend)];
        let indexer = Arc::new(Indexer::with_backends(storage.clone(), backends));
        indexer.register_repo("md", &repo_root).await.unwrap();
        indexer.full_index("md").await.unwrap();

        let server = DataRpc::new(storage.clone(), indexer);
        (work, storage, server)
    }

    /// A Python repo. Python now has a Tier-2 analyzer (imports +
    /// inheritance + refs), so the snapshot's enrichment is Semantic.
    async fn python_fixture() -> (tempfile::TempDir, Arc<Storage>, DataRpc) {
        let work = tempfile::tempdir().unwrap();
        let repo_root = work.path().join("repo");
        std::fs::create_dir_all(repo_root.join("pkg")).unwrap();
        std::fs::write(
            repo_root.join("pkg").join("mod.py"),
            "import os\nfrom collections import OrderedDict\n\n\ndef greet(name):\n    return name\n\n\nclass Base:\n    pass\n\n\nclass Widget(Base):\n    def render(self):\n        return greet(\"w\")\n",
        )
        .unwrap();
        std::fs::create_dir(repo_root.join(".git")).unwrap();
        std::fs::write(
            repo_root.join(".git").join("HEAD"),
            "ref: refs/heads/main\n",
        )
        .unwrap();

        let data_dir = DataDir::with_root(work.path().join("cc"));
        let storage = Arc::new(Storage::open(data_dir).unwrap());
        let backends: Vec<Box<dyn LanguageBackend>> =
            vec![Box::new(cairn_lang_python::PythonBackend)];
        let indexer = Arc::new(Indexer::with_backends(storage.clone(), backends));
        indexer.register_repo("py", &repo_root).await.unwrap();
        indexer.full_index("py").await.unwrap();

        let server = DataRpc::new(storage.clone(), indexer);
        (work, storage, server)
    }

    /// Tier-2 methods on a fully semantic (Rust) snapshot report
    /// `Complete`.
    #[tokio::test]
    async fn tier2_methods_complete_on_semantic_snapshot() {
        let (_w, _s, srv) = fixture().await;
        for line in [
            r#"{"jsonrpc":"2.0","id":1,"method":"find_impls","params":{"repo":"demo","type":"Foo"}}"#,
            r#"{"jsonrpc":"2.0","id":2,"method":"find_references","params":{"repo":"demo","symbol":"hello"}}"#,
            r#"{"jsonrpc":"2.0","id":3,"method":"find_imports","params":{"repo":"demo"}}"#,
        ] {
            let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
            let completeness = resp.result.unwrap()["completeness"].clone();
            assert_eq!(
                completeness,
                serde_json::json!({"status": "complete"}),
                "expected Complete on a semantic snapshot for {line}"
            );
        }
    }

    /// Tier-2 methods on a syntactic-only (markdown) snapshot report
    /// `Partial { missing_tiers: [semantic] }`.
    #[tokio::test]
    async fn tier2_methods_partial_on_syntactic_snapshot() {
        let (_w, _s, srv) = markdown_fixture().await;
        for line in [
            r#"{"jsonrpc":"2.0","id":1,"method":"find_impls","params":{"repo":"md","type":"Anything"}}"#,
            r#"{"jsonrpc":"2.0","id":2,"method":"find_references","params":{"repo":"md","symbol":"anything"}}"#,
            r#"{"jsonrpc":"2.0","id":3,"method":"find_imports","params":{"repo":"md"}}"#,
        ] {
            let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
            let c = resp.result.unwrap()["completeness"].clone();
            assert_eq!(c["status"], "partial", "expected Partial for {line}");
            assert_eq!(
                c["missing_tiers"],
                serde_json::json!(["semantic"]),
                "expected missing_tiers=[semantic] for {line}"
            );
            assert!(
                c["reason"].as_str().unwrap().contains("main"),
                "reason should name the syntactic branch, got {c:?}"
            );
        }
    }

    /// Tier-1 methods stay `Complete` even on a syntactic-only snapshot
    /// — their results don't depend on the semantic layer.
    #[tokio::test]
    async fn tier1_methods_complete_on_syntactic_snapshot() {
        let (_w, _s, srv) = markdown_fixture().await;
        for line in [
            r#"{"jsonrpc":"2.0","id":1,"method":"get_outline","params":{"repo":"md","file":"README.md"}}"#,
            r#"{"jsonrpc":"2.0","id":2,"method":"find_symbols","params":{"repo":"md","query":"Title"}}"#,
        ] {
            let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
            let c = resp.result.unwrap()["completeness"].clone();
            assert_eq!(
                c,
                serde_json::json!({"status": "complete"}),
                "Tier-1 method must stay Complete on a syntactic snapshot for {line}"
            );
        }
    }

    /// Python Tier-2: the analyzer emits imports + inheritance edges, so
    /// the snapshot is Semantic (Tier-2 methods report `Complete`) and
    /// `find_imports` / `find_impls` actually return that data.
    #[tokio::test]
    async fn python_tier2_emits_imports_and_inheritance() {
        let (_w, _s, srv) = python_fixture().await;

        // find_imports returns the module dependencies.
        let line = r#"{"jsonrpc":"2.0","id":1,"method":"find_imports","params":{"repo":"py"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let result = resp.result.unwrap();
        assert_eq!(
            result["completeness"],
            serde_json::json!({"status": "complete"}),
            "Python now has Tier-2, so completeness is Complete"
        );
        let imports: ImportsResult = serde_json::from_value(result).unwrap();
        let modules: Vec<&str> = imports.items.iter().map(|i| i.to_module.as_str()).collect();
        assert!(
            modules.contains(&"os"),
            "expected `os` import, got {modules:?}"
        );
        assert!(
            modules.contains(&"collections"),
            "expected `collections` import, got {modules:?}"
        );

        // find_impls trait=Base answers "what subclasses Base".
        let line = r#"{"jsonrpc":"2.0","id":2,"method":"find_impls","params":{"repo":"py","trait":"Base"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let result: ImplsResult = serde_json::from_value(resp.result.unwrap()).unwrap();
        assert!(
            result.items.iter().any(|h| h.type_qualified == "Widget"
                && h.interface_qualified.as_deref() == Some("Base")
                && h.kind == "inherit"),
            "expected Widget--inherit-->Base edge, got {:?}",
            result.items
        );
    }

    /// Python Tier-2 refs: `find_references` locates the `greet()` call
    /// site inside `Widget.render`, with the enclosing method's
    /// qualified name attached.
    #[tokio::test]
    async fn python_tier2_find_references() {
        let (_w, _s, srv) = python_fixture().await;
        let line = r#"{"jsonrpc":"2.0","id":1,"method":"find_references","params":{"repo":"py","symbol":"greet"}}"#;
        let resp: Response = serde_json::from_str(&srv.handle(line).await.unwrap()).unwrap();
        let result = resp.result.unwrap();
        assert_eq!(
            result["completeness"],
            serde_json::json!({"status": "complete"}),
            "Python has Tier-2, so find_references is Complete"
        );
        let refs: FindReferencesResult = serde_json::from_value(result).unwrap();
        let hit = refs
            .items
            .iter()
            .find(|h| h.target_name == "greet" && h.location.contains("pkg/mod.py"))
            .expect("greet() call site missing");
        assert_eq!(
            hit.enclosing_qualified.as_deref(),
            Some("Widget.render"),
            "call enclosing should be the method, got {:?}",
            hit.enclosing_qualified
        );
    }

    /// The cross-branch aggregation rule: all-semantic ⇒ Complete; any
    /// syntactic ⇒ Partial naming the syntactic branches.
    #[test]
    fn completeness_from_targets_aggregates_conservatively() {
        use cairn_proto::{Completeness, SourceTier};
        let mk = |branch: &str, tier: SourceTier| SnapshotTarget {
            repo_alias: "demo".to_string(),
            branch: branch.to_string(),
            db_path: PathBuf::from("/dev/null"),
            worktree_root: PathBuf::from("/dev/null"),
            enrichment: tier,
        };

        // All semantic → Complete.
        assert_eq!(
            completeness_from_targets(&[
                mk("main", SourceTier::Semantic),
                mk("feature", SourceTier::Semantic),
            ]),
            Completeness::Complete
        );

        // Mixed → Partial naming only the syntactic branch.
        match completeness_from_targets(&[
            mk("main", SourceTier::Semantic),
            mk("legacy", SourceTier::Syntactic),
        ]) {
            Completeness::Partial {
                missing_tiers,
                reason,
            } => {
                assert_eq!(missing_tiers, vec![cairn_proto::MissingTier::Semantic]);
                let r = reason.unwrap();
                assert!(r.contains("legacy"), "reason names syntactic branch: {r}");
                assert!(!r.contains("main"), "reason omits semantic branch: {r}");
            }
            other => panic!("expected Partial, got {other:?}"),
        }

        // Empty target set → Complete (vacuously; no snapshot lacks a tier).
        assert_eq!(completeness_from_targets(&[]), Completeness::Complete);
    }
}
