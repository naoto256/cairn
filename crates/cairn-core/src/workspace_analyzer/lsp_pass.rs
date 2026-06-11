//! Shared LSP definition-pass substrate for Tier-3 analyzers.
//!
//! Every LSP-backed Tier-3 analyzer follows the same shape: spawn (or
//! reuse) a pooled language server, sync each matching document,
//! resolve the definition under every interesting identifier, and map
//! the returned locations back to repo-relative refs. This module owns
//! that pipeline; language crates contribute only the launch spec, the
//! retry policy quirks of their server, and the grammar-specific
//! call-site extraction.

use std::future::Future;
use std::path::Path;
use std::time::Duration;

use cairn_proto::RefKind;
use futures::{StreamExt, stream};
use tracing::{debug, warn};

use crate::lsp::pool::{self as lsp_pool, LspSpawnSpec, PoolKey, PooledLsp};
use crate::lsp::{Location, Position, Url};
use crate::{Error, Result};

use super::path::location_to_repo_path;
use super::{ResolvedRef, WorkspaceFacts, WorkspaceFile};

const MAX_DEFINITION_ATTEMPTS: usize = 3;
const DEFINITION_PIPELINE_CONCURRENCY: usize = 16;
const CONTENT_MODIFIED_RETRY_DELAY: Duration = Duration::from_millis(100);
const TRANSIENT_RETRY_BACKOFF: Duration = Duration::from_millis(200);

/// One identifier a language crate wants resolved via
/// `textDocument/definition`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DefinitionSite {
    /// LSP position of the identifier, zero-based.
    pub position: Position,
    /// Byte offset where the identifier starts.
    pub byte_start: usize,
    /// Byte offset one past the identifier end.
    pub byte_end: usize,
}

/// Per-server retry quirks for `textDocument/definition`.
///
/// Content-modified responses are always retried once; the flags below
/// opt in to the additional behaviors individual servers need. All
/// retries share one attempt budget of [`MAX_DEFINITION_ATTEMPTS`].
#[derive(Debug, Clone, Copy, Default)]
pub struct DefinitionRetryPolicy {
    /// Retry once when the server answers with an empty location list
    /// (pyright and gopls can respond before their first analysis pass
    /// over a freshly opened document completes).
    pub retry_empty_definition: bool,
    /// Retry with backoff when the server reports "file not found"
    /// for a document that was just synced.
    pub retry_file_not_found: bool,
}

/// Everything a language crate hands the substrate to run one
/// definition pass.
pub struct LspDefinitionPass {
    /// Stable analyzer identifier, e.g. `"gopls-lsp"`.
    pub analyzer_id: &'static str,
    /// Optional analyzer id used only for pooling. Defaults to
    /// [`Self::analyzer_id`]. This lets sibling analyzers keep
    /// distinct run/ref sources while intentionally sharing one LSP
    /// subprocess.
    pub pool_analyzer_id: Option<&'static str>,
    /// Pool-key language tag, e.g. `"go"`.
    pub language: &'static str,
    /// Ref kind recorded for every resolved site this pass emits.
    pub ref_kind: RefKind,
    /// Launch and readiness settings for the pooled server.
    pub spawn_spec: LspSpawnSpec,
    pub retry: DefinitionRetryPolicy,
    /// Grammar-specific extraction of the identifiers to resolve.
    pub collect_definition_sites: fn(&[u8]) -> Result<Vec<DefinitionSite>>,
}

/// Run one LSP definition pass over `files` and return the resolved
/// refs as workspace facts.
///
/// # Errors
/// Returns [`Error::Lsp`] for binary availability, spawn, readiness,
/// and protocol failures, and IO errors when a worktree file cannot
/// be read.
pub fn run_lsp_definition_pass(
    pass: LspDefinitionPass,
    repo_root: &Path,
    files: &[WorkspaceFile],
) -> Result<WorkspaceFacts> {
    let key = PoolKey::lsp(
        pass.language,
        repo_root,
        pass.pool_analyzer_id.unwrap_or(pass.analyzer_id),
        &pass.spawn_spec.binary,
        &pass.spawn_spec.config_hash,
    )
    .map_err(Error::Lsp)?;
    let pool = lsp_pool::global().map_err(Error::Lsp)?;
    let repo_root = repo_root.to_path_buf();
    let files = files.to_vec();
    let analyzer_id = pass.analyzer_id;
    let ref_kind = pass.ref_kind;
    let retry = pass.retry;
    let collect = pass.collect_definition_sites;
    pool.with_lsp(key, pass.spawn_spec, move |client| {
        Box::pin(async move {
            let mut facts = WorkspaceFacts::default();
            collect_resolved_refs(
                client,
                &repo_root,
                &files,
                analyzer_id,
                ref_kind,
                retry,
                collect,
                &mut facts,
            )
            .await
            .map_err(core_error_to_lsp)?;
            Ok(facts)
        })
    })
    .map_err(Error::Lsp)
}

#[allow(clippy::too_many_arguments)]
async fn collect_resolved_refs(
    client: &mut PooledLsp<'_>,
    repo_root: &Path,
    files: &[WorkspaceFile],
    analyzer_id: &'static str,
    ref_kind: RefKind,
    retry: DefinitionRetryPolicy,
    collect_definition_sites: fn(&[u8]) -> Result<Vec<DefinitionSite>>,
    facts: &mut WorkspaceFacts,
) -> Result<()> {
    for file in files {
        let Some(path) = &file.worktree_path else {
            continue;
        };
        let source = std::fs::read_to_string(path)?;
        let sites = collect_definition_sites(source.as_bytes())?;
        if sites.is_empty() {
            continue;
        }
        let uri = Url::from_file_path(path).map_err(Error::Lsp)?;
        client
            .sync_document(&uri, &source)
            .await
            .map_err(Error::Lsp)?;
        let client = &*client;
        let resolved_sites = collect_definition_site_locations(
            sites,
            |site| client.definition(&uri, site.position),
            retry,
            analyzer_id,
            &uri,
        )
        .await;
        for resolved_site in resolved_sites {
            for target in resolved_site.locations {
                let target_path = location_to_repo_path(repo_root, &target);
                facts.resolved_refs.push(ResolvedRef {
                    source_path: file.path.clone(),
                    source_position: resolved_site.site.position,
                    source_byte_range: resolved_site.site.byte_start..resolved_site.site.byte_end,
                    kind: ref_kind,
                    target,
                    target_path,
                });
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedDefinitionSite {
    site: DefinitionSite,
    locations: Vec<Location>,
}

async fn collect_definition_site_locations<F, Fut>(
    sites: Vec<DefinitionSite>,
    definition: F,
    retry: DefinitionRetryPolicy,
    analyzer_id: &str,
    uri: &Url,
) -> Vec<ResolvedDefinitionSite>
where
    F: Fn(DefinitionSite) -> Fut,
    Fut: Future<Output = crate::lsp::Result<Vec<Location>>>,
{
    let mut results = stream::iter(sites)
        .map(|site| {
            let definition = &definition;
            async move {
                let result = definition_with_retry_from(
                    || definition(site),
                    retry,
                    analyzer_id,
                    uri,
                    site.position,
                )
                .await;
                (site, result)
            }
        })
        .buffer_unordered(DEFINITION_PIPELINE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    results.sort_by_key(|(site, _)| {
        (
            site.position.line,
            site.position.character,
            site.byte_start,
            site.byte_end,
        )
    });

    let mut resolved = Vec::new();
    for (site, result) in results {
        match result {
            Ok(locations) => resolved.push(ResolvedDefinitionSite { site, locations }),
            Err(err) => {
                warn!(
                    analyzer_id,
                    uri = uri.as_str(),
                    ?site,
                    error = %err,
                    "definition request failed; skipping site"
                );
            }
        }
    }
    resolved
}

async fn definition_with_retry_from<F, Fut>(
    mut definition: F,
    policy: DefinitionRetryPolicy,
    analyzer_id: &str,
    uri: &Url,
    position: Position,
) -> Result<Vec<Location>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = crate::lsp::Result<Vec<Location>>>,
{
    let mut backoff = TRANSIENT_RETRY_BACKOFF;
    let mut retried_empty_definition = false;
    let mut retried_content_modified = false;
    for _attempt in 0..MAX_DEFINITION_ATTEMPTS {
        match definition().await {
            Ok(locations) if !locations.is_empty() => return Ok(locations),
            Ok(locations) => {
                if policy.retry_empty_definition && !retried_empty_definition {
                    retried_empty_definition = true;
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                } else {
                    return Ok(locations);
                }
            }
            Err(err) if err.is_content_modified() && !retried_content_modified => {
                debug!(
                    analyzer_id,
                    uri = uri.as_str(),
                    ?position,
                    "content modified; retrying definition once"
                );
                retried_content_modified = true;
                tokio::time::sleep(CONTENT_MODIFIED_RETRY_DELAY).await;
            }
            Err(err) if policy.retry_file_not_found && is_file_not_found(&err) => {
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
            Err(err) => return Err(Error::Lsp(err)),
        }
    }
    Ok(Vec::new())
}

fn is_file_not_found(err: &crate::lsp::Error) -> bool {
    matches!(err, crate::lsp::Error::Protocol(message) if message.contains("file not found"))
        || matches!(
            err,
            crate::lsp::Error::ResponseError { message, .. } if message.contains("file not found")
        )
}

fn core_error_to_lsp(err: Error) -> crate::lsp::Error {
    match err {
        Error::Lsp(err) => err,
        err => crate::lsp::Error::Protocol(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::future::ready;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Instant;

    use crate::lsp::{CONTENT_MODIFIED_ERROR_CODE, Range};

    fn test_uri() -> Url {
        Url::from("file:///tmp/repo/src/lib.rs")
    }

    fn test_position() -> Position {
        Position {
            line: 3,
            character: 12,
        }
    }

    fn test_location() -> Location {
        Location {
            uri: test_uri(),
            range: Range {
                start: Position {
                    line: 9,
                    character: 4,
                },
                end: Position {
                    line: 9,
                    character: 7,
                },
            },
        }
    }

    fn test_location_at(line: u32) -> Location {
        Location {
            uri: test_uri(),
            range: Range {
                start: Position { line, character: 0 },
                end: Position { line, character: 1 },
            },
        }
    }

    fn test_site(line: u32) -> DefinitionSite {
        DefinitionSite {
            position: Position { line, character: 0 },
            byte_start: line as usize,
            byte_end: line as usize + 1,
        }
    }

    fn content_modified() -> crate::lsp::Error {
        crate::lsp::Error::ResponseError {
            code: CONTENT_MODIFIED_ERROR_CODE,
            message: "content modified".into(),
        }
    }

    fn file_not_found() -> crate::lsp::Error {
        crate::lsp::Error::ResponseError {
            code: -32603,
            message: "file not found".into(),
        }
    }

    async fn run_retry(
        policy: DefinitionRetryPolicy,
        responses: impl Fn(usize) -> crate::lsp::Result<Vec<Location>>,
        attempts: &Cell<usize>,
    ) -> Result<Vec<Location>> {
        definition_with_retry_from(
            || {
                attempts.set(attempts.get() + 1);
                ready(responses(attempts.get()))
            },
            policy,
            "test-lsp",
            &test_uri(),
            test_position(),
        )
        .await
    }

    #[tokio::test]
    async fn content_modified_retry_success_preserves_locations() {
        let attempts = Cell::new(0);
        let locations = run_retry(
            DefinitionRetryPolicy::default(),
            |n| {
                if n == 1 {
                    Err(content_modified())
                } else {
                    Ok(vec![test_location()])
                }
            },
            &attempts,
        )
        .await
        .unwrap();

        assert_eq!(locations, vec![test_location()]);
        assert_eq!(attempts.get(), 2);
    }

    #[tokio::test]
    async fn repeated_content_modified_retries_once_then_returns_error() {
        let attempts = Cell::new(0);
        let err = run_retry(
            DefinitionRetryPolicy::default(),
            |_| Err(content_modified()),
            &attempts,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::Lsp(err) if err.is_content_modified()));
        assert_eq!(attempts.get(), 2);
    }

    #[tokio::test]
    async fn empty_definition_retries_once_then_returns_resolved() {
        let attempts = Cell::new(0);
        let locations = run_retry(
            DefinitionRetryPolicy {
                retry_empty_definition: true,
                ..Default::default()
            },
            |n| {
                if n == 1 {
                    Ok(Vec::new())
                } else {
                    Ok(vec![test_location()])
                }
            },
            &attempts,
        )
        .await
        .unwrap();

        assert_eq!(locations, vec![test_location()]);
        assert_eq!(attempts.get(), 2);
    }

    #[tokio::test]
    async fn repeated_empty_definition_retries_once_then_returns_empty() {
        let attempts = Cell::new(0);
        let locations = run_retry(
            DefinitionRetryPolicy {
                retry_empty_definition: true,
                ..Default::default()
            },
            |_| Ok(Vec::new()),
            &attempts,
        )
        .await
        .unwrap();

        assert!(locations.is_empty());
        assert_eq!(attempts.get(), 2);
    }

    #[tokio::test]
    async fn empty_definition_returns_immediately_when_policy_disabled() {
        let attempts = Cell::new(0);
        let locations = run_retry(
            DefinitionRetryPolicy::default(),
            |_| Ok(Vec::new()),
            &attempts,
        )
        .await
        .unwrap();

        assert!(locations.is_empty());
        assert_eq!(attempts.get(), 1);
    }

    #[tokio::test]
    async fn file_not_found_retries_until_attempts_exhausted() {
        let attempts = Cell::new(0);
        let locations = run_retry(
            DefinitionRetryPolicy {
                retry_file_not_found: true,
                ..Default::default()
            },
            |_| Err(file_not_found()),
            &attempts,
        )
        .await
        .unwrap();

        assert!(locations.is_empty());
        assert_eq!(attempts.get(), MAX_DEFINITION_ATTEMPTS);
    }

    #[tokio::test]
    async fn file_not_found_is_terminal_when_policy_disabled() {
        let attempts = Cell::new(0);
        let err = run_retry(
            DefinitionRetryPolicy::default(),
            |_| Err(file_not_found()),
            &attempts,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::Lsp(_)));
        assert_eq!(attempts.get(), 1);
    }

    #[tokio::test]
    async fn definition_sites_are_pipelined_with_bounded_concurrency() {
        let sites = (0..100).map(test_site).collect::<Vec<_>>();
        let calls = Arc::new(AtomicUsize::new(0));
        let start = Instant::now();
        let resolved = collect_definition_site_locations(
            sites,
            {
                let calls = Arc::clone(&calls);
                move |site| {
                    let calls = Arc::clone(&calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        Ok::<_, crate::lsp::Error>(vec![test_location_at(site.position.line)])
                    }
                }
            },
            DefinitionRetryPolicy::default(),
            "test-lsp",
            &test_uri(),
        )
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 100);
        assert_eq!(resolved.len(), 100);
        assert!(
            start.elapsed() < Duration::from_millis(200),
            "definition pipeline took {:?}",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn definition_site_errors_skip_only_the_failed_site() {
        let sites = (0..5).map(test_site).collect::<Vec<_>>();
        let resolved = collect_definition_site_locations(
            sites,
            |site| async move {
                if site.position.line == 2 {
                    Err(crate::lsp::Error::Protocol("boom".into()))
                } else {
                    Ok(vec![test_location_at(site.position.line)])
                }
            },
            DefinitionRetryPolicy::default(),
            "test-lsp",
            &test_uri(),
        )
        .await;

        let lines = resolved
            .iter()
            .map(|resolved| resolved.site.position.line)
            .collect::<Vec<_>>();
        assert_eq!(lines, vec![0, 1, 3, 4]);
    }

    #[tokio::test]
    async fn definition_site_results_are_sorted_by_source_position() {
        let sites = vec![test_site(9), test_site(1), test_site(5)];
        let resolved = collect_definition_site_locations(
            sites,
            |site| async move {
                tokio::time::sleep(Duration::from_millis(u64::from(10 - site.position.line))).await;
                Ok::<_, crate::lsp::Error>(vec![test_location_at(site.position.line)])
            },
            DefinitionRetryPolicy::default(),
            "test-lsp",
            &test_uri(),
        )
        .await;

        let lines = resolved
            .iter()
            .map(|resolved| resolved.site.position.line)
            .collect::<Vec<_>>();
        assert_eq!(lines, vec![1, 5, 9]);
    }
}
