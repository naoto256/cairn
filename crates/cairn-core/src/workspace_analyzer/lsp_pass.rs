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
use std::time::{Duration, Instant};

use cairn_proto::RefKind;
use futures::{StreamExt, stream::FuturesUnordered};
use tracing::{debug, warn};

use crate::lsp::pool::{self as lsp_pool, LspSpawnSpec, PoolKey, PooledLsp};
use crate::lsp::{Location, Position, Url};
use crate::{Error, Result};

use super::path::location_to_repo_path;
use super::{AnalyzerProgress, ResolvedRef, WorkspaceFacts, WorkspaceFile};

/// Total request budget per definition site, shared across every
/// retry flavour (content-modified, empty-definition, file-not-found).
const MAX_DEFINITION_ATTEMPTS: usize = 3;
/// Maximum in-flight `textDocument/definition` requests per document
/// (the width of the site pipeline).
const DEFINITION_PIPELINE_CONCURRENCY: usize = 16;
/// Abort a definition pass after this many consecutive terminal
/// request timeouts. Successful or otherwise-terminal outcomes reset
/// the streak; retryable outcomes are resolved inside the per-site
/// retry ladder before they reach this budget.
const MAX_CONSECUTIVE_DEFINITION_TIMEOUTS: usize = 10;
/// Fixed delay before the single always-on content-modified retry.
const CONTENT_MODIFIED_RETRY_DELAY: Duration = Duration::from_millis(100);
/// Initial backoff for the opt-in empty-definition and
/// file-not-found retries; doubled after each sleep.
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
    /// Some language servers return the unresolved use-site itself as
    /// the "definition". When enabled, target locations that point at
    /// any requested site in the same document are treated as unresolved.
    pub suppress_definition_targets_at_requested_sites: bool,
}

/// One ref-kind-specific extractor inside a multi-kind LSP pass.
///
/// Grouping collectors lets a backend read and sync each document once
/// while still preserving the ref kind attached to every definition
/// request.
#[derive(Debug, Clone, Copy)]
pub struct LspDefinitionCollector {
    /// Ref kind recorded for every resolved site this collector emits.
    pub ref_kind: RefKind,
    /// Grammar-specific extraction of the identifiers to resolve.
    pub collect_definition_sites: fn(&[u8]) -> Result<Vec<DefinitionSite>>,
}

/// Everything a language crate hands the substrate to run several
/// definition kinds over one document synchronization.
pub struct LspMultiKindDefinitionPass {
    /// Stable analyzer identifier, e.g. `"clangd-cpp-lsp"`.
    pub analyzer_id: &'static str,
    /// Optional analyzer id used only for pooling. Defaults to
    /// [`Self::analyzer_id`].
    pub pool_analyzer_id: Option<&'static str>,
    /// Pool-key language tag, e.g. `"clangd"`.
    pub language: &'static str,
    /// Launch and readiness settings for the pooled server.
    pub spawn_spec: LspSpawnSpec,
    pub retry: DefinitionRetryPolicy,
    /// Ref-kind-specific site extractors run against each source file.
    pub collectors: Vec<LspDefinitionCollector>,
    /// Some language servers return the unresolved use-site itself as
    /// the "definition". When enabled, target locations that point at
    /// any requested site in the same document are treated as unresolved.
    pub suppress_definition_targets_at_requested_sites: bool,
}

/// Run one LSP definition pass over `files` and return the resolved
/// refs as workspace facts.
///
/// # Errors
/// Always returns [`Error::Lsp`]. Beyond binary availability, spawn,
/// readiness, and protocol failures, a worktree file that cannot be
/// read also surfaces here: its IO error is flattened into an
/// `lsp::Error::Protocol` string by `core_error_to_lsp` rather than
/// preserved as a distinct variant, so callers must not match for a
/// separate IO error.
pub fn run_lsp_definition_pass(
    pass: LspDefinitionPass,
    repo_root: &Path,
    files: &[WorkspaceFile],
    progress: &AnalyzerProgress,
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
    let suppress_definition_targets_at_requested_sites =
        pass.suppress_definition_targets_at_requested_sites;
    let progress = progress.clone();
    pool.with_lsp(key, pass.spawn_spec, move |client| {
        Box::pin(async move {
            // `with_lsp` has taken pool ownership and finished readiness before
            // this closure runs, so arming here keeps queueing and server
            // startup out of the stall window and starts it at the first
            // request this pass issues.
            progress.arm_stall_watchdog();
            let mut facts = WorkspaceFacts::default();
            collect_resolved_refs(
                client,
                &repo_root,
                &files,
                analyzer_id,
                ref_kind,
                retry,
                collect,
                suppress_definition_targets_at_requested_sites,
                progress,
                &mut facts,
            )
            .await
            .map_err(core_error_to_lsp)?;
            Ok(facts)
        })
    })
    .map_err(Error::Lsp)
}

/// Run several LSP definition collectors over `files`, synchronizing
/// each document at most once per file.
///
/// # Errors
/// Always returns [`Error::Lsp`]. Beyond binary availability, spawn,
/// readiness, and protocol failures, a worktree file that cannot be
/// read also surfaces here: its IO error is flattened into an
/// `lsp::Error::Protocol` string by `core_error_to_lsp` rather than
/// preserved as a distinct variant, so callers must not match for a
/// separate IO error.
pub fn run_lsp_multi_kind_definition_pass(
    pass: LspMultiKindDefinitionPass,
    repo_root: &Path,
    files: &[WorkspaceFile],
    progress: &AnalyzerProgress,
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
    let retry = pass.retry;
    let collectors = pass.collectors;
    let suppress_definition_targets_at_requested_sites =
        pass.suppress_definition_targets_at_requested_sites;
    let progress = progress.clone();
    pool.with_lsp(key, pass.spawn_spec, move |client| {
        Box::pin(async move {
            // Same active-work boundary as `run_lsp_definition_pass`.
            progress.arm_stall_watchdog();
            let mut facts = WorkspaceFacts::default();
            collect_multi_kind_resolved_refs(
                client,
                &repo_root,
                &files,
                analyzer_id,
                retry,
                &collectors,
                suppress_definition_targets_at_requested_sites,
                progress,
                &mut facts,
            )
            .await
            .map_err(core_error_to_lsp)?;
            Ok(facts)
        })
    })
    .map_err(Error::Lsp)
}

/// Per-file driver for a single-kind pass: read the file from the
/// worktree, extract definition sites, sync the document once,
/// resolve every site concurrently, and append repo-mapped
/// [`ResolvedRef`]s to `facts`.
///
/// Degradation, from softest to hardest:
/// - files without a `worktree_path` are skipped (nothing on disk
///   for the language server to open);
/// - a failed definition request skips only that site (counted and
///   logged by the batch collector), unless consecutive request
///   timeouts exhaust the pass-wide circuit breaker;
/// - out-of-repo definition targets are kept with
///   `target_path = None`; the persist layer decides their fate;
/// - a failed `textDocument/didClose` only logs a warning;
/// - a file read error, document sync failure, or cancellation
///   aborts the whole pass.
#[allow(clippy::too_many_arguments)]
async fn collect_resolved_refs(
    client: &mut PooledLsp<'_>,
    repo_root: &Path,
    files: &[WorkspaceFile],
    analyzer_id: &'static str,
    ref_kind: RefKind,
    retry: DefinitionRetryPolicy,
    collect_definition_sites: fn(&[u8]) -> Result<Vec<DefinitionSite>>,
    suppress_definition_targets_at_requested_sites: bool,
    progress: AnalyzerProgress,
    facts: &mut WorkspaceFacts,
) -> Result<()> {
    let mut timeout_budget = DefinitionTimeoutBudget::default();
    for file in files {
        ensure_analyzer_active(&progress)?;
        let Some(path) = &file.worktree_path else {
            continue;
        };
        let source = std::fs::read_to_string(path)?;
        let sites = collect_definition_sites(source.as_bytes())?;
        if sites.is_empty() {
            progress.tick();
            continue;
        }
        let uri = Url::from_file_path(path).map_err(Error::Lsp)?;
        let sync_started = Instant::now();
        client
            .sync_document(&uri, &source)
            .await
            .map_err(Error::Lsp)?;
        let sync_elapsed = sync_started.elapsed();
        let site_count = sites.len();
        let definition_started = Instant::now();
        let resolved_batch = collect_definition_site_locations(
            sites,
            |site| client.definition(&uri, site.position),
            retry,
            analyzer_id,
            &uri,
            suppress_definition_targets_at_requested_sites,
            progress.clone(),
            &mut timeout_budget,
        )
        .await?;
        ensure_analyzer_active(&progress)?;
        let definition_elapsed = definition_started.elapsed();
        debug!(
            analyzer_id,
            path = %file.path,
            sites = site_count,
            resolved_sites = resolved_batch.resolved.len(),
            site_errors = resolved_batch.error_count,
            sync_elapsed_ms = sync_elapsed.as_millis(),
            definition_elapsed_ms = definition_elapsed.as_millis(),
            "LSP definition pass processed file"
        );
        for resolved_site in resolved_batch.resolved {
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
        if let Err(err) = client.close_document(&uri).await {
            warn!(
                analyzer_id,
                uri = uri.as_str(),
                error = %err,
                "failed to close LSP document after definition pass"
            );
        }
        progress.tick();
    }
    Ok(())
}

/// Multi-kind variant of `collect_resolved_refs`: runs every
/// collector over the file, tags each extracted site with its
/// collector's ref kind, and issues a single document sync for all
/// kinds combined. Degradation behaviour matches the single-kind
/// driver.
#[allow(clippy::too_many_arguments)]
async fn collect_multi_kind_resolved_refs(
    client: &mut PooledLsp<'_>,
    repo_root: &Path,
    files: &[WorkspaceFile],
    analyzer_id: &'static str,
    retry: DefinitionRetryPolicy,
    collectors: &[LspDefinitionCollector],
    suppress_definition_targets_at_requested_sites: bool,
    progress: AnalyzerProgress,
    facts: &mut WorkspaceFacts,
) -> Result<()> {
    let mut timeout_budget = DefinitionTimeoutBudget::default();
    for file in files {
        ensure_analyzer_active(&progress)?;
        let Some(path) = &file.worktree_path else {
            continue;
        };
        let read_started = Instant::now();
        let source = std::fs::read_to_string(path)?;
        let read_elapsed = read_started.elapsed();
        let collect_started = Instant::now();
        let mut sites = Vec::new();
        let mut kind_site_counts = Vec::with_capacity(collectors.len());
        for collector in collectors {
            let collected = (collector.collect_definition_sites)(source.as_bytes())?;
            kind_site_counts.push((collector.ref_kind, collected.len()));
            sites.extend(collected.into_iter().map(|site| DefinitionRequestSite {
                ref_kind: collector.ref_kind,
                site,
            }));
        }
        let collect_elapsed = collect_started.elapsed();
        if sites.is_empty() {
            progress.tick();
            continue;
        }
        let uri = Url::from_file_path(path).map_err(Error::Lsp)?;
        let sync_started = Instant::now();
        client
            .sync_document(&uri, &source)
            .await
            .map_err(Error::Lsp)?;
        let sync_elapsed = sync_started.elapsed();
        let site_count = sites.len();
        let definition_started = Instant::now();
        let resolved_batch = collect_multi_kind_definition_site_locations(
            sites,
            |site| client.definition(&uri, site.position),
            retry,
            analyzer_id,
            &uri,
            suppress_definition_targets_at_requested_sites,
            progress.clone(),
            &mut timeout_budget,
        )
        .await?;
        ensure_analyzer_active(&progress)?;
        let definition_elapsed = definition_started.elapsed();
        debug!(
            analyzer_id,
            path = %file.path,
            sites = site_count,
            kind_site_counts = %format_kind_counts(&kind_site_counts),
            resolved_sites = resolved_batch.resolved.len(),
            site_errors = resolved_batch.error_count,
            kind_error_counts = %format_kind_counts(&resolved_batch.error_counts_by_kind),
            read_elapsed_ms = read_elapsed.as_millis(),
            collect_elapsed_ms = collect_elapsed.as_millis(),
            sync_elapsed_ms = sync_elapsed.as_millis(),
            definition_elapsed_ms = definition_elapsed.as_millis(),
            "LSP multi-kind definition pass processed file"
        );
        for resolved_site in resolved_batch.resolved {
            for target in resolved_site.locations {
                let target_path = location_to_repo_path(repo_root, &target);
                facts.resolved_refs.push(ResolvedRef {
                    source_path: file.path.clone(),
                    source_position: resolved_site.site.position,
                    source_byte_range: resolved_site.site.byte_start..resolved_site.site.byte_end,
                    kind: resolved_site.ref_kind,
                    target,
                    target_path,
                });
            }
        }
        if let Err(err) = client.close_document(&uri).await {
            warn!(
                analyzer_id,
                uri = uri.as_str(),
                error = %err,
                "failed to close LSP document after multi-kind definition pass"
            );
        }
        progress.tick();
    }
    Ok(())
}

/// A site paired with the definition locations the server returned
/// (post-filtering; never empty once stored in a batch).
#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedDefinitionSite {
    site: DefinitionSite,
    locations: Vec<Location>,
}

/// A definition site tagged with the ref kind of the collector that
/// produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DefinitionRequestSite {
    ref_kind: RefKind,
    site: DefinitionSite,
}

/// [`ResolvedDefinitionSite`] plus the originating collector's kind.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedMultiKindDefinitionSite {
    ref_kind: RefKind,
    site: DefinitionSite,
    locations: Vec<Location>,
}

/// Outcome of resolving one document's sites: successfully resolved
/// sites plus a count of sites whose definition request failed.
#[derive(Debug, Default)]
struct DefinitionBatch {
    resolved: Vec<ResolvedDefinitionSite>,
    error_count: usize,
}

/// [`DefinitionBatch`] with per-kind error attribution for the
/// multi-kind debug log line.
#[derive(Debug, Default)]
struct MultiKindDefinitionBatch {
    resolved: Vec<ResolvedMultiKindDefinitionSite>,
    error_count: usize,
    error_counts_by_kind: Vec<(RefKind, usize)>,
}

/// Pass-wide circuit breaker for definition servers that remain live
/// but stop answering requests. The streak follows completion order,
/// which is the only order observable in the concurrent pipeline.
#[derive(Debug, Default)]
struct DefinitionTimeoutBudget {
    consecutive: usize,
}

impl DefinitionTimeoutBudget {
    fn observe<T>(&mut self, result: &Result<T>) -> Result<()> {
        if matches!(result, Err(Error::Lsp(crate::lsp::Error::RequestTimeout))) {
            self.consecutive += 1;
            if self.consecutive >= MAX_CONSECUTIVE_DEFINITION_TIMEOUTS {
                return Err(Error::Lsp(crate::lsp::Error::RequestTimeout));
            }
        } else {
            // Success, cancellation, and other terminal errors all break a
            // consecutive timeout streak. Retryable outcomes never reach this
            // layer: the per-site retry ladder resolves them first.
            self.consecutive = 0;
        }
        Ok(())
    }
}

/// Drive one pass-wide definition pipeline. Once the timeout budget trips,
/// the input iterator is never polled again, while requests that were already
/// in flight are drained so their client-side pending slots reach a terminal
/// outcome instead of being abandoned in the reusable LSP connection.
#[allow(clippy::too_many_arguments)]
async fn run_definition_pipeline<T, F, Fut, S>(
    requests: Vec<T>,
    site_of: S,
    definition: F,
    retry: DefinitionRetryPolicy,
    analyzer_id: &str,
    uri: &Url,
    progress: &AnalyzerProgress,
    timeout_budget: &mut DefinitionTimeoutBudget,
) -> Result<Vec<(T, Result<Vec<Location>>)>>
where
    F: Fn(DefinitionSite) -> Fut,
    Fut: Future<Output = crate::lsp::Result<Vec<Location>>>,
    S: Fn(&T) -> DefinitionSite,
{
    let mut pending = requests.into_iter();
    let make_request = |request: T| {
        let site = site_of(&request);
        let definition = &definition;
        async move {
            let result = definition_with_retry_from(
                || definition(site),
                retry,
                analyzer_id,
                uri,
                site.position,
                Some(progress),
            )
            .await;
            (request, result)
        }
    };
    let mut in_flight = FuturesUnordered::new();
    for request in pending.by_ref().take(DEFINITION_PIPELINE_CONCURRENCY) {
        in_flight.push(make_request(request));
    }

    let mut results = Vec::new();
    let mut budget_error = None;
    while let Some((request, result)) = in_flight.next().await {
        progress.tick();
        if budget_error.is_none() {
            match timeout_budget.observe(&result) {
                Ok(()) => {
                    if let Some(next) = pending.next() {
                        in_flight.push(make_request(next));
                    }
                }
                Err(err) => {
                    warn!(
                        analyzer_id,
                        uri = uri.as_str(),
                        limit = MAX_CONSECUTIVE_DEFINITION_TIMEOUTS,
                        "definition request timeout budget exhausted; aborting pass"
                    );
                    budget_error = Some(err);
                }
            }
        }
        results.push((request, result));
    }

    if let Some(err) = budget_error {
        Err(err)
    } else {
        Ok(results)
    }
}

/// Resolve every site through `definition` with bounded concurrency,
/// then re-sort by source position so batch output is deterministic
/// regardless of completion order. Failed sites are counted, logged,
/// and dropped; sites resolving to zero locations (after the optional
/// requested-site filter) are dropped silently. Cancellation is
/// observed per attempt inside the retry wrapper, so a cancelled
/// batch fails its remaining sites fast and the caller's post-batch
/// check aborts the pass. Consecutive terminal request timeouts abort
/// the pass after already in-flight requests have drained.
#[allow(clippy::too_many_arguments)]
async fn collect_definition_site_locations<F, Fut>(
    sites: Vec<DefinitionSite>,
    definition: F,
    retry: DefinitionRetryPolicy,
    analyzer_id: &str,
    uri: &Url,
    suppress_definition_targets_at_requested_sites: bool,
    progress: AnalyzerProgress,
    timeout_budget: &mut DefinitionTimeoutBudget,
) -> Result<DefinitionBatch>
where
    F: Fn(DefinitionSite) -> Fut,
    Fut: Future<Output = crate::lsp::Result<Vec<Location>>>,
{
    let requested_sites = sites.clone();
    let mut results = run_definition_pipeline(
        sites,
        |site| *site,
        definition,
        retry,
        analyzer_id,
        uri,
        &progress,
        timeout_budget,
    )
    .await?;
    results.sort_by_key(|(site, _)| {
        (
            site.position.line,
            site.position.character,
            site.byte_start,
            site.byte_end,
        )
    });

    let mut batch = DefinitionBatch::default();
    for (site, result) in results {
        match result {
            Ok(locations) => {
                let locations = filter_requested_site_locations(
                    locations,
                    uri,
                    &requested_sites,
                    suppress_definition_targets_at_requested_sites,
                );
                if !locations.is_empty() {
                    batch
                        .resolved
                        .push(ResolvedDefinitionSite { site, locations });
                }
            }
            Err(err) => {
                batch.error_count += 1;
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
    Ok(batch)
}

/// Multi-kind sibling of `collect_definition_site_locations`; the
/// extra ref-kind sort key keeps output deterministic when several
/// collectors emit sites at the same source position.
#[allow(clippy::too_many_arguments)]
async fn collect_multi_kind_definition_site_locations<F, Fut>(
    sites: Vec<DefinitionRequestSite>,
    definition: F,
    retry: DefinitionRetryPolicy,
    analyzer_id: &str,
    uri: &Url,
    suppress_definition_targets_at_requested_sites: bool,
    progress: AnalyzerProgress,
    timeout_budget: &mut DefinitionTimeoutBudget,
) -> Result<MultiKindDefinitionBatch>
where
    F: Fn(DefinitionSite) -> Fut,
    Fut: Future<Output = crate::lsp::Result<Vec<Location>>>,
{
    let requested_sites = sites.iter().map(|request| request.site).collect::<Vec<_>>();
    let mut results = run_definition_pipeline(
        sites,
        |request| request.site,
        definition,
        retry,
        analyzer_id,
        uri,
        &progress,
        timeout_budget,
    )
    .await?;
    results.sort_by_key(|(request, _)| {
        (
            request.site.position.line,
            request.site.position.character,
            request.site.byte_start,
            request.site.byte_end,
            ref_kind_sort_key(request.ref_kind),
        )
    });

    let mut batch = MultiKindDefinitionBatch::default();
    for (request, result) in results {
        match result {
            Ok(locations) => {
                let locations = filter_requested_site_locations(
                    locations,
                    uri,
                    &requested_sites,
                    suppress_definition_targets_at_requested_sites,
                );
                if !locations.is_empty() {
                    batch.resolved.push(ResolvedMultiKindDefinitionSite {
                        ref_kind: request.ref_kind,
                        site: request.site,
                        locations,
                    });
                }
            }
            Err(err) => {
                batch.error_count += 1;
                increment_kind_count(&mut batch.error_counts_by_kind, request.ref_kind);
                warn!(
                    analyzer_id,
                    uri = uri.as_str(),
                    ?request.site,
                    ref_kind = ?request.ref_kind,
                    error = %err,
                    "definition request failed; skipping site"
                );
            }
        }
    }
    Ok(batch)
}

/// Apply the opt-in "server echoed the use-site back" filter: when
/// `suppress` is set, drop target locations that point at any
/// requested site in the same document. The match is a heuristic on
/// (same URI, same range start), so a genuine definition that
/// coincides with a requested site is suppressed too.
fn filter_requested_site_locations(
    locations: Vec<Location>,
    uri: &Url,
    requested_sites: &[DefinitionSite],
    suppress: bool,
) -> Vec<Location> {
    if !suppress {
        return locations;
    }
    locations
        .into_iter()
        .filter(|location| !is_requested_site_location(location, uri, requested_sites))
        .collect()
}

/// True when the location starts exactly at one of the requested
/// sites in the same document. Only the range start is compared.
fn is_requested_site_location(
    location: &Location,
    uri: &Url,
    requested_sites: &[DefinitionSite],
) -> bool {
    location.uri == *uri
        && requested_sites
            .iter()
            .any(|site| location.range.start == site.position)
}

/// Issue one site's definition request with the retry ladder shared
/// by all LSP passes, under a single [`MAX_DEFINITION_ATTEMPTS`]
/// budget:
///
/// - content-modified responses: eligible for one retry after a
///   fixed delay when attempt budget remains (always on);
/// - empty location lists: eligible for one retry with backoff when
///   `retry_empty_definition` is set and attempt budget remains,
///   otherwise returned as-is;
/// - "file not found" errors: retried with doubling backoff while
///   the budget lasts, when `retry_file_not_found` is set;
/// - any other error is terminal.
///
/// Exhausting the attempt budget yields `Ok(vec![])` — the site is
/// treated as unresolved rather than failing the pass — but only
/// when no terminal error was hit along the way: a non-retryable
/// error (and a second content-modified) short-circuits to `Err`
/// immediately, so the budget can only run out across purely
/// retryable outcomes. Cancellation is checked before each attempt.
async fn definition_with_retry_from<F, Fut>(
    mut definition: F,
    policy: DefinitionRetryPolicy,
    analyzer_id: &str,
    uri: &Url,
    position: Position,
    progress: Option<&AnalyzerProgress>,
) -> Result<Vec<Location>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = crate::lsp::Result<Vec<Location>>>,
{
    let mut backoff = TRANSIENT_RETRY_BACKOFF;
    let mut retried_empty_definition = false;
    let mut retried_content_modified = false;
    for _attempt in 0..MAX_DEFINITION_ATTEMPTS {
        if progress.is_some_and(AnalyzerProgress::is_cancelled) {
            return Err(analyzer_cancelled_error());
        }
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

fn ensure_analyzer_active(progress: &AnalyzerProgress) -> Result<()> {
    if progress.is_cancelled() {
        Err(analyzer_cancelled_error())
    } else {
        Ok(())
    }
}

fn analyzer_cancelled_error() -> Error {
    Error::Internal("workspace analyzer cancelled".into())
}

/// Servers report the transient "file not found" condition without a
/// dedicated error code, so match on the message substring at both
/// the protocol and response-error levels.
fn is_file_not_found(err: &crate::lsp::Error) -> bool {
    matches!(err, crate::lsp::Error::Protocol(message) if message.contains("file not found"))
        || matches!(
            err,
            crate::lsp::Error::ResponseError { message, .. } if message.contains("file not found")
        )
}

/// Adapt crate errors to the pool closure's `lsp::Error` signature.
/// Non-LSP failures (worktree file reads, cancellation) are wrapped
/// as `Protocol` strings; the public entry points then wrap the
/// result back into [`Error::Lsp`], so those causes lose their
/// original error variant on the way out.
fn core_error_to_lsp(err: Error) -> crate::lsp::Error {
    match err {
        Error::Lsp(err) => err,
        err => crate::lsp::Error::Protocol(err.to_string()),
    }
}

fn increment_kind_count(counts: &mut Vec<(RefKind, usize)>, ref_kind: RefKind) {
    if let Some((_, count)) = counts.iter_mut().find(|(kind, _)| *kind == ref_kind) {
        *count += 1;
    } else {
        counts.push((ref_kind, 1));
    }
}

fn format_kind_counts(counts: &[(RefKind, usize)]) -> String {
    counts
        .iter()
        .map(|(kind, count)| format!("{}={count}", ref_kind_name(*kind)))
        .collect::<Vec<_>>()
        .join(",")
}

/// Fixed ordering used only as a sort tie-break so multi-kind batch
/// output is deterministic; the numbers carry no semantic priority.
fn ref_kind_sort_key(kind: RefKind) -> u8 {
    match kind {
        RefKind::Call => 0,
        RefKind::Type => 1,
        RefKind::Import => 2,
        RefKind::Instantiate => 3,
        RefKind::Read => 4,
        RefKind::Write => 5,
        RefKind::Override => 6,
        RefKind::MacroInvoke => 7,
        RefKind::Annotation => 8,
    }
}

fn ref_kind_name(kind: RefKind) -> &'static str {
    match kind {
        RefKind::Call => "call",
        RefKind::Type => "type",
        RefKind::Import => "import",
        RefKind::Instantiate => "instantiate",
        RefKind::Read => "read",
        RefKind::Write => "write",
        RefKind::Override => "override",
        RefKind::MacroInvoke => "macro_invoke",
        RefKind::Annotation => "annotation",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use crate::manifest::{ManifestEntry, ManifestId};
    use crate::workspace_analyzer::StallWatchdogEvent;
    #[cfg(unix)]
    use crate::workspace_analyzer::{
        AnalyzerRunRequest, RunStatus, WorkspaceAnalyzer, run_one_workspace_analyzer_with_timeout,
    };
    use std::cell::Cell;
    #[cfg(unix)]
    use std::fs;
    use std::future::ready;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    use std::time::Instant;
    use tokio::sync::Semaphore;

    #[cfg(unix)]
    use crate::lsp::pool::{AvailabilityStrategy, LspClientPool, ReadinessStrategy};
    use crate::lsp::{CONTENT_MODIFIED_ERROR_CODE, Range};
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    #[cfg(unix)]
    use std::path::PathBuf;

    #[cfg(unix)]
    const WATCHDOG_FAKE_LSP: &str = r#"#!/usr/bin/env python3
import json, os, sys, time

methods = os.environ["CAIRN_TEST_METHODS_FILE"]
initialize_marker = os.environ["CAIRN_TEST_INITIALIZE_MARKER"]
initialize_release = os.environ["CAIRN_TEST_INITIALIZE_RELEASE"]
sys.stderr = open(os.environ["CAIRN_TEST_STDERR_FILE"], "a", buffering=1)
pid_file = os.environ.get("CAIRN_TEST_PID_FILE")
if pid_file:
    with open(pid_file, "w") as pid_out:
        pid_out.write(str(os.getpid()))
        pid_out.flush()
        os.fsync(pid_out.fileno())

def read_message():
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line == b"\r\n":
            break
        key, _, value = line.decode().strip().partition(":")
        headers[key] = value
    return json.loads(sys.stdin.buffer.read(int(headers["Content-Length"])))

def respond(identifier, result):
    body = json.dumps({"jsonrpc": "2.0", "id": identifier, "result": result}).encode()
    sys.stdout.buffer.write(f"Content-Length: {len(body)}\r\n\r\n".encode() + body)
    sys.stdout.buffer.flush()

while True:
    message = read_message()
    if message is None:
        break
    method = message.get("method")
    if method:
        with open(methods, "a") as log:
            log.write(method + "\n")
            log.flush()
            os.fsync(log.fileno())
    if method == "initialize":
        with open(initialize_marker, "w") as marker:
            marker.write("initialize\n")
            marker.flush()
            os.fsync(marker.fileno())
        while not os.path.exists(initialize_release):
            time.sleep(0.001)
        respond(message["id"], {"capabilities": {}})
    elif method == "textDocument/definition" and not os.environ.get("CAIRN_TEST_WEDGE_DEFINITION"):
        respond(message["id"], [])
    elif method == "shutdown":
        respond(message["id"], None)
    elif method == "exit":
        break
"#;

    /// Serializes the placement carriers. They use distinct keys in the finite
    /// process-global LSP pool, so parallel carriers can turn an otherwise
    /// valid acquisition into PoolAtCapacity. The lock covers the whole
    /// carrier, including unwind/re-acquisition cleanup.
    #[cfg(unix)]
    static PLACEMENT_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

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

    #[cfg(unix)]
    fn one_definition_site(_source: &[u8]) -> Result<Vec<DefinitionSite>> {
        Ok(vec![DefinitionSite {
            position: Position {
                line: 0,
                character: 0,
            },
            byte_start: 0,
            byte_end: 1,
        }])
    }

    #[cfg(unix)]
    fn many_definition_sites(_source: &[u8]) -> Result<Vec<DefinitionSite>> {
        Ok((0..100).map(test_site).collect())
    }

    #[cfg(unix)]
    struct WedgedDefinitionAnalyzer {
        spawn_spec: LspSpawnSpec,
    }

    #[cfg(unix)]
    impl WorkspaceAnalyzer for WedgedDefinitionAnalyzer {
        fn id(&self) -> &'static str {
            "wedged-definition-budget-test"
        }

        fn revision(&self) -> u32 {
            1
        }

        fn language(&self) -> &'static str {
            "wedged-definition-test"
        }

        fn parser_id(&self) -> &'static str {
            "tree-sitter-wedged-definition-test"
        }

        fn defer_stall_watchdog_until_active_work(&self) -> bool {
            true
        }

        fn analyze_workspace(
            &self,
            repo_root: &Path,
            _manifest_id: ManifestId,
            files: &[WorkspaceFile],
            progress: &AnalyzerProgress,
        ) -> Result<WorkspaceFacts> {
            run_lsp_definition_pass(
                LspDefinitionPass {
                    analyzer_id: self.id(),
                    pool_analyzer_id: None,
                    language: self.language(),
                    ref_kind: RefKind::Call,
                    spawn_spec: self.spawn_spec.clone(),
                    retry: DefinitionRetryPolicy::default(),
                    collect_definition_sites: many_definition_sites,
                    suppress_definition_targets_at_requested_sites: false,
                },
                repo_root,
                files,
                progress,
            )
        }
    }

    #[cfg(unix)]
    fn wait_for_file_contents(path: &Path, expected: &str, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if fs::read_to_string(path).is_ok_and(|contents| contents == expected) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        false
    }

    #[cfg(unix)]
    fn wait_for_logged_definition(path: &Path) -> bool {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if fs::read_to_string(path).is_ok_and(|methods| {
                methods
                    .lines()
                    .any(|method| method == "textDocument/definition")
            }) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        false
    }

    #[cfg(unix)]
    fn watchdog_fixture_evidence(
        methods: &Path,
        marker: &Path,
        release: &Path,
        stderr: &Path,
    ) -> String {
        format!(
            "methods={:?}, marker={:?}, release_exists={}, stderr={:?}",
            fs::read_to_string(methods),
            fs::read_to_string(marker),
            release.exists(),
            fs::read_to_string(stderr),
        )
    }

    #[cfg(unix)]
    fn wait_for_active_leases(
        pool: &LspClientPool,
        key: &PoolKey,
        expected: usize,
        timeout: Duration,
    ) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if pool.active_leases(key) == Some(expected) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        false
    }

    #[cfg(unix)]
    struct PlacementThreadGuard {
        initialize_release: PathBuf,
        owner_work_release: Option<mpsc::SyncSender<()>>,
        owner: Option<std::thread::JoinHandle<()>>,
        runner: Option<std::thread::JoinHandle<()>>,
    }

    #[cfg(unix)]
    struct PlacementGuardUnwindSentinel;

    #[cfg(unix)]
    struct ForeignGuardUnwindSentinel;

    #[cfg(unix)]
    struct PlacementPoolCleanupGuard {
        pool: &'static LspClientPool,
        key: Option<PoolKey>,
    }

    #[cfg(unix)]
    impl PlacementPoolCleanupGuard {
        fn new(pool: &'static LspClientPool, key: PoolKey) -> Self {
            Self {
                pool,
                key: Some(key),
            }
        }

        fn cleanup(&mut self) -> crate::lsp::Result<()> {
            let Some(key) = self.key.take() else {
                return Ok(());
            };
            self.pool.remove_idle_test_entry(&key)
        }

        fn finish(mut self) -> crate::lsp::Result<()> {
            self.cleanup()
        }
    }

    #[cfg(unix)]
    impl Drop for PlacementPoolCleanupGuard {
        fn drop(&mut self) {
            let _ = self.cleanup();
        }
    }

    #[cfg(unix)]
    impl PlacementThreadGuard {
        fn cleanup(&mut self) {
            let _ = fs::write(&self.initialize_release, "release\n");
            if let Some(release) = self.owner_work_release.take() {
                let _ = release.send(());
            }
            if let Some(owner) = self.owner.take() {
                let _ = owner.join();
            }
            if let Some(runner) = self.runner.take() {
                let _ = runner.join();
            }
        }

        fn finish(mut self) {
            self.cleanup();
        }

        fn release_owner_work(&mut self) {
            if let Some(release) = self.owner_work_release.take() {
                let _ = release.send(());
            }
        }
    }

    #[cfg(unix)]
    impl Drop for PlacementThreadGuard {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    #[cfg(unix)]
    fn process_is_alive(pid: u32) -> bool {
        std::process::Command::new("/bin/kill")
            .args(["-0", &pid.to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }

    #[cfg(unix)]
    fn assert_production_pass_arms_after_pool_readiness(
        multi: bool,
        overlap_isolated_cleanup: bool,
    ) {
        let _placement_lock = PLACEMENT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = tempfile::tempdir().unwrap();
        let binary = fixture.path().join("watchdog-fake-lsp.py");
        let methods = fixture.path().join("methods.log");
        let initialize_marker = fixture.path().join("initialize.marker");
        let initialize_release = fixture.path().join("initialize.release");
        let stderr = fixture.path().join("stderr.log");
        let pid_file = fixture.path().join("pid");
        let source = fixture.path().join("source.rb");
        fs::write(&binary, WATCHDOG_FAKE_LSP).unwrap();
        let mut permissions = fs::metadata(&binary).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&binary, permissions).unwrap();
        fs::write(&source, "target\n").unwrap();

        let analyzer_id = if multi {
            "watchdog-multi-test"
        } else {
            "watchdog-single-test"
        };
        let spawn_spec = LspSpawnSpec {
            binary: binary.clone(),
            workspace_root: fixture.path().to_path_buf(),
            config_hash: analyzer_id.to_string(),
            request_timeout: Duration::from_secs(30),
            availability: AvailabilityStrategy::PathExistsExecutable,
            readiness: ReadinessStrategy::InitializeResponseOnly,
            language_id: "ruby",
            launch_args: Vec::new(),
            env: vec![
                (
                    "CAIRN_TEST_METHODS_FILE".into(),
                    methods.display().to_string(),
                ),
                (
                    "CAIRN_TEST_INITIALIZE_MARKER".into(),
                    initialize_marker.display().to_string(),
                ),
                (
                    "CAIRN_TEST_INITIALIZE_RELEASE".into(),
                    initialize_release.display().to_string(),
                ),
                (
                    "CAIRN_TEST_STDERR_FILE".into(),
                    stderr.display().to_string(),
                ),
                ("CAIRN_TEST_PID_FILE".into(), pid_file.display().to_string()),
            ],
            initialization_options: serde_json::json!({}),
        };
        let key = PoolKey::lsp(
            "watchdog-test",
            fixture.path(),
            analyzer_id,
            &binary,
            analyzer_id,
        )
        .unwrap();
        let pool = lsp_pool::global().unwrap();
        let pool_cleanup = PlacementPoolCleanupGuard::new(pool, key.clone());
        let (owner_ready_tx, owner_ready_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let owner_spec = spawn_spec.clone();
        let owner_key = key.clone();
        let owner = std::thread::spawn(move || {
            let _ = pool.with_lsp(owner_key, owner_spec, move |_client| {
                Box::pin(async move {
                    let _ = owner_ready_tx.send(());
                    let _ = release_rx.recv();
                    Ok(())
                })
            });
        });
        let mut threads = PlacementThreadGuard {
            initialize_release: initialize_release.clone(),
            owner_work_release: Some(release_tx),
            owner: Some(owner),
            runner: None,
        };
        assert!(
            wait_for_file_contents(&initialize_marker, "initialize\n", Duration::from_secs(10),),
            "fake LSP did not publish and flush initialize receipt: {}",
            watchdog_fixture_evidence(&methods, &initialize_marker, &initialize_release, &stderr,)
        );
        assert!(
            wait_for_active_leases(pool, &key, 1, Duration::from_secs(10)),
            "owner lease did not reach one: {}",
            watchdog_fixture_evidence(&methods, &initialize_marker, &initialize_release, &stderr,)
        );
        match owner_ready_rx.try_recv() {
            Err(mpsc::TryRecvError::Empty) => {}
            Err(mpsc::TryRecvError::Disconnected) => {
                panic!("owner readiness channel disconnected before initialize release")
            }
            Ok(()) => panic!("owner became ready before initialize release"),
        }

        let (events_tx, events_rx) = mpsc::channel();
        let (progress, _cancel_guard) = AnalyzerProgress::default().with_watchdog_events(events_tx);
        let files = vec![WorkspaceFile {
            path: "source.rb".into(),
            blob_sha: "test".into(),
            worktree_path: Some(source),
            source_bytes: None,
        }];
        let root = fixture.path().to_path_buf();
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let runner = std::thread::spawn(move || {
            let result = if multi {
                run_lsp_multi_kind_definition_pass(
                    LspMultiKindDefinitionPass {
                        analyzer_id,
                        pool_analyzer_id: None,
                        language: "watchdog-test",
                        spawn_spec,
                        retry: DefinitionRetryPolicy::default(),
                        collectors: vec![LspDefinitionCollector {
                            ref_kind: RefKind::Call,
                            collect_definition_sites: one_definition_site,
                        }],
                        suppress_definition_targets_at_requested_sites: false,
                    },
                    &root,
                    &files,
                    &progress,
                )
            } else {
                run_lsp_definition_pass(
                    LspDefinitionPass {
                        analyzer_id,
                        pool_analyzer_id: None,
                        language: "watchdog-test",
                        ref_kind: RefKind::Call,
                        spawn_spec,
                        retry: DefinitionRetryPolicy::default(),
                        collect_definition_sites: one_definition_site,
                        suppress_definition_targets_at_requested_sites: false,
                    },
                    &root,
                    &files,
                    &progress,
                )
            };
            let _ = result_tx.send(result);
        });
        threads.runner = Some(runner);

        let lease_deadline = Instant::now() + Duration::from_secs(10);
        while pool.active_leases(&key) != Some(2) {
            match events_rx.try_recv() {
                Err(mpsc::TryRecvError::Empty) => {}
                Err(mpsc::TryRecvError::Disconnected) => {
                    panic!("placement watchdog channel disconnected before lease acquisition")
                }
                Ok(_) => panic!(
                    "queued pass armed before acquiring its pool lease: {}",
                    watchdog_fixture_evidence(
                        &methods,
                        &initialize_marker,
                        &initialize_release,
                        &stderr,
                    )
                ),
            }
            match result_rx.try_recv() {
                Err(mpsc::TryRecvError::Empty) => {}
                Err(mpsc::TryRecvError::Disconnected) => panic!(
                    "placement runner disconnected before lease acquisition: {}",
                    watchdog_fixture_evidence(
                        &methods,
                        &initialize_marker,
                        &initialize_release,
                        &stderr,
                    )
                ),
                Ok(Ok(_)) => panic!(
                    "placement runner completed before lease acquisition: {}",
                    watchdog_fixture_evidence(
                        &methods,
                        &initialize_marker,
                        &initialize_release,
                        &stderr,
                    )
                ),
                Ok(Err(err)) => panic!(
                    "placement runner failed before lease acquisition ({err:?}): {}",
                    watchdog_fixture_evidence(
                        &methods,
                        &initialize_marker,
                        &initialize_release,
                        &stderr,
                    )
                ),
            }
            assert!(
                Instant::now() < lease_deadline,
                "queued placement lease did not reach two: {}",
                watchdog_fixture_evidence(
                    &methods,
                    &initialize_marker,
                    &initialize_release,
                    &stderr,
                )
            );
            std::thread::yield_now();
        }
        match events_rx.try_recv() {
            Err(mpsc::TryRecvError::Empty) => {}
            Err(mpsc::TryRecvError::Disconnected) => {
                panic!("placement watchdog channel disconnected while queued")
            }
            Ok(_) => panic!(
                "queued pass armed before initialize release: {}",
                watchdog_fixture_evidence(
                    &methods,
                    &initialize_marker,
                    &initialize_release,
                    &stderr,
                )
            ),
        }
        if overlap_isolated_cleanup {
            super::super::run::test_run_isolated_active_stall();
        }
        fs::write(&initialize_release, "release\n").unwrap();
        owner_ready_rx
            .recv_timeout(Duration::from_secs(30))
            .unwrap_or_else(|err| {
                panic!(
                    "owner did not become ready after initialize release ({err:?}): {}",
                    watchdog_fixture_evidence(
                        &methods,
                        &initialize_marker,
                        &initialize_release,
                        &stderr,
                    )
                )
            });
        threads.release_owner_work();
        let StallWatchdogEvent::Arm(acknowledgement) = events_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap_or_else(|err| {
                panic!(
                    "placement pass did not arm after owner release ({err:?}): {}",
                    watchdog_fixture_evidence(
                        &methods,
                        &initialize_marker,
                        &initialize_release,
                        &stderr,
                    )
                )
            })
        else {
            panic!("active-work boundary must emit Arm");
        };
        assert!(
            !fs::read_to_string(&methods)
                .unwrap_or_default()
                .lines()
                .any(|method| method == "textDocument/definition"),
            "Arm must be processed before the first active request"
        );
        acknowledgement.send(()).unwrap();
        result_rx
            .recv_timeout(Duration::from_secs(30))
            .unwrap_or_else(|err| {
                panic!(
                    "placement pass did not finish ({err:?}): {}",
                    watchdog_fixture_evidence(
                        &methods,
                        &initialize_marker,
                        &initialize_release,
                        &stderr,
                    )
                )
            })
            .unwrap();
        assert!(
            wait_for_logged_definition(&methods),
            "definition request was not logged: {}",
            watchdog_fixture_evidence(&methods, &initialize_marker, &initialize_release, &stderr,)
        );
        threads.finish();
        pool_cleanup
            .finish()
            .expect("placement test pool entry cleanup must succeed");
    }

    #[cfg(unix)]
    fn assert_placement_guard_actual_unwind() {
        let _placement_lock = PLACEMENT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = tempfile::tempdir().unwrap();
        let binary = fixture.path().join("watchdog-fake-lsp.py");
        let methods = fixture.path().join("methods.log");
        let initialize_marker = fixture.path().join("initialize.marker");
        let initialize_release = fixture.path().join("initialize.release");
        let stderr = fixture.path().join("stderr.log");
        let pid_file = fixture.path().join("pid");
        let source = fixture.path().join("source.rb");
        fs::write(&binary, WATCHDOG_FAKE_LSP).unwrap();
        let mut permissions = fs::metadata(&binary).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&binary, permissions).unwrap();
        fs::write(&source, "target\n").unwrap();

        let analyzer_id = "watchdog-unwind-test";
        let spawn_spec = LspSpawnSpec {
            binary: binary.clone(),
            workspace_root: fixture.path().to_path_buf(),
            config_hash: analyzer_id.to_string(),
            request_timeout: Duration::from_secs(30),
            availability: AvailabilityStrategy::PathExistsExecutable,
            readiness: ReadinessStrategy::InitializeResponseOnly,
            language_id: "ruby",
            launch_args: Vec::new(),
            env: vec![
                (
                    "CAIRN_TEST_METHODS_FILE".into(),
                    methods.display().to_string(),
                ),
                (
                    "CAIRN_TEST_INITIALIZE_MARKER".into(),
                    initialize_marker.display().to_string(),
                ),
                (
                    "CAIRN_TEST_INITIALIZE_RELEASE".into(),
                    initialize_release.display().to_string(),
                ),
                (
                    "CAIRN_TEST_STDERR_FILE".into(),
                    stderr.display().to_string(),
                ),
                ("CAIRN_TEST_PID_FILE".into(), pid_file.display().to_string()),
            ],
            initialization_options: serde_json::json!({}),
        };
        let key = PoolKey::lsp(
            "watchdog-test",
            fixture.path(),
            analyzer_id,
            &binary,
            analyzer_id,
        )
        .unwrap();
        let pool = lsp_pool::global().unwrap();
        let reacquire_spec = spawn_spec.clone();
        let foreign_key = PoolKey::lsp(
            "watchdog-test",
            fixture.path(),
            "watchdog-foreign-sentinel",
            &binary,
            "watchdog-foreign-sentinel",
        )
        .unwrap();
        // A second live entry proves exact target-only cleanup. Capacity one
        // cannot retain the foreign control and target entry together, so the
        // control is skipped.
        let preserve_foreign = pool.capacity_for_test() > 1;
        let foreign_cleanup = if preserve_foreign {
            let foreign_pid = Cell::new(None);
            let foreign_unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                fs::write(&initialize_release, "release\n").unwrap();
                pool.with_lsp(foreign_key.clone(), spawn_spec.clone(), |_client| {
                    Box::pin(async move { Ok(()) })
                })
                .expect("foreign unwind sentinel entry must start");
                let _foreign_cleanup = PlacementPoolCleanupGuard::new(pool, foreign_key.clone());
                foreign_pid.set(Some(
                    fs::read_to_string(&pid_file)
                        .unwrap()
                        .parse::<u32>()
                        .unwrap(),
                ));
                std::panic::panic_any(ForeignGuardUnwindSentinel);
            }));
            match foreign_unwind {
                Err(payload) if payload.is::<ForeignGuardUnwindSentinel>() => {}
                Err(payload) => std::panic::resume_unwind(payload),
                Ok(()) => panic!("foreign cleanup carrier must panic"),
            }
            assert_eq!(
                pool.active_leases(&foreign_key),
                None,
                "foreign record survived unwind"
            );
            let foreign_pid = foreign_pid
                .get()
                .expect("foreign child PID must be captured");
            assert!(
                !process_is_alive(foreign_pid),
                "foreign child {foreign_pid} survived unwind"
            );
            for path in [&initialize_release, &initialize_marker, &methods, &pid_file] {
                let _ = fs::remove_file(path);
            }

            fs::write(&initialize_release, "release\n").unwrap();
            pool.with_lsp(foreign_key.clone(), spawn_spec.clone(), |_client| {
                Box::pin(async move { Ok(()) })
            })
            .expect("foreign sentinel entry must start");
            let foreign_cleanup = PlacementPoolCleanupGuard::new(pool, foreign_key.clone());
            for path in [&initialize_release, &initialize_marker, &methods, &pid_file] {
                let _ = fs::remove_file(path);
            }
            Some(foreign_cleanup)
        } else {
            None
        };
        let old_pid = Cell::new(None);

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _pool_cleanup = PlacementPoolCleanupGuard::new(pool, key.clone());
            let (owner_ready_tx, owner_ready_rx) = mpsc::sync_channel(1);
            let (release_tx, release_rx) = mpsc::sync_channel(0);
            let owner_spec = spawn_spec.clone();
            let owner_key = key.clone();
            let owner = std::thread::spawn(move || {
                let _ = pool.with_lsp(owner_key, owner_spec, move |_client| {
                    Box::pin(async move {
                        let _ = owner_ready_tx.send(());
                        let _ = release_rx.recv();
                        Ok(())
                    })
                });
            });
            let mut threads = PlacementThreadGuard {
                initialize_release: initialize_release.clone(),
                owner_work_release: Some(release_tx),
                owner: Some(owner),
                runner: None,
            };
            assert!(wait_for_file_contents(
                &initialize_marker,
                "initialize\n",
                Duration::from_secs(10),
            ));
            old_pid.set(Some(
                fs::read_to_string(&pid_file)
                    .unwrap()
                    .parse::<u32>()
                    .unwrap(),
            ));
            assert!(wait_for_active_leases(
                pool,
                &key,
                1,
                Duration::from_secs(10),
            ));
            assert!(matches!(
                owner_ready_rx.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));

            let (events_tx, events_rx) = mpsc::channel();
            let (progress, _cancel_guard) =
                AnalyzerProgress::default().with_watchdog_events(events_tx);
            let files = vec![WorkspaceFile {
                path: "source.rb".into(),
                blob_sha: "test".into(),
                worktree_path: Some(source.clone()),
                source_bytes: None,
            }];
            let root = fixture.path().to_path_buf();
            let runner_spec = spawn_spec.clone();
            let (result_tx, _result_rx) = mpsc::sync_channel(1);
            let runner = std::thread::spawn(move || {
                let result = run_lsp_definition_pass(
                    LspDefinitionPass {
                        analyzer_id,
                        pool_analyzer_id: None,
                        language: "watchdog-test",
                        ref_kind: RefKind::Call,
                        spawn_spec: runner_spec,
                        retry: DefinitionRetryPolicy::default(),
                        collect_definition_sites: one_definition_site,
                        suppress_definition_targets_at_requested_sites: false,
                    },
                    &root,
                    &files,
                    &progress,
                );
                let _ = result_tx.send(result);
            });
            threads.runner = Some(runner);

            let deadline = Instant::now() + Duration::from_secs(10);
            while pool.active_leases(&key) != Some(2) {
                assert!(matches!(
                    events_rx.try_recv(),
                    Err(mpsc::TryRecvError::Empty)
                ));
                assert!(Instant::now() < deadline);
                std::thread::yield_now();
            }
            assert!(matches!(
                events_rx.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            std::panic::panic_any(PlacementGuardUnwindSentinel);
        }));

        match unwind {
            Err(payload) if payload.is::<PlacementGuardUnwindSentinel>() => {}
            Err(payload) => std::panic::resume_unwind(payload),
            Ok(()) => panic!("natural unwind carrier must panic"),
        }
        assert_eq!(
            pool.active_leases(&key),
            None,
            "target record survived unwind"
        );
        let old_pid = old_pid.get().expect("target child PID must be captured");
        assert!(
            !process_is_alive(old_pid),
            "target child {old_pid} survived unwind"
        );
        assert!(
            pool.is_running_for_test(),
            "unwind cleanup stopped the pool"
        );
        if preserve_foreign {
            assert_eq!(
                pool.active_leases(&foreign_key),
                Some(0),
                "target cleanup changed the foreign sentinel record"
            );
        }

        let successor_cleanup = PlacementPoolCleanupGuard::new(pool, key.clone());
        pool.with_lsp(key.clone(), reacquire_spec, |_client| {
            Box::pin(async move { Ok(()) })
        })
        .expect("placement acquisition after natural unwind must succeed");
        successor_cleanup
            .finish()
            .expect("placement unwind pool entry cleanup must succeed");
        if let Some(foreign_cleanup) = foreign_cleanup {
            foreign_cleanup
                .finish()
                .expect("foreign sentinel cleanup must succeed");
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
            None,
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
        let progress = AnalyzerProgress::default();
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
            false,
            progress.clone(),
            &mut DefinitionTimeoutBudget::default(),
        )
        .await
        .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 100);
        assert_eq!(progress.snapshot(), 100);
        assert_eq!(resolved.resolved.len(), 100);
        assert_eq!(resolved.error_count, 0);
        assert!(
            start.elapsed() < Duration::from_millis(200),
            "definition pipeline took {:?}",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn cancelled_definition_stream_starts_no_new_requests() {
        let sites = (0..100).map(test_site).collect::<Vec<_>>();
        let calls = Arc::new(AtomicUsize::new(0));
        let progress = AnalyzerProgress::default();
        progress.cancel();
        let resolved = collect_definition_site_locations(
            sites,
            {
                let calls = Arc::clone(&calls);
                move |_site| {
                    let calls = Arc::clone(&calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, crate::lsp::Error>(Vec::new())
                    }
                }
            },
            DefinitionRetryPolicy::default(),
            "test-lsp",
            &test_uri(),
            false,
            progress,
            &mut DefinitionTimeoutBudget::default(),
        )
        .await
        .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(resolved.resolved.len(), 0);
    }

    #[tokio::test]
    async fn consecutive_definition_timeouts_stop_pipeline_replenishment() {
        let sites = (0..100).map(test_site).collect::<Vec<_>>();
        let calls = Arc::new(AtomicUsize::new(0));
        let release_initial_wave = Arc::new(Semaphore::new(0));
        let progress = AnalyzerProgress::default();
        let err = collect_definition_site_locations(
            sites,
            {
                let calls = Arc::clone(&calls);
                let release_initial_wave = Arc::clone(&release_initial_wave);
                move |_site| {
                    let calls = Arc::clone(&calls);
                    let release_initial_wave = Arc::clone(&release_initial_wave);
                    async move {
                        if calls.fetch_add(1, Ordering::SeqCst) + 1
                            == DEFINITION_PIPELINE_CONCURRENCY
                        {
                            release_initial_wave.add_permits(100);
                        }
                        release_initial_wave.acquire().await.unwrap().forget();
                        Err(crate::lsp::Error::RequestTimeout)
                    }
                }
            },
            DefinitionRetryPolicy::default(),
            "wedged-test-lsp",
            &test_uri(),
            false,
            progress.clone(),
            &mut DefinitionTimeoutBudget::default(),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, Error::Lsp(crate::lsp::Error::RequestTimeout)),
            "unexpected wedged-pass error: {err:?}"
        );
        assert_eq!(
            progress.snapshot(),
            (DEFINITION_PIPELINE_CONCURRENCY + MAX_CONSECUTIVE_DEFINITION_TIMEOUTS - 1) as u64
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            DEFINITION_PIPELINE_CONCURRENCY + MAX_CONSECUTIVE_DEFINITION_TIMEOUTS - 1,
            "only requests admitted before the threshold may start"
        );
    }

    #[test]
    fn timeout_budget_resets_on_success_transient_and_cancellation() {
        let mut budget = DefinitionTimeoutBudget::default();
        let prime = |budget: &mut DefinitionTimeoutBudget| {
            for _ in 0..MAX_CONSECUTIVE_DEFINITION_TIMEOUTS - 1 {
                budget
                    .observe::<()>(&Err(Error::Lsp(crate::lsp::Error::RequestTimeout)))
                    .unwrap();
            }
        };

        prime(&mut budget);
        budget
            .observe(&Ok::<Vec<Location>, Error>(Vec::new()))
            .unwrap();
        assert_eq!(budget.consecutive, 0);
        prime(&mut budget);
        budget
            .observe::<()>(&Err(Error::Lsp(content_modified())))
            .unwrap();
        assert_eq!(budget.consecutive, 0);
        prime(&mut budget);
        budget
            .observe::<()>(&Err(analyzer_cancelled_error()))
            .unwrap();
        assert_eq!(budget.consecutive, 0);
    }

    #[tokio::test]
    async fn definition_site_errors_skip_only_the_failed_site() {
        let sites = (0..5).map(test_site).collect::<Vec<_>>();
        let progress = AnalyzerProgress::default();
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
            false,
            progress.clone(),
            &mut DefinitionTimeoutBudget::default(),
        )
        .await
        .unwrap();

        assert_eq!(progress.snapshot(), 5);
        assert_eq!(resolved.error_count, 1);
        let lines = resolved
            .resolved
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
            false,
            AnalyzerProgress::default(),
            &mut DefinitionTimeoutBudget::default(),
        )
        .await
        .unwrap();

        let lines = resolved
            .resolved
            .iter()
            .map(|resolved| resolved.site.position.line)
            .collect::<Vec<_>>();
        assert_eq!(lines, vec![1, 5, 9]);
    }

    #[tokio::test]
    async fn definition_site_locations_can_suppress_requested_site_echoes() {
        let sites = vec![test_site(1), test_site(2), test_site(3)];
        let resolved = collect_definition_site_locations(
            sites,
            |site| async move {
                let target_line = match site.position.line {
                    // Direct unresolved-use echo.
                    1 => 1,
                    // Cross-use echo: another requested call-site in
                    // the same document, observed from clangd when a C
                    // fallback compile cannot resolve external APIs.
                    2 => 1,
                    // A real definition outside requested sites.
                    _ => 9,
                };
                Ok::<_, crate::lsp::Error>(vec![test_location_at(target_line)])
            },
            DefinitionRetryPolicy::default(),
            "test-lsp",
            &test_uri(),
            true,
            AnalyzerProgress::default(),
            &mut DefinitionTimeoutBudget::default(),
        )
        .await
        .unwrap();

        assert_eq!(resolved.resolved.len(), 1);
        assert_eq!(resolved.resolved[0].site.position.line, 3);
        assert_eq!(resolved.resolved[0].locations, vec![test_location_at(9)]);
    }

    #[tokio::test]
    async fn multi_kind_definition_sites_preserve_ref_kind() {
        let sites = vec![
            DefinitionRequestSite {
                ref_kind: RefKind::Import,
                site: test_site(2),
            },
            DefinitionRequestSite {
                ref_kind: RefKind::Call,
                site: test_site(1),
            },
        ];
        let resolved =
            collect_multi_kind_definition_site_locations(
                sites,
                |site| async move {
                    Ok::<_, crate::lsp::Error>(vec![test_location_at(site.position.line)])
                },
                DefinitionRetryPolicy::default(),
                "test-lsp",
                &test_uri(),
                false,
                AnalyzerProgress::default(),
                &mut DefinitionTimeoutBudget::default(),
            )
            .await
            .unwrap();

        let observed = resolved
            .resolved
            .iter()
            .map(|resolved| (resolved.ref_kind, resolved.site.position.line))
            .collect::<Vec<_>>();
        assert_eq!(observed, vec![(RefKind::Call, 1), (RefKind::Import, 2)]);
        assert_eq!(resolved.error_count, 0);
    }

    #[tokio::test]
    async fn multi_kind_definition_site_errors_are_counted_by_kind() {
        let sites = vec![
            DefinitionRequestSite {
                ref_kind: RefKind::Call,
                site: test_site(1),
            },
            DefinitionRequestSite {
                ref_kind: RefKind::Import,
                site: test_site(2),
            },
            DefinitionRequestSite {
                ref_kind: RefKind::Import,
                site: test_site(3),
            },
        ];
        let progress = AnalyzerProgress::default();
        let resolved = collect_multi_kind_definition_site_locations(
            sites,
            |site| async move {
                if site.position.line == 1 {
                    Ok(vec![test_location_at(site.position.line)])
                } else {
                    Err(crate::lsp::Error::Protocol("boom".into()))
                }
            },
            DefinitionRetryPolicy::default(),
            "test-lsp",
            &test_uri(),
            false,
            progress.clone(),
            &mut DefinitionTimeoutBudget::default(),
        )
        .await
        .unwrap();

        assert_eq!(progress.snapshot(), 3);
        assert_eq!(resolved.resolved.len(), 1);
        assert_eq!(resolved.error_count, 2);
        assert_eq!(resolved.error_counts_by_kind, vec![(RefKind::Import, 2)]);
    }

    #[tokio::test]
    async fn multi_kind_definition_sites_can_suppress_requested_site_echoes() {
        let sites = vec![
            DefinitionRequestSite {
                ref_kind: RefKind::Call,
                site: test_site(1),
            },
            DefinitionRequestSite {
                ref_kind: RefKind::Import,
                site: test_site(2),
            },
        ];
        let resolved = collect_multi_kind_definition_site_locations(
            sites,
            |site| async move {
                let target_line = if site.position.line == 1 { 2 } else { 9 };
                Ok::<_, crate::lsp::Error>(vec![test_location_at(target_line)])
            },
            DefinitionRetryPolicy::default(),
            "test-lsp",
            &test_uri(),
            true,
            AnalyzerProgress::default(),
            &mut DefinitionTimeoutBudget::default(),
        )
        .await
        .unwrap();

        assert_eq!(resolved.resolved.len(), 1);
        assert_eq!(resolved.resolved[0].ref_kind, RefKind::Import);
        assert_eq!(resolved.resolved[0].site.position.line, 2);
        assert_eq!(resolved.resolved[0].locations, vec![test_location_at(9)]);
    }

    #[cfg(unix)]
    #[test]
    fn single_definition_pass_arms_after_pool_readiness_before_request() {
        assert_production_pass_arms_after_pool_readiness(false, false);
    }

    #[cfg(unix)]
    #[test]
    fn wedged_definition_server_hits_shared_budget_and_stops_scheduling() {
        let _placement_lock = PLACEMENT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = tempfile::tempdir().unwrap();
        let binary = fixture.path().join("wedged-definition-lsp.py");
        let methods = fixture.path().join("methods.log");
        let initialize_marker = fixture.path().join("initialize.marker");
        let initialize_release = fixture.path().join("initialize.release");
        let stderr = fixture.path().join("stderr.log");
        let source = fixture.path().join("source.rb");
        fs::write(&binary, WATCHDOG_FAKE_LSP).unwrap();
        let mut permissions = fs::metadata(&binary).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&binary, permissions).unwrap();
        fs::write(&initialize_release, "release\n").unwrap();
        fs::write(&source, "target\n").unwrap();

        let analyzer_id = "wedged-definition-budget-test";
        let spawn_spec = LspSpawnSpec {
            binary: binary.clone(),
            workspace_root: fixture.path().to_path_buf(),
            config_hash: analyzer_id.to_string(),
            request_timeout: Duration::from_secs(1),
            availability: AvailabilityStrategy::PathExistsExecutable,
            readiness: ReadinessStrategy::InitializeResponseOnly,
            language_id: "ruby",
            launch_args: Vec::new(),
            env: vec![
                (
                    "CAIRN_TEST_METHODS_FILE".into(),
                    methods.display().to_string(),
                ),
                (
                    "CAIRN_TEST_INITIALIZE_MARKER".into(),
                    initialize_marker.display().to_string(),
                ),
                (
                    "CAIRN_TEST_INITIALIZE_RELEASE".into(),
                    initialize_release.display().to_string(),
                ),
                (
                    "CAIRN_TEST_STDERR_FILE".into(),
                    stderr.display().to_string(),
                ),
                ("CAIRN_TEST_WEDGE_DEFINITION".into(), "1".into()),
            ],
            initialization_options: serde_json::json!({}),
        };
        let key = PoolKey::lsp(
            "wedged-definition-test",
            fixture.path(),
            analyzer_id,
            &binary,
            analyzer_id,
        )
        .unwrap();
        let pool_cleanup = PlacementPoolCleanupGuard::new(lsp_pool::global().unwrap(), key);
        let progress = AnalyzerProgress::default();
        let store_path = fixture.path().join("store.db");
        let mut conn = crate::cas::store::open(&store_path).unwrap();
        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('test-blob', 'tree-sitter-wedged-definition-test', 1, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (1, 'source.rb', 'test-blob')",
            [],
        )
        .unwrap();
        let entries = [ManifestEntry {
            path: "source.rb".into(),
            blob_sha: "test-blob".into(),
        }];
        let started = Instant::now();
        let execution = run_one_workspace_analyzer_with_timeout(
            &mut conn,
            AnalyzerRunRequest {
                analyzer: Box::new(WedgedDefinitionAnalyzer { spawn_spec }),
                repo_root: fixture.path(),
                manifest_id: ManifestId(1),
                entries: &entries,
                now_ns: 42,
                analyzer_stall_timeout: Duration::from_secs(5),
                job_id: Some(324),
                progress: Some(progress.clone()),
            },
        )
        .unwrap();

        assert_eq!(execution.status, RunStatus::Failed);
        assert!(
            execution
                .error
                .as_deref()
                .is_some_and(|error| error.contains("LSP request timed out")),
            "unexpected wedged-pass execution: {execution:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "definition timeout budget did not bound the wedged pass"
        );
        assert_eq!(
            progress.snapshot(),
            (DEFINITION_PIPELINE_CONCURRENCY + MAX_CONSECUTIVE_DEFINITION_TIMEOUTS - 1) as u64
        );
        let definition_requests = fs::read_to_string(&methods)
            .unwrap()
            .lines()
            .filter(|method| *method == "textDocument/definition")
            .count();
        assert_eq!(
            definition_requests,
            DEFINITION_PIPELINE_CONCURRENCY + MAX_CONSECUTIVE_DEFINITION_TIMEOUTS - 1,
            "the budget must leave every later site unscheduled"
        );
        let durable_status: String = conn
            .query_row(
                "SELECT status FROM workspace_analysis_runs
                 WHERE analyzer_id = 'wedged-definition-budget-test'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            durable_status, "failed",
            "a --wait poll must observe the analyzer job as terminal"
        );
        pool_cleanup
            .finish()
            .expect("wedged definition test pool entry cleanup must succeed");
    }

    #[cfg(unix)]
    #[test]
    fn multi_kind_definition_pass_arms_after_pool_readiness_before_request() {
        assert_production_pass_arms_after_pool_readiness(true, false);
    }

    #[cfg(unix)]
    #[test]
    fn logical_watchdog_cleanup_does_not_stop_parallel_lsp_placement() {
        assert_production_pass_arms_after_pool_readiness(false, true);
    }

    #[cfg(unix)]
    #[test]
    fn placement_failure_guard_releases_threads_and_pool_lease() {
        assert_placement_guard_actual_unwind();
    }
}
