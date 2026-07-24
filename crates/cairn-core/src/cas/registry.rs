//! `index.db` — daemon-global metadata for CAS-managed repos.
//!
//! Responsibilities:
//!
//! - **Repository identity.** `repositories` is the canonical
//!   owner table: `repo_hash` is the primary key, `root_path` is
//!   the canonicalized on-disk root. A repository is 1:1 with a
//!   CAS store directory under `repos/`, a filesystem watcher, and
//!   a durable reconcile state row.
//! - **Alias index.** `aliases` maps a user-facing label to a
//!   repository. Multiple aliases may reference the same
//!   repository (e.g. `main` + `worker` -> hash H). The FK
//!   cascades so removing the repository cleans up its labels.
//!   Retargeting a single label to a different repository is
//!   done by calling [`upsert`] again with the new
//!   `(root_path, repo_hash)` — the alias row is replaced in
//!   place; the underlying `repositories` row is left alone.
//! - **Durable reconcile state.** `repo_reconcile_state` records
//!   the generation-counter state machine (`desired` /
//!   `applied` / `force`), retry metadata, watcher state, and
//!   last error for each repository. The daemon persists dirty
//!   state before any reindex debounce sleeps and recovers
//!   interrupted attempts on startup — no crash window can leave
//!   pending work invisible to the operator.
//! - **Repository lifecycle policy and removal audit.** Canonical owners carry
//!   the missing-root persistence policy and an optional durable pre-delete
//!   intent. `repository_removal_events` records the registry-delete / store-
//!   cleanup boundary so startup can retry incomplete cleanup without
//!   resurrecting canonical state.
//! - **Retired ambiguous `JobId` tombstones.** `ambiguous_job_ids`
//!   records every `JobId` value that was ambiguous across stores
//!   at some restart. See `JobManager::restore_from_db` for the
//!   collision-recycle protocol; the tombstone guarantees that
//!   `cancel(retired_id)` returns `unknown job id` even after a
//!   partial cross-store rewrite crashed midway.

use cairn_proto::RepoReconcileStatus;
use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::Result;
use crate::migration::{Migration, open_with_migrations};

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sql: r#"
CREATE TABLE aliases (
    alias            TEXT PRIMARY KEY,
    root_path        TEXT NOT NULL UNIQUE,
    repo_hash        TEXT NOT NULL,
    registered_at_ns INTEGER NOT NULL
);
"#,
    },
    Migration {
        // Drop the UNIQUE on root_path so multiple aliases can label
        // the same on-disk repo. Recreated via the rename-shuffle since
        // SQLite has no `DROP CONSTRAINT`.
        version: 2,
        sql: r#"
CREATE TABLE aliases_v2 (
    alias            TEXT PRIMARY KEY,
    root_path        TEXT NOT NULL,
    repo_hash        TEXT NOT NULL,
    registered_at_ns INTEGER NOT NULL
);
INSERT INTO aliases_v2 SELECT alias, root_path, repo_hash, registered_at_ns FROM aliases;
DROP TABLE aliases;
ALTER TABLE aliases_v2 RENAME TO aliases;
CREATE INDEX idx_aliases_repo_hash ON aliases(repo_hash);
"#,
    },
    Migration {
        // Durable tombstone for `JobId` values that were ambiguous
        // across stores at some restart. See
        // `crates/cairn-core/src/jobs.rs::restore_from_db` for the
        // full rationale — the collision-group rewrite is not
        // cross-store atomic (per-store `workspace_analysis_runs`
        // UPDATEs can fail after the first one succeeded, leaving
        // one sibling row still carrying the old ambiguous id), and
        // the subsequent `cancel(old_id)` would otherwise silently
        // target whichever store still holds the row. Committing
        // the ambiguous id set to `ambiguous_job_ids` *before*
        // touching any store bounds the failure: even on partial
        // rewrite, a stale client's cancel is rejected as `unknown
        // job id`, and a later restart re-recycles the remaining
        // rows.
        version: 3,
        sql: r#"
CREATE TABLE ambiguous_job_ids (
    job_id        INTEGER PRIMARY KEY,
    retired_at_ns INTEGER NOT NULL
);
"#,
    },
    Migration {
        // Canonical repository identity + durable reconcile state.
        //
        // Before v4: `aliases` held both label -> repo_hash AND
        // label -> root_path, so two aliases labelling the same
        // canonical root could disagree on `root_path` if either
        // was ever updated with a stale value. That inconsistency
        // let the daemon report per-alias reconcile status even
        // though watchers and reindex are inherently per-repo —
        // status could claim "watcher active" for a label whose
        // sibling label had actually failed.
        //
        // v4 normalises the schema:
        //
        // - `repositories` is the canonical owner (repo_hash PK,
        //   root_path UNIQUE). Each row corresponds 1:1 with a CAS
        //   store directory under `repos/`, exactly one filesystem
        //   watcher, and exactly one durable reconcile state row.
        // - `aliases` becomes a many-to-one index into
        //   `repositories` via `repo_hash` FK ON DELETE CASCADE.
        //   `root_path` is JOINed at read time so the public
        //   `AliasEntry` shape is preserved.
        // - `repo_reconcile_state` holds the generation state
        //   machine per repository. Every field either has a
        //   NOT NULL DEFAULT (so `INSERT (repo_hash)` is enough) or
        //   permits NULL to mean "not currently in this phase."
        //
        // Data migration: the `INSERT INTO repositories` uses
        // `SELECT DISTINCT repo_hash, root_path` on the existing
        // aliases; the DISTINCT PK guarantees that if two aliases
        // ever disagreed on the (repo_hash, root_path) mapping,
        // the migration FAILS with a UNIQUE / PK violation rather
        // than silently choosing one via `MIN()`. Daemons whose
        // existing `aliases` table is consistent (the normal case)
        // migrate cleanly.
        version: 4,
        sql: r#"
CREATE TABLE repositories (
    repo_hash        TEXT PRIMARY KEY,
    root_path        TEXT NOT NULL UNIQUE,
    registered_at_ns INTEGER NOT NULL
);

INSERT INTO repositories (repo_hash, root_path, registered_at_ns)
SELECT repo_hash, root_path, MIN(registered_at_ns)
FROM (SELECT DISTINCT repo_hash, root_path, registered_at_ns FROM aliases)
GROUP BY repo_hash, root_path;

CREATE TABLE aliases_v4 (
    alias            TEXT PRIMARY KEY,
    repo_hash        TEXT NOT NULL REFERENCES repositories(repo_hash) ON DELETE CASCADE,
    registered_at_ns INTEGER NOT NULL
);
INSERT INTO aliases_v4 (alias, repo_hash, registered_at_ns)
SELECT alias, repo_hash, registered_at_ns FROM aliases;
DROP TABLE aliases;
ALTER TABLE aliases_v4 RENAME TO aliases;
CREATE INDEX idx_aliases_repo_hash ON aliases(repo_hash);

CREATE TABLE repo_reconcile_state (
    repo_hash             TEXT PRIMARY KEY REFERENCES repositories(repo_hash) ON DELETE CASCADE,
    desired_generation    INTEGER NOT NULL DEFAULT 0 CHECK(desired_generation >= 0),
    applied_generation    INTEGER NOT NULL DEFAULT 0 CHECK(applied_generation >= 0),
    force_generation      INTEGER NOT NULL DEFAULT 0 CHECK(force_generation >= 0),
    attempt_generation    INTEGER,
    dirty_since_ns        INTEGER,
    last_attempt_ns       INTEGER,
    last_success_ns       INTEGER,
    consecutive_failures  INTEGER NOT NULL DEFAULT 0 CHECK(consecutive_failures >= 0),
    next_retry_at_ns      INTEGER,
    last_error            TEXT,
    watcher_state         TEXT NOT NULL DEFAULT 'starting'
        CHECK(watcher_state IN ('starting','active','failed','stopped')),
    watcher_error         TEXT,
    CHECK(applied_generation <= desired_generation),
    CHECK(force_generation <= desired_generation),
    CHECK(attempt_generation IS NULL
          OR (attempt_generation >= 0 AND attempt_generation <= desired_generation))
);

-- Seed one reconcile state row per pre-existing repository so
-- callers can always assume the row exists.
INSERT INTO repo_reconcile_state (repo_hash)
SELECT repo_hash FROM repositories;
"#,
    },
    Migration {
        // Repository lifecycle policy and crash-safe removal bookkeeping.
        // Existing repositories intentionally migrate as ephemeral: a
        // startup sweep removes only roots that are definitively NotFound.
        version: 5,
        sql: r#"
ALTER TABLE repositories ADD COLUMN persistent INTEGER NOT NULL DEFAULT 0
    CHECK(persistent IN (0, 1));
ALTER TABLE repositories ADD COLUMN removal_requested_at_ns INTEGER;
ALTER TABLE repositories ADD COLUMN removal_reason TEXT
    CHECK(removal_reason IS NULL OR removal_reason IN
        ('missing_root','last_alias_removed','alias_retargeted',
         'startup_aliasless','registration_aborted'));

CREATE TABLE repository_removal_events (
    event_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_hash            TEXT NOT NULL,
    root_path            TEXT NOT NULL,
    removed_at_ns        INTEGER NOT NULL,
    reason               TEXT NOT NULL CHECK(reason IN
        ('missing_root','last_alias_removed','alias_retargeted',
         'startup_aliasless','registration_aborted')),
    store_cleanup_state  TEXT NOT NULL
        CHECK(store_cleanup_state IN ('pending','complete','error')),
    cleanup_error        TEXT
);
CREATE INDEX idx_repository_removal_events_cleanup
    ON repository_removal_events(store_cleanup_state, event_id);
"#,
    },
];

/// Insert every id in `ids` into the `ambiguous_job_ids` tombstone
/// table using `INSERT OR IGNORE`. Callers are expected to invoke
/// this inside their own transaction so the whole set commits
/// atomically before any per-store rewrite is attempted.
///
/// # Errors
/// SQLite failures.
pub fn insert_ambiguous_ids(tx: &Transaction<'_>, ids: &[i64], retired_at_ns: i64) -> Result<()> {
    let mut stmt = tx.prepare(
        "INSERT OR IGNORE INTO ambiguous_job_ids (job_id, retired_at_ns) VALUES (?1, ?2)",
    )?;
    for id in ids {
        stmt.execute(rusqlite::params![id, retired_at_ns])?;
    }
    Ok(())
}

/// True when `job_id` has been retired as ambiguous by a prior
/// collision-recycle pass. `cancel(job_id)` checks this before any
/// store scan so a stale pre-restart client cannot silently target a
/// still-live sibling row that survived a partial rewrite.
///
/// # Errors
/// SQLite failures.
pub fn is_ambiguous_job_id(conn: &Connection, job_id: i64) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM ambiguous_job_ids WHERE job_id = ?1",
            rusqlite::params![job_id],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

/// Load every retired ambiguous id from the tombstone table. Used by
/// `restore_from_db` phase 2 so a surviving row that still carries an
/// ambiguous id from a prior restart is recycled even when no
/// sibling row is present this time.
///
/// # Errors
/// SQLite failures.
pub fn all_ambiguous_job_ids(conn: &Connection) -> Result<std::collections::HashSet<i64>> {
    let mut stmt = conn.prepare("SELECT job_id FROM ambiguous_job_ids")?;
    let rows = stmt
        .query_map([], |r| r.get::<_, i64>(0))?
        .collect::<rusqlite::Result<std::collections::HashSet<_>>>()?;
    Ok(rows)
}

/// Max byte length for durable `last_error` / `watcher_error`
/// strings. Truncation is UTF-8-safe (never mid-char) so persisted
/// text is always valid UTF-8 for SQLite and consumer wire types.
pub const MAX_ERROR_STRING_BYTES: usize = 4096;

/// Public read shape for one alias label. `root_path` is JOINed
/// from `repositories` at query time — the alias row itself stores
/// only `repo_hash` — so this shape predates and survives the v4
/// schema normalisation. Unlike `RepositoryEntry`,
/// `registered_at_ns` here is refreshed whenever the label is
/// re-upserted (retargeted).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasEntry {
    pub alias: String,
    pub root_path: String,
    pub repo_hash: String,
    pub registered_at_ns: i64,
}

/// A row in `repositories` — the canonical owner of a repo's
/// on-disk identity. Exactly one exists per (repo_hash,
/// root_path) pair; `repo_hash` is the primary key and
/// `root_path` is UNIQUE, so the mapping is a bijection enforced
/// at the storage layer.
///
/// User-facing labels live in `aliases` and reference this row
/// via `repo_hash` FK with ON DELETE CASCADE — many aliases can
/// share one repository, and dropping the repository row wipes
/// its labels and reconcile state atomically.
///
/// `registered_at_ns` records when the repository was first
/// registered (wall-clock ns since UNIX epoch). Subsequent
/// upserts under the same `(repo_hash, root_path)` do NOT bump
/// this field — the first-write timestamp wins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryEntry {
    pub repo_hash: String,
    pub root_path: String,
    pub registered_at_ns: i64,
    /// Missing-root persistence policy: `true` exempts the repo
    /// from the startup missing-root removal sweep.
    pub persistent: bool,
    /// Durable pre-delete intent, present while removal is in
    /// progress (set by [`mark_removal_requested`]).
    pub removal_request: Option<RepositoryRemovalRequest>,
}

/// Durable reason for removing a canonical repository owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepositoryRemovalReason {
    /// The canonical root is definitively gone from disk (ENOENT)
    /// and the repository is not marked persistent. Recorded by
    /// the startup sweep and the runtime missing-root path.
    MissingRoot,
    /// An unregister deleted the last label referencing the
    /// repository.
    LastAliasRemoved,
    /// A label was re-pointed at a different repository, leaving
    /// this one with no remaining aliases.
    AliasRetargeted,
    /// The startup sweep found a canonical row with zero aliases
    /// (e.g. a crash between alias delete and repository delete).
    StartupAliasless,
    /// Registration failed after the canonical row was created;
    /// the partially registered repository is rolled back.
    RegistrationAborted,
}

impl RepositoryRemovalReason {
    /// Stable SQL string form; must stay in sync with the CHECK
    /// constraints on `repositories.removal_reason` and
    /// `repository_removal_events.reason`.
    #[must_use]
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::MissingRoot => "missing_root",
            Self::LastAliasRemoved => "last_alias_removed",
            Self::AliasRetargeted => "alias_retargeted",
            Self::StartupAliasless => "startup_aliasless",
            Self::RegistrationAborted => "registration_aborted",
        }
    }

    fn from_db_str(value: &str) -> Option<Self> {
        Some(match value {
            "missing_root" => Self::MissingRoot,
            "last_alias_removed" => Self::LastAliasRemoved,
            "alias_retargeted" => Self::AliasRetargeted,
            "startup_aliasless" => Self::StartupAliasless,
            "registration_aborted" => Self::RegistrationAborted,
            _ => return None,
        })
    }
}

/// Durable pre-delete intent stored on the canonical owner row.
/// Present iff `removal_requested_at_ns` is non-NULL. Recording
/// the intent before any destructive step lets startup resume an
/// interrupted removal instead of resurrecting the repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryRemovalRequest {
    pub requested_at_ns: i64,
    pub reason: RepositoryRemovalReason,
}

/// Progress of the post-registry-delete store cleanup for one
/// removal event — mirrors the `store_cleanup_state` DB column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreCleanupState {
    /// Registry delete committed; store-directory cleanup not yet
    /// confirmed. Retried at startup.
    Pending,
    /// Cleanup confirmed done. Retained (bounded) for reporting.
    Complete,
    /// Last cleanup attempt failed; `cleanup_error` carries the
    /// truncated error text. Retried at startup like `Pending`.
    Error,
}

impl StoreCleanupState {
    /// Stable SQL string form, matching the `store_cleanup_state`
    /// CHECK constraint.
    #[must_use]
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Complete => "complete",
            Self::Error => "error",
        }
    }

    fn from_db_str(value: &str) -> Option<Self> {
        Some(match value {
            "pending" => Self::Pending,
            "complete" => Self::Complete,
            "error" => Self::Error,
            _ => return None,
        })
    }
}

/// One row in `repository_removal_events` — the durable audit
/// record straddling the registry-delete / store-cleanup boundary.
/// The canonical `repositories` row is already gone when this row
/// exists, so `repo_hash` / `root_path` are denormalised copies
/// kept for cleanup targeting and operator reporting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryRemovalEvent {
    pub event_id: i64,
    pub repo_hash: String,
    pub root_path: String,
    pub removed_at_ns: i64,
    pub reason: RepositoryRemovalReason,
    pub store_cleanup_state: StoreCleanupState,
    pub cleanup_error: Option<String>,
}

/// Watcher lifecycle state — mirrors the `watcher_state` DB column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatcherState {
    /// Seeded default for a fresh reconcile row; the daemon has
    /// not yet attempted to arm the watcher.
    Starting,
    /// Watcher armed and delivering filesystem events.
    Active,
    /// Arming or runtime failure; `watcher_error` carries detail.
    Failed,
    /// Watcher intentionally not running.
    Stopped,
}

impl WatcherState {
    /// The stable wire / SQL string form of this state. Exposed
    /// so higher layers (status proto mapping, doctor) don't
    /// need to match on the enum verbatim.
    #[must_use]
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Active => "active",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
        }
    }
    fn from_db_str(s: &str) -> Option<Self> {
        Some(match s {
            "starting" => Self::Starting,
            "active" => Self::Active,
            "failed" => Self::Failed,
            "stopped" => Self::Stopped,
            _ => return None,
        })
    }
}

/// One row in `repo_reconcile_state`. All fields mirror the SQL
/// schema exactly. Wire mapping and predicates that depend on
/// relationships between these columns live on this type so
/// status and diagnostic consumers share one interpretation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoReconcileState {
    pub repo_hash: String,
    pub desired_generation: i64,
    pub applied_generation: i64,
    pub force_generation: i64,
    pub attempt_generation: Option<i64>,
    pub dirty_since_ns: Option<i64>,
    pub last_attempt_ns: Option<i64>,
    pub last_success_ns: Option<i64>,
    pub consecutive_failures: i64,
    pub next_retry_at_ns: Option<i64>,
    pub last_error: Option<String>,
    pub watcher_state: WatcherState,
    pub watcher_error: Option<String>,
}

/// A relationship between durable generation columns that the
/// reconcile state machine must never produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileInvariantViolation {
    AppliedExceedsDesired { applied: i64, desired: i64 },
    ForceExceedsDesired { force: i64, desired: i64 },
    AttemptExceedsDesired { attempt: i64, desired: i64 },
}

impl std::fmt::Display for ReconcileInvariantViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AppliedExceedsDesired { applied, desired } => {
                write!(
                    f,
                    "applied_generation={applied} > desired_generation={desired}"
                )
            }
            Self::ForceExceedsDesired { force, desired } => {
                write!(f, "force_generation={force} > desired_generation={desired}")
            }
            Self::AttemptExceedsDesired { attempt, desired } => {
                write!(
                    f,
                    "attempt_generation={attempt} > desired_generation={desired}"
                )
            }
        }
    }
}

impl RepoReconcileState {
    /// Map this durable row into the shared data/control wire type.
    #[must_use]
    pub fn to_wire(&self, repo_hash: &str, aliases: Vec<String>) -> RepoReconcileStatus {
        RepoReconcileStatus {
            repo_hash: repo_hash.to_string(),
            aliases,
            desired_generation: self.desired_generation,
            applied_generation: self.applied_generation,
            force_generation: self.force_generation,
            attempt_generation: self.attempt_generation,
            dirty_since_ns: self.dirty_since_ns,
            last_attempt_ns: self.last_attempt_ns,
            last_success_ns: self.last_success_ns,
            consecutive_failures: self.consecutive_failures,
            next_retry_at_ns: self.next_retry_at_ns,
            last_error: self.last_error.clone(),
            watcher_state: self.watcher_state.as_db_str().to_string(),
            watcher_error: self.watcher_error.clone(),
            pending: self.pending(),
            retry_scheduled: self.retry_scheduled(),
        }
    }

    /// Whether work is durably pending or currently in flight.
    #[must_use]
    pub fn pending(&self) -> bool {
        self.desired_generation > self.applied_generation || self.attempt_generation.is_some()
    }

    /// Whether the durable state carries a scheduled retry time.
    #[must_use]
    pub fn retry_scheduled(&self) -> bool {
        self.next_retry_at_ns.is_some()
    }

    /// Return impossible generation relationships in stable priority order.
    #[must_use]
    pub fn invariant_violations(&self) -> Vec<ReconcileInvariantViolation> {
        let mut violations = Vec::new();
        if self.applied_generation > self.desired_generation {
            violations.push(ReconcileInvariantViolation::AppliedExceedsDesired {
                applied: self.applied_generation,
                desired: self.desired_generation,
            });
        }
        if self.force_generation > self.desired_generation {
            violations.push(ReconcileInvariantViolation::ForceExceedsDesired {
                force: self.force_generation,
                desired: self.desired_generation,
            });
        }
        if let Some(attempt) = self.attempt_generation
            && attempt > self.desired_generation
        {
            violations.push(ReconcileInvariantViolation::AttemptExceedsDesired {
                attempt,
                desired: self.desired_generation,
            });
        }
        violations
    }

    /// Age of an in-flight attempt, saturating at zero for future timestamps.
    #[must_use]
    pub fn attempt_age_ns(&self, now_ns: i64) -> Option<i64> {
        self.attempt_generation.map(|_| {
            self.last_attempt_ns
                .map(|last| now_ns.saturating_sub(last))
                .unwrap_or(0)
        })
    }

    /// Whether failures and a retry timestamp describe active backoff.
    #[must_use]
    pub fn retry_backoff_scheduled(&self) -> bool {
        self.consecutive_failures > 0 && self.next_retry_at_ns.is_some()
    }

    /// Age of an unclaimed dirty gap, or `None` while work is active/retrying.
    #[must_use]
    pub fn dirty_gap_ns(&self, now_ns: i64) -> Option<i64> {
        (self.desired_generation > self.applied_generation
            && self.attempt_generation.is_none()
            && self.next_retry_at_ns.is_none())
        .then(|| {
            self.dirty_since_ns
                .map(|since| now_ns.saturating_sub(since))
                .unwrap_or(0)
        })
    }

    /// Whether the durable watcher lifecycle is failed.
    #[must_use]
    pub fn watcher_failed(&self) -> bool {
        self.watcher_state == WatcherState::Failed
    }
}

/// Open (creating if necessary) the alias index DB at `path`.
///
/// # Errors
/// Filesystem or SQLite failures.
pub fn open(path: &std::path::Path) -> Result<Connection> {
    open_with_migrations(path, MIGRATIONS)
}

/// Insert or replace an alias mapping. Handles both the
/// `repositories` canonical row (idempotent — the (repo_hash,
/// root_path) pair is looked up and inserted if absent) and the
/// `aliases` label row (replaces on conflict by `alias`).
///
/// If the `repo_hash` already exists under a *different*
/// `root_path`, this is a caller bug (canonicalized paths must
/// match) and the SQL UNIQUE constraint on `repositories.root_path`
/// or PK on `repositories.repo_hash` fires — the caller must
/// canonicalize `root_path` before calling.
///
/// A fresh repository row also seeds a matching
/// `repo_reconcile_state` row (all defaults) so state helpers can
/// always assume the row exists.
///
/// # Errors
/// SQLite failures.
pub fn upsert(
    tx: &Transaction<'_>,
    alias: &str,
    root_path: &str,
    repo_hash: &str,
    registered_at_ns: i64,
) -> Result<()> {
    upsert_repository(tx, repo_hash, root_path, registered_at_ns)?;
    tx.execute(
        "INSERT INTO aliases (alias, repo_hash, registered_at_ns)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(alias) DO UPDATE SET
             repo_hash = excluded.repo_hash,
             registered_at_ns = excluded.registered_at_ns",
        params![alias, repo_hash, registered_at_ns],
    )?;
    Ok(())
}

/// Insert a repository row if not present (idempotent), and seed
/// the matching `repo_reconcile_state` row. Repeated calls with
/// the exact same `(repo_hash, root_path)` are no-ops.
///
/// Both mapping directions are enforced fail-closed:
///
/// - existing `repo_hash` with a different `root_path` →
///   [`Error::Internal`] "canonical root_path mismatch" (caller
///   must canonicalize before calling; a mismatch signals hash
///   collision, corrupted registry, or a stale caller).
/// - existing `root_path` under a different `repo_hash` → same
///   contract error, surfaced via the UNIQUE constraint on
///   `repositories.root_path`.
///
/// The pre-check inside a single transaction keeps the invariant
/// atomic — no alias row / state row is inserted when the
/// canonical bijection is violated.
///
/// # Errors
/// - [`Error::Internal`] when the canonical bijection is broken.
/// - SQLite failures.
///
/// [`Error::Internal`]: crate::Error::Internal
pub fn upsert_repository(
    tx: &Transaction<'_>,
    repo_hash: &str,
    root_path: &str,
    registered_at_ns: i64,
) -> Result<()> {
    if let Some(existing_root) = tx
        .query_row(
            "SELECT root_path FROM repositories WHERE repo_hash = ?1",
            params![repo_hash],
            |r| r.get::<_, String>(0),
        )
        .optional()?
        && existing_root != root_path
    {
        return Err(crate::Error::Internal(format!(
            "canonical root_path mismatch: repo_hash={repo_hash} has stored root {existing_root:?} \
             but caller supplied {root_path:?}; canonicalize the path before calling"
        )));
    }
    tx.execute(
        "INSERT INTO repositories (repo_hash, root_path, registered_at_ns)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(repo_hash) DO NOTHING",
        params![repo_hash, root_path, registered_at_ns],
    )?;
    tx.execute(
        "INSERT INTO repo_reconcile_state (repo_hash) VALUES (?1)
         ON CONFLICT(repo_hash) DO NOTHING",
        params![repo_hash],
    )?;
    Ok(())
}

/// Look up one repository by hash. Returns `Ok(None)` if absent.
///
/// # Errors
/// SQLite failures.
pub fn lookup_repository(conn: &Connection, repo_hash: &str) -> Result<Option<RepositoryEntry>> {
    Ok(conn
        .query_row(
            "SELECT repo_hash, root_path, registered_at_ns, persistent,
                    removal_requested_at_ns, removal_reason
             FROM repositories WHERE repo_hash = ?1",
            params![repo_hash],
            row_to_repository,
        )
        .optional()?)
}

/// All registered repositories ordered by `repo_hash`. Cheap
/// summary; use `aliases_for_repo` to enumerate labels per repo.
///
/// # Errors
/// SQLite failures.
pub fn list_repositories(conn: &Connection) -> Result<Vec<RepositoryEntry>> {
    let mut stmt = conn.prepare(
        "SELECT repo_hash, root_path, registered_at_ns, persistent,
                removal_requested_at_ns, removal_reason
         FROM repositories ORDER BY repo_hash",
    )?;
    let rows: rusqlite::Result<Vec<RepositoryEntry>> =
        stmt.query_map([], row_to_repository)?.collect();
    Ok(rows?)
}

/// All aliases pointing at `repo_hash`, ordered by alias. Empty
/// when no labels reference the repository.
///
/// # Errors
/// SQLite failures.
pub fn aliases_for_repo(conn: &Connection, repo_hash: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT alias FROM aliases WHERE repo_hash = ?1 ORDER BY alias")?;
    let rows: rusqlite::Result<Vec<String>> = stmt
        .query_map(params![repo_hash], |r| r.get::<_, String>(0))?
        .collect();
    Ok(rows?)
}

/// Delete a repository row. The FK cascades: every alias pointing
/// at this repository plus the matching `repo_reconcile_state`
/// row are removed atomically. Returns `true` if a row was
/// deleted.
///
/// # Errors
/// SQLite failures.
pub fn delete_repository(tx: &Transaction<'_>, repo_hash: &str) -> Result<bool> {
    let n = tx.execute(
        "DELETE FROM repositories WHERE repo_hash = ?1",
        params![repo_hash],
    )?;
    Ok(n > 0)
}

/// Set the canonical lifecycle policy independently from identity upsert.
///
/// Persistent repositories are exempt from the startup
/// missing-root sweep; ephemeral ones (the default) are removed
/// once their root is definitively gone. Returns `false` when
/// `repo_hash` has no canonical row.
///
/// # Errors
/// SQLite failures.
pub fn set_repository_persistent(
    tx: &Transaction<'_>,
    repo_hash: &str,
    persistent: bool,
) -> Result<bool> {
    Ok(tx.execute(
        "UPDATE repositories SET persistent = ?1 WHERE repo_hash = ?2",
        params![i64::from(persistent), repo_hash],
    )? == 1)
}

/// Persist a pre-delete removal request. The first request wins so retries and
/// duplicate detectors cannot rewrite the operator-visible reason.
///
/// Returns `true` only when this call installed the intent;
/// `false` covers both an unknown `repo_hash` and an
/// already-recorded request (the `removal_requested_at_ns IS
/// NULL` guard in the WHERE clause).
///
/// # Errors
/// SQLite failures.
pub fn mark_removal_requested(
    tx: &Transaction<'_>,
    repo_hash: &str,
    reason: RepositoryRemovalReason,
    now_ns: i64,
) -> Result<bool> {
    Ok(tx.execute(
        "UPDATE repositories
         SET removal_requested_at_ns = ?1, removal_reason = ?2
         WHERE repo_hash = ?3 AND removal_requested_at_ns IS NULL",
        params![now_ns, reason.as_db_str(), repo_hash],
    )? == 1)
}

/// Repositories whose lifecycle owner must resume removal after restart.
///
/// # Errors
/// SQLite failures.
pub fn list_removal_requested(conn: &Connection) -> Result<Vec<RepositoryEntry>> {
    let mut stmt = conn.prepare(
        "SELECT repo_hash, root_path, registered_at_ns, persistent,
                removal_requested_at_ns, removal_reason
         FROM repositories
         WHERE removal_requested_at_ns IS NOT NULL
         ORDER BY repo_hash",
    )?;
    let rows: rusqlite::Result<Vec<_>> = stmt.query_map([], row_to_repository)?.collect();
    Ok(rows?)
}

/// Atomically create the post-registry cleanup record and delete the
/// canonical owner. Aliases and reconcile state cascade with the delete.
///
/// Returns the new removal event id, or `Ok(None)` when no
/// durable removal intent exists for `repo_hash` — deleting
/// without a prior [`mark_removal_requested`] is refused so the
/// audit trail always records why a repository disappeared. The
/// event row starts in `pending` state inside the same
/// transaction as the delete; actually removing the store
/// directory (and flipping the event to `complete` / `error`)
/// happens after commit.
///
/// # Errors
/// - [`Error::Internal`] when the owner row vanishes between the
///   intent check and the delete — impossible within one
///   transaction, so it signals a corrupted registry.
/// - SQLite failures.
///
/// [`Error::Internal`]: crate::Error::Internal
pub fn delete_repository_with_event(
    tx: &Transaction<'_>,
    repo_hash: &str,
    removed_at_ns: i64,
) -> Result<Option<i64>> {
    let row: Option<(String, String)> = tx
        .query_row(
            "SELECT root_path, removal_reason FROM repositories
             WHERE repo_hash = ?1 AND removal_requested_at_ns IS NOT NULL",
            params![repo_hash],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((root_path, reason)) = row else {
        return Ok(None);
    };
    tx.execute(
        "INSERT INTO repository_removal_events
         (repo_hash, root_path, removed_at_ns, reason, store_cleanup_state)
         VALUES (?1, ?2, ?3, ?4, 'pending')",
        params![repo_hash, root_path, removed_at_ns, reason],
    )?;
    let event_id = tx.last_insert_rowid();
    if !delete_repository(tx, repo_hash)? {
        return Err(crate::Error::Internal(format!(
            "repository disappeared while recording removal: {repo_hash}"
        )));
    }
    Ok(Some(event_id))
}

/// Incomplete post-registry store cleanup, retained until it succeeds.
///
/// Includes both `pending` (cleanup never confirmed) and `error`
/// (last attempt failed) events, oldest first, so the startup
/// retry loop processes them in arrival order.
///
/// # Errors
/// SQLite failures.
pub fn list_incomplete_removals(conn: &Connection) -> Result<Vec<RepositoryRemovalEvent>> {
    let mut stmt = conn.prepare(
        "SELECT event_id, repo_hash, root_path, removed_at_ns, reason,
                store_cleanup_state, cleanup_error
         FROM repository_removal_events
         WHERE store_cleanup_state IN ('pending', 'error')
         ORDER BY event_id",
    )?;
    let rows: rusqlite::Result<Vec<_>> = stmt.query_map([], row_to_removal_event)?.collect();
    Ok(rows?)
}

/// Recent completed events for doctor/operator reporting.
///
/// Newest first, capped at `limit`. Only rows that survived the
/// bounded retention prune are visible.
///
/// # Errors
/// SQLite failures.
pub fn list_recent_completed_removals(
    conn: &Connection,
    limit: usize,
) -> Result<Vec<RepositoryRemovalEvent>> {
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    let mut stmt = conn.prepare(
        "SELECT event_id, repo_hash, root_path, removed_at_ns, reason,
                store_cleanup_state, cleanup_error
         FROM repository_removal_events
         WHERE store_cleanup_state = 'complete'
         ORDER BY event_id DESC LIMIT ?1",
    )?;
    let rows: rusqlite::Result<Vec<_>> = stmt
        .query_map(params![limit], row_to_removal_event)?
        .collect();
    Ok(rows?)
}

/// Flip a removal event to `complete` and clear any recorded
/// cleanup error, then prune completed-event retention to the
/// newest 100 rows so the audit table stays bounded without a
/// separate maintenance pass. Returns `false` when `event_id`
/// does not exist (the retention prune still runs).
///
/// # Errors
/// SQLite failures.
pub fn mark_store_cleanup_complete(tx: &Transaction<'_>, event_id: i64) -> Result<bool> {
    let changed = tx.execute(
        "UPDATE repository_removal_events
         SET store_cleanup_state = 'complete', cleanup_error = NULL
         WHERE event_id = ?1",
        params![event_id],
    )? == 1;
    prune_completed_removal_events(tx, 100)?;
    Ok(changed)
}

/// Record a failed store-cleanup attempt. The event moves to
/// `error` state but stays in [`list_incomplete_removals`], so the
/// next startup retries the cleanup. `error` text is truncated
/// UTF-8-safely to [`MAX_ERROR_STRING_BYTES`]. Returns `false`
/// when `event_id` does not exist.
///
/// # Errors
/// SQLite failures.
pub fn mark_store_cleanup_error(tx: &Transaction<'_>, event_id: i64, error: &str) -> Result<bool> {
    Ok(tx.execute(
        "UPDATE repository_removal_events
         SET store_cleanup_state = 'error', cleanup_error = ?1
         WHERE event_id = ?2",
        params![truncate_utf8(error, MAX_ERROR_STRING_BYTES), event_id],
    )? == 1)
}

/// Delete all but the newest `keep` events in `complete` state.
/// `pending` / `error` rows are never pruned — an unconfirmed
/// cleanup must stay visible until it succeeds. Returns the number
/// of rows deleted.
///
/// # Errors
/// SQLite failures.
pub fn prune_completed_removal_events(tx: &Transaction<'_>, keep: usize) -> Result<usize> {
    let keep = i64::try_from(keep).unwrap_or(i64::MAX);
    Ok(tx.execute(
        "DELETE FROM repository_removal_events
         WHERE store_cleanup_state = 'complete'
           AND event_id NOT IN (
             SELECT event_id FROM repository_removal_events
             WHERE store_cleanup_state = 'complete'
             ORDER BY event_id DESC LIMIT ?1
           )",
        params![keep],
    )?)
}

/// Count how many aliases reference `repo_hash`. Used by
/// `remove_repo` to decide whether the per-repo store directory is
/// still in use by another label.
///
/// # Errors
/// SQLite failures.
pub fn count_aliases_for_repo(conn: &Connection, repo_hash: &str) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM aliases WHERE repo_hash = ?1",
        params![repo_hash],
        |r| r.get(0),
    )?)
}

/// Look up one alias. Returns `Ok(None)` if absent. The public
/// `AliasEntry` shape is preserved by JOINing `repositories` to
/// recover the canonical `root_path`.
///
/// # Errors
/// SQLite failures.
pub fn lookup_by_alias(conn: &Connection, alias: &str) -> Result<Option<AliasEntry>> {
    Ok(conn
        .query_row(
            "SELECT a.alias, r.root_path, a.repo_hash, a.registered_at_ns
             FROM aliases a JOIN repositories r ON r.repo_hash = a.repo_hash
             WHERE a.alias = ?1",
            params![alias],
            row_to_entry,
        )
        .optional()?)
}

/// All registered aliases ordered by alias, JOINed with
/// `repositories` to expose `root_path`.
///
/// # Errors
/// SQLite failures.
pub fn list_all(conn: &Connection) -> Result<Vec<AliasEntry>> {
    let mut stmt = conn.prepare(
        "SELECT a.alias, r.root_path, a.repo_hash, a.registered_at_ns
         FROM aliases a JOIN repositories r ON r.repo_hash = a.repo_hash
         ORDER BY a.alias",
    )?;
    let rows: rusqlite::Result<Vec<AliasEntry>> = stmt.query_map([], row_to_entry)?.collect();
    Ok(rows?)
}

/// Remove one alias by name. Returns `true` if a row was deleted.
/// This does NOT cascade to the repository — callers that want
/// to remove the last alias AND the repository must call
/// [`delete_repository`] separately.
///
/// # Errors
/// SQLite failures.
pub fn delete(tx: &Transaction<'_>, alias: &str) -> Result<bool> {
    let n = tx.execute("DELETE FROM aliases WHERE alias = ?1", params![alias])?;
    Ok(n > 0)
}

fn row_to_entry(r: &rusqlite::Row<'_>) -> rusqlite::Result<AliasEntry> {
    Ok(AliasEntry {
        alias: r.get(0)?,
        root_path: r.get(1)?,
        repo_hash: r.get(2)?,
        registered_at_ns: r.get(3)?,
    })
}

/// Fail-closed mapper for `repositories` rows: the removal-intent
/// pair (`removal_requested_at_ns`, `removal_reason`) must be both
/// NULL or both set. A half-written pair means a corrupted
/// registry and surfaces as a conversion error rather than a
/// silently dropped (or invented) removal request.
fn row_to_repository(r: &rusqlite::Row<'_>) -> rusqlite::Result<RepositoryEntry> {
    let requested_at_ns: Option<i64> = r.get(4)?;
    let reason: Option<String> = r.get(5)?;
    let removal_request = match (requested_at_ns, reason) {
        (None, None) => None,
        (Some(requested_at_ns), Some(reason)) => Some(RepositoryRemovalRequest {
            requested_at_ns,
            reason: RepositoryRemovalReason::from_db_str(&reason).ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    5,
                    rusqlite::types::Type::Text,
                    format!("unknown removal_reason: {reason}").into(),
                )
            })?,
        }),
        _ => {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Null,
                "removal request timestamp/reason must both be NULL or non-NULL".into(),
            ));
        }
    };
    Ok(RepositoryEntry {
        repo_hash: r.get(0)?,
        root_path: r.get(1)?,
        registered_at_ns: r.get(2)?,
        persistent: r.get::<_, i64>(3)? != 0,
        removal_request,
    })
}

fn row_to_removal_event(r: &rusqlite::Row<'_>) -> rusqlite::Result<RepositoryRemovalEvent> {
    let reason: String = r.get(4)?;
    let state: String = r.get(5)?;
    Ok(RepositoryRemovalEvent {
        event_id: r.get(0)?,
        repo_hash: r.get(1)?,
        root_path: r.get(2)?,
        removed_at_ns: r.get(3)?,
        reason: RepositoryRemovalReason::from_db_str(&reason).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Text,
                format!("unknown repository removal reason: {reason}").into(),
            )
        })?,
        store_cleanup_state: StoreCleanupState::from_db_str(&state).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                5,
                rusqlite::types::Type::Text,
                format!("unknown store cleanup state: {state}").into(),
            )
        })?,
        cleanup_error: r.get(6)?,
    })
}

fn row_to_reconcile_state(r: &rusqlite::Row<'_>) -> rusqlite::Result<RepoReconcileState> {
    let watcher_state_str: String = r.get(11)?;
    let watcher_state = WatcherState::from_db_str(&watcher_state_str).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            11,
            rusqlite::types::Type::Text,
            format!("unknown watcher_state: {watcher_state_str}").into(),
        )
    })?;
    Ok(RepoReconcileState {
        repo_hash: r.get(0)?,
        desired_generation: r.get(1)?,
        applied_generation: r.get(2)?,
        force_generation: r.get(3)?,
        attempt_generation: r.get(4)?,
        dirty_since_ns: r.get(5)?,
        last_attempt_ns: r.get(6)?,
        last_success_ns: r.get(7)?,
        consecutive_failures: r.get(8)?,
        next_retry_at_ns: r.get(9)?,
        last_error: r.get(10)?,
        watcher_state,
        watcher_error: r.get(12)?,
    })
}

/// Shared SELECT column list for `repo_reconcile_state` queries.
/// The order must match the positional `r.get(n)` indices in
/// `row_to_reconcile_state`.
const RECONCILE_STATE_COLUMNS: &str = "repo_hash, desired_generation, applied_generation, \
     force_generation, attempt_generation, dirty_since_ns, last_attempt_ns, \
     last_success_ns, consecutive_failures, next_retry_at_ns, last_error, \
     watcher_state, watcher_error";

/// Fetch the durable reconcile state for `repo_hash`. Returns
/// `Ok(None)` when the repository has no row yet (i.e. the
/// repository is not registered).
///
/// # Errors
/// SQLite failures.
pub fn get_reconcile_state(
    conn: &Connection,
    repo_hash: &str,
) -> Result<Option<RepoReconcileState>> {
    let sql =
        format!("SELECT {RECONCILE_STATE_COLUMNS} FROM repo_reconcile_state WHERE repo_hash = ?1");
    Ok(conn
        .query_row(&sql, params![repo_hash], row_to_reconcile_state)
        .optional()?)
}

/// Truncate `s` to at most `max_bytes` bytes without splitting a
/// UTF-8 code point. Returns the (possibly-borrowed) trimmed
/// value.
fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut cut = max_bytes;
    while cut > 0 && !s.is_char_boundary(cut) {
        cut -= 1;
    }
    &s[..cut]
}

/// Bump `desired_generation` under `tx` and return the new value.
/// If `dirty_since_ns` was NULL, set it to `now_ns`. This is the
/// durable half of the trigger: callers MUST commit the
/// transaction before starting a debounce window / kicking a
/// worker; a crash between commit and worker wake still leaves
/// the desired > applied gap intact.
///
/// # Errors
/// - [`Error::Internal`] if the counter would overflow `i64`.
/// - SQLite failures.
///
/// [`Error::Internal`]: crate::Error::Internal
pub fn increment_desired_generation(
    tx: &Transaction<'_>,
    repo_hash: &str,
    now_ns: i64,
) -> Result<i64> {
    let current: i64 = tx.query_row(
        "SELECT desired_generation FROM repo_reconcile_state WHERE repo_hash = ?1",
        params![repo_hash],
        |r| r.get(0),
    )?;
    let next = current
        .checked_add(1)
        .ok_or_else(|| crate::Error::Internal("reconcile desired_generation overflow".into()))?;
    tx.execute(
        "UPDATE repo_reconcile_state
         SET desired_generation = ?1,
             dirty_since_ns = COALESCE(dirty_since_ns, ?2)
         WHERE repo_hash = ?3",
        params![next, now_ns, repo_hash],
    )?;
    Ok(next)
}

/// Bump `desired_generation` and make the resulting work immediately
/// eligible to run. Registration catch-up and daemon-startup priming use
/// this path because an old retry deadline must not delay a newly observed
/// filesystem state. Failure counters and error text remain intact until a
/// successful attempt clears them.
///
/// # Errors
/// See [`increment_desired_generation`].
pub fn increment_immediate_desired_generation(
    tx: &Transaction<'_>,
    repo_hash: &str,
    now_ns: i64,
) -> Result<i64> {
    let next = increment_desired_generation(tx, repo_hash, now_ns)?;
    let changed = tx.execute(
        "UPDATE repo_reconcile_state
         SET next_retry_at_ns = NULL
         WHERE repo_hash = ?1",
        params![repo_hash],
    )?;
    if changed != 1 {
        return Err(invalid_transition(&format!(
            "immediate dirty: missing repo_hash={repo_hash}"
        )));
    }
    Ok(next)
}

/// Prime every canonical repository that is not being removed for one
/// immediate startup reconcile. Callers wrap this helper in one transaction,
/// so a counter overflow or any SQLite failure rolls back every bump.
pub fn prime_startup_generations(tx: &Transaction<'_>, now_ns: i64) -> Result<Vec<(String, i64)>> {
    let mut stmt = tx.prepare(
        "SELECT repo_hash FROM repositories
         WHERE removal_requested_at_ns IS NULL
         ORDER BY repo_hash",
    )?;
    let repo_hashes = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);

    let mut primed = Vec::with_capacity(repo_hashes.len());
    for repo_hash in repo_hashes {
        let generation = increment_immediate_desired_generation(tx, &repo_hash, now_ns)?;
        primed.push((repo_hash, generation));
    }
    Ok(primed)
}

/// Conditionally record a low-frequency full reconcile.
///
/// A periodic generation is added only when the repository is active, clean,
/// idle, and its last successful scan is at least `due_age_ns` old. The
/// predicate and bump execute under the caller's transaction, preventing a
/// watcher request racing this check from losing or duplicating work.
///
/// Returns `Ok(None)` when not due; an unknown or
/// removal-requested `repo_hash` also reads as not due rather
/// than an error.
///
/// # Errors
/// - [`Error::InvalidArgument`] when `due_age_ns` is negative.
/// - See [`increment_desired_generation`].
///
/// [`Error::InvalidArgument`]: crate::Error::InvalidArgument
pub fn increment_periodic_generation_if_due(
    tx: &Transaction<'_>,
    repo_hash: &str,
    now_ns: i64,
    due_age_ns: i64,
) -> Result<Option<i64>> {
    if due_age_ns < 0 {
        return Err(crate::Error::InvalidArgument(
            "periodic reconcile due age must be non-negative".into(),
        ));
    }
    let cutoff = now_ns.saturating_sub(due_age_ns);
    let due: bool = tx
        .query_row(
            "SELECT r.removal_requested_at_ns IS NULL
                    AND s.desired_generation = s.applied_generation
                    AND s.attempt_generation IS NULL
                    AND (s.last_success_ns IS NULL OR s.last_success_ns <= ?1)
             FROM repositories r
             JOIN repo_reconcile_state s ON s.repo_hash = r.repo_hash
             WHERE r.repo_hash = ?2",
            params![cutoff, repo_hash],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(false);
    if !due {
        return Ok(None);
    }
    Ok(Some(increment_desired_generation(tx, repo_hash, now_ns)?))
}

/// Bump `desired_generation` AND `force_generation` under `tx`.
/// Used by manual `cairn ctl repo reindex` requests so a
/// concurrent FS event that also happens to bump desired cannot
/// "eat" the operator's force signal — the reconcile executor
/// picks the max desired but honours `force_generation` for the
/// analyzer-enqueue variant.
///
/// A manual force request clears an existing retry deadline so
/// the operator's newly requested attempt can run immediately.
/// If that attempt fails, `mark_attempt_failure` installs a new
/// deadline which the worker must honour even while this force
/// generation remains pending.
///
/// # Errors
/// See [`increment_desired_generation`].
pub fn increment_force_generation(
    tx: &Transaction<'_>,
    repo_hash: &str,
    now_ns: i64,
) -> Result<i64> {
    let next = increment_immediate_desired_generation(tx, repo_hash, now_ns)?;
    tx.execute(
        "UPDATE repo_reconcile_state SET force_generation = ?1 WHERE repo_hash = ?2",
        params![next, repo_hash],
    )?;
    Ok(next)
}

/// Error constructor shared by every fail-closed transition helper
/// whose WHERE clause + affected-rows check rejected the write.
fn invalid_transition(context: &str) -> crate::Error {
    crate::Error::Internal(format!("reconcile state transition rejected: {context}"))
}

/// Persist that we are about to attempt `generation`. Called
/// under the repository op_lock, immediately before running the
/// actual reindex work — if the daemon crashes between this call
/// and success/failure, startup recovery observes a non-NULL
/// `attempt_generation` and treats it as interrupted.
///
/// Illegal transitions are rejected fail-closed by the WHERE
/// clause + affected-rows==1 contract:
///
/// - `generation < 0`
/// - the repository has no reconcile state row (missing repo)
/// - an attempt is already in flight (`attempt_generation IS NOT NULL`)
/// - `generation <= applied_generation` (nothing to reconcile
///   past what we already applied)
/// - `generation > desired_generation` (attempting a generation
///   that was never requested)
///
/// # Errors
/// - [`Error::Internal`] when any precondition is violated (the
///   UPDATE affected 0 rows).
/// - SQLite failures.
///
/// [`Error::Internal`]: crate::Error::Internal
pub fn mark_attempt_start(
    tx: &Transaction<'_>,
    repo_hash: &str,
    generation: i64,
    now_ns: i64,
) -> Result<()> {
    if generation < 0 {
        return Err(invalid_transition(&format!(
            "start: negative generation {generation}"
        )));
    }
    let n = tx.execute(
        "UPDATE repo_reconcile_state
         SET attempt_generation = ?1,
             last_attempt_ns = ?2
         WHERE repo_hash = ?3
           AND attempt_generation IS NULL
           AND applied_generation < ?1
           AND ?1 <= desired_generation",
        params![generation, now_ns, repo_hash],
    )?;
    if n != 1 {
        return Err(invalid_transition(&format!(
            "start: no valid row for repo_hash={repo_hash} generation={generation} \
             (missing repo, in-flight attempt, gen<=applied, or gen>desired)"
        )));
    }
    Ok(())
}

/// Persist a successful attempt: advance `applied_generation` to
/// `max(applied, generation)`, clear the attempt slot, clear the
/// last error / retry counters. `dirty_since_ns` is cleared iff
/// `desired_generation <= applied_generation` after the update
/// (i.e. no newer events arrived while we were reconciling).
///
/// Illegal transitions are rejected fail-closed: the call MUST
/// match `attempt_generation = generation` under `repo_hash`, so
/// a stale completion cannot silently clear a newer attempt.
///
/// # Errors
/// - [`Error::Internal`] when no row matches (missing repo, no
///   in-flight attempt, or generation mismatch).
/// - SQLite failures.
///
/// [`Error::Internal`]: crate::Error::Internal
pub fn mark_attempt_success(
    tx: &Transaction<'_>,
    repo_hash: &str,
    generation: i64,
    now_ns: i64,
) -> Result<()> {
    let n = tx.execute(
        "UPDATE repo_reconcile_state
         SET applied_generation = MAX(applied_generation, ?1),
             attempt_generation = NULL,
             last_success_ns = ?2,
             consecutive_failures = 0,
             next_retry_at_ns = NULL,
             last_error = NULL,
             dirty_since_ns = CASE
                 WHEN desired_generation <= MAX(applied_generation, ?1) THEN NULL
                 ELSE dirty_since_ns
             END
         WHERE repo_hash = ?3
           AND attempt_generation = ?1",
        params![generation, now_ns, repo_hash],
    )?;
    if n != 1 {
        return Err(invalid_transition(&format!(
            "success: no active attempt matching repo_hash={repo_hash} generation={generation}"
        )));
    }
    Ok(())
}

/// Persist a failed attempt: clear the attempt slot but leave
/// `applied_generation` and `dirty_since_ns` alone (the desired >
/// applied gap remains durable). Record the error text (truncated
/// UTF-8-safely to [`MAX_ERROR_STRING_BYTES`]), bump the
/// consecutive-failure counter (`saturating_add`, so an
/// astronomical failure streak caps rather than wraps), and stamp
/// `next_retry_at_ns`.
///
/// Illegal transitions are rejected fail-closed: same
/// `attempt_generation = generation` contract as
/// [`mark_attempt_success`].
///
/// # Errors
/// - [`Error::Internal`] when no row matches.
/// - SQLite failures.
///
/// [`Error::Internal`]: crate::Error::Internal
pub fn mark_attempt_failure(
    tx: &Transaction<'_>,
    repo_hash: &str,
    generation: i64,
    error: &str,
    next_retry_at_ns: i64,
) -> Result<()> {
    let error_trimmed = truncate_utf8(error, MAX_ERROR_STRING_BYTES);
    // The failure counter is read then written as two statements;
    // the caller's transaction makes the pair atomic, and the
    // UPDATE's WHERE re-checks the attempt identity so a stale or
    // wrong generation is rejected fail-closed instead of
    // clobbering a newer attempt.
    let current_failures: Option<i64> = tx
        .query_row(
            "SELECT consecutive_failures FROM repo_reconcile_state
             WHERE repo_hash = ?1 AND attempt_generation = ?2",
            params![repo_hash, generation],
            |r| r.get(0),
        )
        .optional()?;
    let Some(current_failures) = current_failures else {
        return Err(invalid_transition(&format!(
            "failure: no active attempt matching repo_hash={repo_hash} generation={generation}"
        )));
    };
    let next_failures = current_failures.saturating_add(1);
    let n = tx.execute(
        "UPDATE repo_reconcile_state
         SET attempt_generation = NULL,
             last_error = ?1,
             consecutive_failures = ?2,
             next_retry_at_ns = ?3
         WHERE repo_hash = ?4 AND attempt_generation = ?5",
        params![
            error_trimmed,
            next_failures,
            next_retry_at_ns,
            repo_hash,
            generation
        ],
    )?;
    if n != 1 {
        return Err(invalid_transition(&format!(
            "failure: race lost updating repo_hash={repo_hash} generation={generation}"
        )));
    }
    Ok(())
}

/// Sweep every repository whose `attempt_generation` is non-NULL:
/// clear the attempt slot, stamp an interrupted-attempt error
/// (unless one is already present), and return the affected
/// repo_hashes so the manager can enqueue an immediate retry.
///
/// Called from daemon startup before sockets come up, so
/// operators never see a `Clean` status for a repository whose
/// last attempt is unaccounted for.
///
/// # Errors
/// SQLite failures.
pub fn recover_interrupted_attempts(tx: &Transaction<'_>) -> Result<Vec<String>> {
    let mut stmt = tx.prepare(
        "SELECT repo_hash FROM repo_reconcile_state WHERE attempt_generation IS NOT NULL",
    )?;
    let hashes: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let msg = truncate_utf8(
        "reconcile attempt interrupted by daemon restart",
        MAX_ERROR_STRING_BYTES,
    );
    for hash in &hashes {
        tx.execute(
            "UPDATE repo_reconcile_state
             SET attempt_generation = NULL,
                 last_error = COALESCE(last_error, ?1)
             WHERE repo_hash = ?2",
            params![msg, hash],
        )?;
    }
    Ok(hashes)
}

/// Persist the watcher lifecycle state for `repo_hash`. `error`
/// is truncated UTF-8-safely; pass `None` to clear it (e.g. on
/// transition from `Failed` to `Active`).
///
/// Fail-closed on missing repo: a caller that has not registered
/// the repository (no reconcile_state row) MUST NOT silently
/// succeed — that would let watcher lifecycle transitions
/// vanish, which reads as "watcher healthy" on status. The
/// UPDATE-affected-rows == 1 contract catches this.
///
/// # Errors
/// - [`Error::Internal`] when `repo_hash` has no reconcile row.
/// - SQLite failures.
///
/// [`Error::Internal`]: crate::Error::Internal
pub fn set_watcher_state(
    tx: &Transaction<'_>,
    repo_hash: &str,
    state: WatcherState,
    error: Option<&str>,
) -> Result<()> {
    let trimmed = error.map(|s| truncate_utf8(s, MAX_ERROR_STRING_BYTES).to_string());
    let n = tx.execute(
        "UPDATE repo_reconcile_state
         SET watcher_state = ?1,
             watcher_error = ?2
         WHERE repo_hash = ?3",
        params![state.as_db_str(), trimmed, repo_hash],
    )?;
    if n != 1 {
        return Err(invalid_transition(&format!(
            "watcher_state: no reconcile row for repo_hash={repo_hash}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> (tempfile::TempDir, Connection) {
        let tmp = tempfile::tempdir().unwrap();
        let conn = open(&tmp.path().join("index.db")).unwrap();
        (tmp, conn)
    }

    fn reconcile_state() -> RepoReconcileState {
        RepoReconcileState {
            repo_hash: "stored-hash".into(),
            desired_generation: 4,
            applied_generation: 2,
            force_generation: 1,
            attempt_generation: Some(3),
            dirty_since_ns: Some(100),
            last_attempt_ns: Some(200),
            last_success_ns: Some(150),
            consecutive_failures: 2,
            next_retry_at_ns: Some(500),
            last_error: Some("EMFILE".into()),
            watcher_state: WatcherState::Failed,
            watcher_error: Some("watch failed".into()),
        }
    }

    #[test]
    fn reconcile_state_to_wire_maps_columns_and_derived_flags() {
        let wire = reconcile_state().to_wire("wire-hash", vec!["a".into(), "b".into()]);
        assert_eq!(wire.repo_hash, "wire-hash");
        assert_eq!(wire.aliases, vec!["a", "b"]);
        assert_eq!(wire.desired_generation, 4);
        assert_eq!(wire.applied_generation, 2);
        assert_eq!(wire.force_generation, 1);
        assert_eq!(wire.attempt_generation, Some(3));
        assert_eq!(wire.dirty_since_ns, Some(100));
        assert_eq!(wire.last_attempt_ns, Some(200));
        assert_eq!(wire.last_success_ns, Some(150));
        assert_eq!(wire.consecutive_failures, 2);
        assert_eq!(wire.next_retry_at_ns, Some(500));
        assert_eq!(wire.last_error.as_deref(), Some("EMFILE"));
        assert_eq!(wire.watcher_state, "failed");
        assert_eq!(wire.watcher_error.as_deref(), Some("watch failed"));
        assert!(wire.pending);
        assert!(wire.retry_scheduled);
    }

    #[test]
    fn reconcile_invariant_violations_keep_doctor_priority_order() {
        let mut state = reconcile_state();
        state.desired_generation = 1;
        state.applied_generation = 4;
        state.force_generation = 3;
        state.attempt_generation = Some(2);
        assert_eq!(
            state.invariant_violations(),
            vec![
                ReconcileInvariantViolation::AppliedExceedsDesired {
                    applied: 4,
                    desired: 1,
                },
                ReconcileInvariantViolation::ForceExceedsDesired {
                    force: 3,
                    desired: 1,
                },
                ReconcileInvariantViolation::AttemptExceedsDesired {
                    attempt: 2,
                    desired: 1,
                },
            ]
        );
    }

    #[test]
    fn reconcile_operational_predicates_preserve_column_semantics() {
        let state = reconcile_state();
        assert_eq!(state.attempt_age_ns(1_000), Some(800));
        assert!(state.retry_backoff_scheduled());
        assert!(state.watcher_failed());
        assert_eq!(state.dirty_gap_ns(1_000), None);

        let idle_dirty = RepoReconcileState {
            attempt_generation: None,
            next_retry_at_ns: None,
            consecutive_failures: 0,
            watcher_state: WatcherState::Active,
            ..state
        };
        assert_eq!(idle_dirty.dirty_gap_ns(1_000), Some(900));
        assert_eq!(idle_dirty.attempt_age_ns(1_000), None);
        assert!(!idle_dirty.retry_backoff_scheduled());
        assert!(!idle_dirty.watcher_failed());
    }

    #[test]
    fn upsert_then_lookup_roundtrips() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert(&tx, "demo", "/some/path", "h0", 1234).unwrap();
        tx.commit().unwrap();

        let entry = lookup_by_alias(&c, "demo").unwrap().unwrap();
        assert_eq!(entry.alias, "demo");
        assert_eq!(entry.root_path, "/some/path");
        assert_eq!(entry.repo_hash, "h0");
        assert_eq!(entry.registered_at_ns, 1234);
    }

    #[test]
    fn lookup_returns_none_for_missing() {
        let (_t, c) = fresh();
        assert!(lookup_by_alias(&c, "nope").unwrap().is_none());
    }

    #[test]
    fn upsert_replaces_same_alias_with_new_target() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert(&tx, "demo", "/a", "h0", 1).unwrap();
        upsert(&tx, "demo", "/b", "h1", 2).unwrap();
        tx.commit().unwrap();
        let entry = lookup_by_alias(&c, "demo").unwrap().unwrap();
        assert_eq!(entry.root_path, "/b");
        assert_eq!(entry.repo_hash, "h1");
    }

    #[test]
    fn upsert_allows_multiple_aliases_for_same_path() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert(&tx, "first", "/p", "h", 1).unwrap();
        upsert(&tx, "second", "/p", "h", 2).unwrap();
        tx.commit().unwrap();
        // Both aliases survive; neither is silently dropped.
        assert_eq!(
            lookup_by_alias(&c, "first").unwrap().unwrap().root_path,
            "/p"
        );
        assert_eq!(
            lookup_by_alias(&c, "second").unwrap().unwrap().root_path,
            "/p"
        );
        assert_eq!(count_aliases_for_repo(&c, "h").unwrap(), 2);
    }

    #[test]
    fn count_aliases_for_repo_is_zero_for_unknown() {
        let (_t, c) = fresh();
        assert_eq!(count_aliases_for_repo(&c, "ghost").unwrap(), 0);
    }

    #[test]
    fn list_all_orders_by_alias() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert(&tx, "z", "/z", "hz", 0).unwrap();
        upsert(&tx, "a", "/a", "ha", 0).unwrap();
        upsert(&tx, "m", "/m", "hm", 0).unwrap();
        tx.commit().unwrap();
        let names: Vec<String> = list_all(&c).unwrap().into_iter().map(|e| e.alias).collect();
        assert_eq!(names, vec!["a", "m", "z"]);
    }

    #[test]
    fn delete_removes_one_and_reports() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert(&tx, "demo", "/p", "h", 0).unwrap();
        assert!(delete(&tx, "demo").unwrap());
        assert!(!delete(&tx, "demo").unwrap());
        tx.commit().unwrap();
    }

    // ─── Migration v4: repository / reconcile state APIs ────────

    #[test]
    fn upsert_creates_one_repository_row_for_two_aliases_same_hash() {
        // Two aliases sharing the same (repo_hash, root_path)
        // must collapse to a single canonical repositories row —
        // the identity invariant the reconcile state machine
        // depends on (one canonical row per watcher / reindex
        // owner).
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert(&tx, "main", "/p", "h", 1).unwrap();
        upsert(&tx, "worker", "/p", "h", 2).unwrap();
        tx.commit().unwrap();
        let repos = list_repositories(&c).unwrap();
        assert_eq!(repos.len(), 1);
        assert_eq!(repos[0].repo_hash, "h");
        assert_eq!(repos[0].root_path, "/p");
        assert_eq!(
            repos[0].registered_at_ns, 1,
            "first-write registration_ns is kept"
        );
        let aliases = aliases_for_repo(&c, "h").unwrap();
        assert_eq!(aliases, vec!["main".to_string(), "worker".to_string()]);
    }

    #[test]
    fn upsert_repository_same_hash_different_root_fails_without_alias_insert() {
        // Canonical bijection: (repo_hash, root_path) is 1:1.
        // A second `upsert` under an existing repo_hash with a
        // different root MUST fail with a caller-visible
        // Internal error, and MUST NOT leave a partial alias /
        // state / repository mutation behind — the check runs
        // before any INSERT so a rolled-back tx observes only the
        // first successful upsert.
        let (_t, mut c) = fresh();
        {
            let tx = c.transaction().unwrap();
            upsert(&tx, "a", "/p", "h", 1).unwrap();
            tx.commit().unwrap();
        }
        // Second tx: attempt the illegal mapping.
        {
            let tx = c.transaction().unwrap();
            let err = upsert(&tx, "b", "/q", "h", 2).unwrap_err();
            match err {
                crate::Error::Internal(msg) => {
                    assert!(
                        msg.contains("canonical root_path mismatch"),
                        "expected canonical bijection error, got {msg}"
                    );
                }
                other => panic!("unexpected error variant: {other:?}"),
            }
            // Roll back the failed tx — the failed insert must
            // not leak.
            tx.rollback().unwrap();
        }
        // State after rollback: only the first alias survives,
        // repositories keeps original root, state row untouched.
        assert_eq!(lookup_repository(&c, "h").unwrap().unwrap().root_path, "/p");
        assert_eq!(aliases_for_repo(&c, "h").unwrap(), vec!["a"]);
        let s = get_reconcile_state(&c, "h").unwrap().unwrap();
        assert_eq!(s.desired_generation, 0);
    }

    #[test]
    fn upsert_repository_conflicting_root_under_new_hash_fails_on_unique() {
        // Different repo_hash, same root_path → violates the
        // UNIQUE constraint on repositories.root_path.
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert(&tx, "a", "/p", "h1", 1).unwrap();
        let result = upsert(&tx, "b", "/p", "h2", 2);
        assert!(result.is_err(), "conflicting root under new hash must fail");
    }

    #[test]
    fn alias_entry_join_preserves_root_path_api() {
        // Public AliasEntry keeps `root_path` even though the
        // storage moved to `repositories`. Lookup / list JOIN.
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert(&tx, "demo", "/root", "h", 42).unwrap();
        tx.commit().unwrap();
        let e = lookup_by_alias(&c, "demo").unwrap().unwrap();
        assert_eq!(e.root_path, "/root");
        assert_eq!(e.repo_hash, "h");
        assert_eq!(e.registered_at_ns, 42);
        assert_eq!(list_all(&c).unwrap().len(), 1);
    }

    #[test]
    fn delete_repository_cascades_aliases_and_state() {
        // Removing the canonical row wipes the labels AND the
        // reconcile state row atomically via FK ON DELETE
        // CASCADE.
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert(&tx, "a", "/p", "h", 1).unwrap();
        upsert(&tx, "b", "/p", "h", 2).unwrap();
        tx.commit().unwrap();
        assert!(get_reconcile_state(&c, "h").unwrap().is_some());
        assert_eq!(aliases_for_repo(&c, "h").unwrap().len(), 2);
        let tx = c.transaction().unwrap();
        assert!(delete_repository(&tx, "h").unwrap());
        tx.commit().unwrap();
        assert!(get_reconcile_state(&c, "h").unwrap().is_none());
        assert_eq!(aliases_for_repo(&c, "h").unwrap().len(), 0);
        let n_repositories: i64 = c
            .query_row("SELECT COUNT(*) FROM repositories", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n_repositories, 0);
    }

    #[test]
    fn deleting_one_of_two_aliases_leaves_repository_and_state() {
        // Only `delete_repository` cascades — a single-alias
        // delete leaves the canonical row and the reconcile
        // state intact.
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert(&tx, "a", "/p", "h", 1).unwrap();
        upsert(&tx, "b", "/p", "h", 2).unwrap();
        delete(&tx, "a").unwrap();
        tx.commit().unwrap();
        assert!(get_reconcile_state(&c, "h").unwrap().is_some());
        assert_eq!(aliases_for_repo(&c, "h").unwrap(), vec!["b"]);
    }

    #[test]
    fn increment_desired_generation_bumps_and_stamps_dirty_since() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        assert_eq!(increment_desired_generation(&tx, "h", 100).unwrap(), 1);
        assert_eq!(increment_desired_generation(&tx, "h", 200).unwrap(), 2);
        tx.commit().unwrap();
        let s = get_reconcile_state(&c, "h").unwrap().unwrap();
        assert_eq!(s.desired_generation, 2);
        // dirty_since_ns is set on FIRST increment and preserved.
        assert_eq!(s.dirty_since_ns, Some(100));
    }

    #[test]
    fn attempt_success_advances_applied_and_clears_dirty_when_desired_matches() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        increment_desired_generation(&tx, "h", 10).unwrap();
        mark_attempt_start(&tx, "h", 1, 20).unwrap();
        mark_attempt_success(&tx, "h", 1, 30).unwrap();
        tx.commit().unwrap();
        let s = get_reconcile_state(&c, "h").unwrap().unwrap();
        assert_eq!(s.applied_generation, 1);
        assert!(s.attempt_generation.is_none());
        assert_eq!(s.dirty_since_ns, None, "cleared: desired == applied");
        assert!(s.last_error.is_none());
        assert_eq!(s.consecutive_failures, 0);
    }

    #[test]
    fn attempt_success_preserves_dirty_when_new_event_bumped_desired() {
        // Event during attempt: desired bumped again while we
        // were running. On success, applied catches up to the
        // attempt generation but dirty_since must be preserved
        // so the executor loops immediately.
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        increment_desired_generation(&tx, "h", 10).unwrap();
        mark_attempt_start(&tx, "h", 1, 20).unwrap();
        increment_desired_generation(&tx, "h", 25).unwrap(); // event during attempt
        mark_attempt_success(&tx, "h", 1, 30).unwrap();
        tx.commit().unwrap();
        let s = get_reconcile_state(&c, "h").unwrap().unwrap();
        assert_eq!(s.applied_generation, 1);
        assert_eq!(s.desired_generation, 2);
        assert_eq!(s.dirty_since_ns, Some(10), "still dirty: desired > applied");
    }

    #[test]
    fn attempt_failure_persists_error_and_leaves_gap() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        increment_desired_generation(&tx, "h", 10).unwrap();
        mark_attempt_start(&tx, "h", 1, 20).unwrap();
        mark_attempt_failure(&tx, "h", 1, "git failed: EMFILE", 5000).unwrap();
        tx.commit().unwrap();
        let s = get_reconcile_state(&c, "h").unwrap().unwrap();
        assert_eq!(s.applied_generation, 0, "applied MUST NOT advance");
        assert_eq!(s.desired_generation, 1);
        assert_eq!(s.dirty_since_ns, Some(10));
        assert!(s.attempt_generation.is_none());
        assert_eq!(s.last_error.as_deref(), Some("git failed: EMFILE"));
        assert_eq!(s.consecutive_failures, 1);
        assert_eq!(s.next_retry_at_ns, Some(5000));
    }

    #[test]
    fn immediate_generation_clears_retry_deadline_but_preserves_failure_evidence() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        increment_desired_generation(&tx, "h", 10).unwrap();
        mark_attempt_start(&tx, "h", 1, 20).unwrap();
        mark_attempt_failure(&tx, "h", 1, "transient failure", 5_000).unwrap();
        assert_eq!(
            increment_immediate_desired_generation(&tx, "h", 30).unwrap(),
            2
        );
        tx.commit().unwrap();

        let state = get_reconcile_state(&c, "h").unwrap().unwrap();
        assert_eq!(state.desired_generation, 2);
        assert_eq!(state.next_retry_at_ns, None);
        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(state.last_error.as_deref(), Some("transient failure"));
    }

    #[test]
    fn startup_prime_bumps_active_repositories_and_skips_removing() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "active", "/active", 0).unwrap();
        upsert_repository(&tx, "removing", "/removing", 0).unwrap();
        mark_removal_requested(&tx, "removing", RepositoryRemovalReason::MissingRoot, 1).unwrap();
        let primed = prime_startup_generations(&tx, 10).unwrap();
        tx.commit().unwrap();

        assert_eq!(primed, vec![("active".to_string(), 1)]);
        assert_eq!(
            get_reconcile_state(&c, "active")
                .unwrap()
                .unwrap()
                .desired_generation,
            1
        );
        assert_eq!(
            get_reconcile_state(&c, "removing")
                .unwrap()
                .unwrap()
                .desired_generation,
            0
        );
    }

    #[test]
    fn startup_prime_overflow_rolls_back_every_repository() {
        let (_t, mut c) = fresh();
        {
            let tx = c.transaction().unwrap();
            upsert_repository(&tx, "a", "/a", 0).unwrap();
            upsert_repository(&tx, "z", "/z", 0).unwrap();
            tx.execute(
                "UPDATE repo_reconcile_state SET desired_generation = ?1 WHERE repo_hash = 'z'",
                params![i64::MAX],
            )
            .unwrap();
            tx.commit().unwrap();
        }

        let tx = c.transaction().unwrap();
        let err = prime_startup_generations(&tx, 10).unwrap_err();
        assert!(format!("{err}").contains("overflow"));
        tx.rollback().unwrap();
        assert_eq!(
            get_reconcile_state(&c, "a")
                .unwrap()
                .unwrap()
                .desired_generation,
            0,
            "the earlier bump must roll back with the overflowing row"
        );
    }

    #[test]
    fn periodic_generation_requires_clean_idle_and_aged_state() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        increment_desired_generation(&tx, "h", 1).unwrap();
        mark_attempt_start(&tx, "h", 1, 2).unwrap();
        mark_attempt_success(&tx, "h", 1, 100).unwrap();

        assert_eq!(
            increment_periodic_generation_if_due(&tx, "h", 120, 30).unwrap(),
            None,
            "a scan younger than the max age is not due"
        );
        assert_eq!(
            increment_periodic_generation_if_due(&tx, "h", 130, 30).unwrap(),
            Some(2)
        );
        assert_eq!(
            increment_periodic_generation_if_due(&tx, "h", 200, 30).unwrap(),
            None,
            "a durable dirty gap prevents periodic generation stacking"
        );
        tx.commit().unwrap();
    }

    #[test]
    fn periodic_generation_can_be_due_before_freshness_expiry() {
        const SECOND_NS: i64 = 1_000_000_000;

        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        increment_desired_generation(&tx, "h", SECOND_NS).unwrap();
        mark_attempt_start(&tx, "h", 1, 2 * SECOND_NS).unwrap();
        mark_attempt_success(&tx, "h", 1, 100 * SECOND_NS).unwrap();

        assert_eq!(
            increment_periodic_generation_if_due(&tx, "h", 124 * SECOND_NS, 25 * SECOND_NS,)
                .unwrap(),
            None,
            "a scan younger than the margin-adjusted due age is not due"
        );
        assert_eq!(
            increment_periodic_generation_if_due(&tx, "h", 126 * SECOND_NS, 25 * SECOND_NS,)
                .unwrap(),
            Some(2),
            "the scan is due before the 30-second freshness horizon expires"
        );
        tx.commit().unwrap();
    }

    #[test]
    fn attempt_failure_truncates_long_error_utf8_safely() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        increment_desired_generation(&tx, "h", 5).unwrap();
        mark_attempt_start(&tx, "h", 1, 10).unwrap();
        // Use a multi-byte character so an off-by-one cut would
        // produce invalid UTF-8.
        let long = "あ".repeat(3000); // 9000 bytes
        mark_attempt_failure(&tx, "h", 1, &long, 0).unwrap();
        tx.commit().unwrap();
        let s = get_reconcile_state(&c, "h").unwrap().unwrap();
        let stored = s.last_error.unwrap();
        assert!(stored.len() <= MAX_ERROR_STRING_BYTES);
        assert!(stored.chars().all(|c| c == 'あ'), "must remain valid UTF-8");
    }

    #[test]
    fn recover_interrupted_attempts_clears_and_annotates() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h1", "/p1", 0).unwrap();
        upsert_repository(&tx, "h2", "/p2", 0).unwrap();
        increment_desired_generation(&tx, "h1", 10).unwrap();
        mark_attempt_start(&tx, "h1", 1, 20).unwrap();
        // h2 has no in-flight attempt.
        let hashes = recover_interrupted_attempts(&tx).unwrap();
        tx.commit().unwrap();
        assert_eq!(hashes, vec!["h1"]);
        let s1 = get_reconcile_state(&c, "h1").unwrap().unwrap();
        assert!(s1.attempt_generation.is_none());
        assert!(s1.last_error.unwrap().contains("interrupted"));
        let s2 = get_reconcile_state(&c, "h2").unwrap().unwrap();
        assert!(s2.last_error.is_none());
    }

    #[test]
    fn set_watcher_state_persists_and_can_clear_error() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        set_watcher_state(&tx, "h", WatcherState::Failed, Some("git open failed")).unwrap();
        set_watcher_state(&tx, "h", WatcherState::Active, None).unwrap();
        tx.commit().unwrap();
        let s = get_reconcile_state(&c, "h").unwrap().unwrap();
        assert_eq!(s.watcher_state, WatcherState::Active);
        assert_eq!(s.watcher_error, None);
    }

    #[test]
    fn set_watcher_state_truncates_long_error_utf8_safely() {
        // Same MAX_ERROR_STRING_BYTES contract as
        // `mark_attempt_failure`: watcher_error must survive
        // truncation as valid UTF-8 even when the raw text
        // straddles a multi-byte boundary at the byte cap.
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        let long = "あ".repeat(3000); // 9000 bytes
        set_watcher_state(&tx, "h", WatcherState::Failed, Some(&long)).unwrap();
        tx.commit().unwrap();
        let s = get_reconcile_state(&c, "h").unwrap().unwrap();
        let stored = s.watcher_error.unwrap();
        assert!(stored.len() <= MAX_ERROR_STRING_BYTES);
        assert!(stored.chars().all(|ch| ch == 'あ'), "must remain UTF-8");
    }

    #[test]
    fn increment_force_generation_bumps_both_desired_and_force_and_clears_retry() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        assert_eq!(increment_desired_generation(&tx, "h", 10).unwrap(), 1);
        mark_attempt_start(&tx, "h", 1, 20).unwrap();
        mark_attempt_failure(&tx, "h", 1, "transient failure", 5_000).unwrap();
        assert_eq!(increment_force_generation(&tx, "h", 30).unwrap(), 2);
        tx.commit().unwrap();
        let s = get_reconcile_state(&c, "h").unwrap().unwrap();
        assert_eq!(s.desired_generation, 2);
        assert_eq!(s.force_generation, 2);
        assert_eq!(s.next_retry_at_ns, None);
        assert_eq!(s.consecutive_failures, 1);
        assert_eq!(s.last_error.as_deref(), Some("transient failure"));
    }

    // ─── MF-3: direct v3 → v4 migration path ────────────────────

    /// Open a fresh temp DB and apply migrations v1..v3 ONLY,
    /// then hand back the tempdir + connection so the test can
    /// seed v3-shaped data before running the v4 step in
    /// isolation. Mirrors the on-disk shape a real 0.7.x daemon
    /// leaves behind before upgrading to a v4-carrying release.
    fn open_at_v3() -> (tempfile::TempDir, Connection) {
        use crate::migration::{apply, apply_standard_pragmas};
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("index.db");
        let mut conn = Connection::open(&path).unwrap();
        apply_standard_pragmas(&conn).unwrap();
        let up_to_v3: Vec<_> = MIGRATIONS
            .iter()
            .filter(|m| m.version <= 3)
            .cloned()
            .collect();
        apply(&mut conn, &up_to_v3).unwrap();
        let v: u32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 3, "harness must leave DB at v3 before v4 step");
        (tmp, conn)
    }

    fn apply_v4_only(conn: &mut Connection) -> Result<()> {
        use crate::migration::apply;
        let only_v4: Vec<_> = MIGRATIONS
            .iter()
            .filter(|m| m.version == 4)
            .cloned()
            .collect();
        apply(conn, &only_v4)
    }

    #[test]
    fn v3_to_v4_migration_empty_db_yields_empty_v4_tables() {
        let (_t, mut c) = open_at_v3();
        apply_v4_only(&mut c).unwrap();
        let v: u32 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 4);
        let n_repositories: i64 = c
            .query_row("SELECT COUNT(*) FROM repositories", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n_repositories, 0);
        // aliases table exists and is empty; the index the v2/v4
        // migration created must still resolve.
        let n_aliases: i64 = c
            .query_row("SELECT COUNT(*) FROM aliases", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n_aliases, 0);
        let n_state: i64 = c
            .query_row("SELECT COUNT(*) FROM repo_reconcile_state", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(n_state, 0);
    }

    #[test]
    fn v3_to_v4_migration_collapses_two_aliases_sharing_hash_and_root() {
        // Two aliases labelling the same on-disk repo must
        // collapse to ONE repositories row + ONE reconcile state
        // row after v4. Both aliases must survive as FK'd labels.
        let (_t, mut c) = open_at_v3();
        c.execute(
            "INSERT INTO aliases (alias, root_path, repo_hash, registered_at_ns) VALUES
                ('main',   '/repos/a', 'h_a', 100),
                ('worker', '/repos/a', 'h_a', 200)",
            [],
        )
        .unwrap();
        apply_v4_only(&mut c).unwrap();

        let repo: (String, String, i64) = c
            .query_row(
                "SELECT repo_hash, root_path, registered_at_ns FROM repositories",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(repo.0, "h_a");
        assert_eq!(repo.1, "/repos/a");
        assert_eq!(repo.2, 100, "MIN(registered_at_ns) wins on collapse");

        let mut labels = aliases_for_repo(&c, "h_a").unwrap();
        labels.sort();
        assert_eq!(labels, vec!["main".to_string(), "worker".to_string()]);

        // Reconcile state row is seeded exactly once, at
        // generation 0 with defaults intact.
        let s = get_reconcile_state(&c, "h_a").unwrap().unwrap();
        assert_eq!(s.desired_generation, 0);
        assert_eq!(s.applied_generation, 0);
        assert_eq!(s.force_generation, 0);
        assert!(s.attempt_generation.is_none());
        assert_eq!(s.consecutive_failures, 0);
        assert_eq!(s.watcher_state, WatcherState::Starting);
    }

    #[test]
    fn v3_to_v4_migration_fails_when_two_aliases_share_hash_but_disagree_on_root() {
        // On a v3 DB where two aliases point to the same
        // repo_hash but different root_path, the v4 data
        // migration's `SELECT DISTINCT repo_hash, root_path`
        // yields two rows sharing a PK — the INSERT must fail
        // rather than silently pick a winner. Fail-closed here
        // keeps the canonical bijection intact even on a dirty
        // pre-v4 DB; operators recover by inspecting the aliases
        // and deleting the wrong mapping before retrying.
        let (_t, mut c) = open_at_v3();
        c.execute(
            "INSERT INTO aliases (alias, root_path, repo_hash, registered_at_ns) VALUES
                ('a', '/repos/x', 'h_dup', 100),
                ('b', '/repos/y', 'h_dup', 200)",
            [],
        )
        .unwrap();
        let err = apply_v4_only(&mut c).unwrap_err();
        // Message shape is SQLite's, we don't care about exact
        // wording — just that the step aborted.
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("unique") || msg.contains("primary") || msg.contains("constraint"),
            "expected PK/UNIQUE violation, got {msg:?}"
        );
        // Migration ran in a transaction — user_version stays 3.
        let v: u32 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 3, "failed migration must not bump user_version");
    }

    #[test]
    fn v3_to_v4_migration_fails_when_two_aliases_share_root_but_disagree_on_hash() {
        // Same shape, opposite axis: shared root_path with
        // different repo_hash. `repositories.root_path` is
        // UNIQUE so the second INSERT must fail.
        let (_t, mut c) = open_at_v3();
        c.execute(
            "INSERT INTO aliases (alias, root_path, repo_hash, registered_at_ns) VALUES
                ('a', '/repos/z', 'h1', 100),
                ('b', '/repos/z', 'h2', 200)",
            [],
        )
        .unwrap();
        let err = apply_v4_only(&mut c).unwrap_err();
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("unique") || msg.contains("constraint"),
            "expected UNIQUE(root_path) violation, got {msg:?}"
        );
        let v: u32 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 3);
    }

    #[test]
    fn v3_to_v4_migration_preserves_public_alias_entry_shape() {
        // After a real v3→v4 upgrade, `lookup_by_alias` and
        // `list_all` must keep serving `AliasEntry { alias,
        // root_path, repo_hash, registered_at_ns }` — the
        // storage moved to `repositories` but the public API
        // JOIN must hide that from consumers.
        let (_t, mut c) = open_at_v3();
        c.execute(
            "INSERT INTO aliases (alias, root_path, repo_hash, registered_at_ns) VALUES
                ('demo', '/root', 'h', 42)",
            [],
        )
        .unwrap();
        apply_v4_only(&mut c).unwrap();

        let e = lookup_by_alias(&c, "demo").unwrap().unwrap();
        assert_eq!(e.alias, "demo");
        assert_eq!(e.root_path, "/root");
        assert_eq!(e.repo_hash, "h");
        assert_eq!(e.registered_at_ns, 42);
        assert_eq!(list_all(&c).unwrap().len(), 1);
    }

    // ─── MF-2: attempt transition identity checks ──────────────

    fn assert_invalid_transition(err: crate::Error, needle: &str) {
        match err {
            crate::Error::Internal(msg) => assert!(
                msg.contains(needle),
                "expected invalid-transition error containing {needle:?}, got {msg}"
            ),
            other => panic!("expected Error::Internal, got {other:?}"),
        }
    }

    #[test]
    fn mark_attempt_start_rejects_negative_generation() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        let err = mark_attempt_start(&tx, "h", -1, 10).unwrap_err();
        assert_invalid_transition(err, "negative generation");
    }

    #[test]
    fn mark_attempt_start_rejects_generation_above_desired() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        increment_desired_generation(&tx, "h", 10).unwrap(); // desired=1
        let err = mark_attempt_start(&tx, "h", 2, 20).unwrap_err();
        assert_invalid_transition(err, "start:");
    }

    #[test]
    fn mark_attempt_start_rejects_generation_at_or_below_applied() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        increment_desired_generation(&tx, "h", 10).unwrap(); // desired=1
        mark_attempt_start(&tx, "h", 1, 20).unwrap();
        mark_attempt_success(&tx, "h", 1, 30).unwrap(); // applied=1
        // Re-starting at the already-applied generation must be
        // rejected — no work to do.
        let err = mark_attempt_start(&tx, "h", 1, 40).unwrap_err();
        assert_invalid_transition(err, "start:");
    }

    #[test]
    fn mark_attempt_start_rejects_second_start_over_in_flight_attempt() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        increment_desired_generation(&tx, "h", 10).unwrap();
        increment_desired_generation(&tx, "h", 20).unwrap(); // desired=2
        mark_attempt_start(&tx, "h", 1, 30).unwrap();
        // Second start while attempt_generation is non-NULL must
        // fail even if the new generation is otherwise valid.
        let err = mark_attempt_start(&tx, "h", 2, 40).unwrap_err();
        assert_invalid_transition(err, "start:");
    }

    #[test]
    fn mark_attempt_start_rejects_missing_repo() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        let err = mark_attempt_start(&tx, "ghost", 1, 10).unwrap_err();
        assert_invalid_transition(err, "start:");
    }

    #[test]
    fn mark_attempt_success_rejects_generation_mismatch() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        increment_desired_generation(&tx, "h", 10).unwrap(); // desired=1
        mark_attempt_start(&tx, "h", 1, 20).unwrap();
        // Stale completion of a different generation must fail;
        // must NOT clear the in-flight attempt slot.
        let err = mark_attempt_success(&tx, "h", 2, 30).unwrap_err();
        assert_invalid_transition(err, "success:");
        let s = get_reconcile_state(&tx, "h").unwrap().unwrap();
        assert_eq!(
            s.attempt_generation,
            Some(1),
            "in-flight attempt must not be cleared by stale success"
        );
        assert_eq!(s.applied_generation, 0);
    }

    #[test]
    fn mark_attempt_success_rejects_without_active_attempt() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        increment_desired_generation(&tx, "h", 10).unwrap();
        // No mark_attempt_start — attempt_generation IS NULL.
        let err = mark_attempt_success(&tx, "h", 1, 20).unwrap_err();
        assert_invalid_transition(err, "success:");
    }

    #[test]
    fn mark_attempt_success_rejects_missing_repo() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        let err = mark_attempt_success(&tx, "ghost", 1, 10).unwrap_err();
        assert_invalid_transition(err, "success:");
    }

    #[test]
    fn mark_attempt_failure_rejects_generation_mismatch() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        increment_desired_generation(&tx, "h", 10).unwrap();
        mark_attempt_start(&tx, "h", 1, 20).unwrap();
        let err = mark_attempt_failure(&tx, "h", 2, "stale", 100).unwrap_err();
        assert_invalid_transition(err, "failure:");
        let s = get_reconcile_state(&tx, "h").unwrap().unwrap();
        assert_eq!(
            s.attempt_generation,
            Some(1),
            "stale failure must not clear the in-flight attempt"
        );
        assert!(s.last_error.is_none());
        assert_eq!(s.consecutive_failures, 0);
    }

    #[test]
    fn mark_attempt_failure_rejects_without_active_attempt() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert_repository(&tx, "h", "/p", 0).unwrap();
        increment_desired_generation(&tx, "h", 10).unwrap();
        let err = mark_attempt_failure(&tx, "h", 1, "orphan", 100).unwrap_err();
        assert_invalid_transition(err, "failure:");
    }

    #[test]
    fn mark_attempt_failure_rejects_missing_repo() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        let err = mark_attempt_failure(&tx, "ghost", 1, "gone", 100).unwrap_err();
        assert_invalid_transition(err, "failure:");
    }

    #[test]
    fn set_watcher_state_rejects_missing_repo() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        let err = set_watcher_state(&tx, "ghost", WatcherState::Active, None).unwrap_err();
        assert_invalid_transition(err, "watcher_state:");
    }

    #[test]
    fn truncate_utf8_never_splits_a_char() {
        assert_eq!(truncate_utf8("abcdef", 3), "abc");
        // "あ" is 3 bytes; asking for 2 must trim to 0.
        assert_eq!(truncate_utf8("あ", 2), "");
        // "aあb" is 5 bytes; asking for 4 must trim to "a" (1 byte).
        assert_eq!(truncate_utf8("aあb", 4), "aあ");
    }

    #[test]
    fn v5_defaults_existing_repositories_to_ephemeral() {
        let mut conn = Connection::open_in_memory().unwrap();
        crate::migration::apply(&mut conn, &MIGRATIONS[..4]).unwrap();
        {
            let tx = conn.transaction().unwrap();
            upsert(&tx, "demo", "/repo", "hash", 1).unwrap();
            tx.commit().unwrap();
        }

        crate::migration::apply(&mut conn, MIGRATIONS).unwrap();

        let repo = lookup_repository(&conn, "hash").unwrap().unwrap();
        assert!(!repo.persistent);
        assert!(repo.removal_request.is_none());
        assert_eq!(aliases_for_repo(&conn, "hash").unwrap(), vec!["demo"]);
        assert!(get_reconcile_state(&conn, "hash").unwrap().is_some());
    }

    #[test]
    fn persistence_updates_are_separate_from_identity_upsert() {
        let (_t, mut conn) = fresh();
        let tx = conn.transaction().unwrap();
        upsert(&tx, "demo", "/repo", "hash", 1).unwrap();
        assert!(set_repository_persistent(&tx, "hash", true).unwrap());
        upsert_repository(&tx, "hash", "/repo", 2).unwrap();
        tx.commit().unwrap();

        assert!(
            lookup_repository(&conn, "hash")
                .unwrap()
                .unwrap()
                .persistent
        );
    }

    #[test]
    fn removal_request_and_registry_delete_event_are_atomic() {
        let (_t, mut conn) = fresh();
        let tx = conn.transaction().unwrap();
        upsert(&tx, "demo", "/repo", "hash", 1).unwrap();
        assert!(
            mark_removal_requested(&tx, "hash", RepositoryRemovalReason::MissingRoot, 10).unwrap()
        );
        assert!(
            !mark_removal_requested(&tx, "hash", RepositoryRemovalReason::LastAliasRemoved, 20,)
                .unwrap()
        );
        let event_id = delete_repository_with_event(&tx, "hash", 30)
            .unwrap()
            .unwrap();
        tx.commit().unwrap();

        assert!(lookup_repository(&conn, "hash").unwrap().is_none());
        assert!(lookup_by_alias(&conn, "demo").unwrap().is_none());
        let events = list_incomplete_removals(&conn).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_id, event_id);
        assert_eq!(events[0].reason, RepositoryRemovalReason::MissingRoot);
        assert_eq!(events[0].store_cleanup_state, StoreCleanupState::Pending);
    }

    #[test]
    fn completed_event_retention_never_prunes_pending_or_error() {
        let (_t, mut conn) = fresh();
        for id in 0..5 {
            conn.execute(
                "INSERT INTO repository_removal_events
                 (repo_hash, root_path, removed_at_ns, reason, store_cleanup_state)
                 VALUES (?1, '/repo', ?2, 'missing_root', ?3)",
                params![
                    format!("h{id}"),
                    id,
                    if id == 0 { "pending" } else { "complete" }
                ],
            )
            .unwrap();
        }
        conn.execute(
            "UPDATE repository_removal_events
             SET store_cleanup_state = 'error', cleanup_error = 'failed'
             WHERE repo_hash = 'h1'",
            [],
        )
        .unwrap();

        let tx = conn.transaction().unwrap();
        assert_eq!(prune_completed_removal_events(&tx, 2).unwrap(), 1);
        tx.commit().unwrap();

        let incomplete = list_incomplete_removals(&conn).unwrap();
        assert_eq!(incomplete.len(), 2);
        assert_eq!(list_recent_completed_removals(&conn, 10).unwrap().len(), 2);
    }

    #[test]
    fn cleanup_error_is_utf8_safely_truncated() {
        let (_t, mut conn) = fresh();
        conn.execute(
            "INSERT INTO repository_removal_events
             (repo_hash, root_path, removed_at_ns, reason, store_cleanup_state)
             VALUES ('h', '/repo', 1, 'missing_root', 'pending')",
            [],
        )
        .unwrap();
        let event_id = conn.last_insert_rowid();
        let tx = conn.transaction().unwrap();
        mark_store_cleanup_error(&tx, event_id, &"あ".repeat(3000)).unwrap();
        tx.commit().unwrap();

        let error = list_incomplete_removals(&conn).unwrap()[0]
            .cleanup_error
            .clone()
            .unwrap();
        assert!(error.len() <= MAX_ERROR_STRING_BYTES);
        assert!(error.chars().all(|ch| ch == 'あ'));
    }
}
