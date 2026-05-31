//! `cairn-core` — daemon internals.
//!
//! Owns the on-disk storage layout (a central registry DB plus one
//! data DB per `(worktree, branch)` snapshot), the schema migration
//! framework, the parser pool, and the MCP / control dispatch. The
//! CLI crate (`cairn-cli`) is a thin driver that selects between the
//! `daemon`, `serve`, and `ctl` subcommands and delegates the work
//! here.

// `linkme`'s `#[distributed_slice]` expansion emits a static with
// `link_section`, which rustc flags under the `unsafe_code` lint
// group. We write no unsafe code ourselves; `deny` keeps the door
// shut against accidental `unsafe` blocks while allowing explicit
// `#[allow(unsafe_code)]` on the linker-section attributes the
// distributed-slice machinery requires. Sibling crates that only
// host the slice DECLARATION (cairn-lang-api) can stay on `forbid`;
// crates with the ENTRIES (this one) cannot.
#![deny(unsafe_code)]

pub mod anchor;
pub mod cas;
pub mod ctl;
pub mod daemon;
pub mod data_db;
pub mod data_rpc;
pub mod indexer;
pub mod manifest;
pub mod migration;
pub mod paths;
pub mod query;
pub mod register;
pub mod registry_db;
pub mod snapshot_stats;
pub mod sockets;
pub mod storage;
pub mod watcher;

pub use paths::DataDir;
pub use registry_db::{Repo, Snapshot, SnapshotEnrichment, SnapshotStatus, Worktree};
pub use sockets::SocketPaths;

/// Indexer revision. Stamped onto every snapshot the daemon writes;
/// on daemon startup, snapshots whose stored revision is below this
/// constant are scheduled for a background `full_index` so any
/// extractor changes that landed since the snapshot was last built
/// (new fact kinds, schema-affecting fixes) take effect without the
/// operator having to remember to `cairn ctl reindex-repo`.
///
/// Bump this **whenever the extractor pipeline produces materially
/// different output for the same input** — e.g. a new `RefKind` is
/// captured, a Tier-2 analyzer learns a new construct, or a parser
/// version with semantic differences ships. Do not bump for pure
/// query-side changes (the data on disk is unchanged) or for
/// cosmetic refactors.
///
/// History:
/// - 1 (2026-05): first stamped revision. Tracks the addition of
///   `find_references` (BodyVisitor populating the `refs` table) so
///   any snapshot written before this lands gets re-indexed at
///   startup, populating refs that the previous binary couldn't.
/// - 2 (2026-05): Rust analyzer extended to emit `RefKind::Type`,
///   `RefKind::Instantiate`, `RefKind::Annotation`, and
///   `RefKind::MacroInvoke` in addition to `Call`. Older snapshots
///   only have `Call` rows; bump forces a reindex so all kinds
///   land.
/// - 3 (2026-05): Rust tree-sitter pass now strips generic args
///   from impl self-types when building `qualified`, so
///   `impl<T> Foo<T> { fn bar() }` produces `Foo::bar` instead of
///   `Foo<T>::bar`. Aligns with the syn analyzer's
///   `enclosing_qualified` for refs; without this fix the
///   apply_pending_refs join silently lost ~3 % of refs' enclosing
///   attribution (every body-level call inside a generic-typed
///   impl method). Existing snapshots have the old qualified
///   format and the misjoined refs; reindex repairs both.
/// - 4 (2026-05): Indexer now skips any single file larger than
///   4 MiB (`MAX_INDEXED_FILE_BYTES`). Older snapshots may contain
///   rows for oversized generated files; bump forces a reindex so
///   the snapshot tracks the new policy.
/// - 5 (2026-05): Python gained a Tier-2 analyzer (imports +
///   class-inheritance edges via tree-sitter). Python snapshots
///   indexed at revision ≤ 4 have no `imports` / `implementations`
///   rows and `enrichment = Syntactic`; bump forces a reindex so they
///   pick up the semantic facts and flip to `enrichment = Semantic`.
/// - 6 (2026-05): Python Tier-2 analyzer extended to emit `refs`
///   (call sites `RefKind::Call` + signature type annotations
///   `RefKind::Type`). Python snapshots indexed at revision 5 have
///   no `refs` rows; bump forces a reindex so `find_references` works.
pub const INDEXER_REVISION: u32 = 6;

/// Crate-level error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("schema corruption: {0}")]
    SchemaCorruption(String),
}

pub type Result<T> = std::result::Result<T, Error>;
