//! `cairn-resolver-eval` — accuracy harness for Tier-2 (syntactic) and
//! Tier-3 (LSP) resolvers, built on mini-repo fixtures with hand-curated
//! golden expectations.
//!
//! ## What it is
//!
//! A `GoldenCase` pins a (language, tool, query) tuple to the set of
//! hits the resolver *should* return. The runner copies a fixture into
//! a fresh tempdir, `git init` + commits it (cairn requires a git repo),
//! registers it against a throwaway CAS store, runs the query in-process
//! against `cairn-core`'s public query API, and diffs the actual hits
//! against the golden set to produce a `TierReport` with precision /
//! recall.
//!
//! ## Tier-2 vs Tier-3
//!
//! `register_repo` runs syntactic (Tier-2) analyzers synchronously
//! in-process. Tier-3 (LSP) backends need external language servers
//! that are not guaranteed to be available in CI, so this crate only
//! force-links the Tier-2 backends. The `Tier3` track is preserved as
//! a structural placeholder for the upcoming Tier-2.5 resolver: cases
//! ship with both `tier2_expected` and `tier3_expected`, and a future
//! runner addition can drive the Tier-3 evaluation without changing
//! the golden tables.
//!
//! ## Why this lives in a dedicated crate
//!
//! Production crates must not depend on `tempfile` and must not bundle
//! every language backend; this crate does both. Tests here are the
//! only consumers of the harness — there is no `pub` API meant for
//! downstream code yet, but the modules are `pub` so integration tests
//! (`tests/golden.rs`) can drive them.

#![deny(unsafe_code)]

// Force-link Tier-2 language backends so their `#[distributed_slice]`
// entries reach the integration-test binary. Without these `use _`
// references, dead-code elimination drops the crates and
// `all_backends()` returns empty for our fixture languages — exactly
// the "register_repo parsed zero blobs" failure mode.
use cairn_lang_java as _;
use cairn_lang_python as _;
use cairn_lang_ruby as _;
use cairn_lang_rust as _;
use cairn_lang_typescript as _;

pub mod cases;
pub mod fixture;
pub mod report;
pub mod runner;
pub mod types;

pub use report::{EvalReport, TierReport};
pub use runner::run_case;
pub use types::{ActualHit, ExpectedHit, GoldenCase, Query, Tool};
