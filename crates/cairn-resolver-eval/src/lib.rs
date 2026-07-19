//! `cairn-resolver-eval` — accuracy harness for Tier-2 (syntactic),
//! Tier-2.5 (cross-file syntactic), and Tier-3 (LSP) resolvers, built on
//! mini-repo fixtures with hand-curated golden expectations.
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
//! ## Resolver tiers
//!
//! `register_repo` runs syntactic (Tier-2) analyzers synchronously
//! in-process, then invokes every force-linked Tier-2.5 workspace
//! analyzer and persists its resolution rows. Query results expose the
//! winning row's `kind_source`, allowing the harness to score Tier-2.5
//! independently from the all-row Tier-2 baseline. Tier-3 (LSP)
//! backends need external language servers that are not guaranteed in
//! CI, so that track remains a structural placeholder.
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
use cairn_lang_csharp as _;
use cairn_lang_java as _;
use cairn_lang_kotlin as _;
use cairn_lang_php as _;
use cairn_lang_python as _;
use cairn_lang_ruby as _;
use cairn_lang_rust as _;
use cairn_lang_swift as _;
use cairn_lang_typescript as _;

// Same trick for Tier-2.5 cross-file syntactic backends. Without these
// references, `linkme`'s `WORKSPACE_ANALYZERS` entries can be dropped
// by dead-code elimination and the golden gate would silently measure
// Tier-2 fallback rows only.
/// Keep concrete analyzer vtables reachable from the test executable.
///
/// A dependency-only `use crate as _` is not a linker anchor: the crate can
/// still be omitted from the final integration-test binary, along with its
/// `WORKSPACE_ANALYZERS` distributed-slice entry. Constructing trait objects
/// creates a real vtable reference for every backend. Call this before reading
/// the registry so a missing backend becomes a test failure, not a zero-hit
/// measurement.
pub(crate) fn force_link_tier25_analyzers() {
    use cairn_core::workspace_analyzer::WorkspaceAnalyzer;

    let analyzers: [&dyn WorkspaceAnalyzer; 7] = [
        &cairn_lang_csharp_tier25::CSharpTier25Analyzer,
        &cairn_lang_javascript_tier25::JavaScriptTier25Analyzer,
        &cairn_lang_kotlin_tier25::KotlinTier25Analyzer,
        &cairn_lang_php_tier25::PhpTier25Analyzer,
        &cairn_lang_python_tier25::PythonTier25Analyzer,
        &cairn_lang_ruby_tier25::RubyTier25Analyzer,
        &cairn_lang_swift_tier25::SwiftTier25Analyzer,
    ];

    std::hint::black_box(analyzers);
}

pub mod cases;
pub mod fixture;
pub mod report;
pub mod runner;
pub mod types;

pub use report::{EvalReport, TierReport};
pub use runner::{RegisteredFixture, register_fixture, run_case};
pub use types::{ActualHit, ExpectedHit, GoldenCase, Query, Tool};
