//! Golden integration tests.
//!
//! Each `#[test]` runs all cases for one language and asserts that the
//! Tier-2 recall meets the per-language baseline floor. Precision is
//! reported but not gated yet — false positives on syntactic backends
//! are common and the immediate value is "didn't lose a known-good
//! hit". Tier-3 is structural-only (see `runner.rs`); we assert it
//! scored without panicking but don't gate its numbers.
//!
//! Run `cargo test -p cairn-resolver-eval -- --nocapture` to see the
//! per-case report.

use cairn_resolver_eval::cases::{
    java_cases, python_cases, ruby_cases, rust_cases, typescript_cases,
};
use cairn_resolver_eval::runner::run_case;
use cairn_resolver_eval::types::GoldenCase;

/// Per-language Tier-2 recall floor. Tier-2 (syntactic) is incomplete
/// by design (no cross-file resolution for `find_callers` /
/// `find_callees`), so the floor is the minimum we can guarantee
/// today, observed in the first run.
const RUST_FLOOR: f64 = 0.6;
const PYTHON_FLOOR: f64 = 0.6;
const TYPESCRIPT_FLOOR: f64 = 0.6;
const JAVA_FLOOR: f64 = 0.6;
// Ruby Tier-2 floor is intentionally 0.0: the Ruby cases are authored
// against the Tier-2.5 spec, and several of their `tier2_expected`
// rows are observational placeholders the Tier-2 backend may or may
// not produce today. They will be tightened (and the floor raised)
// once Session B's resolver lands and we have a stable joint baseline.
const RUBY_FLOOR: f64 = 0.0;

fn assert_tier2_floor(cases: Vec<GoldenCase>, floor: f64) {
    let mut total_recall = 0.0;
    let mut total_precision = 0.0;
    let mut counted = 0;

    for case in &cases {
        let report = run_case(case).unwrap_or_else(|e| panic!("case {}: {e}", case.name));
        eprintln!(
            "[{}/{}] T2 prec={:.2} recall={:.2} matched={} missing={} extra={}",
            case.language,
            case.name,
            report.tier2.precision,
            report.tier2.recall,
            report.tier2.matched,
            report.tier2.missing.len(),
            report.tier2.extra.len(),
        );
        if !report.tier2.missing.is_empty() {
            eprintln!("  missing: {:#?}", report.tier2.missing);
        }
        if !report.tier2.extra.is_empty() && report.tier2.extra.len() <= 5 {
            eprintln!("  extra: {:#?}", report.tier2.extra);
        }
        // Skip averaging for vacuous (empty expected) cases.
        if !case.tier2_expected.is_empty() {
            total_recall += report.tier2.recall;
            total_precision += report.tier2.precision;
            counted += 1;
        }
    }

    assert!(counted > 0, "no scoreable cases");
    let avg_recall = total_recall / counted as f64;
    let avg_precision = total_precision / counted as f64;
    eprintln!(
        "summary: cases={counted} avg_precision={avg_precision:.2} avg_recall={avg_recall:.2} floor={floor:.2}"
    );
    assert!(
        avg_recall >= floor,
        "Tier-2 recall regressed: avg={avg_recall:.2} < floor={floor:.2}"
    );
}

#[test]
fn rust_tier2_baseline() {
    assert_tier2_floor(rust_cases(), RUST_FLOOR);
}

#[test]
fn python_tier2_baseline() {
    assert_tier2_floor(python_cases(), PYTHON_FLOOR);
}

#[test]
fn typescript_tier2_baseline() {
    assert_tier2_floor(typescript_cases(), TYPESCRIPT_FLOOR);
}

#[test]
fn java_tier2_baseline() {
    assert_tier2_floor(java_cases(), JAVA_FLOOR);
}

/// Ruby Tier-2 smoke: register the fixture and run every case without
/// panicking. Recall is not gated yet (see `RUBY_FLOOR`); we just
/// confirm the Tier-2 backend stays able to parse the fixtures and
/// the queries return without error.
#[test]
fn ruby_tier2_baseline() {
    assert_tier2_floor(ruby_cases(), RUBY_FLOOR);
}

/// Ruby Tier-2.5 baseline. The `cairn-lang-ruby-tier25` backend is
/// force-linked from `cairn-resolver-eval` (via `use _` in `lib.rs`)
/// and runs inside `register_repo`, persisting resolutions into the
/// CAS store. The scored `actual` set is shared with the Tier-2 path:
/// `find_subtypes` / `find_supertypes` already LEFT JOIN the
/// `resolutions` table (Phase 4, see `find_impls.rs`); `find_references`
/// gained the parallel join in the same pass (see `find_references.rs`),
/// so cases 4-7 surface Tier-2.5-resolved rows too. Case 8 is the
/// "retreat line" — dynamic dispatch that Tier-2.5 MUST NOT resolve
/// — and carries an empty `tier25_expected`, exempting it from the
/// averaged recall floor.
#[test]
fn ruby_tier25_baseline() {
    let cases = ruby_cases();
    let mut total_recall = 0.0;
    let mut counted = 0;
    for case in &cases {
        let report = run_case(case).unwrap_or_else(|e| panic!("case {}: {e}", case.name));
        eprintln!(
            "[{}/{}] T2.5 prec={:.2} recall={:.2} matched={} missing={} extra={}",
            case.language,
            case.name,
            report.tier25.precision,
            report.tier25.recall,
            report.tier25.matched,
            report.tier25.missing.len(),
            report.tier25.extra.len(),
        );
        if !report.tier25.missing.is_empty() {
            eprintln!("  missing: {:#?}", report.tier25.missing);
        }
        if !case.tier25_expected.is_empty() {
            total_recall += report.tier25.recall;
            counted += 1;
        }
    }
    assert!(counted > 0, "no scoreable tier25 cases");
    let avg_recall = total_recall / counted as f64;
    // Tight floor once the resolver lands. Until then this assertion
    // intentionally fails when `--ignored` is unset off, which is the
    // signal that Session B has work to do.
    assert!(
        avg_recall >= 0.8,
        "Tier-2.5 recall below floor: avg={avg_recall:.2} (resolver landed yet?)"
    );
}
