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

use cairn_resolver_eval::cases::{java_cases, python_cases, rust_cases, typescript_cases};
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
