//! Golden integration tests.
//!
//! Each `#[test]` runs all cases for one language and reports both
//! precision and recall. Tier-2 and Tier-2.5 recall are gated by
//! per-language ratchet floors; Tier-3 remains structural-only because
//! CI does not provision external language servers.
//!
//! Run `cargo test -p cairn-resolver-eval -- --nocapture` to see the
//! per-case report.

use cairn_resolver_eval::cases::{
    csharp_cases, java_cases, javascript_cases, kotlin_cases, php_cases, python_cases, ruby_cases,
    rust_cases, swift_cases, typescript_cases,
};
use cairn_resolver_eval::runner::run_case;
use cairn_resolver_eval::types::GoldenCase;

/// Per-language Tier-2 precision and recall floor. Tier-2 is incomplete
/// by design, so each floor is a ratchet below the measured baseline.
const RUST_FLOOR: f64 = 0.6;
const PYTHON_FLOOR: f64 = 0.6;
const TYPESCRIPT_FLOOR: f64 = 0.6;
const JAVA_FLOOR: f64 = 0.6;
const PHP_FLOOR: f64 = 0.95;
const KOTLIN_FLOOR: f64 = 0.95;
const SWIFT_FLOOR: f64 = 0.95;
const CSHARP_FLOOR: f64 = 0.95;
const JAVASCRIPT_FLOOR: f64 = 0.95;
const RUBY_FLOOR: f64 = 0.95;

const RUBY_TIER25_FLOOR: f64 = 0.95;
const PHP_TIER25_FLOOR: f64 = 0.95;
const PYTHON_TIER25_FLOOR: f64 = 0.95;
const KOTLIN_TIER25_FLOOR: f64 = 0.95;
const SWIFT_TIER25_FLOOR: f64 = 0.95;
const CSHARP_TIER25_FLOOR: f64 = 0.95;
const JAVASCRIPT_TIER25_FLOOR: f64 = 0.95;

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
    assert!(
        avg_precision >= floor,
        "Tier-2 precision regressed: avg={avg_precision:.2} < floor={floor:.2}"
    );
}

fn assert_tier25_floor(cases: Vec<GoldenCase>, floor: f64) {
    let mut total_recall = 0.0;
    let mut total_precision = 0.0;
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
        if !report.tier25.extra.is_empty() && report.tier25.extra.len() <= 10 {
            eprintln!("  extra: {:#?}", report.tier25.extra);
        }
        if !case.tier25_expected.is_empty() {
            total_recall += report.tier25.recall;
            total_precision += report.tier25.precision;
            counted += 1;
        }
    }

    assert!(counted > 0, "no scoreable tier25 cases");
    let avg_recall = total_recall / counted as f64;
    let avg_precision = total_precision / counted as f64;
    eprintln!(
        "tier25 summary: cases={counted} avg_precision={avg_precision:.2} avg_recall={avg_recall:.2} floor={floor:.2}"
    );
    assert!(
        avg_recall >= floor,
        "Tier-2.5 recall regressed: avg={avg_recall:.2} < floor={floor:.2}"
    );
    assert!(
        avg_precision >= floor,
        "Tier-2.5 precision regressed: avg={avg_precision:.2} < floor={floor:.2}"
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

#[test]
fn php_tier2_baseline() {
    assert_tier2_floor(php_cases(), PHP_FLOOR);
}

#[test]
fn kotlin_tier2_baseline() {
    assert_tier2_floor(kotlin_cases(), KOTLIN_FLOOR);
}

#[test]
fn swift_tier2_baseline() {
    assert_tier2_floor(swift_cases(), SWIFT_FLOOR);
}

#[test]
fn csharp_tier2_baseline() {
    assert_tier2_floor(csharp_cases(), CSHARP_FLOOR);
}

#[test]
fn javascript_tier2_baseline() {
    assert_tier2_floor(javascript_cases(), JAVASCRIPT_FLOOR);
}

#[test]
fn ruby_tier2_baseline() {
    assert_tier2_floor(ruby_cases(), RUBY_FLOOR);
}

#[test]
fn ruby_tier25_baseline() {
    assert_tier25_floor(ruby_cases(), RUBY_TIER25_FLOOR);
}

#[test]
fn php_tier25_baseline() {
    assert_tier25_floor(php_cases(), PHP_TIER25_FLOOR);
}

#[test]
fn python_tier25_baseline() {
    assert_tier25_floor(python_cases(), PYTHON_TIER25_FLOOR);
}

#[test]
fn kotlin_tier25_baseline() {
    assert_tier25_floor(kotlin_cases(), KOTLIN_TIER25_FLOOR);
}

#[test]
fn swift_tier25_baseline() {
    assert_tier25_floor(swift_cases(), SWIFT_TIER25_FLOOR);
}

#[test]
fn csharp_tier25_baseline() {
    assert_tier25_floor(csharp_cases(), CSHARP_TIER25_FLOOR);
}

#[test]
fn javascript_tier25_baseline() {
    assert_tier25_floor(javascript_cases(), JAVASCRIPT_TIER25_FLOOR);
}
