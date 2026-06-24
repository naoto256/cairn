//! Precision / recall scoring.
//!
//! Hits are matched on the normalized triple `(path, line,
//! target_qualified)`. The qualified name is normalized by stripping
//! leading `::`, collapsing whitespace, and trimming — this lets a
//! golden author write either `crate::foo::Bar::method` or
//! `foo::Bar::method` and have both compare equal to what the
//! resolver returns on a given fixture (qualified names vary slightly
//! across backends).

use std::collections::HashSet;

use crate::types::{ActualHit, ExpectedHit};

/// Per-tier scorecard. `missing` are hits the resolver should have
/// returned but didn't; `extra` are hits it returned that no golden
/// row matches.
#[derive(Debug, Clone)]
pub struct TierReport {
    pub precision: f64,
    pub recall: f64,
    pub matched: usize,
    pub missing: Vec<ExpectedHit>,
    pub extra: Vec<ActualHit>,
}

impl TierReport {
    /// Score `actual` against `expected`. Empty `expected` is treated
    /// as a vacuous pass (precision = 1.0 iff actual is also empty,
    /// recall = 1.0).
    pub fn score(expected: &[ExpectedHit], actual: &[ActualHit]) -> Self {
        let exp_keys: HashSet<(String, u32, String)> = expected
            .iter()
            .map(|e| {
                (
                    e.path.clone(),
                    e.line,
                    normalize_qualified(&e.target_qualified),
                )
            })
            .collect();
        let act_keys: HashSet<(String, u32, String)> = actual
            .iter()
            .map(|a| {
                (
                    a.path.clone(),
                    a.line,
                    normalize_qualified(&a.target_qualified),
                )
            })
            .collect();

        let matched = exp_keys.intersection(&act_keys).count();
        let missing: Vec<ExpectedHit> = expected
            .iter()
            .filter(|e| {
                !act_keys.contains(&(
                    e.path.clone(),
                    e.line,
                    normalize_qualified(&e.target_qualified),
                ))
            })
            .cloned()
            .collect();
        let extra: Vec<ActualHit> = actual
            .iter()
            .filter(|a| {
                !exp_keys.contains(&(
                    a.path.clone(),
                    a.line,
                    normalize_qualified(&a.target_qualified),
                ))
            })
            .cloned()
            .collect();

        let recall = if exp_keys.is_empty() {
            1.0
        } else {
            matched as f64 / exp_keys.len() as f64
        };
        let precision = if act_keys.is_empty() {
            if exp_keys.is_empty() { 1.0 } else { 0.0 }
        } else {
            matched as f64 / act_keys.len() as f64
        };

        Self {
            precision,
            recall,
            matched,
            missing,
            extra,
        }
    }
}

/// Full case report — both tiers side by side. Tier-3 may carry an
/// empty actual set in the current build (LSP not driven in-process);
/// callers can detect that with `tier3.extra.is_empty() &&
/// tier3.missing == tier3_expected`.
#[derive(Debug, Clone)]
pub struct EvalReport {
    pub case: &'static str,
    pub language: &'static str,
    pub tier2: TierReport,
    /// Score for the Tier-2.5 (cross-file syntactic) resolver. Until
    /// the per-language Tier-2.5 backend is wired into the runner the
    /// actual set is empty and recall reflects nothing more than the
    /// case author's promise.
    pub tier25: TierReport,
    pub tier3: TierReport,
}

/// Strip leading `::`, collapse internal whitespace, trim. Backends
/// disagree on whether to prefix the crate / module root in qualified
/// names; this normalization keeps the golden table portable.
pub fn normalize_qualified(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_ws = false;
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !last_ws {
                out.push(' ');
                last_ws = true;
            }
        } else {
            out.push(ch);
            last_ws = false;
        }
    }
    let trimmed = out.trim().trim_start_matches("::");
    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exp(path: &str, line: u32, q: &str) -> ExpectedHit {
        ExpectedHit {
            path: path.to_string(),
            line,
            target_qualified: q.to_string(),
        }
    }
    fn act(path: &str, line: u32, q: &str) -> ActualHit {
        ActualHit {
            path: path.to_string(),
            line,
            target_qualified: q.to_string(),
            parser_id: "test".to_string(),
        }
    }

    #[test]
    fn perfect_match_scores_one() {
        let e = vec![exp("a.rs", 1, "foo")];
        let a = vec![act("a.rs", 1, "foo")];
        let r = TierReport::score(&e, &a);
        assert_eq!(r.precision, 1.0);
        assert_eq!(r.recall, 1.0);
        assert!(r.missing.is_empty() && r.extra.is_empty());
    }

    #[test]
    fn normalization_handles_leading_colons() {
        let e = vec![exp("a.rs", 1, "::foo::Bar")];
        let a = vec![act("a.rs", 1, "foo::Bar")];
        assert_eq!(TierReport::score(&e, &a).recall, 1.0);
    }

    #[test]
    fn missing_and_extra_split_correctly() {
        let e = vec![exp("a.rs", 1, "foo"), exp("b.rs", 2, "bar")];
        let a = vec![act("a.rs", 1, "foo"), act("c.rs", 3, "baz")];
        let r = TierReport::score(&e, &a);
        assert_eq!(r.matched, 1);
        assert_eq!(r.missing.len(), 1);
        assert_eq!(r.extra.len(), 1);
        assert!((r.precision - 0.5).abs() < 1e-9);
        assert!((r.recall - 0.5).abs() < 1e-9);
    }
}
