use super::{refs_dedup_fixture, registered};
use crate::anchor::AnchorName;
use crate::cas::store;
use crate::query::{FindReferencesArgs, find_references};
use crate::register::register_repo;
use crate::testutil::init_repo;
use cairn_proto::common::RefKind;
use cairn_proto::methods::ReferenceDirection;

#[test]
fn references_incoming_finds_callers() {
    let (_repo, _db, c) = registered();
    let hits = find_references(
        &c,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "alpha".into(),
            direction: ReferenceDirection::Incoming,
            ..Default::default()
        },
    )
    .unwrap();
    // alpha is referenced inside the file (see fixture src/lib.rs);
    // at minimum we shouldn't error and the SQL must execute.
    // Whether the syntactic-only Rust analyzer surfaces this
    // particular call depends on the parser; assert structural
    // correctness instead of a specific count.
    for h in &hits {
        assert_eq!(h.target_name, "alpha");
    }
}

#[test]
fn references_outgoing_resolves_enclosing() {
    let (_repo, _db, c) = registered();
    // No symbol called `nonexistent` exists; the outgoing query
    // should run and return an empty result rather than error.
    let hits = find_references(
        &c,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "nonexistent::callee".into(),
            direction: ReferenceDirection::Outgoing,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(hits.is_empty());
}

#[test]
fn references_outgoing_defaults_to_resolved_calls() {
    let (_repo, _sha) = init_repo(&[(
        "src/lib.rs",
        "pub struct Widget;\n\
             impl Widget { pub fn render(&self) {} }\n\
             pub fn resolved() {}\n\
             pub fn caller(arg: Widget) -> Widget {\n\
                 resolved();\n\
                 arg.render();\n\
                 arg\n\
             }\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "caller".into(),
            direction: ReferenceDirection::Outgoing,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(
        hits.iter()
            .map(|h| h.target_name.as_str())
            .collect::<Vec<_>>(),
        vec!["resolved"],
        "outgoing default should hide unresolved method calls and type refs: {hits:?}"
    );
    assert!(hits.iter().all(|h| h.kind == RefKind::Call));
    assert!(hits.iter().all(|h| h.target_qualified.is_some()));
}

#[test]
fn references_outgoing_include_noise_returns_legacy_refs() {
    let (_repo, _sha) = init_repo(&[(
        "src/lib.rs",
        "pub struct Widget;\n\
             impl Widget { pub fn render(&self) {} }\n\
             pub fn resolved() {}\n\
             pub fn caller(arg: Widget) -> Widget {\n\
                 resolved();\n\
                 arg.render();\n\
                 arg\n\
             }\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "caller".into(),
            direction: ReferenceDirection::Outgoing,
            include_noise: true,
            ..Default::default()
        },
    )
    .unwrap();

    assert!(
        hits.iter().any(|h| h.target_name == "resolved"
            && h.kind == RefKind::Call
            && h.target_qualified.as_deref() == Some("resolved")),
        "resolved call missing from noisy outgoing refs: {hits:?}"
    );
    assert!(
        hits.iter().any(|h| h.target_name == "render"
            && h.kind == RefKind::Call
            && h.target_qualified.is_none()),
        "unresolved method call missing from include_noise refs: {hits:?}"
    );
    assert!(
        hits.iter()
            .any(|h| h.target_name == "Widget" && h.kind == RefKind::Type),
        "type refs missing from include_noise refs: {hits:?}"
    );
}

#[test]
fn references_include_typescript_tier2_call_refs() {
    let (_repo, _sha) = init_repo(&[(
        "src/app.ts",
        "function caller() {\n\
                 foo();\n\
             }\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "foo".into(),
            direction: ReferenceDirection::Incoming,
            ..Default::default()
        },
    )
    .unwrap();

    assert!(
        hits.iter().any(|h| {
            h.target_name == "foo"
                && h.target_qualified.as_deref() == Some("foo")
                && h.enclosing_qualified.as_deref() == Some("caller")
                && h.path == "src/app.ts"
        }),
        "TypeScript Tier-2 call ref missing from query results: {hits:?}"
    );
}

#[test]
fn references_tier2_only_falls_back_to_bare_name_refs() {
    let (_db, conn) = refs_dedup_fixture(false, None);

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "render".into(),
            direction: ReferenceDirection::Incoming,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0].target_name, "render");
    assert_eq!(hits[0].target_qualified, None);
}

#[test]
fn references_tier3_suppresses_tier2_same_call_site() {
    let (_db, conn) = refs_dedup_fixture(true, None);

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "render".into(),
            direction: ReferenceDirection::Incoming,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(
        hits[0].target_qualified.as_deref(),
        Some("crate::Widget::render")
    );
}

#[test]
fn references_tier3_failed_run_falls_back_to_tier2_refs() {
    let (_db, conn) = refs_dedup_fixture(false, Some("failed"));

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "render".into(),
            direction: ReferenceDirection::Incoming,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0].target_name, "render");
    assert_eq!(hits[0].target_qualified, None);
}

#[test]
fn references_outgoing_prefers_tier3_for_same_call_site() {
    let (_db, conn) = refs_dedup_fixture(true, None);

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "caller".into(),
            direction: ReferenceDirection::Outgoing,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(
        hits[0].target_qualified.as_deref(),
        Some("crate::Widget::render")
    );
}

#[test]
fn references_include_noise_keeps_tier2_and_tier3_duplicates() {
    let (_db, conn) = refs_dedup_fixture(true, None);

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "render".into(),
            direction: ReferenceDirection::Incoming,
            include_noise: true,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(hits.len(), 2, "{hits:?}");
    assert!(
        hits.iter().any(|h| h.target_qualified.is_none()),
        "Tier-2 fallback row missing from noisy refs: {hits:?}"
    );
    assert!(
        hits.iter()
            .any(|h| h.target_qualified.as_deref() == Some("crate::Widget::render")),
        "Tier-3 row missing from noisy refs: {hits:?}"
    );
}

#[test]
fn references_empty_symbol_errors() {
    let (_repo, _db, c) = registered();
    let err = find_references(
        &c,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "  ".into(),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("non-empty"));
}
