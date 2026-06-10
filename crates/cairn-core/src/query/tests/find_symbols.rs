use super::{language_fixture, registered, source_tier_fixture};
use crate::anchor::AnchorName;
use crate::cas::store;
use crate::query::{FindSymbolsArgs, OutlineFilter, find_symbols, get_outline_under_path};
use crate::register::register_repo;
use crate::testutil::init_repo;
use cairn_proto::common::SourceTier;

#[test]
fn find_by_name_returns_matching_symbol() {
    let (_repo, _db, c) = registered();
    let hits = find_symbols(
        &c,
        &AnchorName::head(),
        &FindSymbolsArgs {
            query: Some("alpha".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].name, "alpha");
    assert_eq!(hits[0].path, "src/lib.rs");
}

#[test]
fn fuzzy_multi_token_query_matches_all_tokens() {
    let (_repo, _db, c) = registered();
    let hits = find_symbols(
        &c,
        &AnchorName::head(),
        &FindSymbolsArgs {
            query: Some("User Authentication".into()),
            fuzzy: true,
            ..Default::default()
        },
    )
    .unwrap();
    let names: Vec<&str> = hits.iter().map(|h| h.name.as_str()).collect();
    assert!(names.contains(&"auth_user"), "{hits:?}");
    assert!(names.contains(&"reverse_auth_doc"), "{hits:?}");
}

#[test]
fn fuzzy_quoted_query_is_exact_order_phrase() {
    let (_repo, _db, c) = registered();
    let hits = find_symbols(
        &c,
        &AnchorName::head(),
        &FindSymbolsArgs {
            query: Some("\"User Authentication\"".into()),
            fuzzy: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0].name, "auth_user");
}

#[test]
fn fuzzy_prefix_matching_requires_star() {
    let (_repo, _db, c) = registered();
    let no_prefix = find_symbols(
        &c,
        &AnchorName::head(),
        &FindSymbolsArgs {
            query: Some("Authent".into()),
            fuzzy: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(no_prefix.is_empty(), "{no_prefix:?}");

    let prefix = find_symbols(
        &c,
        &AnchorName::head(),
        &FindSymbolsArgs {
            query: Some("Authent*".into()),
            fuzzy: true,
            ..Default::default()
        },
    )
    .unwrap();
    let names: Vec<&str> = prefix.iter().map(|h| h.name.as_str()).collect();
    assert!(names.contains(&"auth_user"), "{prefix:?}");
    assert!(names.contains(&"reverse_auth_doc"), "{prefix:?}");
}

#[test]
fn find_by_kind_filters() {
    let (_repo, _db, c) = registered();
    let hits = find_symbols(
        &c,
        &AnchorName::head(),
        &FindSymbolsArgs {
            kind: Some("struct".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        hits.iter().any(|h| h.name == "Widget"),
        "Widget not in {hits:?}"
    );
}

#[test]
fn find_symbols_reports_source_tier_from_blob_analyzer_id() {
    let (_db, c) = source_tier_fixture();
    let hits = find_symbols(
        &c,
        &AnchorName::head(),
        &FindSymbolsArgs {
            kind: Some("function".into()),
            ..Default::default()
        },
    )
    .unwrap();

    let syntactic = hits.iter().find(|h| h.name == "syntactic_fn").unwrap();
    assert_eq!(syntactic.source_tier, SourceTier::Syntactic);
    assert_eq!(syntactic.language, None);
    let semantic = hits.iter().find(|h| h.name == "semantic_fn").unwrap();
    assert_eq!(semantic.source_tier, SourceTier::Semantic);
    assert_eq!(semantic.language.as_deref(), Some("rust"));
}

#[test]
fn find_symbols_returns_language_and_sorts_language_path_line() {
    let (_repo, _db, c) = language_fixture();
    let hits = find_symbols(
        &c,
        &AnchorName::head(),
        &FindSymbolsArgs {
            path_prefix: Some("src/".into()),
            ..Default::default()
        },
    )
    .unwrap();

    let rows: Vec<(&str, Option<&str>, &str)> = hits
        .iter()
        .map(|h| (h.name.as_str(), h.language.as_deref(), h.path.as_str()))
        .collect();
    assert_eq!(
        rows,
        vec![
            ("py_user", Some("python"), "src/a.py"),
            ("rust_user", Some("rust"), "src/b.rs"),
            ("User", None, "src/z.md"),
        ]
    );
}

#[test]
fn directory_outline_returns_items_per_file_under_path_prefix_sorted() {
    let (repo, _sha) = init_repo(&[
        ("a/foo.rs", "pub fn foo_one() {}\npub fn foo_two() {}\n"),
        ("a/bar.rs", "pub fn bar_one() {}\n"),
        ("b/baz.rs", "pub fn baz_one() {}\n"),
    ]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, repo.path(), 0).unwrap();

    let hits = get_outline_under_path(
        &conn,
        &AnchorName::head(),
        "a/",
        None,
        10,
        &OutlineFilter::default(),
    )
    .unwrap();
    let rows: Vec<(&str, &str, u32)> = hits
        .iter()
        .map(|h| (h.file.as_deref().unwrap(), h.name.as_str(), h.line))
        .collect();

    assert_eq!(
        rows,
        vec![
            ("a/bar.rs", "bar_one", 1),
            ("a/foo.rs", "foo_one", 1),
            ("a/foo.rs", "foo_two", 2),
        ]
    );
}

#[test]
fn find_by_container_matches_qualified_prefix() {
    let (_repo, _db, c) = registered();
    let hits = find_symbols(
        &c,
        &AnchorName::head(),
        &FindSymbolsArgs {
            container: Some("Widget".into()),
            ..Default::default()
        },
    )
    .unwrap();
    // Widget::render and possibly Widget::Widget depending on
    // how the tree-sitter pass names the impl block; at minimum
    // the method shows up.
    assert!(
        hits.iter().any(|h| h.name == "render"),
        "render not in {hits:?}"
    );
}

#[test]
fn find_by_path_prefix_limits_scope() {
    let (_repo, _db, c) = registered();
    let hits = find_symbols(
        &c,
        &AnchorName::head(),
        &FindSymbolsArgs {
            kind: Some("function".into()),
            path_prefix: Some("src/util.rs".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        hits.iter().all(|h| h.path == "src/util.rs"),
        "leaked across path prefix: {hits:?}"
    );
    assert!(hits.iter().any(|h| h.name == "helper"));
}

#[test]
fn limit_caps_results() {
    let (_repo, _db, c) = registered();
    let hits = find_symbols(
        &c,
        &AnchorName::head(),
        &FindSymbolsArgs {
            kind: Some("function".into()),
            limit: Some(1),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(hits.len(), 1);
}

#[test]
fn no_filter_is_an_error() {
    let (_repo, _db, c) = registered();
    let err = find_symbols(&c, &AnchorName::head(), &FindSymbolsArgs::default()).unwrap_err();
    assert!(err.to_string().contains("at least one"));
}

#[test]
fn unknown_anchor_is_an_error() {
    let (_repo, _db, c) = registered();
    let err = find_symbols(
        &c,
        &AnchorName::branch("does-not-exist"),
        &FindSymbolsArgs {
            query: Some("alpha".into()),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("anchor not found"));
}
