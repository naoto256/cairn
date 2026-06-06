use crate::anchor::AnchorName;
use crate::cas::store;
use crate::query::{FindSymbolsArgs, find_symbols};
use crate::register::register_repo;
use crate::testutil::init_repo;
use std::fs;

#[test]
fn tentative_sees_uncommitted_file() {
    let (repo, _sha) = init_repo(&[("src/lib.rs", "pub fn committed() {}\n")]);
    // Add an extra unstaged file.
    fs::write(
        repo.path().join("src/staged.rs"),
        "pub fn uncommitted() {}\n",
    )
    .unwrap();
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    let outcome = register_repo(&mut conn, repo.path(), 0).unwrap();

    let tent_anchor = AnchorName::tentative(outcome.worktree_id);
    let hits = find_symbols(
        &conn,
        &tent_anchor,
        &FindSymbolsArgs {
            query: Some("uncommitted".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(hits.len(), 1, "uncommitted symbol missing under tentative");

    // The committed anchor must NOT see it.
    let head_hits = find_symbols(
        &conn,
        &AnchorName::head(),
        &FindSymbolsArgs {
            query: Some("uncommitted".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(head_hits.is_empty(), "committed anchor leaked uncommitted");
}
