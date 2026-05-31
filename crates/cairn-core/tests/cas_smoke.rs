//! Smoke test for the CAS path: register `cairn-ng` itself against a
//! fresh CAS store and find a known symbol via the new query path.
//!
//! Marked `#[ignore]` because it walks the whole workspace tree
//! (slower than a unit test). Run with `cargo test -p cairn-core
//! --test cas_smoke -- --ignored`.

use std::path::PathBuf;

use cairn_core::anchor::AnchorName;
use cairn_core::cas::store;
use cairn_core::query::{FindSymbolsArgs, find_symbols};
use cairn_core::register::register_repo;

// Force-link the language backends so their `#[distributed_slice]`
// entries land in this integration-test binary; without these `use _`
// references dead-code elimination drops the crates and `all_backends()`
// returns empty.
use cairn_lang_markdown as _;
use cairn_lang_python as _;
use cairn_lang_rust as _;

fn workspace_root() -> PathBuf {
    // crates/cairn-core/ → ../..
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root above cairn-core/")
        .to_path_buf()
}

#[test]
#[ignore]
fn register_cairn_ng_and_find_git_blob_sha() {
    let repo = workspace_root();
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();

    let outcome = register_repo(&mut conn, &repo, 0).expect("register_repo");
    assert!(
        outcome.blobs_parsed > 10,
        "expected to parse many blobs, got {}",
        outcome.blobs_parsed
    );
    assert_eq!(outcome.branch.as_deref(), Some("feat/cas-skeleton"));

    // `git_blob_sha` was defined in S3 at cas/hash.rs and is
    // re-exported via `cas::git_blob_sha`. find_symbols against
    // the committed HEAD anchor must surface it.
    let hits = find_symbols(
        &conn,
        &AnchorName::head(),
        &FindSymbolsArgs {
            query: Some("git_blob_sha".into()),
            ..Default::default()
        },
    )
    .expect("find_symbols");
    let in_hash_rs = hits
        .iter()
        .find(|h| h.path == "crates/cairn-core/src/cas/hash.rs");
    assert!(
        in_hash_rs.is_some(),
        "git_blob_sha not located in cas/hash.rs; hits = {hits:#?}"
    );

    // Container query: every symbol qualified under `cairn-core`'s
    // `cas::blob` should include `ParsedData`.
    let blob_hits = find_symbols(
        &conn,
        &AnchorName::head(),
        &FindSymbolsArgs {
            query: Some("ParsedData".into()),
            ..Default::default()
        },
    )
    .expect("find_symbols ParsedData");
    assert!(
        blob_hits
            .iter()
            .any(|h| h.path == "crates/cairn-core/src/cas/blob.rs"),
        "ParsedData not found in cas/blob.rs"
    );
}
