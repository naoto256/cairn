//! Shared data-RPC query infrastructure.
//!
//! Every snapshot-scoped symbol/navigation method (find_symbols,
//! get_outline, find_references, ...) routed through this helper
//! drives the same pipeline via [`query_one_or_all_snapshots`];
//! `list_repos` / `list_jobs` / `repo_status` build their own
//! per-repo pipelines instead:
//!
//! 1. Resolve one snapshot per repository via
//!    [`crate::freshness::evaluate_snapshot`] inside the same SQLite read
//!    transaction that will run the query SQL, so an anchor move
//!    mid-query cannot mix rows from two manifests.
//! 2. Optionally verify that a required exact file is a member of the
//!    resolved manifest, run the caller's query closure against the
//!    pinned manifest, then commit the read transaction.
//! 3. Reopen the store on a fresh connection and call
//!    [`crate::freshness::revalidate_snapshot`]; a changed durable
//!    fingerprint replaces the initial verdict.
//! 4. Build tier-3 status limited to the parser ids the returned rows
//!    actually touch, then compose diagnostics and hints so snapshot
//!    uncertainty outranks speculative empty-result advice.
//!
//! Multi-repository queries acquire per-repo leases individually so a
//! single `Removing` repository does not fail an unscoped scan;
//! explicitly requested repositories keep the strict "acquire or
//! error" contract of `acquire_by_repo_hash`.

mod feedback;
mod query;
mod tier_status;

pub(crate) use feedback::*;
pub(crate) use query::*;
pub(crate) use tier_status::*;

use super::DataCtx;

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use cairn_proto::Completeness;
    use rusqlite::params;
    use serde_json::Value;

    use crate::anchor;
    use crate::cas::{registry as cas_registry, store as cas_store};
    use crate::paths::{CasDataDir, path_hash};
    use crate::register::register_repo;
    use crate::testutil::init_repo;

    use super::DataCtx;
    use crate::data_rpc::DataMethod;

    pub(crate) struct DataRpcFixture {
        pub(crate) _repo: tempfile::TempDir,
        pub(crate) _data: tempfile::TempDir,
        pub(crate) ctx: DataCtx,
    }

    pub(crate) fn registered_fixture() -> DataRpcFixture {
        registered_fixture_with_files(&[(
            "src/lib.rs",
            "use std::fmt;\n\
             use std::fs;\n\
             use std::io;\n\
             pub trait Trait {}\n\
             pub struct A;\n\
             pub struct B;\n\
             pub struct C;\n\
             impl Trait for A {}\n\
             impl Trait for B {}\n\
             impl Trait for C {}\n\
             pub fn target() {}\n\
             pub fn caller_a() { target(); }\n\
             pub fn caller_b() { target(); }\n\
             pub fn caller_c() { target(); }\n",
        )])
    }

    pub(crate) fn registered_fixture_with_files(files: &[(&str, &str)]) -> DataRpcFixture {
        let (repo, _sha) = init_repo(files);
        let data = tempfile::tempdir().unwrap();
        let cas = CasDataDir::with_root(data.path().to_path_buf());
        cas.ensure().unwrap();
        let canonical = std::fs::canonicalize(repo.path()).unwrap();
        let repo_hash = path_hash(&canonical);
        let store_path = cas.store_db_path(&repo_hash);
        let mut store = cas_store::open(&store_path).unwrap();
        let now_ns = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        )
        .unwrap_or(i64::MAX);
        let registration = register_repo(&mut store, &canonical, now_ns).unwrap();

        let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::upsert(
            &tx,
            "demo",
            &canonical.to_string_lossy(),
            &repo_hash,
            now_ns,
        )
        .unwrap();
        tx.commit().unwrap();
        index
            .execute(
                "UPDATE repo_reconcile_state
                 SET desired_generation = 1,
                     applied_generation = 1,
                     last_success_ns = ?1,
                     watcher_state = 'active'
                 WHERE repo_hash = ?2",
                params![now_ns, repo_hash],
            )
            .unwrap();
        let tx = store.transaction().unwrap();
        anchor::set_reconciled(
            &tx,
            &anchor::AnchorName::tentative(registration.worktree_id),
            registration.tentative_manifest,
            now_ns,
            1,
        )
        .unwrap();
        tx.commit().unwrap();

        DataRpcFixture {
            _repo: repo,
            _data: data,
            ctx: DataCtx {
                cas_data_dir: Arc::new(cas),
                lifecycle: None,
            },
        }
    }

    pub(crate) async fn assert_limit_probe(
        method: &dyn DataMethod,
        exact_params: Value,
        over_params: Value,
    ) {
        let fixture = registered_fixture();

        let exact = method.dispatch(&fixture.ctx, exact_params).await.unwrap();
        assert_eq!(exact["items"].as_array().unwrap().len(), 3);
        assert_eq!(
            serde_json::from_value::<Completeness>(exact["completeness"].clone()).unwrap(),
            Completeness::Complete
        );

        let over = method.dispatch(&fixture.ctx, over_params).await.unwrap();
        assert_eq!(over["items"].as_array().unwrap().len(), 2);
        assert_eq!(
            serde_json::from_value::<Completeness>(over["completeness"].clone()).unwrap(),
            Completeness::partial_truncated("cap")
        );
    }
}
