//! Shared blocking helpers for data-RPC methods.

use cairn_proto::{Completeness, PartialReason};

use crate::cas::{registry as cas_registry, store as cas_store};
use crate::{Error, Result};

use super::DataCtx;

/// Open the CAS store for one user-facing repo alias inside a blocking task.
pub(crate) async fn with_repo_conn<F, R>(
    ctx: &DataCtx,
    repo_alias: &str,
    method_name: &'static str,
    f: F,
) -> Result<R>
where
    F: FnOnce(cas_registry::AliasEntry, rusqlite::Connection) -> Result<R> + Send + 'static,
    R: Send + 'static,
{
    let cas_data_dir = ctx.cas_data_dir.clone();
    let repo_alias = repo_alias.to_string();
    tokio::task::spawn_blocking(move || -> Result<R> {
        let index = cas_registry::open(&cas_data_dir.index_db_path())?;
        let entry = cas_registry::lookup_by_alias(&index, &repo_alias)?.ok_or_else(|| {
            Error::RepoNotFound {
                alias: repo_alias.clone(),
            }
        })?;
        let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
        let conn = cas_store::open(&store_path)?;
        f(entry, conn)
    })
    .await
    .map_err(|e| Error::InvalidArgument(format!("{method_name} task panicked: {e}")))?
}

pub(crate) fn limit_with_probe(effective_limit: u32) -> u32 {
    effective_limit.saturating_add(1)
}

pub(crate) fn trim_to_requested_limit<T>(rows: &mut Vec<T>, effective_limit: u32) -> bool {
    let requested = effective_limit as usize;
    if rows.len() > requested {
        rows.truncate(requested);
        true
    } else {
        false
    }
}

pub(crate) fn completeness_for_cap(capped: bool) -> Completeness {
    if capped {
        Completeness::partial_truncated(PartialReason::Cap)
    } else {
        Completeness::complete()
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use cairn_proto::Completeness;
    use serde_json::Value;

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
        let (repo, _sha) = init_repo(&[(
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
        )]);
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
        register_repo(&mut store, &canonical, now_ns).unwrap();

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

        DataRpcFixture {
            _repo: repo,
            _data: data,
            ctx: DataCtx {
                cas_data_dir: Arc::new(cas),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_limit_rows_are_complete() {
        let mut rows = vec![1, 2];
        assert!(!trim_to_requested_limit(&mut rows, 2));
        assert_eq!(rows, vec![1, 2]);
    }

    #[test]
    fn over_limit_rows_are_partial_and_truncated() {
        let mut rows = vec![1, 2, 3];
        assert!(trim_to_requested_limit(&mut rows, 2));
        assert_eq!(rows, vec![1, 2]);
    }

    #[test]
    fn probe_limit_adds_one() {
        assert_eq!(limit_with_probe(2), 3);
    }

    #[test]
    fn completeness_for_cap_marks_partial_with_cap_reason() {
        assert_eq!(completeness_for_cap(false), Completeness::Complete);
        assert_eq!(
            completeness_for_cap(true),
            Completeness::Partial {
                missing_tiers: Vec::new(),
                reason: Some(PartialReason::Cap),
            }
        );
    }
}
