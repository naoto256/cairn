//! Shared blocking helpers for data-RPC methods.

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
        let entry = cas_registry::lookup_by_alias(&index, &repo_alias)?
            .ok_or_else(|| Error::InvalidArgument(format!("unknown repo alias: `{repo_alias}`")))?;
        let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
        let conn = cas_store::open(&store_path)?;
        f(entry, conn)
    })
    .await
    .map_err(|e| Error::InvalidArgument(format!("{method_name} task panicked: {e}")))?
}
