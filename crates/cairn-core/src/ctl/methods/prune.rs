//! `prune` — remove blob rows for parser IDs no current backend owns.

use std::collections::BTreeSet;

use cairn_lang_api::all_backends;
use cairn_proto::control::{PruneArgs, PruneRepoEntry, PruneResult};
use linkme::distributed_slice;
use rusqlite::{Connection, params_from_iter};
use serde_json::Value;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx, parse_params};
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::{Error, Result};

struct Prune;

#[async_trait::async_trait]
impl ControlMethod for Prune {
    fn name(&self) -> &'static str {
        "prune"
    }

    async fn dispatch(&self, ctx: &CtlCtx, params: Value) -> Result<Value> {
        let args: PruneArgs = parse_params(params)?;
        let cas_data_dir = ctx.cas_data_dir.clone();

        let result = tokio::task::spawn_blocking(move || -> Result<PruneResult> {
            let backends = all_backends();
            let current_parser_ids: BTreeSet<String> =
                backends.iter().map(|b| b.parser_id().to_string()).collect();

            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let entries = match args.repo {
                Some(alias) => {
                    let entry = cas_registry::lookup_by_alias(&index, &alias)?
                        .ok_or_else(|| Error::RepoNotFound { alias })?;
                    vec![entry]
                }
                None => cas_registry::list_all(&index)?,
            };

            let mut repos = Vec::with_capacity(entries.len());
            let mut total_deleted = 0_u64;
            for entry in entries {
                let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
                let mut conn = cas_store::open(&store_path)?;
                let deleted = prune_store(&mut conn, &current_parser_ids)?;
                total_deleted += deleted;
                repos.push(PruneRepoEntry {
                    alias: entry.alias,
                    deleted_blob_count: deleted,
                });
            }
            Ok(PruneResult {
                repos,
                total_deleted,
            })
        })
        .await
        .map_err(|e| Error::InvalidArgument(format!("prune task panicked: {e}")))??;

        Ok(serde_json::to_value(result).unwrap())
    }
}

fn prune_store(conn: &mut Connection, current_parser_ids: &BTreeSet<String>) -> Result<u64> {
    if current_parser_ids.is_empty() {
        // Prune requires at least one backend so a misconfigured daemon cannot erase every blob.
        return Err(Error::InvalidArgument(
            "no language backends are registered; check daemon configuration".into(),
        ));
    }

    let tx = conn.transaction()?;
    let placeholders = std::iter::repeat_n("?", current_parser_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("DELETE FROM blobs WHERE parser_id NOT IN ({placeholders})");
    let deleted = tx.execute(&sql, params_from_iter(current_parser_ids.iter()))?;
    tx.commit()?;
    Ok(u64::try_from(deleted).unwrap_or(u64::MAX))
}

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static REGISTER: fn() -> Box<dyn ControlMethod> = || Box::new(Prune);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::store;

    #[test]
    fn prune_store_rejects_empty_parser_ids_without_deleting_blobs() {
        let tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&tmp.path().join("store.db")).unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('rust', 'tree-sitter-rust', 1, 0),
                    ('python', 'tree-sitter-python', 1, 0)",
            [],
        )
        .unwrap();
        let empty = BTreeSet::new();

        let err = prune_store(&mut conn, &empty).unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));

        let remaining: i64 = conn
            .query_row("SELECT COUNT(*) FROM blobs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(remaining, 2);
    }

    #[test]
    fn prune_store_removes_only_unknown_parser_ids() {
        let tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&tmp.path().join("store.db")).unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('valid', 'tree-sitter-rust', 1, 0),
                    ('old', 'tree-sitter-rust@0.1.0', 1, 0)",
            [],
        )
        .unwrap();
        let current = BTreeSet::from(["tree-sitter-rust".to_string()]);

        let deleted = prune_store(&mut conn, &current).unwrap();
        assert_eq!(deleted, 1);

        let remaining: Vec<String> = conn
            .prepare("SELECT parser_id FROM blobs ORDER BY parser_id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(remaining, vec!["tree-sitter-rust"]);
    }
}
