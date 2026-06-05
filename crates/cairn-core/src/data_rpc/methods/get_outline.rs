//! `get_outline` — per-file symbol structure on top of the CAS store.

use cairn_proto::common::SourceTier;
use cairn_proto::methods::{OutlineArgs, OutlineItem, OutlineResult};
use linkme::distributed_slice;
use serde_json::Value;
use tracing::debug;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::data_rpc::helpers::{
    completeness_for_cap, limit_with_probe, trim_to_requested_limit, with_repo_conn,
};
use crate::query::{self, OutlineItem as QueryOutlineItem};
use crate::{Error, Result};

pub struct GetOutline;

#[async_trait::async_trait]
impl DataMethod for GetOutline {
    fn name(&self) -> &'static str {
        "get_outline"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: OutlineArgs = parse_params(params)?;
        if args.file.is_none() && args.path.is_none() {
            return Err(Error::InvalidArgument(
                "get_outline: one of `file` / `path` must be supplied".into(),
            ));
        }

        let repo_alias = args.repo.clone();
        let file = args.file.clone();
        let path = args.path.clone();
        let effective_limit = args.limit.unwrap_or(200).clamp(1, 1000);

        let (items, capped) = with_repo_conn(
            ctx,
            &repo_alias,
            "outline",
            move |_entry, conn| -> Result<(Vec<OutlineItem>, bool)> {
                let anchor = crate::anchor::resolve_explicit_or_default(&conn, None, None)?;
                if let Some(file) = file {
                    let raw = match query::get_outline(&conn, &anchor, &file, None) {
                        Ok(r) => r,
                        Err(Error::AnchorNotFound { .. }) => Vec::new(),
                        Err(other) => return Err(other),
                    };
                    return Ok((raw.into_iter().map(into_wire_item).collect(), false));
                }

                let path = path.expect("validated path when file is absent");
                let mut raw = match query::get_outline_under_path(
                    &conn,
                    &anchor,
                    &path,
                    None,
                    limit_with_probe(effective_limit),
                ) {
                    Ok(r) => r,
                    Err(Error::AnchorNotFound { .. }) => Vec::new(),
                    Err(other) => return Err(other),
                };
                let capped = trim_to_requested_limit(&mut raw, effective_limit);
                Ok((raw.into_iter().map(into_wire_item).collect(), capped))
            },
        )
        .await?;

        debug!(
            repo = %args.repo,
            file = ?args.file,
            path = ?args.path,
            count = items.len(),
            "outline served"
        );
        Ok(serde_json::to_value(OutlineResult {
            items,
            completeness: completeness_for_cap(capped),
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(GetOutline);

fn into_wire_item(q: QueryOutlineItem) -> OutlineItem {
    OutlineItem {
        file: q.file,
        kind: q.kind,
        name: q.name,
        qualified: q.qualified,
        signature: q.signature,
        line: q.line,
        doc: q.doc,
        // CAS query layer doesn't yet round-trip per-fact source-tier;
        // mirror the find_symbols default until it does.
        source: SourceTier::Syntactic,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use cairn_proto::Completeness;
    use serde_json::json;

    use super::*;
    use crate::cas::{registry as cas_registry, store as cas_store};
    use crate::paths::{CasDataDir, path_hash};
    use crate::register::register_repo;
    use crate::testutil::init_repo;

    #[tokio::test]
    async fn directory_outline_caps_at_limit() {
        let fixture = outline_fixture();
        let result = GetOutline
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "path": "a/", "limit": 2}),
            )
            .await
            .unwrap();

        let items = result["items"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["file"], "a/bar.rs");
        assert_eq!(items[1]["file"], "a/foo.rs");
        assert_eq!(
            serde_json::from_value::<Completeness>(result["completeness"].clone()).unwrap(),
            Completeness::partial_truncated("cap")
        );
    }

    #[tokio::test]
    async fn rejects_when_neither_file_nor_path() {
        let fixture = outline_fixture();
        let err = GetOutline
            .dispatch(&fixture.ctx, json!({"repo": "demo"}))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    struct OutlineFixture {
        _repo: tempfile::TempDir,
        _data: tempfile::TempDir,
        ctx: DataCtx,
    }

    fn outline_fixture() -> OutlineFixture {
        let (repo, _sha) = init_repo(&[
            ("a/foo.rs", "pub fn foo_one() {}\npub fn foo_two() {}\n"),
            ("a/bar.rs", "pub fn bar_one() {}\n"),
            ("b/baz.rs", "pub fn baz_one() {}\n"),
        ]);
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

        OutlineFixture {
            _repo: repo,
            _data: data,
            ctx: DataCtx {
                cas_data_dir: Arc::new(cas),
            },
        }
    }
}
