//! `get_outline` — per-file symbol structure on top of the CAS store.

use cairn_proto::common::SourceTier;
use cairn_proto::methods::{OutlineArgs, OutlineItem, OutlineResult};
use linkme::distributed_slice;
use serde_json::Value;
use tracing::debug;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::data_rpc::helpers::{
    EmissionContext, QueryArgsView, build_diagnostics, build_hints, completeness_for_cap,
    limit_with_probe, parser_id_filter, tier3_status_for_query, with_one_or_all_stores,
};
use crate::query::{self, OutlineFilter, OutlineItem as QueryOutlineItem};
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

        let repo_alias = args.scope.repo.clone();
        let file = args.file.clone();
        let path = args.path.clone();
        let effective_limit = args.pagination.limit.unwrap_or(200).clamp(1, 1000);
        let kind_filter_set = args.kind.is_some();
        let filter = OutlineFilter {
            kind: args.kind,
            max_depth: args.max_depth,
        };

        let (hits, capped) = with_one_or_all_stores(
            ctx,
            repo_alias,
            "outline",
            effective_limit,
            move |_entry, conn| -> Result<Vec<(OutlineItem, String)>> {
                let anchor = crate::anchor::resolve_explicit_or_default(conn, None, None)?;
                if let Some(file) = file.as_deref() {
                    let raw = match query::get_outline(conn, &anchor, file, None) {
                        Ok(r) => r,
                        Err(Error::AnchorNotFound { .. }) => Vec::new(),
                        Err(other) => return Err(other),
                    };
                    let filtered: Vec<_> = raw
                        .into_iter()
                        .filter(|i| filter.kind.as_ref().is_none_or(|k| &i.kind == k))
                        .map(|item| {
                            let parser_id = item.parser_id.clone();
                            (into_wire_item(item), parser_id)
                        })
                        .collect();
                    return Ok(filtered);
                }

                let path = path.as_deref().expect("validated path when file is absent");
                let raw = match query::get_outline_under_path(
                    conn,
                    &anchor,
                    path,
                    None,
                    limit_with_probe(effective_limit),
                    &filter,
                ) {
                    Ok(r) => r,
                    Err(Error::AnchorNotFound { .. }) => Vec::new(),
                    Err(other) => return Err(other),
                };
                Ok(raw
                    .into_iter()
                    .map(|item| {
                        let parser_id = item.parser_id.clone();
                        (into_wire_item(item), parser_id)
                    })
                    .collect())
            },
            |_out: &mut Vec<(OutlineItem, String)>| {},
        )
        .await?;
        let parser_ids = parser_id_filter(hits.iter().map(|(_, parser_id)| parser_id.clone()));
        let items: Vec<OutlineItem> = hits.into_iter().map(|(item, _)| item).collect();

        debug!(
            repo = ?args.scope.repo,
            file = ?args.file,
            path = ?args.path,
            count = items.len(),
            "outline served"
        );
        let tier3_status = tier3_status_for_query(
            ctx,
            args.scope.repo.clone(),
            None,
            None,
            parser_ids,
            args.tier3.verbose_tier3,
            "get_outline",
        )
        .await?;
        let completeness = completeness_for_cap(capped);
        let emission_ctx = EmissionContext {
            items_empty: items.is_empty(),
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                repo: args.scope.repo.as_deref(),
                fuzzy: true,
                kind: kind_filter_set,
                container: None,
                path: args.path.as_deref().or(args.file.as_deref()),
            },
        };
        let diagnostics = build_diagnostics(&emission_ctx);
        let hints = build_hints(&emission_ctx);
        Ok(serde_json::to_value(OutlineResult {
            items,
            completeness,
            tier3_status,
            diagnostics,
            hints,
            timing: cairn_proto::Timing::default(),
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
        assert_eq!(result["hints"][0]["code"], "capped_increase_limit");
    }

    #[tokio::test]
    async fn directory_outline_filters_by_kind() {
        let fixture = outline_fixture();
        let result = GetOutline
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "path": "a/", "kind": "function"}),
            )
            .await
            .unwrap();
        let items = result["items"].as_array().unwrap();
        assert!(!items.is_empty());
        assert!(result.get("diagnostics").is_none());
        assert!(result.get("hints").is_none());
        for item in items {
            assert_eq!(item["kind"], "function");
        }
    }

    #[tokio::test]
    async fn directory_outline_caps_depth() {
        let fixture = nested_outline_fixture();
        let shallow = GetOutline
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "path": "src/", "max_depth": 1}),
            )
            .await
            .unwrap();
        let files: Vec<&str> = shallow["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["file"].as_str().unwrap())
            .collect();
        assert!(
            files
                .iter()
                .all(|f| !f.trim_start_matches("src/").contains('/'))
        );
        assert!(files.contains(&"src/top.rs"));

        let deep = GetOutline
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "path": "src/", "max_depth": 2}),
            )
            .await
            .unwrap();
        let deep_files: Vec<&str> = deep["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["file"].as_str().unwrap())
            .collect();
        assert!(deep_files.contains(&"src/nest/inner.rs"));
    }

    fn nested_outline_fixture() -> OutlineFixture {
        let (repo, _sha) = init_repo(&[
            ("src/top.rs", "pub fn top_one() {}\n"),
            ("src/nest/inner.rs", "pub fn inner_one() {}\n"),
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
