//! Query layer over the CAS store.
//!
//! This is the new-path counterpart to `data_rpc::methods::*`. It
//! resolves an anchor to a `manifest_id`, then joins `symbols` (or
//! `refs` / `imports` in future) against `manifest_entries` filtered
//! by `manifest_id` to surface results scoped to one snapshot's
//! visible blobs.
//!
//! Only `find_symbols` is implemented here; the rest of the surface
//! ports in later work.

use cairn_lang_api::Visibility;
use cairn_proto::common::SymbolKind;
use rusqlite::{Connection, ToSql};

use crate::Result;
use crate::anchor::{self, AnchorName};
use crate::cas::kind_conv::{symbol_kind_from_str, visibility_from_str};
use crate::manifest::ManifestId;

/// One symbol hit. Mirrors the public-fact subset of
/// `cairn_proto::methods::FindSymbolHit` but skips the wire-format
/// envelope (repo / branch / location) so callers compose them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolHit {
    pub id: i64,
    pub name: String,
    pub qualified: String,
    pub kind: SymbolKind,
    pub signature: Option<String>,
    pub visibility: Option<Visibility>,
    pub path: String,
    pub line: u32,
    pub blob_sha: String,
}

/// Filters for `find_symbols`. All optional; the caller must supply
/// at least one of `query` / `kind` / `container` / `path_prefix` to
/// avoid dumping the whole index.
#[derive(Debug, Clone, Default)]
pub struct FindSymbolsArgs {
    pub query: Option<String>,
    pub kind: Option<String>,
    pub container: Option<String>,
    pub path_prefix: Option<String>,
    pub limit: Option<u32>,
}

/// Query the symbols visible from `anchor`. `anchor` resolves to one
/// manifest; the join scopes hits to blobs that appear in that
/// manifest.
///
/// # Errors
/// Returns [`crate::Error::InvalidArgument`] when no filter is set or
/// the anchor does not resolve. SQLite errors otherwise.
pub fn find_symbols(
    conn: &Connection,
    anchor: &AnchorName,
    args: &FindSymbolsArgs,
) -> Result<Vec<SymbolHit>> {
    let any_filter = args.query.as_deref().is_some_and(|q| !q.is_empty())
        || args.kind.as_deref().is_some_and(|k| !k.is_empty())
        || args.container.as_deref().is_some_and(|c| !c.is_empty())
        || args
            .path_prefix
            .as_deref()
            .is_some_and(|p| !p.is_empty());
    if !any_filter {
        return Err(crate::Error::InvalidArgument(
            "find_symbols: at least one of `query`, `kind`, `container`, or `path_prefix` \
             must be set"
                .to_string(),
        ));
    }

    let manifest_id = anchor::resolve(conn, anchor)?.ok_or_else(|| {
        crate::Error::InvalidArgument(format!("anchor not found: {}", anchor.as_str()))
    })?;

    run_find_symbols(conn, manifest_id, args)
}

fn run_find_symbols(
    conn: &Connection,
    manifest_id: ManifestId,
    args: &FindSymbolsArgs,
) -> Result<Vec<SymbolHit>> {
    let limit = args.limit.unwrap_or(50).max(1);

    // Base query: pull symbols whose blob_sha is in the manifest's
    // entry set, joined to manifest_entries so we can return the
    // file path the blob was mounted at.
    let mut sql = String::from(
        "SELECT s.id, s.name, s.qualified, s.kind, s.signature, s.visibility,
                 me.path, s.line_start, s.blob_sha
           FROM symbols s
           JOIN manifest_entries me
             ON me.manifest_id = ?1
            AND me.blob_sha = s.blob_sha
          WHERE 1=1",
    );
    let mut bound: Vec<Box<dyn ToSql>> = vec![Box::new(manifest_id.0)];

    if let Some(q) = args.query.as_deref()
        && !q.is_empty()
    {
        sql.push_str(" AND (s.name = ?  OR s.qualified = ?)");
        bound.push(Box::new(q.to_string()));
        bound.push(Box::new(q.to_string()));
    }
    if let Some(k) = args.kind.as_deref()
        && !k.is_empty()
    {
        sql.push_str(" AND s.kind = ?");
        bound.push(Box::new(k.to_string()));
    }
    if let Some(c) = args.container.as_deref()
        && !c.is_empty()
    {
        sql.push_str(" AND (s.qualified LIKE ? OR s.qualified LIKE ?)");
        bound.push(Box::new(format!("{c}::%")));
        bound.push(Box::new(format!("{c}.%")));
    }
    if let Some(p) = args.path_prefix.as_deref()
        && !p.is_empty()
    {
        sql.push_str(" AND me.path LIKE ?");
        bound.push(Box::new(format!("{p}%")));
    }
    sql.push_str(" ORDER BY s.qualified LIMIT ?");
    bound.push(Box::new(i64::from(limit)));

    let param_refs: Vec<&dyn ToSql> = bound.iter().map(|b| b.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let rows: rusqlite::Result<Vec<SymbolHit>> = stmt
        .query_map(param_refs.as_slice(), row_to_hit)?
        .collect();
    Ok(rows?)
}

fn row_to_hit(row: &rusqlite::Row<'_>) -> rusqlite::Result<SymbolHit> {
    Ok(SymbolHit {
        id: row.get(0)?,
        name: row.get(1)?,
        qualified: row.get(2)?,
        kind: symbol_kind_from_str(&row.get::<_, String>(3)?),
        signature: row.get(4)?,
        visibility: row
            .get::<_, Option<String>>(5)?
            .as_deref()
            .map(visibility_from_str),
        path: row.get(6)?,
        line: u32::try_from(row.get::<_, i64>(7)?).unwrap_or(0),
        blob_sha: row.get(8)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::store;
    use crate::register::register_repo;
    use std::fs;
    use std::path::Path;
    use std::process::Command;

    fn run_git(repo: &Path, args: &[&str]) {
        let out = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    fn init_repo(files: &[(&str, &str)]) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path();
        run_git(repo, &["init", "-q", "-b", "main"]);
        for (rel, content) in files {
            let p = repo.join(rel);
            fs::create_dir_all(p.parent().unwrap()).unwrap();
            fs::write(p, content).unwrap();
        }
        run_git(repo, &["add", "-A"]);
        run_git(repo, &["commit", "-q", "-m", "init"]);
        tmp
    }

    fn registered() -> (tempfile::TempDir, tempfile::TempDir, Connection) {
        let repo = init_repo(&[
            (
                "src/lib.rs",
                "pub fn alpha() -> i32 { 1 }\n\
                 pub fn beta() {}\n\
                 pub struct Widget;\n\
                 impl Widget {\n    pub fn render(&self) {}\n}\n",
            ),
            ("src/util.rs", "pub fn helper() {}\n"),
        ]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        register_repo(&mut conn, repo.path(), 0).unwrap();
        (repo, db_tmp, conn)
    }

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

    #[test]
    fn tentative_sees_uncommitted_file() {
        let repo = init_repo(&[("src/lib.rs", "pub fn committed() {}\n")]);
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
}
