//! Anchor layer: named pointers to manifests.
//!
//! An anchor names a git-shaped reference (`branch/<name>`,
//! `tag/<name>`, `HEAD`, `tentative/<worktree_id>`) and binds it to
//! one `manifest_id`. The query layer resolves an anchor to a
//! manifest, then resolves the manifest's `(path, blob_sha)` pairs
//! to blob-keyed parsed data.
//!
//! Tentative anchors may additionally carry a `reconcile_generation`
//! receipt proving which reconcile attempt published them; see
//! [`set_reconciled`].

use std::path::Path;

use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::Result;
use crate::manifest::ManifestId;

/// Anchor naming convention. The string form is what the `anchors`
/// table stores in `anchor_name`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AnchorName(String);

impl AnchorName {
    /// `branch/<name>` — moves with the branch tip.
    #[must_use]
    pub fn branch(name: &str) -> Self {
        Self(format!("branch/{name}"))
    }

    /// `tag/<name>` — immutable in practice.
    #[must_use]
    pub fn tag(name: &str) -> Self {
        Self(format!("tag/{name}"))
    }

    /// The repo's current HEAD. Always present once a repo is
    /// registered.
    #[must_use]
    pub fn head() -> Self {
        Self("HEAD".to_string())
    }

    /// `tentative/<worktree_id>` — reflects worktree state. One per
    /// registered worktree.
    #[must_use]
    pub fn tentative(worktree_id: i64) -> Self {
        Self(format!("tentative/{worktree_id}"))
    }

    /// Raw stored form.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Parsed view of what the name represents. Returns `None` for
    /// names that don't match one of the four standard prefixes —
    /// future anchor kinds get explicit variants rather than a
    /// catch-all.
    #[must_use]
    pub fn kind(&self) -> Option<AnchorKind> {
        if self.0 == "HEAD" {
            Some(AnchorKind::Head)
        } else if let Some(name) = self.0.strip_prefix("branch/") {
            Some(AnchorKind::Branch(name.to_string()))
        } else if let Some(name) = self.0.strip_prefix("tag/") {
            Some(AnchorKind::Tag(name.to_string()))
        } else if let Some(rest) = self.0.strip_prefix("tentative/") {
            rest.parse::<i64>().ok().map(AnchorKind::Tentative)
        } else {
            None
        }
    }
}

/// Wraps a raw stored/wire name without validation; unrecognized
/// shapes are representable and [`AnchorName::kind`] reports them
/// as `None`.
impl From<String> for AnchorName {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Pick an anchor from the wire-side `(anchor, branch)` pair the read
/// methods take: a raw anchor name wins; otherwise wrap a bare branch
/// name; otherwise fall back to `HEAD`.
#[must_use]
pub fn resolve_wire(anchor: Option<&str>, branch: Option<&str>) -> AnchorName {
    match anchor {
        Some(a) => AnchorName::from(a.to_string()),
        None => branch.map_or_else(AnchorName::head, AnchorName::branch),
    }
}

/// Pick an anchor for a read query, preferring the store's tentative
/// snapshot when the caller gave no explicit anchor or branch.
///
/// The tentative snapshot reflects the working tree (= dirty /
/// uncommitted files included), which matches the "always-current"
/// promise the daemon's file watcher upholds. Falling back to `HEAD`
/// when no tentative anchor exists yet (e.g. a brand-new store) keeps
/// the read query from failing.
///
/// Resolution order:
///   1. Explicit `anchor` arg wins, even if it's `HEAD`.
///   2. Explicit `branch` arg wraps to `branch/<name>`.
///   3. Otherwise, the first `tentative/*` anchor present in this
///      store. There is at most one per registered worktree, so this
///      is unambiguous in practice.
///   4. Otherwise `HEAD`.
///
/// # Errors
/// SQLite failure during the tentative-lookup step (step 3).
pub fn resolve_explicit_or_default(
    conn: &Connection,
    anchor: Option<&str>,
    branch: Option<&str>,
) -> Result<AnchorName> {
    if let Some(a) = anchor {
        return Ok(AnchorName::from(a.to_string()));
    }
    if let Some(b) = branch {
        return Ok(AnchorName::branch(b));
    }
    let tentative = list_prefix(conn, "tentative/")?;
    if let Some(first) = tentative.into_iter().next() {
        return Ok(first.name);
    }
    Ok(AnchorName::head())
}

/// Sort key for anchor names in wire output. Used to order:
/// 1. The labels within a single `branches: Vec<String>` group.
/// 2. The snapshot groups themselves in `list_repos` / `status`.
///
/// Order: `HEAD` first, then bare branches (`branch/<n>` shape)
/// alphabetically by `<n>`, then prefix-tagged anchors (`tag/`,
/// `tentative/`, other) alphabetically by full internal name.
///
/// Operates on the internal anchor name (not the wire label), i.e.
/// `branch/main`, not `main`.
#[must_use]
pub(crate) fn order_key(internal: &str) -> (u8, String) {
    if internal == "HEAD" {
        (0, String::new())
    } else if let Some(rest) = internal.strip_prefix("branch/") {
        (1, rest.to_string())
    } else if internal.starts_with("tag/") {
        (2, internal.to_string())
    } else if internal.starts_with("tentative/") {
        (3, internal.to_string())
    } else {
        (4, internal.to_string())
    }
}

/// Parsed counterpart of [`AnchorName`], produced by
/// [`AnchorName::kind`]. Deliberately has no catch-all variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnchorKind {
    Branch(String),
    Tag(String),
    Head,
    Tentative(i64),
}

/// One row from the `anchors` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Anchor {
    pub name: AnchorName,
    pub manifest_id: ManifestId,
    /// Publication time in nanoseconds since the Unix epoch.
    pub last_updated_ns: i64,
    /// Reconcile generation that atomically published this tentative
    /// anchor. `None` means the publication is not freshness-verified.
    pub reconcile_generation: Option<i64>,
}

/// Upsert an anchor. If `name` already exists, its `manifest_id` and
/// `last_updated_ns` are replaced. Direct publication always clears any
/// prior reconcile receipt so an unrelated write cannot preserve stale proof.
///
/// # Errors
/// SQLite failure (including FK violation if `manifest_id` doesn't
/// exist).
pub fn set(
    tx: &Transaction<'_>,
    name: &AnchorName,
    manifest_id: ManifestId,
    last_updated_ns: i64,
) -> Result<()> {
    tx.execute(
        "INSERT INTO anchors
             (anchor_name, manifest_id, last_updated_ns, reconcile_generation)
         VALUES (?1, ?2, ?3, NULL)
         ON CONFLICT(anchor_name) DO UPDATE SET
             manifest_id = excluded.manifest_id,
             last_updated_ns = excluded.last_updated_ns,
             reconcile_generation = NULL",
        params![name.as_str(), manifest_id.0, last_updated_ns],
    )?;
    Ok(())
}

/// Publish a tentative anchor with durable proof of the reconcile generation
/// that produced it. The receipt is committed in the same transaction as the
/// manifest and anchor move.
///
/// # Errors
/// Returns [`crate::Error::InvalidArgument`] for a non-tentative anchor or a
/// negative generation. SQLite failures otherwise.
pub fn set_reconciled(
    tx: &Transaction<'_>,
    name: &AnchorName,
    manifest_id: ManifestId,
    last_updated_ns: i64,
    generation: i64,
) -> Result<()> {
    if !matches!(name.kind(), Some(AnchorKind::Tentative(_))) {
        return Err(crate::Error::InvalidArgument(format!(
            "reconcile publication requires a tentative anchor, got `{}`",
            name.as_str()
        )));
    }
    if generation < 0 {
        return Err(crate::Error::InvalidArgument(
            "reconcile generation must be non-negative".into(),
        ));
    }
    tx.execute(
        "INSERT INTO anchors
             (anchor_name, manifest_id, last_updated_ns, reconcile_generation)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(anchor_name) DO UPDATE SET
             manifest_id = excluded.manifest_id,
             last_updated_ns = excluded.last_updated_ns,
             reconcile_generation = excluded.reconcile_generation",
        params![name.as_str(), manifest_id.0, last_updated_ns, generation],
    )?;
    Ok(())
}

/// Look up one anchor by name. Returns `Ok(None)` if absent.
///
/// # Errors
/// SQLite failure.
pub fn get(conn: &Connection, name: &AnchorName) -> Result<Option<Anchor>> {
    Ok(conn
        .query_row(
            "SELECT anchor_name, manifest_id, last_updated_ns, reconcile_generation
             FROM anchors WHERE anchor_name = ?1",
            params![name.as_str()],
            row_to_anchor,
        )
        .optional()?)
}

/// Convenience: resolve an anchor straight to its `manifest_id`.
///
/// # Errors
/// SQLite failure.
pub fn resolve(conn: &Connection, name: &AnchorName) -> Result<Option<ManifestId>> {
    Ok(get(conn, name)?.map(|a| a.manifest_id))
}

/// All anchors in storage, ordered by name.
///
/// # Errors
/// SQLite failure.
pub fn list_all(conn: &Connection) -> Result<Vec<Anchor>> {
    let mut stmt = conn.prepare(
        "SELECT anchor_name, manifest_id, last_updated_ns, reconcile_generation
         FROM anchors ORDER BY anchor_name",
    )?;
    let rows: rusqlite::Result<Vec<Anchor>> = stmt.query_map([], row_to_anchor)?.collect();
    Ok(rows?)
}

/// All anchors whose name starts with `prefix` (e.g. `"branch/"` to
/// enumerate every branch anchor).
///
/// # Errors
/// SQLite failure.
pub fn list_prefix(conn: &Connection, prefix: &str) -> Result<Vec<Anchor>> {
    let mut stmt = conn.prepare(
        "SELECT anchor_name, manifest_id, last_updated_ns, reconcile_generation
         FROM anchors WHERE anchor_name LIKE ?1 || '%' ORDER BY anchor_name",
    )?;
    let rows: rusqlite::Result<Vec<Anchor>> = stmt.query_map([prefix], row_to_anchor)?.collect();
    Ok(rows?)
}

/// Remove an anchor. Returns `true` if a row was deleted.
///
/// # Errors
/// SQLite failure.
pub fn delete(tx: &Transaction<'_>, name: &AnchorName) -> Result<bool> {
    let n = tx.execute(
        "DELETE FROM anchors WHERE anchor_name = ?1",
        params![name.as_str()],
    )?;
    Ok(n > 0)
}

/// Resolve a registered repo's `tentative/<worktree_id>` anchor to its
/// current `manifest_id`, given the worktree's filesystem root.
///
/// Used by the daemon's revision-staleness scanner and any other caller
/// that has a repo root path but not the worktree id. Two SQL hops, both
/// optional:
///
///   1. `worktrees` → `worktree_id` (returns `Ok(None)` if the worktree
///      row is missing — possible during a teardown race).
///   2. `anchors WHERE anchor_name = 'tentative/<id>'` (returns
///      `Ok(None)` if the tentative anchor has not been written yet,
///      e.g. between `register` opening the DB and the first
///      `manifest::build_from_worktree` completing).
///
/// Both `None` cases are non-error so that callers can `continue` past
/// a partially-initialized alias without bubbling a per-store hiccup
/// into a fatal failure. SQL errors still surface as `Err`.
///
/// # Errors
/// SQLite failure on either lookup.
pub fn resolve_tentative_manifest_id(
    conn: &Connection,
    repo_root: &Path,
) -> Result<Option<ManifestId>> {
    // Exact string match against `worktrees.path`. Registration
    // stores the root via the same lossy conversion and does not
    // canonicalize, so callers must pass the identical root form.
    let path_str = repo_root.to_string_lossy().to_string();
    let worktree_id: Option<i64> = conn
        .query_row(
            "SELECT worktree_id FROM worktrees WHERE path = ?1",
            params![path_str],
            |r| r.get(0),
        )
        .optional()?;
    let Some(worktree_id) = worktree_id else {
        return Ok(None);
    };
    let manifest_id: Option<i64> = conn
        .query_row(
            "SELECT manifest_id FROM anchors WHERE anchor_name = ?1",
            params![format!("tentative/{worktree_id}")],
            |r| r.get(0),
        )
        .optional()?;
    Ok(manifest_id.map(ManifestId))
}

/// Shared row mapper; column order must match every `SELECT` above.
fn row_to_anchor(r: &rusqlite::Row<'_>) -> rusqlite::Result<Anchor> {
    Ok(Anchor {
        name: AnchorName(r.get::<_, String>(0)?),
        manifest_id: ManifestId(r.get::<_, i64>(1)?),
        last_updated_ns: r.get::<_, i64>(2)?,
        reconcile_generation: r.get::<_, Option<i64>>(3)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::store;
    use rusqlite::params;

    fn fresh() -> (tempfile::TempDir, Connection) {
        let tmp = tempfile::tempdir().unwrap();
        let conn = store::open(&tmp.path().join("store.db")).unwrap();
        (tmp, conn)
    }

    /// Insert a placeholder manifest row so anchors have something to
    /// point at. Returns its id.
    fn placeholder_manifest(tx: &Transaction<'_>, kind: &str) -> ManifestId {
        tx.execute(
            "INSERT INTO manifests (kind, built_at_ns) VALUES (?1, 0)",
            params![kind],
        )
        .unwrap();
        ManifestId(tx.last_insert_rowid())
    }

    #[test]
    fn name_constructors_roundtrip_to_kind() {
        assert_eq!(AnchorName::head().kind(), Some(AnchorKind::Head));
        assert_eq!(
            AnchorName::branch("main").kind(),
            Some(AnchorKind::Branch("main".into()))
        );
        assert_eq!(
            AnchorName::tag("v1.0").kind(),
            Some(AnchorKind::Tag("v1.0".into()))
        );
        assert_eq!(
            AnchorName::tentative(7).kind(),
            Some(AnchorKind::Tentative(7))
        );
        // Names that don't fit the prefixes report None — future
        // anchor kinds add explicit variants instead of being silently
        // absorbed.
        assert_eq!(AnchorName::from("weird-anchor".to_string()).kind(), None);
        assert_eq!(AnchorName::from("tentative/oops".to_string()).kind(), None);
    }

    #[test]
    fn branch_name_with_slashes_roundtrips() {
        let n = AnchorName::branch("release/0.1.0");
        assert_eq!(n.as_str(), "branch/release/0.1.0");
        assert_eq!(n.kind(), Some(AnchorKind::Branch("release/0.1.0".into())));
    }

    #[test]
    fn order_key_ranks_head_first_then_branches_then_kinded() {
        let mut names = vec![
            "tag/v1",
            "branch/zebra",
            "HEAD",
            "branch/main",
            "tentative/1",
        ];
        names.sort_by_key(|a| order_key(a));
        assert_eq!(
            names,
            vec![
                "HEAD",
                "branch/main",
                "branch/zebra",
                "tag/v1",
                "tentative/1"
            ]
        );
    }

    #[test]
    fn set_then_get_returns_anchor() {
        let (_tmp, mut c) = fresh();
        let tx = c.transaction().unwrap();
        let mid = placeholder_manifest(&tx, "committed");
        let name = AnchorName::head();
        set(&tx, &name, mid, 1234).unwrap();
        tx.commit().unwrap();

        let got = get(&c, &name).unwrap().unwrap();
        assert_eq!(got.name, name);
        assert_eq!(got.manifest_id, mid);
        assert_eq!(got.last_updated_ns, 1234);
        assert_eq!(got.reconcile_generation, None);
    }

    #[test]
    fn get_returns_none_for_missing() {
        let (_tmp, c) = fresh();
        assert!(get(&c, &AnchorName::branch("nope")).unwrap().is_none());
    }

    #[test]
    fn set_upserts_existing_anchor() {
        let (_tmp, mut c) = fresh();
        let tx = c.transaction().unwrap();
        let m1 = placeholder_manifest(&tx, "committed");
        let m2 = placeholder_manifest(&tx, "committed");
        let name = AnchorName::branch("main");
        set(&tx, &name, m1, 100).unwrap();
        set(&tx, &name, m2, 200).unwrap();
        tx.commit().unwrap();

        let got = get(&c, &name).unwrap().unwrap();
        assert_eq!(got.manifest_id, m2);
        assert_eq!(got.last_updated_ns, 200);
        assert_eq!(got.reconcile_generation, None);
    }

    #[test]
    fn reconciled_set_stamps_generation_and_direct_set_clears_it() {
        let (_tmp, mut c) = fresh();
        let tx = c.transaction().unwrap();
        let m = placeholder_manifest(&tx, "tentative");
        let name = AnchorName::tentative(7);
        set_reconciled(&tx, &name, m, 100, 9).unwrap();
        tx.commit().unwrap();

        let stamped = get(&c, &name).unwrap().unwrap();
        assert_eq!(stamped.reconcile_generation, Some(9));

        let tx = c.transaction().unwrap();
        set(&tx, &name, m, 200).unwrap();
        tx.commit().unwrap();
        let direct = get(&c, &name).unwrap().unwrap();
        assert_eq!(direct.reconcile_generation, None);
    }

    #[test]
    fn set_reconciled_rejects_non_tentative_anchor() {
        let (_tmp, mut c) = fresh();
        let tx = c.transaction().unwrap();
        let m = placeholder_manifest(&tx, "committed");

        let err = set_reconciled(&tx, &AnchorName::head(), m, 100, 1).unwrap_err();

        assert!(matches!(err, crate::Error::InvalidArgument(_)));
        assert!(get(&tx, &AnchorName::head()).unwrap().is_none());
    }

    #[test]
    fn failed_reconcile_publication_transaction_preserves_prior_receipt() {
        let (_tmp, mut c) = fresh();
        let tx = c.transaction().unwrap();
        let m = placeholder_manifest(&tx, "tentative");
        let name = AnchorName::tentative(3);
        set_reconciled(&tx, &name, m, 100, 4).unwrap();
        tx.commit().unwrap();

        {
            let tx = c.transaction().unwrap();
            set_reconciled(&tx, &name, m, 200, 5).unwrap();
            // Dropping the transaction models any failure before commit.
        }

        let got = get(&c, &name).unwrap().unwrap();
        assert_eq!(got.last_updated_ns, 100);
        assert_eq!(got.reconcile_generation, Some(4));
    }

    #[test]
    fn resolve_returns_manifest_id_directly() {
        let (_tmp, mut c) = fresh();
        let tx = c.transaction().unwrap();
        let mid = placeholder_manifest(&tx, "tentative");
        set(&tx, &AnchorName::tentative(42), mid, 0).unwrap();
        tx.commit().unwrap();

        assert_eq!(resolve(&c, &AnchorName::tentative(42)).unwrap(), Some(mid));
    }

    #[test]
    fn list_all_returns_sorted() {
        let (_tmp, mut c) = fresh();
        let tx = c.transaction().unwrap();
        let m = placeholder_manifest(&tx, "committed");
        set(&tx, &AnchorName::head(), m, 0).unwrap();
        set(&tx, &AnchorName::branch("main"), m, 0).unwrap();
        set(&tx, &AnchorName::branch("dev"), m, 0).unwrap();
        set(&tx, &AnchorName::tag("v0"), m, 0).unwrap();
        tx.commit().unwrap();

        let names: Vec<String> = list_all(&c)
            .unwrap()
            .into_iter()
            .map(|a| a.name.as_str().to_string())
            .collect();
        assert_eq!(names, vec!["HEAD", "branch/dev", "branch/main", "tag/v0"]);
    }

    #[test]
    fn list_prefix_filters_by_kind() {
        let (_tmp, mut c) = fresh();
        let tx = c.transaction().unwrap();
        let m = placeholder_manifest(&tx, "committed");
        set(&tx, &AnchorName::head(), m, 0).unwrap();
        set(&tx, &AnchorName::branch("main"), m, 0).unwrap();
        set(&tx, &AnchorName::branch("dev"), m, 0).unwrap();
        set(&tx, &AnchorName::tag("v0"), m, 0).unwrap();
        tx.commit().unwrap();

        let branches = list_prefix(&c, "branch/").unwrap();
        assert_eq!(branches.len(), 2);
        let tags = list_prefix(&c, "tag/").unwrap();
        assert_eq!(tags.len(), 1);
    }

    #[test]
    fn delete_removes_one_and_reports() {
        let (_tmp, mut c) = fresh();
        let tx = c.transaction().unwrap();
        let m = placeholder_manifest(&tx, "tentative");
        set(&tx, &AnchorName::head(), m, 0).unwrap();
        let dropped = delete(&tx, &AnchorName::head()).unwrap();
        assert!(dropped);
        let again = delete(&tx, &AnchorName::head()).unwrap();
        assert!(!again);
        tx.commit().unwrap();
    }

    #[test]
    fn set_rejects_missing_manifest_via_fk() {
        let (_tmp, mut c) = fresh();
        let tx = c.transaction().unwrap();
        let err = set(&tx, &AnchorName::head(), ManifestId(9999), 0).unwrap_err();
        assert!(err.to_string().to_lowercase().contains("foreign"));
    }

    #[test]
    fn resolve_explicit_or_default_prefers_tentative_when_no_args() {
        let (_tmp, mut c) = fresh();
        let tx = c.transaction().unwrap();
        let m_head = placeholder_manifest(&tx, "committed");
        let m_tent = placeholder_manifest(&tx, "tentative");
        set(&tx, &AnchorName::head(), m_head, 0).unwrap();
        set(&tx, &AnchorName::tentative(42), m_tent, 0).unwrap();
        tx.commit().unwrap();

        // No explicit args → tentative wins over HEAD.
        let resolved = resolve_explicit_or_default(&c, None, None).unwrap();
        assert_eq!(resolved.as_str(), "tentative/42");
    }

    #[test]
    fn resolve_explicit_or_default_falls_back_to_head_without_tentative() {
        let (_tmp, mut c) = fresh();
        let tx = c.transaction().unwrap();
        let m = placeholder_manifest(&tx, "committed");
        set(&tx, &AnchorName::head(), m, 0).unwrap();
        tx.commit().unwrap();

        // No tentative anchor present → HEAD.
        let resolved = resolve_explicit_or_default(&c, None, None).unwrap();
        assert_eq!(resolved.as_str(), "HEAD");
    }

    #[test]
    fn resolve_explicit_or_default_respects_explicit_anchor_over_tentative() {
        let (_tmp, mut c) = fresh();
        let tx = c.transaction().unwrap();
        let m_head = placeholder_manifest(&tx, "committed");
        let m_tent = placeholder_manifest(&tx, "tentative");
        set(&tx, &AnchorName::head(), m_head, 0).unwrap();
        set(&tx, &AnchorName::tentative(7), m_tent, 0).unwrap();
        tx.commit().unwrap();

        // Explicit HEAD beats the tentative default.
        let resolved = resolve_explicit_or_default(&c, Some("HEAD"), None).unwrap();
        assert_eq!(resolved.as_str(), "HEAD");

        // Explicit branch arg also beats the tentative default.
        let resolved = resolve_explicit_or_default(&c, None, Some("main")).unwrap();
        assert_eq!(resolved.as_str(), "branch/main");
    }
}
