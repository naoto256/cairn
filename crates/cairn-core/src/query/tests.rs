use crate::cas::store;
use crate::register::register_repo;
use crate::testutil::init_repo;
use cairn_lang_markdown as _;
use cairn_lang_python as _;
use cairn_lang_rust as _;
use cairn_lang_typescript as _;
use rusqlite::Connection;

fn registered() -> (tempfile::TempDir, tempfile::TempDir, Connection) {
    let (repo, _sha) = init_repo(&[
        (
            "src/lib.rs",
            "/// User Authentication handler.\n\
                 pub fn auth_user() {}\n\
                 /// Authentication User reversed.\n\
                 pub fn reverse_auth_doc() {}\n\
                 pub fn alpha() -> i32 { 1 }\n\
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

fn refs_dedup_fixture(
    tier3: bool,
    analyzer_status: Option<&str>,
) -> (tempfile::TempDir, Connection) {
    let db_tmp = tempfile::tempdir().unwrap();
    let conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    conn.execute(
        "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO anchors (anchor_name, manifest_id, last_updated_ns)
             VALUES ('HEAD', 1, 0)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (1, 'src/lib.rs', 'sha-ref')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('sha-ref', 'tree-sitter-rust', 1, 0)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO symbols
               (id, blob_sha, parser_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES
               (1, 'sha-ref', 'tree-sitter-rust', 'caller', 'caller', 'function',
                0, 100, 1, 8, 'rust-syn')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO refs
               (blob_sha, parser_id, enclosing_id, target_name, target_qualified, kind,
                byte_start, byte_end, line, source)
             VALUES
               ('sha-ref', 'tree-sitter-rust', 1, 'render', NULL, 'call',
                42, 48, 5, 'rust-syn')",
        [],
    )
    .unwrap();
    if tier3 {
        conn.execute(
            "INSERT INTO refs
                   (blob_sha, parser_id, enclosing_id, target_name, target_qualified, kind,
                    byte_start, byte_end, line, source)
                 VALUES
                   ('sha-ref', 'tree-sitter-rust', 1, 'render', 'crate::Widget::render', 'call',
                    42, 48, 5, 'tier3-rust-analyzer')",
            [],
        )
        .unwrap();
    }
    if let Some(status) = analyzer_status {
        conn.execute(
            "INSERT INTO workspace_analysis_runs
                   (manifest_id, analyzer_id, analyzer_revision, config_hash, status,
                    started_at_ns, finished_at_ns, error)
                 VALUES
                   (1, 'rust-analyzer-lsp', 1, 'config', ?1, 0, 1, 'rust-analyzer unavailable')",
            rusqlite::params![status],
        )
        .unwrap();
    }
    (db_tmp, conn)
}

fn source_tier_fixture() -> (tempfile::TempDir, Connection) {
    let db_tmp = tempfile::tempdir().unwrap();
    let conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    conn.execute(
        "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO anchors (anchor_name, manifest_id, last_updated_ns)
             VALUES ('HEAD', 1, 0)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO blobs
               (blob_sha, parser_id, parser_revision, analyzer_id,
                analyzer_revision, parsed_at_ns)
             VALUES
               ('sha-syn', 'tree-sitter-rust', 1, NULL, NULL, 0),
               ('sha-sem', 'tree-sitter-rust', 1, 'rust-syn', 1, 0)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES
               (1, 'src/syntactic.rs', 'sha-syn'),
               (1, 'src/semantic.rs', 'sha-sem')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO symbols
               (blob_sha, parser_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES
               ('sha-syn', 'tree-sitter-rust', 'syntactic_fn', 'syntactic_fn',
                'function', 0, 10, 1, 1, 'tree-sitter-rust'),
               ('sha-sem', 'tree-sitter-rust', 'semantic_fn', 'semantic_fn',
                'function', 0, 10, 1, 1, 'rust-syn')",
        [],
    )
    .unwrap();
    (db_tmp, conn)
}

fn language_fixture() -> (tempfile::TempDir, tempfile::TempDir, Connection) {
    let (repo, _sha) = init_repo(&[
        ("src/b.rs", "pub fn rust_user() {}\n"),
        ("src/a.py", "def py_user():\n    pass\n"),
        ("src/z.md", "# User\n"),
    ]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, repo.path(), 0).unwrap();
    (repo, db_tmp, conn)
}

mod find_impls;
mod find_imports;
mod find_references;
mod find_symbols;
mod tentative;
