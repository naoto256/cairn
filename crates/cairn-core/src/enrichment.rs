use std::collections::HashSet;

use cairn_lang_api::LanguageBackend;
use cairn_proto::{LanguageEnrichment, SourceTier};
use rusqlite::{Connection, params};

use crate::Result;
use crate::workspace_analyzer::all_workspace_analyzers;

/// Build the per-language enrichment matrix for one manifest.
pub(crate) fn collect_enrichment(
    conn: &Connection,
    manifest_id: i64,
    backends: &[Box<dyn LanguageBackend>],
) -> Result<Vec<LanguageEnrichment>> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT b.parser_id
           FROM blobs b
           JOIN manifest_entries me ON me.blob_sha = b.blob_sha
          WHERE me.manifest_id = ?1
          ORDER BY b.parser_id",
    )?;
    let parser_ids = stmt
        .query_map(params![manifest_id], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let workspace_analyzer_ids = all_workspace_analyzers()
        .into_iter()
        .map(|a| (a.parser_id(), a.id()))
        .collect::<HashSet<_>>();

    let mut out: Vec<LanguageEnrichment> = Vec::with_capacity(parser_ids.len());
    for parser_id in parser_ids {
        let backend = backends
            .iter()
            .find(|b| b.parser_id() == parser_id)
            .map(|b| b.as_ref());
        let language = backend.map_or_else(|| short_lang_name(&parser_id), |b| b.name().into());
        let has_analyzer = backend.is_some_and(|b| b.analyzer().is_some())
            || workspace_analyzer_ids
                .iter()
                .any(|(workspace_parser_id, _)| *workspace_parser_id == parser_id);
        let analyzer_id = backend.and_then(|b| b.analyzer().map(|a| a.name()));
        let tier = if manifest_has_analyzer_run(conn, manifest_id, &parser_id, analyzer_id)? {
            SourceTier::Semantic
        } else {
            SourceTier::Syntactic
        };

        if let Some(existing) = out.iter_mut().find(|e| e.language == language) {
            if matches!(tier, SourceTier::Semantic) {
                existing.tier = SourceTier::Semantic;
            }
            existing.has_analyzer |= has_analyzer;
        } else {
            out.push(LanguageEnrichment {
                language,
                tier,
                has_analyzer,
            });
        }
    }
    out.sort_by(|a, b| a.language.cmp(&b.language));
    Ok(out)
}

fn short_lang_name(parser_id: &str) -> String {
    parser_id
        .strip_prefix("tree-sitter-")
        .and_then(|rest| rest.split('@').next())
        .map_or_else(|| parser_id.to_string(), str::to_string)
}

fn manifest_has_analyzer_run(
    conn: &Connection,
    manifest_id: i64,
    parser_id: &str,
    analyzer_id: Option<&str>,
) -> Result<bool> {
    let Some(analyzer_id) = analyzer_id else {
        return Ok(false);
    };
    let ran: bool = conn.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM blobs b
             JOIN manifest_entries me ON me.blob_sha = b.blob_sha
            WHERE me.manifest_id = ?1
              AND b.parser_id = ?2
              AND b.analyzer_id = ?3
         )",
        params![manifest_id, parser_id, analyzer_id],
        |r| r.get(0),
    )?;
    Ok(ran)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use cairn_lang_api::{Analyzer, ExtractError, SemanticFacts, SyntacticFacts};

    #[test]
    fn short_lang_name_strips_tree_sitter_prefix_and_version() {
        assert_eq!(short_lang_name("tree-sitter-rust@1.2.3"), "rust");
        assert_eq!(short_lang_name("custom-parser"), "custom-parser");
    }

    #[test]
    fn analyzer_run_marks_manifest_semantic_even_without_facts() {
        let (_tmp, conn) = fresh_store();
        seed_manifest_blob(
            &conn,
            "sha-sem",
            "tree-sitter-empty@1",
            Some("empty-analyzer"),
            Some(1),
        );
        let backends: Vec<Box<dyn LanguageBackend>> = vec![Box::new(EmptyBackend)];

        let enrichment = collect_enrichment(&conn, 1, &backends).unwrap();
        assert_eq!(enrichment.len(), 1);
        assert_eq!(enrichment[0].language, "empty");
        assert_eq!(enrichment[0].tier, SourceTier::Semantic);
        assert!(enrichment[0].has_analyzer);
    }

    #[test]
    fn missing_analyzer_run_keeps_manifest_syntactic() {
        let (_tmp, conn) = fresh_store();
        seed_manifest_blob(&conn, "sha-syn", "tree-sitter-empty@1", None, None);
        let backends: Vec<Box<dyn LanguageBackend>> = vec![Box::new(EmptyBackend)];

        let enrichment = collect_enrichment(&conn, 1, &backends).unwrap();
        assert_eq!(enrichment.len(), 1);
        assert_eq!(enrichment[0].language, "empty");
        assert_eq!(enrichment[0].tier, SourceTier::Syntactic);
        assert!(enrichment[0].has_analyzer);
    }

    #[test]
    fn registered_workspace_analyzer_marks_manifest_as_analyzable() {
        let (_tmp, conn) = fresh_store();
        seed_manifest_blob(&conn, "sha-workspace", "fake-parser", None, None);
        let backends: Vec<Box<dyn LanguageBackend>> = Vec::new();

        let enrichment = collect_enrichment(&conn, 1, &backends).unwrap();
        assert_eq!(enrichment.len(), 1);
        assert_eq!(enrichment[0].language, "fake-parser");
        assert_eq!(enrichment[0].tier, SourceTier::Syntactic);
        assert!(enrichment[0].has_analyzer);
    }

    #[test]
    fn parser_without_tier2_or_workspace_analyzer_is_not_analyzable() {
        let (_tmp, conn) = fresh_store();
        seed_manifest_blob(&conn, "sha-plain", "tree-sitter-plain", None, None);
        let backends: Vec<Box<dyn LanguageBackend>> = Vec::new();

        let enrichment = collect_enrichment(&conn, 1, &backends).unwrap();
        assert_eq!(enrichment.len(), 1);
        assert_eq!(enrichment[0].language, "plain");
        assert_eq!(enrichment[0].tier, SourceTier::Syntactic);
        assert!(!enrichment[0].has_analyzer);
    }

    fn fresh_store() -> (tempfile::TempDir, Connection) {
        let tmp = tempfile::tempdir().unwrap();
        let conn = crate::cas::store::open(&tmp.path().join("store.db")).unwrap();
        (tmp, conn)
    }

    fn seed_manifest_blob(
        conn: &Connection,
        blob_sha: &str,
        parser_id: &str,
        analyzer_id: Option<&str>,
        analyzer_revision: Option<u32>,
    ) {
        conn.execute(
            "INSERT INTO blobs
               (blob_sha, parser_id, parser_revision, parsed_at_ns, analyzer_id, analyzer_revision)
             VALUES (?1, ?2, 1, 0, ?3, ?4)",
            params![blob_sha, parser_id, analyzer_id, analyzer_revision],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, commit_sha, built_at_ns)
             VALUES (1, 'committed', 'abc', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (1, 'x.empty', ?1)",
            params![blob_sha],
        )
        .unwrap();
    }

    struct EmptyBackend;

    impl LanguageBackend for EmptyBackend {
        fn name(&self) -> &'static str {
            "empty"
        }

        fn file_patterns(&self) -> &'static [&'static str] {
            &["*.empty"]
        }

        fn parser_id(&self) -> &'static str {
            "tree-sitter-empty@1"
        }

        fn extract_syntactic(
            &self,
            _source: &[u8],
        ) -> std::result::Result<SyntacticFacts, ExtractError> {
            Ok(SyntacticFacts::default())
        }

        fn analyzer(&self) -> Option<Arc<dyn Analyzer>> {
            Some(Arc::new(EmptyAnalyzer))
        }
    }

    struct EmptyAnalyzer;

    impl Analyzer for EmptyAnalyzer {
        fn name(&self) -> &'static str {
            "empty-analyzer"
        }

        fn extract_semantic(
            &self,
            _source: &[u8],
        ) -> std::result::Result<SemanticFacts, ExtractError> {
            Ok(SemanticFacts::default())
        }
    }
}
