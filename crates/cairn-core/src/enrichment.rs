use cairn_lang_api::LanguageBackend;
use cairn_proto::{LanguageEnrichment, SourceTier};
use rusqlite::{Connection, params};

use crate::Result;

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

    let mut out: Vec<LanguageEnrichment> = Vec::with_capacity(parser_ids.len());
    for parser_id in parser_ids {
        let backend = backends
            .iter()
            .find(|b| b.parser_id() == parser_id)
            .map(|b| b.as_ref());
        let language = backend.map_or_else(|| short_lang_name(&parser_id), |b| b.name().into());
        let has_analyzer = backend.is_some_and(|b| b.analyzer().is_some());
        let tier = if manifest_has_semantic_facts(conn, manifest_id, &parser_id)? {
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

fn manifest_has_semantic_facts(
    conn: &Connection,
    manifest_id: i64,
    parser_id: &str,
) -> Result<bool> {
    let semantic_refs: bool = conn.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM refs r
             JOIN manifest_entries me ON me.blob_sha = r.blob_sha
            WHERE me.manifest_id = ?1
              AND r.parser_id = ?2
              AND r.source = 'semantic'
         )",
        params![manifest_id, parser_id],
        |r| r.get(0),
    )?;
    if semantic_refs {
        return Ok(true);
    }

    let implementations: bool = conn.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM implementations i
             JOIN manifest_entries me ON me.blob_sha = i.blob_sha
            WHERE me.manifest_id = ?1
              AND i.parser_id = ?2
         )",
        params![manifest_id, parser_id],
        |r| r.get(0),
    )?;
    Ok(implementations)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_lang_name_strips_tree_sitter_prefix_and_version() {
        assert_eq!(short_lang_name("tree-sitter-rust@1.2.3"), "rust");
        assert_eq!(short_lang_name("custom-parser"), "custom-parser");
    }
}
