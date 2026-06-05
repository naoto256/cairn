//! Bundle a backend's syntactic + (optional) semantic facts into
//! a single `ParsedData` ready for `cas::blob::insert`.

use cairn_lang_api::{ExtractError, LanguageBackend};
use tracing::debug;

use crate::cas::ParsedData;

/// Run `backend`'s syntactic pass and, if it exposes an analyzer,
/// the semantic pass over the same `content`, returning the merged
/// result. The CAS insert side splits syntactic vs semantic when
/// stamping `source`, so the merge here is just bundling — no facts
/// are dropped.
///
/// # Errors
/// [`ExtractError`] from the syntactic pass, propagated as-is. Semantic
/// extraction errors degrade to `semantic: None` so Tier-1 facts remain
/// indexable.
pub fn parse(backend: &dyn LanguageBackend, content: &[u8]) -> Result<ParsedData, ExtractError> {
    let syntactic = backend.extract_syntactic(content)?;
    let semantic =
        backend
            .analyzer()
            .and_then(|analyzer| match analyzer.extract_semantic(content) {
                Ok(facts) => Some(facts),
                Err(err) => {
                    debug!(
                        analyzer = analyzer.name(),
                        error = %err,
                        "tier-2 semantic extraction failed; preserving syntactic facts only"
                    );
                    None
                }
            });
    Ok(ParsedData {
        syntactic,
        semantic,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use cairn_lang_api::{Analyzer, SymbolFact, SymbolKind, SyntacticFacts, Visibility};
    use cairn_lang_rust::RustBackend;

    #[test]
    fn rust_backend_produces_symbols_via_parse() {
        let src = b"pub fn hello() -> u32 { 42 }\n";
        let data = parse(&RustBackend, src).unwrap();
        assert!(
            data.syntactic.symbols.iter().any(|s| s.name == "hello"),
            "no `hello` symbol in {:?}",
            data.syntactic
                .symbols
                .iter()
                .map(|s| &s.name)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn rust_backend_emits_semantic_when_analyzer_present() {
        // Bare `use` should give us an import via the syntactic or
        // semantic pass; the bundle must contain at least one.
        let src = b"use std::io::Read;\npub fn f() {}\n";
        let data = parse(&RustBackend, src).unwrap();
        let import_total =
            data.syntactic.imports.len() + data.semantic.as_ref().map_or(0, |s| s.imports.len());
        assert!(import_total >= 1, "no imports surfaced");
    }

    #[test]
    fn semantic_error_degrades_to_syntactic_only() {
        let data = parse(&BackendWithFailingAnalyzer, b"ignored").unwrap();

        assert!(data.semantic.is_none());
        assert_eq!(data.syntactic.symbols.len(), 1);
        assert_eq!(data.syntactic.symbols[0].name, "syntactic_only");
    }

    #[test]
    fn syntactic_error_still_propagates() {
        let err = parse(&BackendWithFailingSyntax, b"ignored").unwrap_err();
        assert!(err.to_string().contains("syntactic failed"));
    }

    struct BackendWithFailingAnalyzer;

    impl LanguageBackend for BackendWithFailingAnalyzer {
        fn name(&self) -> &'static str {
            "test-syntax-ok"
        }

        fn file_patterns(&self) -> &'static [&'static str] {
            &["*.test"]
        }

        fn parser_id(&self) -> &'static str {
            "test-syntax-ok"
        }

        fn extract_syntactic(&self, _source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
            Ok(SyntacticFacts {
                symbols: vec![SymbolFact {
                    name: "syntactic_only".into(),
                    qualified: "syntactic_only".into(),
                    kind: SymbolKind::Function,
                    signature: None,
                    doc: None,
                    visibility: Some(Visibility::Public),
                    byte_range: 0..1,
                    line_range: 1..1,
                    body_start: None,
                    parent_idx: None,
                }],
                ..SyntacticFacts::default()
            })
        }

        fn analyzer(&self) -> Option<Arc<dyn Analyzer>> {
            Some(Arc::new(FailingAnalyzer))
        }
    }

    struct BackendWithFailingSyntax;

    impl LanguageBackend for BackendWithFailingSyntax {
        fn name(&self) -> &'static str {
            "test-syntax-fails"
        }

        fn file_patterns(&self) -> &'static [&'static str] {
            &["*.test"]
        }

        fn parser_id(&self) -> &'static str {
            "test-syntax-fails"
        }

        fn extract_syntactic(&self, _source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
            Err(ExtractError::ParserFailure("syntactic failed".into()))
        }
    }

    struct FailingAnalyzer;

    impl Analyzer for FailingAnalyzer {
        fn name(&self) -> &'static str {
            "test-semantic-fails"
        }

        fn extract_semantic(
            &self,
            _source: &[u8],
        ) -> Result<cairn_lang_api::SemanticFacts, ExtractError> {
            Err(ExtractError::ParserFailure("semantic failed".into()))
        }
    }
}
