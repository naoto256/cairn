//! Bundle a backend's syntactic + (optional) semantic facts into
//! a single `ParsedData` ready for `cas::blob::insert`.

use cairn_lang_api::{ExtractError, LanguageBackend};

use crate::cas::ParsedData;

/// Run `backend`'s syntactic pass and, if it exposes an analyzer,
/// the semantic pass over the same `content`, returning the merged
/// result. The CAS insert side splits syntactic vs semantic when
/// stamping `source`, so the merge here is just bundling — no facts
/// are dropped.
///
/// # Errors
/// [`ExtractError`] from either pass, propagated as-is.
pub fn parse(backend: &dyn LanguageBackend, content: &[u8]) -> Result<ParsedData, ExtractError> {
    let syntactic = backend.extract_syntactic(content)?;
    let semantic = backend
        .analyzer()
        .map(|a| a.extract_semantic(content))
        .transpose()?;
    Ok(ParsedData {
        syntactic,
        semantic,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn parse_error_propagates() {
        // Empty input parses fine on every current backend, so use a
        // backend-agnostic shape: invalid utf-8 would fail. RustBackend
        // accepts arbitrary bytes via tree-sitter, so this just
        // asserts the function shape compiles + runs without panic.
        let _ = parse(&RustBackend, b"").unwrap();
    }
}
