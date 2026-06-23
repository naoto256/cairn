//! C / C++ header (`.h`) backend disambiguation.
//!
//! `.h` files are valid in both C and C++ codebases; the manifest layer
//! must include them before content is loaded, then this module's
//! `pick_c_family_header_backend` picks one parser id so the blob is
//! indexed exactly once.
//!
//! Split out of `register.rs` so the dispatch helpers in the parent
//! module stay focused on orchestration, and to leave room for
//! additional backend-routing heuristics without growing `register.rs`
//! further.

use std::path::Path;

use cairn_lang_api::LanguageBackend;

const C_FAMILY_HEADER_SCAN_LIMIT: usize = 64 * 1024;

pub(super) fn is_c_family_header_path(path: &str) -> bool {
    Path::new(path).extension().and_then(|ext| ext.to_str()) == Some("h")
}

pub(super) fn pick_c_family_header_backend<'a>(
    backends: &'a [Box<dyn LanguageBackend>],
    path: &str,
    content: &[u8],
) -> Option<&'a dyn LanguageBackend> {
    if !is_c_family_header_path(path) {
        return None;
    }
    // Ambiguous `.h` files are common in both C and C++ projects. The
    // manifest must include them before content is loaded, then parsing
    // chooses one parser id here so the blob is indexed exactly once.
    let parser_id = if header_looks_cpp(content) {
        "tree-sitter-cpp"
    } else {
        "tree-sitter-c"
    };
    backends
        .iter()
        .find(|backend| backend.parser_id() == parser_id)
        .map(|backend| backend.as_ref())
}

fn header_looks_cpp(content: &[u8]) -> bool {
    let window = &content[..content.len().min(C_FAMILY_HEADER_SCAN_LIMIT)];
    let masked = mask_c_family_comments_and_literals(window);
    has_cpp_std_include(&masked)
        || contains_cpp_word(&masked, b"namespace")
        || contains_cpp_class_keyword(&masked)
        || contains_cpp_word(&masked, b"template")
        || contains_cpp_word(&masked, b"typename")
        || contains_cpp_word(&masked, b"concept")
        || contains_cpp_word(&masked, b"requires")
        || contains_cpp_word(&masked, b"constexpr")
        // C23 added `nullptr`, so this can misroute future pure-C headers.
        // It remains a strong practical C++ signal for today's codebases.
        || contains_cpp_word(&masked, b"nullptr")
        || contains_cpp_word(&masked, b"noexcept")
        || contains_cpp_word(&masked, b"operator")
        || contains_access_specifier(&masked, b"public")
        || contains_access_specifier(&masked, b"private")
        || contains_access_specifier(&masked, b"protected")
        || contains_subslice(&masked, b"::")
}

fn mask_c_family_comments_and_literals(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        match input[i] {
            b'/' if input.get(i + 1) == Some(&b'/') => {
                out.extend_from_slice(b"  ");
                i += 2;
                while i < input.len() && input[i] != b'\n' {
                    out.push(b' ');
                    i += 1;
                }
            }
            b'/' if input.get(i + 1) == Some(&b'*') => {
                out.extend_from_slice(b"  ");
                i += 2;
                while i < input.len() {
                    if input[i] == b'\n' {
                        out.push(b'\n');
                        i += 1;
                        continue;
                    }
                    if input[i] == b'*' && input.get(i + 1) == Some(&b'/') {
                        out.extend_from_slice(b"  ");
                        i += 2;
                        break;
                    }
                    out.push(b' ');
                    i += 1;
                }
            }
            b'"' | b'\'' => {
                let quote = input[i];
                out.push(b' ');
                i += 1;
                while i < input.len() {
                    if input[i] == b'\n' {
                        out.push(b'\n');
                        i += 1;
                        break;
                    }
                    if input[i] == b'\\' {
                        out.push(b' ');
                        if i + 1 < input.len() {
                            out.push(if input[i + 1] == b'\n' { b'\n' } else { b' ' });
                        }
                        i += 2;
                        continue;
                    }
                    let done = input[i] == quote;
                    out.push(b' ');
                    i += 1;
                    if done {
                        break;
                    }
                }
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    out
}

fn has_cpp_std_include(masked: &[u8]) -> bool {
    masked.split(|byte| *byte == b'\n').any(|line| {
        let mut i = skip_ascii_ws(line, 0);
        if line.get(i) != Some(&b'#') {
            return false;
        }
        i = skip_ascii_ws(line, i + 1);
        if !starts_with_word_at(line, i, b"include") {
            return false;
        }
        i = skip_ascii_ws(line, i + b"include".len());
        if line.get(i) != Some(&b'<') {
            return false;
        }
        let header_start = i + 1;
        let Some(header_end) = line[header_start..]
            .iter()
            .position(|byte| *byte == b'>')
            .map(|offset| header_start + offset)
        else {
            return false;
        };
        matches!(
            &line[header_start..header_end],
            b"iostream"
                | b"string"
                | b"vector"
                | b"memory"
                | b"optional"
                | b"variant"
                | b"span"
                | b"unordered_map"
                | b"unordered_set"
                | b"map"
                | b"set"
                | b"array"
                | b"tuple"
                | b"type_traits"
                | b"utility"
        )
    })
}

fn contains_cpp_word(masked: &[u8], word: &[u8]) -> bool {
    find_words(masked, word).any(|_| true)
}

fn contains_cpp_class_keyword(masked: &[u8]) -> bool {
    find_words(masked, b"class").any(|start| start == 0 || masked[start - 1] != b'@')
}

fn contains_access_specifier(masked: &[u8], word: &[u8]) -> bool {
    find_words(masked, word).any(|start| {
        let after = skip_ascii_ws(masked, start + word.len());
        masked.get(after) == Some(&b':')
    })
}

fn find_words<'a>(haystack: &'a [u8], word: &'a [u8]) -> impl Iterator<Item = usize> + 'a {
    haystack
        .windows(word.len())
        .enumerate()
        .filter_map(move |(i, w)| {
            (w == word
                && i.checked_sub(1)
                    .and_then(|prev| haystack.get(prev))
                    .is_none_or(|byte| !is_identifier_byte(*byte))
                && haystack
                    .get(i + word.len())
                    .is_none_or(|byte| !is_identifier_byte(*byte)))
            .then_some(i)
        })
}

fn starts_with_word_at(haystack: &[u8], offset: usize, word: &[u8]) -> bool {
    haystack
        .get(offset..offset + word.len())
        .is_some_and(|candidate| candidate == word)
        && haystack
            .get(offset + word.len())
            .is_none_or(|byte| !is_identifier_byte(*byte))
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn skip_ascii_ws(bytes: &[u8], mut offset: usize) -> usize {
    while matches!(bytes.get(offset), Some(b' ' | b'\t' | b'\r' | b'\n')) {
        offset += 1;
    }
    offset
}

fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}
