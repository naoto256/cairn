//! C / C++ / Objective-C header (`.h`) backend disambiguation.
//!
//! `.h` files are valid in C, C++, and Objective-C codebases; the
//! manifest layer must include them before content is loaded, then
//! this module's `pick_c_family_header_backend` picks one parser id
//! so the blob is indexed exactly once.
//!
//! Dispatch priority is **ObjC > C++ > C**. ObjC has unambiguous
//! `@interface` / `@implementation` / `@protocol` keywords that never
//! appear in valid C or C++ source, so a positive ObjC signal is
//! authoritative. C++ detection relies on weaker heuristics (`class`,
//! `namespace`, `template`, etc.) that overlap with C identifiers, so
//! it is only consulted after ObjC has been ruled out. C is the
//! fallback when no language-specific signal is found.
//!
//! ObjC++ (`.mm`) is out of scope here — those files are not `.h` and
//! are not currently claimed by any backend.
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
    // Ambiguous `.h` files are common in C, C++, and Objective-C
    // projects. The manifest must include them before content is
    // loaded, then parsing chooses one parser id here so the blob is
    // indexed exactly once. ObjC is checked first because its
    // `@interface` / `@implementation` / `@protocol` keywords never
    // appear in valid C or C++ source — a positive signal there is
    // authoritative. C++ keyword detection (`class`, `namespace`, …)
    // is weaker, so it is only consulted after ObjC is ruled out.
    let window = &content[..content.len().min(C_FAMILY_HEADER_SCAN_LIMIT)];
    let masked = mask_c_family_comments_and_literals(window);
    let parser_id = if header_looks_objc(&masked, window) {
        "tree-sitter-objc"
    } else if header_looks_cpp_masked(&masked) {
        "tree-sitter-cpp"
    } else {
        "tree-sitter-c"
    };
    backends
        .iter()
        .find(|backend| backend.parser_id() == parser_id)
        .map(|backend| backend.as_ref())
}

/// Returns true if the header content looks like Objective-C.
///
/// Looks for an ObjC directive (`@interface`, `@implementation`,
/// `@protocol`, `@class`, `@end`) at the start of any line in the
/// masked window, or for an `#import "…"` / `#import <…>` directive in
/// the raw window — `#import` is the ObjC-idiomatic include form and
/// almost never appears in pure C or C++ headers.
///
/// `masked` must be `mask_c_family_comments_and_literals(raw)` so the
/// directive scan cannot be tricked by `@interface` inside a string
/// or comment. The `#import` scan needs the raw bytes because masking
/// blanks the quotes / brackets that distinguish a real include from
/// a bare `#import` token.
pub(super) fn header_looks_objc(masked: &[u8], raw: &[u8]) -> bool {
    const DIRECTIVES: &[&[u8]] = &[
        b"@interface",
        b"@implementation",
        b"@protocol",
        b"@class",
        b"@end",
    ];
    for line in masked.split(|byte| *byte == b'\n') {
        let i = skip_ascii_ws(line, 0);
        let rest = &line[i..];
        for directive in DIRECTIVES {
            if rest.starts_with(directive) {
                // Require the next byte to be a non-identifier so
                // `@interfaceFoo` (an invented identifier) doesn't trip.
                // `@end` legitimately has nothing after it, so EOF /
                // whitespace counts as a terminator.
                let after = rest.get(directive.len()).copied();
                let terminated = match after {
                    None => true,
                    Some(b) => !is_identifier_byte(b),
                };
                if terminated {
                    return true;
                }
            }
        }
    }
    // `#import` (with either `"…"` or `<…>`) is ObjC-idiomatic. Run
    // against the raw window because masking blanks the quote and
    // angle-bracket bytes we need to disambiguate from bare `#import`.
    // A line-level `//` strip is enough to avoid commented-out imports
    // — block comments containing `#import` at column 0 are vanishingly
    // rare in practice and the directive-scan above is the main signal.
    for line in raw.split(|byte| *byte == b'\n') {
        let line = strip_line_comment(line);
        let i = skip_ascii_ws(line, 0);
        let rest = &line[i..];
        if rest.first() != Some(&b'#') {
            continue;
        }
        let after_hash = skip_ascii_ws(rest, 1);
        if !starts_with_word_at(rest, after_hash, b"import") {
            continue;
        }
        let after = skip_ascii_ws(rest, after_hash + b"import".len());
        if matches!(rest.get(after), Some(&b'"') | Some(&b'<')) {
            return true;
        }
    }
    false
}

fn strip_line_comment(line: &[u8]) -> &[u8] {
    for i in 0..line.len().saturating_sub(1) {
        if line[i] == b'/' && line[i + 1] == b'/' {
            return &line[..i];
        }
    }
    line
}

fn header_looks_cpp_masked(masked: &[u8]) -> bool {
    has_cpp_std_include(masked)
        || contains_cpp_word(masked, b"namespace")
        || contains_cpp_class_keyword(masked)
        || contains_cpp_word(masked, b"template")
        || contains_cpp_word(masked, b"typename")
        || contains_cpp_word(masked, b"concept")
        || contains_cpp_word(masked, b"requires")
        || contains_cpp_word(masked, b"constexpr")
        // C23 added `nullptr`, so this can misroute future pure-C headers.
        // It remains a strong practical C++ signal for today's codebases.
        || contains_cpp_word(masked, b"nullptr")
        || contains_cpp_word(masked, b"noexcept")
        || contains_cpp_word(masked, b"operator")
        || contains_access_specifier(masked, b"public")
        || contains_access_specifier(masked, b"private")
        || contains_access_specifier(masked, b"protected")
        || contains_subslice(masked, b"::")
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

#[cfg(test)]
mod tests {
    use super::*;

    fn looks_objc(src: &[u8]) -> bool {
        let masked = mask_c_family_comments_and_literals(src);
        header_looks_objc(&masked, src)
    }

    fn looks_cpp(src: &[u8]) -> bool {
        let masked = mask_c_family_comments_and_literals(src);
        header_looks_cpp_masked(&masked)
    }

    #[test]
    fn is_c_family_header_path_matches_dot_h_only() {
        assert!(is_c_family_header_path("foo.h"));
        assert!(is_c_family_header_path("path/to/Foo.h"));
        assert!(!is_c_family_header_path("foo.hpp"));
        assert!(!is_c_family_header_path("foo.c"));
        assert!(!is_c_family_header_path("foo.m"));
        assert!(!is_c_family_header_path("foo"));
    }

    #[test]
    fn objc_interface_declaration_detected() {
        let src = br#"
#import <Foundation/Foundation.h>

@interface AFHTTPSessionManager : AFURLSessionManager <NSSecureCoding, NSCopying>
@end
"#;
        assert!(looks_objc(src));
    }

    #[test]
    fn objc_protocol_declaration_detected() {
        let src = b"@protocol Drawable\n- (void)draw;\n@end\n";
        assert!(looks_objc(src));
    }

    #[test]
    fn objc_forward_class_declaration_detected() {
        let src = b"@class NSString;\n@class NSArray;\n";
        assert!(looks_objc(src));
    }

    #[test]
    fn objc_implementation_detected() {
        let src = b"@implementation Foo\n- (void)bar {}\n@end\n";
        assert!(looks_objc(src));
    }

    #[test]
    fn objc_import_quoted_or_system_detected() {
        assert!(looks_objc(b"#import \"Local.h\"\n"));
        assert!(looks_objc(b"#import <Foundation/Foundation.h>\n"));
        // `#import` without a path target is not a usable signal.
        assert!(!looks_objc(b"#import\n"));
    }

    #[test]
    fn plain_c_header_is_not_objc() {
        let src = br#"
#ifndef FOO_H
#define FOO_H
#include <stdio.h>
int foo(int x);
struct point { int x; int y; };
#endif
"#;
        assert!(!looks_objc(src));
    }

    #[test]
    fn cpp_class_header_is_not_objc() {
        let src = br#"
#pragma once
#include <vector>
namespace ns {
class Foo {
public:
    Foo();
private:
    int x_;
};
}
"#;
        assert!(!looks_objc(src));
        assert!(looks_cpp(src));
    }

    #[test]
    fn at_interface_inside_string_literal_does_not_trigger_objc() {
        // String literals are masked out, so `@interface` inside one
        // must not register as an ObjC directive.
        let src = br#"
static const char *DOC = "@interface in docstring should not count";
int foo(void);
"#;
        assert!(!looks_objc(src));
    }

    #[test]
    fn at_interface_inside_block_comment_does_not_trigger_objc() {
        let src = br#"
/* example:
   @interface Foo : Bar
   @end
*/
int foo(void);
"#;
        assert!(!looks_objc(src));
    }

    #[test]
    fn at_interface_inside_ifdef_still_detected() {
        // Realistic case: an ObjC-only header guarded by a macro.
        let src = br#"
#ifdef __OBJC__
@interface Foo : NSObject
@end
#endif
"#;
        assert!(looks_objc(src));
    }

    #[test]
    fn at_interfacefoo_not_a_directive() {
        // An identifier that merely begins with `@interface` must not
        // trigger detection — the boundary check should reject it.
        let src = b"int @interfaceFoo;\n";
        assert!(!looks_objc(src));
    }

    #[test]
    fn pick_backend_prefers_objc_over_cpp_when_both_keywords_present() {
        // A header that mixes ObjC directives with C++ish identifiers
        // (e.g. an Objective-C++ migration boundary) routes to ObjC —
        // the dispatch priority guarantees the unambiguous signal wins.
        // (ObjC++ `.mm` is out of scope; this only exercises priority.)
        let src = br#"
@interface Foo : NSObject
@end
namespace ns { class Helper; }
"#;
        let masked = mask_c_family_comments_and_literals(src);
        assert!(header_looks_objc(&masked, src));
        assert!(header_looks_cpp_masked(&masked));
        // Priority order in pick_c_family_header_backend handles the rest.
    }
}
