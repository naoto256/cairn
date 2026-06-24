//! Pre-parse blanking of Apple Cocoa/Foundation decoration macros.
//!
//! tree-sitter-objc does not model the Apple SDK's pervasive
//! nullability / availability / Swift-bridging macros. When one of
//! them sits *between* declarations (the most damaging case, since
//! Apple headers wrap every `.h` file in `NS_ASSUME_NONNULL_BEGIN` …
//! `NS_ASSUME_NONNULL_END`), the parser folds the following
//! `@interface` into a malformed `declaration` and the `class_interface`
//! node never appears — so neither the Tier-1 extractor nor the
//! Tier-2 analyzer ever sees the class.
//!
//! Fix: blank known macro tokens (and any balanced `(...)` argument
//! list they take) with spaces *before* parsing. Whitespace replacement
//! preserves byte offsets and line numbers exactly, which keeps
//! downstream `line_of` / `byte_range` reporting correct.
//!
//! Tree-sitter is the source of truth; this is a narrow pre-pass that
//! only neutralizes known-unparseable tokens. Anything not on the list
//! is left untouched (best effort — unknown macros tucked between
//! declarations may still break parsing, but the bulk of Apple SDK and
//! AFNetworking-style code is covered).
//!
//! Performance: a single linear scan over the source; macro names are
//! ASCII identifiers checked against a fixed list and a `NS_*` /
//! `UI_*` / `CG_*` family prefix filter.

/// Apple SDK macros that decorate or wrap declarations. Each entry is
/// the bare token name; if the source has the call form `NAME(...)`,
/// the balanced argument list is blanked too.
///
/// The list covers the macros explicitly named in the bug report plus
/// closely-related neighbors that share the same parse failure mode
/// (nullability annotations, availability shields, attribute bridges).
/// Anything not listed here is best-effort; the prefix families below
/// catch the long-tail `NS_AVAILABLE_*`, `NS_DEPRECATED_*`,
/// `API_AVAILABLE`, etc. without needing every variant enumerated.
const KNOWN_MACROS: &[&str] = &[
    // Nullability wrappers (most critical — they bracket whole headers).
    "NS_ASSUME_NONNULL_BEGIN",
    "NS_ASSUME_NONNULL_END",
    // Swift-bridging / initializer / dispatch markers.
    "NS_SWIFT_NAME",
    "NS_SWIFT_UNAVAILABLE",
    "NS_REFINED_FOR_SWIFT",
    "NS_DESIGNATED_INITIALIZER",
    "NS_REQUIRES_SUPER",
    "NS_UNAVAILABLE",
    "NS_NOESCAPE",
    "NS_RETURNS_RETAINED",
    "NS_RETURNS_NOT_RETAINED",
    "NS_FORMAT_FUNCTION",
    "NS_FORMAT_ARGUMENT",
    // Deprecation shields (no arguments form; the `_*` family is caught
    // by the prefix filter below for the WITH-arguments variants like
    // `NS_DEPRECATED_IOS(2_0, 7_0)`).
    "DEPRECATED_ATTRIBUTE",
    // GCC / Clang attribute syntax: `__attribute__((...))`.
    "__attribute__",
];

/// Token prefixes whose entire family is treated as a decoration macro.
/// Matches identifiers like `NS_CLASS_AVAILABLE_IOS`, `NS_AVAILABLE_MAC`,
/// `NS_DEPRECATED_IOS`, `API_AVAILABLE`, `API_UNAVAILABLE`,
/// `__IOS_AVAILABLE`, etc. — all of which take a parenthesized argument
/// list and disrupt parsing the same way `NS_SWIFT_NAME(...)` does.
const KNOWN_MACRO_PREFIXES: &[&str] = &[
    "NS_AVAILABLE",
    "NS_DEPRECATED",
    "NS_CLASS_AVAILABLE",
    "NS_CLASS_DEPRECATED",
    "NS_ENUM_AVAILABLE",
    "NS_EXTENSION_UNAVAILABLE",
    "API_AVAILABLE",
    "API_UNAVAILABLE",
    "API_DEPRECATED",
    "__OSX_AVAILABLE",
    "__IOS_AVAILABLE",
    "__TVOS_AVAILABLE",
    "__WATCHOS_AVAILABLE",
];

/// Return a copy of `source` with known Apple decoration macros blanked
/// to spaces. Newlines are preserved so line numbers stay aligned.
///
/// If no macros are present the original buffer is returned unchanged
/// (a `Cow::Borrowed`) to avoid the allocation on most non-Apple ObjC.
pub(crate) fn neutralize_apple_macros(source: &[u8]) -> std::borrow::Cow<'_, [u8]> {
    // Fast path: only allocate if at least one candidate token appears.
    // Checking for `NS_` / `API_` / `__` / `DEPRECATED_ATTRIBUTE` covers
    // every entry in both lists without scanning identifier boundaries.
    let needs_work = memchr_any(source, b"NS_")
        || memchr_any(source, b"API_")
        || memchr_any(source, b"__")
        || memchr_any(source, b"DEPRECATED_ATTRIBUTE");
    if !needs_work {
        return std::borrow::Cow::Borrowed(source);
    }

    let mut out = source.to_vec();
    let mut i = 0;
    while i < source.len() {
        // Only consider identifier starts that are not preceded by
        // another identifier character — avoids matching the tail of
        // a longer token like `MY_NS_SWIFT_NAME`.
        let prev_is_id = i > 0 && is_ident_byte(source[i - 1]);
        if !prev_is_id && is_ident_start(source[i]) {
            let end = scan_ident_end(source, i);
            let ident = &source[i..end];
            if is_known_macro(ident) {
                // If a `(...)` follows, blank it too (handles
                // `NS_SWIFT_NAME(foo)` and `__attribute__((...))`).
                let mut blank_end = end;
                // Skip whitespace between the identifier and `(`.
                let mut j = end;
                while j < source.len() && (source[j] == b' ' || source[j] == b'\t') {
                    j += 1;
                }
                if j < source.len() && source[j] == b'(' {
                    if let Some(close) = find_balanced_paren(source, j) {
                        blank_end = close + 1;
                    }
                }
                blank_range(&mut out, i, blank_end);
                i = blank_end;
                continue;
            }
            i = end;
        } else {
            i += 1;
        }
    }
    std::borrow::Cow::Owned(out)
}

fn memchr_any(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

fn is_ident_start(b: u8) -> bool {
    b == b'_' || b.is_ascii_alphabetic()
}

fn is_ident_byte(b: u8) -> bool {
    b == b'_' || b.is_ascii_alphanumeric()
}

fn scan_ident_end(src: &[u8], start: usize) -> usize {
    let mut i = start;
    while i < src.len() && is_ident_byte(src[i]) {
        i += 1;
    }
    i
}

fn is_known_macro(ident: &[u8]) -> bool {
    let s = match std::str::from_utf8(ident) {
        Ok(s) => s,
        Err(_) => return false,
    };
    if KNOWN_MACROS.contains(&s) {
        return true;
    }
    for prefix in KNOWN_MACRO_PREFIXES {
        if s == *prefix || s.starts_with(&format!("{prefix}_")) {
            return true;
        }
    }
    false
}

/// Walk a balanced parenthesis run starting at `open` (where
/// `src[open] == b'('`). Returns the byte offset of the matching `)`,
/// or `None` if the parens are unbalanced. String / char literal
/// awareness is intentionally omitted — Apple decoration macros take
/// only identifier and integer arguments in practice; treating quotes
/// literally is good enough and avoids the cost of full lexing.
fn find_balanced_paren(src: &[u8], open: usize) -> Option<usize> {
    debug_assert_eq!(src[open], b'(');
    let mut depth: i32 = 0;
    let mut i = open;
    while i < src.len() {
        match src[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn blank_range(buf: &mut [u8], start: usize, end: usize) {
    for byte in &mut buf[start..end] {
        if *byte != b'\n' && *byte != b'\r' {
            *byte = b' ';
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blank(s: &str) -> String {
        String::from_utf8(neutralize_apple_macros(s.as_bytes()).into_owned()).unwrap()
    }

    #[test]
    fn no_macros_returns_borrowed() {
        let src = b"@interface Foo : Bar\n@end\n";
        match neutralize_apple_macros(src) {
            std::borrow::Cow::Borrowed(_) => {}
            std::borrow::Cow::Owned(_) => panic!("should be borrowed"),
        }
    }

    #[test]
    fn blanks_ns_assume_nonnull() {
        let out = blank("NS_ASSUME_NONNULL_BEGIN\n@interface F : B\n@end\nNS_ASSUME_NONNULL_END\n");
        assert!(out.starts_with("                       \n@interface F : B"));
        assert!(out.contains("\n                     \n"));
    }

    #[test]
    fn blanks_ns_swift_name_with_args() {
        let out = blank("@interface Foo NS_SWIFT_NAME(Bar) : Base\n@end\n");
        assert_eq!(out, "@interface Foo                    : Base\n@end\n");
    }

    #[test]
    fn blanks_attribute_with_nested_parens() {
        let out = blank("__attribute__((deprecated(\"x\"))) @interface Foo : Bar\n@end\n");
        assert!(out.starts_with("                                 @interface Foo : Bar"));
    }

    #[test]
    fn ignores_suffix_of_longer_identifier() {
        // `MY_NS_SWIFT_NAME` must not be partially blanked.
        let out = blank("int MY_NS_SWIFT_NAME_VAR;\n");
        assert_eq!(out, "int MY_NS_SWIFT_NAME_VAR;\n");
    }

    #[test]
    fn handles_availability_family() {
        let out = blank("NS_CLASS_AVAILABLE_IOS(7_0) @interface Foo : Bar\n@end\n");
        assert!(out.contains("@interface Foo : Bar"));
        assert!(!out.contains("NS_CLASS_AVAILABLE_IOS"));
    }

    #[test]
    fn line_numbers_preserved() {
        let src = "NS_ASSUME_NONNULL_BEGIN\n@interface F : B\n@end\n";
        let out = blank(src);
        // Same number of newlines, same positions.
        let src_newlines: Vec<usize> = src
            .bytes()
            .enumerate()
            .filter(|(_, b)| *b == b'\n')
            .map(|(i, _)| i)
            .collect();
        let out_newlines: Vec<usize> = out
            .bytes()
            .enumerate()
            .filter(|(_, b)| *b == b'\n')
            .map(|(i, _)| i)
            .collect();
        assert_eq!(src_newlines, out_newlines);
    }
}
