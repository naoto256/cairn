//! Golden cases — hand-curated (language, tool, query, expected hits).
//!
//! The qualified-name and line values are taken from the observed
//! Tier-2 output on the checked-in fixtures (first-time baseline, then
//! frozen). When a backend's qualified-name shape or line numbering
//! changes, expect breakage here first — that's the point.
//!
//! Each `pub fn <lang>_cases()` returns the cases for that language.

use crate::types::{ExpectedHit, GoldenCase, Query, Tool};

fn h(path: &str, line: u32, q: &str) -> ExpectedHit {
    ExpectedHit {
        path: path.to_string(),
        line,
        target_qualified: q.to_string(),
    }
}

pub fn rust_cases() -> Vec<GoldenCase> {
    vec![
        GoldenCase {
            name: "rust_find_symbols_struct",
            language: "rust",
            tool: Tool::FindSymbols,
            query: Query {
                symbol: Some("Logger".into()),
                kind: Some("struct".into()),
                limit: Some(50),
            },
            // tree-sitter-rust emits bare qualified names and pins the
            // struct to the `pub struct Logger {` line (3), not the
            // `use` import line (1).
            tier2_expected: vec![h("src/logger.rs", 3, "Logger")],
            tier3_expected: vec![h("src/logger.rs", 3, "Logger")],
        },
        GoldenCase {
            name: "rust_find_subtypes_trait",
            language: "rust",
            tool: Tool::FindSubtypes,
            query: Query {
                symbol: Some("Greeter".into()),
                kind: None,
                limit: Some(50),
            },
            // `impl Greeter for Logger` block opens at line 13;
            // `impl Greeter for Shouter` at line 5.
            tier2_expected: vec![
                h("src/logger.rs", 13, "Logger"),
                h("src/shouter.rs", 5, "Shouter"),
            ],
            tier3_expected: vec![
                h("src/logger.rs", 13, "Logger"),
                h("src/shouter.rs", 5, "Shouter"),
            ],
        },
        GoldenCase {
            name: "rust_find_supertypes_impl",
            language: "rust",
            tool: Tool::FindSupertypes,
            query: Query {
                symbol: Some("Logger".into()),
                kind: None,
                limit: Some(50),
            },
            // Two impl blocks for Logger: the inherent block at line 7
            // (no supertype edge — surfaces as a self-edge in the
            // index) and the `impl Greeter for Logger` block at 13.
            // Both rows are present in current Tier-2 output; we pin
            // the Greeter edge.
            tier2_expected: vec![h("src/logger.rs", 13, "Greeter")],
            tier3_expected: vec![h("src/logger.rs", 13, "Greeter")],
        },
        GoldenCase {
            name: "rust_find_callers_same_file",
            language: "rust",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("greet".into()),
                kind: None,
                limit: Some(50),
            },
            // Tier-2 resolves the call name only (no receiver type),
            // so `logger.greet("world")` at lib.rs:11 surfaces under
            // bare `greet`. shouter call is on the same line (in the
            // fixture lib.rs both calls share a name).
            tier2_expected: vec![h("src/lib.rs", 11, "greet")],
            tier3_expected: vec![h("src/lib.rs", 11, "greet")],
        },
        GoldenCase {
            name: "rust_find_callers_cross_file",
            language: "rust",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("hello".into()),
                kind: None,
                limit: Some(50),
            },
            // `hello(name)` is called from logger.rs:15 inside the
            // greet impl — cross-file because hello is defined in
            // greeter.rs.
            tier2_expected: vec![h("src/logger.rs", 15, "hello")],
            tier3_expected: vec![h("src/logger.rs", 15, "hello")],
        },
    ]
}

pub fn python_cases() -> Vec<GoldenCase> {
    vec![
        GoldenCase {
            name: "python_find_symbols_class",
            language: "python",
            tool: Tool::FindSymbols,
            query: Query {
                symbol: Some("Dog".into()),
                kind: Some("class".into()),
                limit: Some(50),
            },
            // tree-sitter-python emits bare qualified names — no
            // module prefix.
            tier2_expected: vec![h("dog.py", 4, "Dog")],
            tier3_expected: vec![h("dog.py", 4, "Dog")],
        },
        GoldenCase {
            name: "python_find_subtypes_class",
            language: "python",
            tool: Tool::FindSubtypes,
            query: Query {
                symbol: Some("Animal".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("dog.py", 4, "Dog"), h("cat.py", 4, "Cat")],
            tier3_expected: vec![h("dog.py", 4, "Dog"), h("cat.py", 4, "Cat")],
        },
        GoldenCase {
            name: "python_find_supertypes_class",
            language: "python",
            tool: Tool::FindSupertypes,
            query: Query {
                symbol: Some("Dog".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("dog.py", 4, "Animal")],
            tier3_expected: vec![h("dog.py", 4, "Animal")],
        },
    ]
}

pub fn typescript_cases() -> Vec<GoldenCase> {
    vec![
        GoldenCase {
            name: "typescript_find_symbols_interface",
            language: "typescript",
            tool: Tool::FindSymbols,
            query: Query {
                symbol: Some("Greeter".into()),
                kind: Some("interface".into()),
                limit: Some(50),
            },
            tier2_expected: vec![h("greeter.ts", 1, "Greeter")],
            tier3_expected: vec![h("greeter.ts", 1, "Greeter")],
        },
        GoldenCase {
            name: "typescript_find_subtypes_interface",
            language: "typescript",
            tool: Tool::FindSubtypes,
            query: Query {
                symbol: Some("Greeter".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("hello.ts", 3, "Hello"), h("shout.ts", 3, "Shout")],
            tier3_expected: vec![h("hello.ts", 3, "Hello"), h("shout.ts", 3, "Shout")],
        },
    ]
}

pub fn java_cases() -> Vec<GoldenCase> {
    vec![
        GoldenCase {
            name: "java_find_symbols_interface",
            language: "java",
            tool: Tool::FindSymbols,
            query: Query {
                symbol: Some("Greeter".into()),
                kind: Some("interface".into()),
                limit: Some(50),
            },
            // tree-sitter-java emits bare qualified names — no
            // package prefix.
            tier2_expected: vec![h("com/example/Greeter.java", 3, "Greeter")],
            tier3_expected: vec![h("com/example/Greeter.java", 3, "Greeter")],
        },
        GoldenCase {
            name: "java_find_subtypes_interface",
            language: "java",
            tool: Tool::FindSubtypes,
            query: Query {
                symbol: Some("Greeter".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![
                h("com/example/Hello.java", 3, "Hello"),
                h("com/example/Shout.java", 3, "Shout"),
            ],
            tier3_expected: vec![
                h("com/example/Hello.java", 3, "Hello"),
                h("com/example/Shout.java", 3, "Shout"),
            ],
        },
    ]
}

/// All cases, flattened.
pub fn all_cases() -> Vec<GoldenCase> {
    let mut v = Vec::new();
    v.extend(rust_cases());
    v.extend(python_cases());
    v.extend(typescript_cases());
    v.extend(java_cases());
    v
}
