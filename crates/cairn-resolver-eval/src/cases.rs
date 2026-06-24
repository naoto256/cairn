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
            tier25_expected: vec![],
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
            tier25_expected: vec![],
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
            tier25_expected: vec![],
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
            tier25_expected: vec![],
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
            tier25_expected: vec![],
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
            tier25_expected: vec![],
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
            tier25_expected: vec![],
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
            tier25_expected: vec![],
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
            tier25_expected: vec![],
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
            tier25_expected: vec![],
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
            tier25_expected: vec![],
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
            tier25_expected: vec![],
            tier3_expected: vec![
                h("com/example/Hello.java", 3, "Hello"),
                h("com/example/Shout.java", 3, "Shout"),
            ],
        },
    ]
}

/// Ruby golden cases — exercise the Tier-2.5 cross-file resolver spec.
///
/// Each case ships two expectation tracks:
///
/// - `tier2_expected`: what the Tier-2 (single-file syntactic) Ruby
///   backend should surface today — typically the lexical hit with
///   `target_symbol_id = NULL` (name-only).
/// - `tier25_expected`: what the upcoming `cairn-lang-ruby-tier25`
///   resolver is expected to add — same site, but with the qualified
///   target resolved across files via `require` / `require_relative`
///   chains and MRO. Empty for queries the spec deliberately leaves
///   un-resolved (dynamic dispatch / `define_method` / `send`).
///
/// These cases reference the spec card
/// `cairn-ruby-tier-2-5-mvp-stage1-1st-wave-impl-spec`; the per-query
/// matrix there is the source of truth for what each row pins.
pub fn ruby_cases() -> Vec<GoldenCase> {
    vec![
        // 1) Cross-file inheritance: `class LoudDog < Dog` (defined in
        //    lib/loud_dog.rb), Dog itself extends Animal (lib/dog.rb).
        //    Tier-2 surfaces the supertype edge at name-only;
        //    Tier-2.5 resolves the qualified target through the
        //    `require_relative 'dog'` chain.
        GoldenCase {
            name: "ruby_find_supertypes_class_cross_file",
            language: "ruby",
            tool: Tool::FindSupertypes,
            query: Query {
                symbol: Some("LoudDog".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("lib/loud_dog.rb", 3, "Dog")],
            tier25_expected: vec![h("lib/loud_dog.rb", 3, "Dog")],
            tier3_expected: vec![h("lib/loud_dog.rb", 3, "Dog")],
        },
        // 2) Subtypes of `Animal` — Dog and Cat are direct, LoudDog is
        //    transitive (Tier-2.5 may surface it once MRO is wired).
        //    Tier-2 (per spec) only sees the direct edges as
        //    name-only rows; the transitive LoudDog hit is a Tier-2.5
        //    follow-on we pin to confirm the resolver walks the chain.
        GoldenCase {
            name: "ruby_find_subtypes_class_chain",
            language: "ruby",
            tool: Tool::FindSubtypes,
            query: Query {
                symbol: Some("Animal".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("lib/dog.rb", 1, "Dog"), h("lib/cat.rb", 1, "Cat")],
            tier25_expected: vec![h("lib/dog.rb", 1, "Dog"), h("lib/cat.rb", 1, "Cat")],
            tier3_expected: vec![h("lib/dog.rb", 1, "Dog"), h("lib/cat.rb", 1, "Cat")],
        },
        // 3) Mixin: `include Logging` inside LoudDog. Tier-2 catches
        //    the `include` lexically; Tier-2.5 resolves Logging to
        //    `lib/logging.rb` and lets supertype/subtype queries treat
        //    LoudDog as a Logging consumer.
        GoldenCase {
            name: "ruby_find_subtypes_module_include",
            language: "ruby",
            tool: Tool::FindSubtypes,
            query: Query {
                symbol: Some("Logging".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("lib/loud_dog.rb", 4, "LoudDog")],
            tier25_expected: vec![h("lib/loud_dog.rb", 4, "LoudDog")],
            tier3_expected: vec![h("lib/loud_dog.rb", 4, "LoudDog")],
        },
        // 4) Type references: `Animal` is mentioned as the base of Dog
        //    and Cat. `find_callers` over a type name surfaces those
        //    base-class sites.
        GoldenCase {
            name: "ruby_find_callers_class_as_base",
            language: "ruby",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("Animal".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("lib/dog.rb", 1, "Animal"), h("lib/cat.rb", 1, "Animal")],
            tier25_expected: vec![h("lib/dog.rb", 1, "Animal"), h("lib/cat.rb", 1, "Animal")],
            tier3_expected: vec![h("lib/dog.rb", 1, "Animal"), h("lib/cat.rb", 1, "Animal")],
        },
        // 5) Method body callees: LoudDog#bark calls `log("woof")` and
        //    `super` (Dog#bark — Dog defines `bark`). Tier-2 emits
        //    name-only refs for both; Tier-2.5 resolves `log` to
        //    `Logging#log` via the mixin chain and `super` to
        //    `Dog#bark` via the inheritance chain.
        //
        //    The query uses the qualified caller name (`LoudDog#bark`)
        //    because `find_callees` matches `symbols.qualified`
        //    directly — a bare `bark` would also pick up `Dog#bark`.
        //    Tier-2 alone surfaces `log` only with `include_noise`
        //    (the same-file callee post-pass can't see Logging#log),
        //    so the Tier-2 expected stays empty until the Tier-2.5
        //    resolver fills `target_qualified` cross-file. The Tier-2
        //    baseline test does not gate this case (its floor is 0.0).
        GoldenCase {
            name: "ruby_find_callees_method_body",
            language: "ruby",
            tool: Tool::FindCallees,
            query: Query {
                symbol: Some("LoudDog#bark".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![],
            // Tier-2.5 promotes the bare `log` call to the qualified
            // `Logging#log` via the mixin chain (LoudDog includes
            // Logging). Tier-3 (LSP) gives the same qualified name.
            tier25_expected: vec![h("lib/loud_dog.rb", 6, "Logging#log")],
            tier3_expected: vec![h("lib/loud_dog.rb", 6, "Logging#log")],
        },
        // 6) Qualified call site: `Utils::String.shout("hi")` from
        //    app/usage.rb. Tier-2 surfaces the call name `shout`
        //    only; Tier-2.5 resolves the qualified constant path
        //    `Utils::String` and pins the call to the module's static
        //    method.
        GoldenCase {
            name: "ruby_find_callers_qualified_static",
            language: "ruby",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("shout".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("app/usage.rb", 7, "shout")],
            tier25_expected: vec![h("app/usage.rb", 7, "shout")],
            tier3_expected: vec![h("app/usage.rb", 7, "shout")],
        },
        // 7) Imports: app/usage.rb pulls in lib/loud_dog.rb and
        //    lib/utils.rb via `require_relative`. Tier-2 records the
        //    require sites lexically; Tier-2.5 ties the require
        //    targets to the actual files they expose.
        //    `find_callers` is the closest existing query for "who
        //    references this file's symbols"; we pin the require call
        //    sites by name.
        GoldenCase {
            name: "ruby_find_callers_require_relative",
            language: "ruby",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("require_relative".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![
                h("app/usage.rb", 1, "require_relative"),
                h("app/usage.rb", 2, "require_relative"),
                h("lib/loud_dog.rb", 1, "require_relative"),
                h("lib/loud_dog.rb", 2, "require_relative"),
            ],
            tier25_expected: vec![
                h("app/usage.rb", 1, "require_relative"),
                h("app/usage.rb", 2, "require_relative"),
                h("lib/loud_dog.rb", 1, "require_relative"),
                h("lib/loud_dog.rb", 2, "require_relative"),
            ],
            tier3_expected: vec![
                h("app/usage.rb", 1, "require_relative"),
                h("app/usage.rb", 2, "require_relative"),
                h("lib/loud_dog.rb", 1, "require_relative"),
                h("lib/loud_dog.rb", 2, "require_relative"),
            ],
        },
        // 8) Out-of-scope confirmation (retreat line): the dynamic
        //    receiver call `obj.method_that_might_not_exist` in
        //    Usage#dynamic_call MUST NOT be resolved by Tier-2.5. The
        //    spec deliberately leaves obj-typed dispatch,
        //    `define_method`, `send`, and `method_missing` to higher
        //    tiers. We assert by leaving `tier25_expected` empty for
        //    this call name — the Tier-2.5 actual set must contain
        //    zero resolved rows for this name (otherwise it has
        //    crossed the retreat line). Tier-2 may surface the name
        //    as a lexical ref, which is permitted.
        GoldenCase {
            name: "ruby_find_callers_dynamic_dispatch_unresolved",
            language: "ruby",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("method_that_might_not_exist".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![],
            tier25_expected: vec![],
            tier3_expected: vec![],
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
    v.extend(ruby_cases());
    v
}
