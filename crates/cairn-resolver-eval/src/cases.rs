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
            tier2_expected: vec![h("dog.py", 5, "Dog")],
            tier25_expected: vec![],
            tier3_expected: vec![h("dog.py", 5, "Dog")],
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
            tier2_expected: vec![h("dog.py", 5, "Dog"), h("cat.py", 4, "Cat")],
            tier25_expected: vec![h("dog.py", 5, "Dog"), h("cat.py", 4, "Cat")],
            tier3_expected: vec![h("dog.py", 5, "Dog"), h("cat.py", 4, "Cat")],
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
            tier2_expected: vec![h("dog.py", 5, "Animal")],
            tier25_expected: vec![h("dog.py", 5, "Animal")],
            tier3_expected: vec![h("dog.py", 5, "Animal")],
        },
        GoldenCase {
            name: "python_find_callers_super_method",
            language: "python",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("speak".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("dog.py", 10, "speak"), h("dog.py", 11, "speak")],
            tier25_expected: vec![h("dog.py", 10, "speak"), h("dog.py", 11, "speak")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "python_find_callers_class_receiver",
            language: "python",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("build".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("dog.py", 12, "build")],
            tier25_expected: vec![h("dog.py", 12, "build")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "python_find_callers_import_alias",
            language: "python",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("aliased_helper".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("dog.py", 13, "aliased_helper")],
            tier25_expected: vec![h("dog.py", 13, "aliased_helper")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "python_find_imports_cross_file",
            language: "python",
            tool: Tool::FindImports,
            query: Query {
                symbol: Some("dog.py".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("dog.py", 1, "animal.py"), h("dog.py", 2, "util.py")],
            tier25_expected: vec![h("dog.py", 1, "animal.py"), h("dog.py", 2, "util.py")],
            tier3_expected: vec![],
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

pub fn php_cases() -> Vec<GoldenCase> {
    vec![
        GoldenCase {
            name: "php_find_supertypes_service",
            language: "php",
            tool: Tool::FindSupertypes,
            query: Query {
                symbol: Some("App\\Service".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![
                h("src/Service.php", 7, "IntermediateService"),
                h("src/Service.php", 7, "Greeter"),
                h("src/Service.php", 8, "Logging"),
            ],
            tier25_expected: vec![
                h("src/Service.php", 7, "IntermediateService"),
                h("src/Service.php", 7, "Greeter"),
                h("src/Service.php", 8, "Logging"),
            ],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "php_find_subtypes_interface",
            language: "php",
            tool: Tool::FindSubtypes,
            query: Query {
                symbol: Some("Greeter".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("src/Service.php", 7, "App\\Service")],
            tier25_expected: vec![h("src/Service.php", 7, "App\\Service")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "php_find_callers_static_alias",
            language: "php",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("build".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![
                h("src/Service.php", 13, "App\\Service::build"),
                h("src/main.php", 6, "App\\Service::build"),
            ],
            tier25_expected: vec![
                h("src/Service.php", 13, "App\\Service::build"),
                h("src/main.php", 6, "App\\Service::build"),
            ],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "php_find_callers_parent",
            language: "php",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("step".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("src/Service.php", 14, "App\\BaseService::step")],
            tier25_expected: vec![h("src/Service.php", 14, "App\\BaseService::step")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "php_find_imports_alias",
            language: "php",
            tool: Tool::FindImports,
            query: Query {
                symbol: Some("src/main.php".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("src/main.php", 4, "src/Service.php")],
            tier25_expected: vec![h("src/main.php", 4, "src/Service.php")],
            tier3_expected: vec![],
        },
    ]
}

pub fn kotlin_cases() -> Vec<GoldenCase> {
    vec![
        GoldenCase {
            name: "kotlin_find_supertypes_service",
            language: "kotlin",
            tool: Tool::FindSupertypes,
            query: Query {
                symbol: Some("Service".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![
                h("src/app/Service.kt", 7, "IntermediateService"),
                h("src/app/Service.kt", 7, "Greeter"),
            ],
            tier25_expected: vec![
                h("src/app/Service.kt", 7, "IntermediateService"),
                h("src/app/Service.kt", 7, "Greeter"),
            ],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "kotlin_find_subtypes_interface",
            language: "kotlin",
            tool: Tool::FindSubtypes,
            query: Query {
                symbol: Some("Greeter".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("src/app/Service.kt", 7, "Service")],
            tier25_expected: vec![h("src/app/Service.kt", 7, "Service")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "kotlin_find_callers_dispatch",
            language: "kotlin",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("build".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("src/app/Service.kt", 15, "app.Service.Companion.build")],
            tier25_expected: vec![h("src/app/Service.kt", 15, "app.Service.Companion.build")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "kotlin_find_callers_super",
            language: "kotlin",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("step".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("src/app/Service.kt", 14, "base.BaseService.step")],
            tier25_expected: vec![h("src/app/Service.kt", 14, "base.BaseService.step")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "kotlin_find_callers_import_alias",
            language: "kotlin",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("aliasedHelper".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("src/app/Service.kt", 16, "util.helper")],
            tier25_expected: vec![h("src/app/Service.kt", 16, "util.helper")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "kotlin_find_imports_cross_file",
            language: "kotlin",
            tool: Tool::FindImports,
            query: Query {
                symbol: Some("src/app/Service.kt".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![
                h("src/app/Service.kt", 3, "src/api/Greeter.kt"),
                h("src/app/Service.kt", 4, "src/base/IntermediateService.kt"),
                h("src/app/Service.kt", 5, "src/util/Helpers.kt"),
            ],
            tier25_expected: vec![
                h("src/app/Service.kt", 3, "src/api/Greeter.kt"),
                h("src/app/Service.kt", 4, "src/base/IntermediateService.kt"),
                h("src/app/Service.kt", 5, "src/util/Helpers.kt"),
            ],
            tier3_expected: vec![],
        },
    ]
}

pub fn swift_cases() -> Vec<GoldenCase> {
    vec![
        GoldenCase {
            name: "swift_find_supertypes_service",
            language: "swift",
            tool: Tool::FindSupertypes,
            query: Query {
                symbol: Some("Service".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![
                h("Service.swift", 1, "IntermediateService"),
                h("Service.swift", 1, "Greeter"),
            ],
            tier25_expected: vec![
                h("Service.swift", 1, "IntermediateService"),
                h("Service.swift", 1, "Greeter"),
            ],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "swift_find_subtypes_protocol",
            language: "swift",
            tool: Tool::FindSubtypes,
            query: Query {
                symbol: Some("Greeter".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("Service.swift", 1, "Service")],
            tier25_expected: vec![h("Service.swift", 1, "Service")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "swift_find_callers_super",
            language: "swift",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("step".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("Service.swift", 6, "BaseService.step")],
            tier25_expected: vec![h("Service.swift", 6, "BaseService.step")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "swift_find_callers_static",
            language: "swift",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("build".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("Service.swift", 11, "Service.build")],
            tier25_expected: vec![h("Service.swift", 11, "Service.build")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "swift_find_callers_top_level",
            language: "swift",
            tool: Tool::FindCallees,
            query: Query {
                symbol: Some("runHelper".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("Main.swift", 4, "helper")],
            tier25_expected: vec![h("Main.swift", 4, "helper")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "swift_find_imports_framework",
            language: "swift",
            tool: Tool::FindImports,
            query: Query {
                symbol: Some("Main.swift".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("Main.swift", 1, "Foundation")],
            tier25_expected: vec![h("Main.swift", 1, "Foundation")],
            tier3_expected: vec![],
        },
    ]
}

pub fn csharp_cases() -> Vec<GoldenCase> {
    vec![
        GoldenCase {
            name: "csharp_find_supertypes_service",
            language: "csharp",
            tool: Tool::FindSupertypes,
            query: Query {
                symbol: Some("App.Service".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![
                h("src/Service.cs", 7, "IntermediateService"),
                h("src/Service.cs", 7, "IGreeter"),
            ],
            tier25_expected: vec![
                h("src/Service.cs", 7, "IntermediateService"),
                h("src/Service.cs", 7, "IGreeter"),
            ],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "csharp_find_subtypes_interface",
            language: "csharp",
            tool: Tool::FindSubtypes,
            query: Query {
                symbol: Some("IGreeter".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("src/Service.cs", 7, "App.Service")],
            tier25_expected: vec![h("src/Service.cs", 7, "App.Service")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "csharp_find_callers_static_alias",
            language: "csharp",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("Build".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![
                h("src/Service.cs", 14, "App.Service.Build"),
                h("src/Main.cs", 7, "App.Service.Build"),
            ],
            tier25_expected: vec![
                h("src/Service.cs", 14, "App.Service.Build"),
                h("src/Main.cs", 7, "App.Service.Build"),
            ],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "csharp_find_callers_base",
            language: "csharp",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("Step".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("src/Service.cs", 13, "Lib.BaseService.Step")],
            tier25_expected: vec![h("src/Service.cs", 13, "Lib.BaseService.Step")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "csharp_find_callers_using_static",
            language: "csharp",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("RunHelper".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("src/Service.cs", 15, "Lib.Helpers.RunHelper")],
            tier25_expected: vec![h("src/Service.cs", 15, "Lib.Helpers.RunHelper")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "csharp_find_imports_alias",
            language: "csharp",
            tool: Tool::FindImports,
            query: Query {
                symbol: Some("src/Main.cs".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("src/Main.cs", 1, "src/Service.cs")],
            tier25_expected: vec![h("src/Main.cs", 1, "src/Service.cs")],
            tier3_expected: vec![],
        },
    ]
}

pub fn javascript_cases() -> Vec<GoldenCase> {
    vec![
        GoldenCase {
            name: "javascript_find_supertypes_service",
            language: "javascript",
            tool: Tool::FindSupertypes,
            query: Query {
                symbol: Some("Service".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("src/service.js", 3, "IntermediateService")],
            tier25_expected: vec![h("src/service.js", 3, "IntermediateService")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "javascript_find_subtypes_base",
            language: "javascript",
            tool: Tool::FindSubtypes,
            query: Query {
                symbol: Some("BaseService".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("src/intermediate.js", 3, "IntermediateService")],
            tier25_expected: vec![h("src/intermediate.js", 3, "IntermediateService")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "javascript_find_callers_super_and_self",
            language: "javascript",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("step".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![
                h("src/service.js", 7, "BaseService.step"),
                h("src/service.js", 12, "Service.step"),
            ],
            tier25_expected: vec![
                h("src/service.js", 7, "BaseService.step"),
                h("src/service.js", 12, "Service.step"),
            ],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "javascript_find_callers_static_alias",
            language: "javascript",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("build".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![
                h("src/service.js", 11, "Service.build"),
                h("src/main.js", 4, "Service.build"),
            ],
            tier25_expected: vec![
                h("src/service.js", 11, "Service.build"),
                h("src/main.js", 4, "Service.build"),
            ],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "javascript_find_callers_import_alias",
            language: "javascript",
            tool: Tool::FindCallers,
            query: Query {
                symbol: Some("runHelper".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![h("src/main.js", 5, "helper")],
            tier25_expected: vec![h("src/main.js", 5, "helper")],
            tier3_expected: vec![],
        },
        GoldenCase {
            name: "javascript_find_imports_cross_file",
            language: "javascript",
            tool: Tool::FindImports,
            query: Query {
                symbol: Some("src/main.js".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![
                h("src/main.js", 1, "src/service.js"),
                h("src/main.js", 2, "src/helpers.js"),
            ],
            tier25_expected: vec![
                h("src/main.js", 1, "src/service.js"),
                h("src/main.js", 2, "src/helpers.js"),
            ],
            tier3_expected: vec![],
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
/// - `tier25_expected`: what `cairn-lang-ruby-tier25` must resolve —
///   the same site, but with the qualified
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
            tier2_expected: vec![
                h("lib/loud_dog.rb", 3, "Dog"),
                h("lib/loud_dog.rb", 4, "Logging"),
            ],
            tier25_expected: vec![
                h("lib/loud_dog.rb", 3, "Dog"),
                h("lib/loud_dog.rb", 4, "Logging"),
            ],
            tier3_expected: vec![
                h("lib/loud_dog.rb", 3, "Dog"),
                h("lib/loud_dog.rb", 4, "Logging"),
            ],
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
        // 6) Qualified call site: `Utils.shout("hi")` from
        //    app/usage.rb. Tier-2 surfaces the call name `shout`
        //    only; Tier-2.5 resolves the qualified constant path
        //    `Utils` and pins the call to the module's static
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
            tier2_expected: vec![h("app/usage.rb", 7, "Utils.shout")],
            tier25_expected: vec![h("app/usage.rb", 7, "Utils.shout")],
            tier3_expected: vec![h("app/usage.rb", 7, "Utils.shout")],
        },
        // 7) Imports: app/usage.rb pulls in lib/loud_dog.rb and
        //    lib/utils.rb via `require_relative`. Tier-2 records the
        //    require sites lexically; Tier-2.5 ties the require
        //    targets to the actual files they expose.
        //    `find_imports` exposes the import-site target path and
        //    provenance directly, so the Tier-2.5 track cannot pass on
        //    the lexical `require_relative` call alone.
        GoldenCase {
            name: "ruby_find_imports_require_relative",
            language: "ruby",
            tool: Tool::FindImports,
            query: Query {
                symbol: Some("app/usage.rb".into()),
                kind: None,
                limit: Some(50),
            },
            tier2_expected: vec![
                h("app/usage.rb", 1, "lib/loud_dog.rb"),
                h("app/usage.rb", 2, "lib/utils.rb"),
            ],
            tier25_expected: vec![
                h("app/usage.rb", 1, "lib/loud_dog.rb"),
                h("app/usage.rb", 2, "lib/utils.rb"),
            ],
            tier3_expected: vec![
                h("app/usage.rb", 1, "lib/loud_dog.rb"),
                h("app/usage.rb", 2, "lib/utils.rb"),
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
    v.extend(php_cases());
    v.extend(kotlin_cases());
    v.extend(swift_cases());
    v.extend(csharp_cases());
    v.extend(javascript_cases());
    v.extend(ruby_cases());
    v
}
