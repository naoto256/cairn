use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("cairn-core lives under crates/cairn-core");
    let workspace_manifest = workspace_root.join("Cargo.toml");
    let cli_manifest = workspace_root.join("crates/cairn/Cargo.toml");

    println!("cargo:rerun-if-changed={}", workspace_manifest.display());
    println!("cargo:rerun-if-changed={}", cli_manifest.display());

    let workspace_members = language_workspace_members(&workspace_manifest);
    let cli_deps = language_cli_dependencies(&cli_manifest);
    let entries = cli_deps
        .into_iter()
        .filter(|crate_name| workspace_members.iter().any(|member| member == crate_name))
        .filter(|crate_name| crate_name != "cairn-lang-api")
        .flat_map(|crate_name| expected_entries(&crate_name))
        .collect::<Vec<_>>();

    let mut source = String::from(
        "#[derive(Debug, Clone, Copy, PartialEq, Eq)]\n\
         enum ExpectedRegistry {\n\
         \tLanguageBackend,\n\
         \tWorkspaceAnalyzer,\n\
         }\n\n\
         #[derive(Debug, Clone, Copy, PartialEq, Eq)]\n\
         struct ExpectedBackendCrate {\n\
         \tcrate_name: &'static str,\n\
         \tregistry: ExpectedRegistry,\n\
         \truntime_id: &'static str,\n\
         \timport_hint: &'static str,\n\
         }\n\n\
         const EXPECTED_BACKEND_CRATES: &[ExpectedBackendCrate] = &[\n",
    );
    for entry in entries {
        source.push_str(&format!(
            "\tExpectedBackendCrate {{ crate_name: {crate_name:?}, registry: ExpectedRegistry::{registry}, runtime_id: {runtime_id:?}, import_hint: {import_hint:?} }},\n",
            crate_name = entry.crate_name,
            registry = entry.registry,
            runtime_id = entry.runtime_id,
            import_hint = entry.import_hint,
        ));
    }
    source.push_str("];\n");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    fs::write(out_dir.join("expected_backend_crates.rs"), source)
        .expect("write expected_backend_crates.rs");
}

#[derive(Debug)]
struct Entry {
    crate_name: String,
    registry: &'static str,
    runtime_id: String,
    import_hint: String,
}

fn expected_entries(crate_name: &str) -> Vec<Entry> {
    let import_hint = format!("use {} as _;", crate_name.replace('-', "_"));
    if crate_name == "cairn-lang-clangd-tier3" {
        return ["clangd-c-lsp", "clangd-cpp-lsp", "clangd-objc-lsp"]
            .into_iter()
            .map(|runtime_id| Entry {
                crate_name: crate_name.to_string(),
                registry: "WorkspaceAnalyzer",
                runtime_id: runtime_id.to_string(),
                import_hint: import_hint.clone(),
            })
            .collect();
    }
    if crate_name == "cairn-lang-typescript-tier3" {
        return [
            "typescript-language-server-ts-lsp",
            "typescript-language-server-js-lsp",
            "typescript-language-server-tsx-lsp",
        ]
        .into_iter()
        .map(|runtime_id| Entry {
            crate_name: crate_name.to_string(),
            registry: "WorkspaceAnalyzer",
            runtime_id: runtime_id.to_string(),
            import_hint: import_hint.clone(),
        })
        .collect();
    }
    if crate_name == "cairn-lang-rust-tier3"
        || crate_name == "cairn-lang-python-tier3"
        || crate_name == "cairn-lang-php-tier3"
        || crate_name == "cairn-lang-go-tier3"
        || crate_name == "cairn-lang-csharp-tier3"
        || crate_name == "cairn-lang-java-tier3"
        || crate_name == "cairn-lang-kotlin-tier3"
        || crate_name == "cairn-lang-ruby-tier3"
        || crate_name == "cairn-lang-ruby-tier25"
        || crate_name == "cairn-lang-php-tier25"
        || crate_name == "cairn-lang-python-tier25"
        || crate_name == "cairn-lang-kotlin-tier25"
        || crate_name == "cairn-lang-csharp-tier25"
        || crate_name == "cairn-lang-swift-tier3"
    {
        return vec![Entry {
            crate_name: crate_name.to_string(),
            registry: "WorkspaceAnalyzer",
            runtime_id: match crate_name {
                "cairn-lang-rust-tier3" => "rust-analyzer-lsp",
                "cairn-lang-python-tier3" => "pyright-lsp",
                "cairn-lang-php-tier3" => "phpantom-lsp",
                "cairn-lang-go-tier3" => "gopls-lsp",
                "cairn-lang-csharp-tier3" => "csharp-ls",
                "cairn-lang-java-tier3" => "jdtls-lsp",
                "cairn-lang-kotlin-tier3" => "kotlin-language-server",
                "cairn-lang-ruby-tier3" => "ruby-lsp",
                "cairn-lang-ruby-tier25" => "ruby-resolver",
                "cairn-lang-php-tier25" => "php-resolver",
                "cairn-lang-python-tier25" => "python-resolver",
                "cairn-lang-kotlin-tier25" => "kotlin-resolver",
                "cairn-lang-csharp-tier25" => "csharp-resolver",
                "cairn-lang-swift-tier3" => "sourcekit-lsp",
                _ => unreachable!(),
            }
            .to_string(),
            import_hint,
        }];
    }

    vec![Entry {
        crate_name: crate_name.to_string(),
        registry: "LanguageBackend",
        runtime_id: crate_name
            .strip_prefix("cairn-lang-")
            .expect("language crate prefix")
            .to_string(),
        import_hint,
    }]
}

fn language_workspace_members(path: &Path) -> Vec<String> {
    let manifest = fs::read_to_string(path).expect("read workspace Cargo.toml");
    let members = array_values(&manifest, "members");
    members
        .into_iter()
        .filter_map(|member| member.strip_prefix("crates/").map(str::to_string))
        .filter(|name| name.starts_with("cairn-lang-"))
        .collect()
}

fn language_cli_dependencies(path: &Path) -> Vec<String> {
    let manifest = fs::read_to_string(path).expect("read cairn Cargo.toml");
    let mut in_dependencies = false;
    let mut deps = Vec::new();
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_dependencies = trimmed == "[dependencies]";
            continue;
        }
        if !in_dependencies || !trimmed.starts_with("cairn-lang-") {
            continue;
        }
        if let Some((name, _)) = trimmed.split_once('=') {
            let name = name.trim().trim_end_matches(".workspace");
            deps.push(name.to_string());
        }
    }
    deps
}

fn array_values(manifest: &str, key: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut in_array = false;
    for line in manifest.lines() {
        let trimmed = line.trim();
        if !in_array {
            if trimmed == format!("{key} = [") {
                in_array = true;
            }
            continue;
        }
        if trimmed == "]" {
            break;
        }
        let value = trimmed.trim_end_matches(',').trim_matches('"');
        if !value.is_empty() {
            values.push(value.to_string());
        }
    }
    values
}
