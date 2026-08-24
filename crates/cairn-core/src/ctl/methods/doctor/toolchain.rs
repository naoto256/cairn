use std::path::PathBuf;

use cairn_proto::control::{DoctorCheck, DoctorStatus};

use crate::lsp_discovery::{
    discover_lsp_binary, discover_lsp_binary_candidates, discover_sourcekit_lsp,
};

use super::{EXPECTED_BACKEND_CRATES, ExpectedRegistry, doctor_check};

pub(super) fn tier3_binary_checks() -> Vec<DoctorCheck> {
    vec![
        rust_analyzer_binary_check(),
        pyright_binary_check(),
        gopls_binary_check(),
        clangd_binary_check(),
        typescript_language_server_binary_check(),
        csharp_ls_binary_check(),
        csharp_dotnet_sdk_check(),
        phpantom_lsp_binary_check(),
        jdtls_binary_check(),
        kotlin_language_server_binary_check(),
        ruby_lsp_binary_check(),
        sourcekit_lsp_binary_check(),
    ]
}

fn rust_analyzer_binary_check() -> DoctorCheck {
    binary_check(
        "rust-analyzer binary discoverable",
        resolve_rust_analyzer(),
        "rust-analyzer not on PATH",
        "Install rust-analyzer (`rustup component add rust-analyzer`) and ensure it's on the daemon's PATH; Tier-3 (LSP) facts will not be available until then.",
    )
}

fn pyright_binary_check() -> DoctorCheck {
    binary_check(
        "pyright binary discoverable",
        resolve_pyright(),
        "pyright-langserver not on PATH",
        "Install pyright (`pip install pyright` or `npm i -g pyright`) and ensure pyright-langserver is on the daemon's PATH; Python Tier-3 (LSP) facts will not be available until then.",
    )
}

fn gopls_binary_check() -> DoctorCheck {
    binary_check(
        "gopls binary discoverable",
        resolve_gopls(),
        "gopls not on PATH",
        "Install gopls (`go install golang.org/x/tools/gopls@latest`) and ensure it's on the daemon's PATH; Go Tier-3 (LSP) facts will not be available until then.",
    )
}

fn clangd_binary_check() -> DoctorCheck {
    binary_check(
        "clangd binary discoverable",
        resolve_clangd(),
        "clangd not on PATH",
        "Install clangd (for example through LLVM / Xcode command line tools) and ensure it's on the daemon's PATH; C, C++, and Objective-C Tier-3 (LSP) facts will not be available until then.",
    )
}

fn typescript_language_server_binary_check() -> DoctorCheck {
    binary_check(
        "typescript-language-server binary discoverable",
        resolve_typescript_language_server(),
        "typescript-language-server not on PATH",
        "Install typescript-language-server (`npm i -g typescript typescript-language-server`) and ensure it's on the daemon's PATH; TypeScript, JavaScript, and TSX Tier-3 (LSP) facts will not be available until then.",
    )
}

fn csharp_ls_binary_check() -> DoctorCheck {
    binary_check(
        "csharp-ls binary discoverable",
        resolve_csharp_ls(),
        "csharp-ls not discoverable via CSHARP_LS or PATH",
        "Install csharp-ls (`dotnet tool install -g csharp-ls`) and ensure the .NET tools directory is on the daemon's PATH, or set CSHARP_LS; C# Tier-3 (LSP) facts will not be available until then.",
    )
}

fn csharp_dotnet_sdk_check() -> DoctorCheck {
    match dotnet_sdk_root(
        std::env::var_os("DOTNET_ROOT").map(PathBuf::from),
        standard_dotnet_roots(),
    ) {
        Some(root) => doctor_check(
            ".NET SDK root discoverable for csharp-ls",
            DoctorStatus::Pass,
            Some(root.display().to_string()),
            None,
        ),
        None => doctor_check(
            ".NET SDK root discoverable for csharp-ls",
            DoctorStatus::Warn,
            Some("DOTNET_ROOT unset and no SDK found in standard dotnet roots".into()),
            Some("Install the .NET SDK or set DOTNET_ROOT so csharp-ls can locate MSBuild under daemon launch environments.".into()),
        ),
    }
}

fn dotnet_sdk_root(
    dotnet_root: Option<PathBuf>,
    roots: impl IntoIterator<Item = PathBuf>,
) -> Option<PathBuf> {
    if let Some(root) = dotnet_root {
        if root.join("sdk").is_dir() {
            return Some(root);
        }
    }
    roots.into_iter().find(|root| root.join("sdk").is_dir())
}

fn standard_dotnet_roots() -> Vec<PathBuf> {
    let mut roots = vec![
        PathBuf::from("/usr/local/share/dotnet"),
        PathBuf::from("/opt/homebrew/share/dotnet"),
        PathBuf::from("/opt/homebrew/opt/dotnet/libexec"),
    ];
    if let Some(home) = dirs::home_dir() {
        roots.push(home.join(".dotnet"));
    }
    roots
}

fn phpantom_lsp_binary_check() -> DoctorCheck {
    binary_check(
        "phpantom-lsp binary discoverable",
        resolve_phpantom_lsp(),
        "PHPantom LSP not discoverable via PHPANTOM_LSP or PATH",
        "Install PHPantom LSP (`brew install phpantom-lsp` or `cargo install phpantom_lsp --locked`) and ensure `phpantom_lsp` or `phpantom-lsp` is on the daemon's PATH, or set PHPANTOM_LSP; PHP Tier-3 (LSP) facts will not be available until then.",
    )
}

fn jdtls_binary_check() -> DoctorCheck {
    binary_check(
        "jdtls binary discoverable",
        resolve_jdtls(),
        "jdtls not on PATH",
        "Install an Eclipse JDT Language Server wrapper script named `jdtls`, or set JDTLS to that wrapper; Java Tier-3 (LSP) facts will not be available until then.",
    )
}

fn kotlin_language_server_binary_check() -> DoctorCheck {
    binary_check(
        "kotlin-language-server binary discoverable",
        resolve_kotlin_language_server(),
        "kotlin-language-server not discoverable via KOTLIN_LANGUAGE_SERVER or PATH",
        "Install kotlin-language-server (`brew install kotlin-language-server`, or download a release zip from https://github.com/fwcd/kotlin-language-server/releases) and ensure its wrapper script is on the daemon's PATH, or set KOTLIN_LANGUAGE_SERVER. JVM 11+ is required; Kotlin Tier-3 (LSP) facts will not be available until then.",
    )
}

fn ruby_lsp_binary_check() -> DoctorCheck {
    binary_check(
        "ruby-lsp binary discoverable",
        resolve_ruby_lsp(),
        "ruby-lsp not on PATH",
        "Install ruby-lsp (`gem install ruby-lsp`) and ensure it's on the daemon's PATH, or set RUBY_LSP; Ruby Tier-3 (LSP) facts will not be available until then.",
    )
}

fn sourcekit_lsp_binary_check() -> DoctorCheck {
    binary_check(
        "sourcekit-lsp binary discoverable",
        resolve_sourcekit_lsp(),
        "sourcekit-lsp not discoverable via SOURCEKIT_LSP, xcrun, or PATH",
        "Install Xcode command line tools (`xcode-select --install`) or a Swift toolchain that provides sourcekit-lsp, then ensure `xcrun --find sourcekit-lsp` or PATH can find it; Swift Tier-3 (LSP) facts will not be available until then.",
    )
}

/// Shared shape for Tier-3 binary probes. Resolved → `Pass` with
/// the resolved path as detail; not resolved → `Warn` (never
/// `Fail`). Missing Tier-3 support is a partial-capability state,
/// not a broken daemon: the daemon still serves Tier-1 / Tier-2
/// facts for the affected language, so promoting this to `Fail`
/// would be misleading.
fn binary_check(
    name: &str,
    resolved: Option<PathBuf>,
    missing_detail: &str,
    remediation: &str,
) -> DoctorCheck {
    match resolved {
        Some(path) => doctor_check(
            name,
            DoctorStatus::Pass,
            Some(path.to_string_lossy().to_string()),
            None,
        ),
        None => doctor_check(
            name,
            DoctorStatus::Warn,
            Some(missing_detail.into()),
            Some(remediation.into()),
        ),
    }
}

fn resolve_rust_analyzer() -> Option<PathBuf> {
    discover_lsp_binary("rust-analyzer", Some("RUST_ANALYZER"))
}

fn resolve_pyright() -> Option<PathBuf> {
    discover_lsp_binary("pyright-langserver", Some("PYRIGHT"))
}

fn resolve_gopls() -> Option<PathBuf> {
    discover_lsp_binary("gopls", Some("GOPLS"))
}

fn resolve_clangd() -> Option<PathBuf> {
    discover_lsp_binary("clangd", Some("CLANGD"))
}

fn resolve_typescript_language_server() -> Option<PathBuf> {
    discover_lsp_binary(
        "typescript-language-server",
        Some("TYPESCRIPT_LANGUAGE_SERVER"),
    )
}

fn resolve_csharp_ls() -> Option<PathBuf> {
    discover_lsp_binary("csharp-ls", Some("CSHARP_LS"))
}

fn resolve_phpantom_lsp() -> Option<PathBuf> {
    discover_lsp_binary_candidates(&["phpantom_lsp", "phpantom-lsp"], Some("PHPANTOM_LSP"))
}

fn resolve_jdtls() -> Option<PathBuf> {
    discover_lsp_binary("jdtls", Some("JDTLS"))
}

fn resolve_kotlin_language_server() -> Option<PathBuf> {
    discover_lsp_binary("kotlin-language-server", Some("KOTLIN_LANGUAGE_SERVER"))
}

fn resolve_ruby_lsp() -> Option<PathBuf> {
    discover_lsp_binary("ruby-lsp", Some("RUBY_LSP"))
}

fn resolve_sourcekit_lsp() -> Option<PathBuf> {
    discover_sourcekit_lsp()
}
/// Cross-references the build-time `EXPECTED_BACKEND_CRATES`
/// manifest (generated by build.rs from workspace Cargo metadata)
/// against the runtime-linked language backends and workspace
/// analyzers. Any expected crate whose runtime id is absent from
/// its target registry surfaces as `Warn`, not `Fail`: dev builds
/// legitimately omit backends (feature flags, custom `main.rs`),
/// and the remediation names the exact import symbol that is most
/// likely missing from `crates/cairn/src/main.rs`.
pub(super) fn backend_registration_coherence_check(
    language_backend_names: &[&str],
    workspace_analyzer_ids: &[&str],
) -> DoctorCheck {
    let missing = EXPECTED_BACKEND_CRATES
        .iter()
        .filter(|expected| match expected.registry {
            ExpectedRegistry::LanguageBackend => {
                !language_backend_names.contains(&expected.runtime_id)
            }
            ExpectedRegistry::WorkspaceAnalyzer => {
                !workspace_analyzer_ids.contains(&expected.runtime_id)
            }
        })
        .collect::<Vec<_>>();

    if missing.is_empty() {
        return doctor_check(
            "backend registration coherence",
            DoctorStatus::Pass,
            Some(format!(
                "{} runtime backend crate(s) registered",
                EXPECTED_BACKEND_CRATES.len()
            )),
            None,
        );
    }

    doctor_check(
        "backend registration coherence",
        DoctorStatus::Warn,
        Some(
            missing
                .into_iter()
                .map(|expected| {
                    format!(
                        "{} is declared for runtime linking but `{}` is missing from {} - likely missing `{}` in crates/cairn/src/main.rs",
                        expected.crate_name,
                        expected.runtime_id,
                        expected.registry.label(),
                        expected.import_hint
                    )
                })
                .collect::<Vec<_>>()
                .join("; "),
        ),
        None,
    )
}

impl ExpectedRegistry {
    fn label(self) -> &'static str {
        match self {
            Self::LanguageBackend => "LANGUAGE_BACKENDS",
            Self::WorkspaceAnalyzer => "WORKSPACE_ANALYZERS",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_registration_coherence_passes_when_expected_entries_are_registered() {
        let language_backends = [
            "rust",
            "python",
            "markdown",
            "ruby",
            "typescript",
            "go",
            "csharp",
            "php",
            "kotlin",
            "swift",
            "objc",
            "c",
            "cpp",
            "java",
        ];
        let workspace_analyzers = [
            "clangd-c-lsp",
            "clangd-cpp-lsp",
            "clangd-objc-lsp",
            "csharp-ls",
            "csharp-resolver",
            "gopls-lsp",
            "javascript-resolver",
            "jdtls-lsp",
            "kotlin-language-server",
            "kotlin-resolver",
            "php-resolver",
            "phpantom-lsp",
            "pyright-lsp",
            "python-resolver",
            "ruby-lsp",
            "ruby-resolver",
            "rust-analyzer-lsp",
            "sourcekit-lsp",
            "swift-resolver",
            "typescript-language-server-js-lsp",
            "typescript-language-server-ts-lsp",
            "typescript-language-server-tsx-lsp",
        ];

        let check = backend_registration_coherence_check(&language_backends, &workspace_analyzers);

        assert_eq!(check.status, DoctorStatus::Pass);
    }

    #[test]
    fn dotnet_sdk_root_respects_existing_dotnet_root() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("dotnet");
        std::fs::create_dir_all(root.join("sdk")).unwrap();

        assert_eq!(
            dotnet_sdk_root(Some(root.clone()), std::iter::empty()),
            Some(root)
        );
    }

    #[test]
    fn dotnet_sdk_root_falls_back_when_dotnet_root_has_no_sdk() {
        let tmp = tempfile::tempdir().unwrap();
        let invalid = tmp.path().join("invalid");
        let standard = tmp.path().join("standard");
        std::fs::create_dir_all(&invalid).unwrap();
        std::fs::create_dir_all(standard.join("sdk")).unwrap();

        assert_eq!(
            dotnet_sdk_root(Some(invalid), [standard.clone()]),
            Some(standard)
        );
    }

    #[test]
    fn dotnet_sdk_root_finds_first_standard_root_with_sdk() {
        let tmp = tempfile::tempdir().unwrap();
        let without_sdk = tmp.path().join("without-sdk");
        let with_sdk = tmp.path().join("with-sdk");
        std::fs::create_dir_all(&without_sdk).unwrap();
        std::fs::create_dir_all(with_sdk.join("sdk")).unwrap();

        assert_eq!(
            dotnet_sdk_root(None, [without_sdk, with_sdk.clone()]),
            Some(with_sdk)
        );
    }

    #[test]
    fn dotnet_sdk_root_is_none_without_env_or_standard_sdk() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("dotnet");
        std::fs::create_dir_all(&root).unwrap();

        assert_eq!(dotnet_sdk_root(None, [root]), None);
    }

    #[test]
    fn backend_registration_coherence_warns_for_missing_runtime_entry() {
        let language_backends = [
            "rust", "python", "markdown", "ruby", "go", "csharp", "php", "kotlin", "swift", "objc",
            "c", "cpp", "java",
        ];
        let workspace_analyzers = [
            "clangd-c-lsp",
            "clangd-cpp-lsp",
            "clangd-objc-lsp",
            "gopls-lsp",
            "jdtls-lsp",
            "pyright-lsp",
            "ruby-lsp",
            "rust-analyzer-lsp",
            "sourcekit-lsp",
            "typescript-language-server-js-lsp",
            "typescript-language-server-ts-lsp",
            "typescript-language-server-tsx-lsp",
        ];

        let check = backend_registration_coherence_check(&language_backends, &workspace_analyzers);

        assert_eq!(check.status, DoctorStatus::Warn);
        let detail = check.detail.expect("warning detail");
        assert!(detail.contains("cairn-lang-typescript"));
        assert!(detail.contains("LANGUAGE_BACKENDS"));
        assert!(detail.contains("use cairn_lang_typescript as _;"));
    }
}
