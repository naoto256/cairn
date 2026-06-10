//! `doctor` — environment / dependency / registry sanity checks.

use std::path::{Path, PathBuf};

use cairn_proto::control::{DoctorCheck, DoctorReport, DoctorStatus};
use linkme::distributed_slice;
use rusqlite::{OptionalExtension, params};
use serde_json::Value;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx};
use crate::Result;
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::paths::CasDataDir;
use crate::workspace_analyzer::all_workspace_analyzers;

include!(concat!(env!("OUT_DIR"), "/expected_backend_crates.rs"));

struct Doctor;

#[async_trait::async_trait]
impl ControlMethod for Doctor {
    fn name(&self) -> &'static str {
        "doctor"
    }

    async fn dispatch(&self, ctx: &CtlCtx, _params: Value) -> Result<Value> {
        let mut checks: Vec<DoctorCheck> = Vec::new();

        let backend_names: Vec<&'static str> = cairn_lang_api::all_backends()
            .iter()
            .map(|b| b.name())
            .collect();
        checks.push(doctor_check(
            "language backends linked",
            if backend_names.is_empty() {
                DoctorStatus::Fail
            } else {
                DoctorStatus::Pass
            },
            Some(if backend_names.is_empty() {
                "none linked".into()
            } else {
                format!(
                    "{} backend(s): {}",
                    backend_names.len(),
                    backend_names.join(", ")
                )
            }),
            None,
        ));
        checks.push(backend_registration_coherence_check(
            &backend_names,
            &workspace_analyzer_ids(),
        ));

        let cas_root = ctx.cas_data_dir.root().to_path_buf();
        let writable = std::fs::metadata(&cas_root)
            .map(|m| !m.permissions().readonly())
            .unwrap_or(false);
        checks.push(doctor_check(
            "data directory",
            if writable {
                DoctorStatus::Pass
            } else {
                DoctorStatus::Fail
            },
            Some(if writable {
                cas_root.to_string_lossy().to_string()
            } else {
                format!("not writable: {}", cas_root.display())
            }),
            None,
        ));

        checks.extend(tier3_binary_checks());

        let cas_data_dir = ctx.cas_data_dir.clone();
        let aliases_result =
            tokio::task::spawn_blocking(move || -> Result<Vec<cas_registry::AliasEntry>> {
                let index = cas_registry::open(&cas_data_dir.index_db_path())?;
                cas_registry::list_all(&index)
            })
            .await
            .map_err(|e| crate::Error::InvalidArgument(format!("doctor task panicked: {e}")))?;

        match aliases_result {
            Ok(entries) if entries.is_empty() => checks.push(doctor_check(
                "registered repositories",
                DoctorStatus::Warn,
                Some("no repos registered yet".into()),
                None,
            )),
            Ok(entries) => {
                for entry in &entries {
                    checks.push(registered_repo_path_check(entry));
                }
                if let Some(watch_manager) = ctx.watch_manager.as_ref() {
                    checks.extend(alias_watcher_checks(&entries, watch_manager));
                }

                let store_data_dir = ctx.cas_data_dir.clone();
                let store_entries = entries.clone();
                let store_probes = tokio::task::spawn_blocking(move || {
                    probe_alias_stores(&store_data_dir, &store_entries)
                })
                .await
                .map_err(|e| crate::Error::InvalidArgument(format!("doctor task panicked: {e}")))?;
                checks.extend(tentative_snapshot_checks(&store_probes));
                checks.extend(tier3_run_checks(&store_probes));
            }
            Err(e) => checks.push(doctor_check(
                "alias index readable",
                DoctorStatus::Fail,
                Some(e.to_string()),
                None,
            )),
        }

        Ok(serde_json::to_value(DoctorReport { checks }).unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static REGISTER: fn() -> Box<dyn ControlMethod> = || Box::new(Doctor);

fn doctor_check(
    name: impl Into<String>,
    status: DoctorStatus,
    detail: Option<String>,
    remediation: Option<String>,
) -> DoctorCheck {
    DoctorCheck {
        name: name.into(),
        status,
        detail,
        remediation,
    }
}

fn workspace_analyzer_ids() -> Vec<&'static str> {
    all_workspace_analyzers()
        .iter()
        .map(|analyzer| analyzer.id())
        .collect()
}

fn registered_repo_path_check(entry: &cas_registry::AliasEntry) -> DoctorCheck {
    let exists = Path::new(&entry.root_path).is_dir();
    doctor_check(
        format!("repo `{}` root present", entry.alias),
        if exists {
            DoctorStatus::Pass
        } else {
            DoctorStatus::Fail
        },
        Some(if exists {
            entry.root_path.clone()
        } else {
            format!("missing: {}", entry.root_path)
        }),
        (!exists).then(|| {
            format!(
                "Run `cairn ctl remove-repo --alias {}` to drop the alias entry (on-disk data is kept for any other aliases at the same path), or restore the directory at {}.",
                entry.alias, entry.root_path
            )
        }),
    )
}

fn alias_watcher_checks(
    entries: &[cas_registry::AliasEntry],
    watch_manager: &crate::watcher::WatchManager,
) -> Vec<DoctorCheck> {
    entries
        .iter()
        .map(|entry| {
            let watching = watch_manager.is_watching_alias(&entry.alias);
            doctor_check(
                format!("repo `{}` watcher active", entry.alias),
                if watching {
                    DoctorStatus::Pass
                } else {
                    DoctorStatus::Warn
                },
                Some(if watching {
                    format!("watching {}", entry.root_path)
                } else {
                    "not watching (alias registered but no live FS watcher; tentative-default reads will fall back to HEAD until the next reindex_repo)".into()
                }),
                (!watching).then(|| {
                    format!(
                        "Run `cairn ctl remove-repo --alias {}` then `cairn ctl register-repo --alias {} {}` to re-establish the FS watcher. Restarting the daemon is an alternative that re-installs every alias's watcher in one shot.",
                        entry.alias, entry.alias, entry.root_path
                    )
                }),
            )
        })
        .collect()
}

#[derive(Debug, Clone)]
struct AliasStoreProbe {
    alias: String,
    store_path: PathBuf,
    result: std::result::Result<AliasStoreState, String>,
}

#[derive(Debug, Clone)]
struct AliasStoreState {
    tentative_manifest_id: Option<i64>,
    tier3_runs: Vec<Tier3Run>,
}

#[derive(Debug, Clone)]
struct Tier3Run {
    analyzer_id: String,
    manifest_id: i64,
    status: String,
    error: Option<String>,
}

fn probe_alias_stores(
    cas_data_dir: &CasDataDir,
    entries: &[cas_registry::AliasEntry],
) -> Vec<AliasStoreProbe> {
    entries
        .iter()
        .map(|entry| probe_alias_store(cas_data_dir, entry))
        .collect()
}

fn probe_alias_store(
    cas_data_dir: &CasDataDir,
    entry: &cas_registry::AliasEntry,
) -> AliasStoreProbe {
    let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
    let result = probe_alias_store_inner(&store_path, &entry.root_path).map_err(|e| e.to_string());
    AliasStoreProbe {
        alias: entry.alias.clone(),
        store_path,
        result,
    }
}

fn probe_alias_store_inner(store_path: &Path, root_path: &str) -> Result<AliasStoreState> {
    if !store_path.exists() {
        return Err(crate::Error::InvalidArgument(format!(
            "CAS store does not exist: {}",
            store_path.display()
        )));
    }
    let conn = cas_store::open(store_path)?;
    let worktree_id = conn
        .query_row(
            "SELECT worktree_id FROM worktrees WHERE path = ?1",
            params![root_path],
            |r| r.get::<_, i64>(0),
        )
        .optional()?;
    let tentative_manifest_id = match worktree_id {
        Some(id) => conn
            .query_row(
                "SELECT manifest_id FROM anchors WHERE anchor_name = ?1",
                params![format!("tentative/{id}")],
                |r| r.get::<_, i64>(0),
            )
            .optional()?,
        None => None,
    };
    let tier3_runs = match tentative_manifest_id {
        Some(manifest_id) => {
            let mut stmt = conn.prepare(
                "SELECT analyzer_id, manifest_id, status, error FROM workspace_analysis_runs
                 WHERE manifest_id = ?1
                 ORDER BY analyzer_id",
            )?;
            stmt.query_map(params![manifest_id], |r| {
                Ok(Tier3Run {
                    analyzer_id: r.get(0)?,
                    manifest_id: r.get(1)?,
                    status: r.get(2)?,
                    error: r.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        }
        None => Vec::new(),
    };
    Ok(AliasStoreState {
        tentative_manifest_id,
        tier3_runs,
    })
}

fn tentative_snapshot_checks(probes: &[AliasStoreProbe]) -> Vec<DoctorCheck> {
    probes
        .iter()
        .map(|probe| match &probe.result {
            Ok(state) => match state.tentative_manifest_id {
                Some(manifest_id) => doctor_check(
                    format!("repo `{}` tentative snapshot present", probe.alias),
                    DoctorStatus::Pass,
                    Some(format!("tentative anchor -> manifest_id {manifest_id}")),
                    None,
                ),
                None => doctor_check(
                    format!("repo `{}` tentative snapshot present", probe.alias),
                    DoctorStatus::Warn,
                    Some("no tentative anchor yet (reads will fall back to HEAD)".into()),
                    Some(format!(
                        "Run `cairn ctl reindex-repo --alias {}` to build the tentative snapshot.",
                        probe.alias
                    )),
                ),
            },
            Err(error) => doctor_check(
                format!("repo `{}` tentative snapshot present", probe.alias),
                DoctorStatus::Fail,
                Some(error.clone()),
                Some(format!(
                    "Run `cairn ctl remove-repo --alias {}` then re-register, or restore the CAS file at {}.",
                    probe.alias,
                    probe.store_path.display()
                )),
            ),
        })
        .collect()
}

fn tier3_binary_checks() -> Vec<DoctorCheck> {
    vec![
        rust_analyzer_binary_check(),
        pyright_binary_check(),
        gopls_binary_check(),
        clangd_binary_check(),
        typescript_language_server_binary_check(),
        csharp_ls_binary_check(),
        jdtls_binary_check(),
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

fn jdtls_binary_check() -> DoctorCheck {
    binary_check(
        "jdtls binary discoverable",
        resolve_jdtls(),
        "jdtls not on PATH",
        "Install an Eclipse JDT Language Server wrapper script named `jdtls`, or set JDTLS to that wrapper; Java Tier-3 (LSP) facts will not be available until then.",
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
    if let Some(path) = std::env::var_os("RUST_ANALYZER")
        .map(PathBuf::from)
        .filter(|path| path.is_file())
    {
        return Some(path.canonicalize().unwrap_or(path));
    }
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|dir| dir.join("rust-analyzer"))
        .find(|path| path.is_file())
        .map(|path| path.canonicalize().unwrap_or(path))
}

fn resolve_pyright() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("PYRIGHT")
        .map(PathBuf::from)
        .filter(|path| path.is_file())
    {
        return Some(path.canonicalize().unwrap_or(path));
    }
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|dir| dir.join("pyright-langserver"))
        .find(|path| path.is_file())
        .map(|path| path.canonicalize().unwrap_or(path))
}

fn resolve_gopls() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("GOPLS")
        .map(PathBuf::from)
        .filter(|path| path.is_file())
    {
        return Some(path.canonicalize().unwrap_or(path));
    }
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|dir| dir.join("gopls"))
        .find(|path| path.is_file())
        .map(|path| path.canonicalize().unwrap_or(path))
}

fn resolve_clangd() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("CLANGD")
        .map(PathBuf::from)
        .filter(|path| path.is_file())
    {
        return Some(path.canonicalize().unwrap_or(path));
    }
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|dir| dir.join("clangd"))
        .find(|path| path.is_file())
        .map(|path| path.canonicalize().unwrap_or(path))
}

fn resolve_typescript_language_server() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("TYPESCRIPT_LANGUAGE_SERVER")
        .map(PathBuf::from)
        .filter(|path| path.is_file())
    {
        return Some(path.canonicalize().unwrap_or(path));
    }
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|dir| dir.join("typescript-language-server"))
        .find(|path| path.is_file())
        .map(|path| path.canonicalize().unwrap_or(path))
}

fn resolve_csharp_ls() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("CSHARP_LS")
        .map(PathBuf::from)
        .filter(|path| path.is_file())
    {
        return Some(path.canonicalize().unwrap_or(path));
    }
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|dir| dir.join("csharp-ls"))
        .find(|path| path.is_file())
        .map(|path| path.canonicalize().unwrap_or(path))
}

fn resolve_jdtls() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("JDTLS")
        .map(PathBuf::from)
        .filter(|path| path.is_file())
    {
        return Some(path.canonicalize().unwrap_or(path));
    }
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|dir| dir.join("jdtls"))
        .find(|path| path.is_file())
        .map(|path| path.canonicalize().unwrap_or(path))
}

fn resolve_ruby_lsp() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("RUBY_LSP")
        .map(PathBuf::from)
        .filter(|path| path.is_file())
    {
        return Some(path.canonicalize().unwrap_or(path));
    }
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|dir| dir.join("ruby-lsp"))
        .find(|path| path.is_file())
        .map(|path| path.canonicalize().unwrap_or(path))
}

fn resolve_sourcekit_lsp() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("SOURCEKIT_LSP")
        .map(PathBuf::from)
        .filter(|path| path.is_file())
    {
        return Some(path.canonicalize().unwrap_or(path));
    }
    if let Some(path) = sourcekit_lsp_from_xcrun() {
        return Some(path.canonicalize().unwrap_or(path));
    }
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|dir| dir.join("sourcekit-lsp"))
        .find(|path| path.is_file())
        .map(|path| path.canonicalize().unwrap_or(path))
}

fn sourcekit_lsp_from_xcrun() -> Option<PathBuf> {
    // macOS installs sourcekit-lsp inside the selected Xcode/Swift toolchain,
    // where PATH often does not include it. `xcrun --find` respects
    // xcode-select, while non-macOS Swift toolchains are handled by PATH below.
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("xcrun")
            .args(["--find", "sourcekit-lsp"])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let path = String::from_utf8(output.stdout).ok()?;
        let path = PathBuf::from(path.trim());
        path.is_file().then_some(path)
    }
    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

fn tier3_run_checks(probes: &[AliasStoreProbe]) -> Vec<DoctorCheck> {
    probes
        .iter()
        .map(|probe| match &probe.result {
            Ok(state) => tier3_run_check(&probe.alias, state),
            Err(error) => doctor_check(
                format!("repo `{}` Tier-3 analyzer status", probe.alias),
                DoctorStatus::Fail,
                Some(error.clone()),
                Some(format!(
                    "Run `cairn ctl remove-repo --alias {}` then re-register, or restore the CAS file at {}.",
                    probe.alias,
                    probe.store_path.display()
                )),
            ),
        })
        .collect()
}

fn tier3_run_check(alias: &str, state: &AliasStoreState) -> DoctorCheck {
    if state.tier3_runs.is_empty() {
        return doctor_check(
            format!("repo `{alias}` Tier-3 analyzer status"),
            DoctorStatus::Warn,
            Some("no Tier-3 run recorded for this alias".into()),
            Some(format!(
                "Trigger a reindex with `cairn ctl reindex-repo --alias {alias}` or wait for the next file edit to drive a watcher tick."
            )),
        );
    }

    let detail = tier3_runs_detail(&state.tier3_runs);
    if let Some(run) = state.tier3_runs.iter().find(|run| run.status == "failed") {
        return doctor_check(
            format!("repo `{alias}` Tier-3 analyzer status"),
            DoctorStatus::Warn,
            Some(format!(
                "{detail}; {} failed: {}",
                run.analyzer_id,
                run.error.as_deref().unwrap_or("unknown error")
            )),
            Some(format!(
                "Check daemon logs near manifest {}; transient failures usually recover on the next watcher tick. If persistent, try `cairn ctl reindex-repo --alias {alias}`.",
                run.manifest_id
            )),
        );
    }

    if let Some(run) = state.tier3_runs.iter().find(|run| {
        !matches!(
            run.status.as_str(),
            "succeeded" | "skipped" | "pending" | "running"
        )
    }) {
        return doctor_check(
            format!("repo `{alias}` Tier-3 analyzer status"),
            DoctorStatus::Warn,
            Some(format!(
                "{detail}; {} reported status `{}` at manifest {} (not recognized by this doctor build)",
                run.analyzer_id, run.status, run.manifest_id
            )),
            Some(format!(
                "Trigger a reindex with `cairn ctl reindex-repo --alias {alias}` and check daemon logs if the status persists."
            )),
        );
    }

    doctor_check(
        format!("repo `{alias}` Tier-3 analyzer status"),
        DoctorStatus::Pass,
        Some(detail),
        None,
    )
}

fn tier3_runs_detail(runs: &[Tier3Run]) -> String {
    let manifest_id = runs
        .iter()
        .map(|run| run.manifest_id)
        .min()
        .unwrap_or_default();
    let statuses = runs
        .iter()
        .map(|run| {
            let status = tier3_status_label(run);
            format!("{}={status}", run.analyzer_id)
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("Tier-3 analyzer runs at manifest {manifest_id}: {statuses}")
}

fn tier3_status_label(run: &Tier3Run) -> String {
    match (run.status.as_str(), run.error.as_deref()) {
        ("succeeded", _) => "succeeded".into(),
        ("skipped", Some(error)) => format!("skipped ({error})"),
        ("skipped", None) => "skipped".into(),
        ("pending", _) => "queued".into(),
        ("running", _) => "in progress".into(),
        (status, _) => status.into(),
    }
}

fn backend_registration_coherence_check(
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
    use crate::cas::registry;
    use crate::paths::CasDataDir;
    use crate::watcher::WatchManager;
    use cairn_watch::WatchBackend;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::sync::Notify;

    const RUST_ANALYZER_ID: &str = "rust-analyzer-lsp";

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

        assert_eq!(check.status, DoctorStatus::Pass);
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

    #[test]
    fn missing_repo_path_check_includes_remediation() {
        let entry = cas_registry::AliasEntry {
            alias: "gone".into(),
            root_path: "/definitely/missing/cairn/repo".into(),
            repo_hash: "hash".into(),
            registered_at_ns: 0,
        };

        let check = registered_repo_path_check(&entry);

        assert_eq!(check.status, DoctorStatus::Fail);
        assert_eq!(
            check.detail.as_deref(),
            Some("missing: /definitely/missing/cairn/repo")
        );
        let remediation = check.remediation.expect("remediation");
        assert!(remediation.contains("remove-repo --alias gone"));
        assert!(remediation.contains("/definitely/missing/cairn/repo"));
    }

    #[test]
    fn watcher_check_warns_with_remediation_when_alias_is_not_watched() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = WatchManager::with_backend(
            Arc::new(CasDataDir::with_root(tmp.path().join("data"))),
            WatchBackend::Poll,
        );
        let entries = [cas_registry::AliasEntry {
            alias: "demo".into(),
            root_path: tmp.path().join("repo").to_string_lossy().to_string(),
            repo_hash: "hash".into(),
            registered_at_ns: 0,
        }];

        let checks = alias_watcher_checks(&entries, &manager);

        assert_eq!(checks[0].status, DoctorStatus::Warn);
        assert_eq!(
            checks[0].detail.as_deref(),
            Some(
                "not watching (alias registered but no live FS watcher; tentative-default reads will fall back to HEAD until the next reindex_repo)"
            )
        );
        assert!(
            checks[0]
                .remediation
                .as_deref()
                .unwrap()
                .contains("register-repo --alias demo")
        );
    }

    #[test]
    fn tentative_snapshot_checks_cover_present_missing_and_store_error() {
        let probes = vec![
            AliasStoreProbe {
                alias: "ok".into(),
                store_path: PathBuf::from("/tmp/ok/store.db"),
                result: Ok(AliasStoreState {
                    tentative_manifest_id: Some(7),
                    tier3_runs: Vec::new(),
                }),
            },
            AliasStoreProbe {
                alias: "missing".into(),
                store_path: PathBuf::from("/tmp/missing/store.db"),
                result: Ok(AliasStoreState {
                    tentative_manifest_id: None,
                    tier3_runs: Vec::new(),
                }),
            },
            AliasStoreProbe {
                alias: "bad".into(),
                store_path: PathBuf::from("/tmp/bad/store.db"),
                result: Err("not a database".into()),
            },
        ];

        let checks = tentative_snapshot_checks(&probes);

        assert_eq!(checks[0].status, DoctorStatus::Pass);
        assert!(
            checks[0]
                .detail
                .as_deref()
                .unwrap()
                .contains("manifest_id 7")
        );
        assert_eq!(checks[1].status, DoctorStatus::Warn);
        assert_eq!(
            checks[1].detail.as_deref(),
            Some("no tentative anchor yet (reads will fall back to HEAD)")
        );
        assert!(
            checks[1]
                .remediation
                .as_deref()
                .unwrap()
                .contains("reindex-repo")
        );
        assert_eq!(checks[2].status, DoctorStatus::Fail);
        assert_eq!(checks[2].detail.as_deref(), Some("not a database"));
        assert!(
            checks[2]
                .remediation
                .as_deref()
                .unwrap()
                .contains("remove-repo")
        );
    }

    #[test]
    fn tier3_run_checks_map_statuses_to_actionable_results() {
        let succeeded = tier3_run_check(
            "ok",
            &AliasStoreState {
                tentative_manifest_id: Some(1),
                tier3_runs: vec![Tier3Run {
                    analyzer_id: "demo-analyzer".into(),
                    manifest_id: 1,
                    status: "succeeded".into(),
                    error: None,
                }],
            },
        );
        let skipped = tier3_run_check(
            "skip",
            &AliasStoreState {
                tentative_manifest_id: Some(2),
                tier3_runs: vec![Tier3Run {
                    analyzer_id: "demo-analyzer".into(),
                    manifest_id: 2,
                    status: "skipped".into(),
                    error: Some("ContentModified".into()),
                }],
            },
        );
        let pending = tier3_run_check(
            "pending",
            &AliasStoreState {
                tentative_manifest_id: Some(5),
                tier3_runs: vec![Tier3Run {
                    analyzer_id: "demo-analyzer".into(),
                    manifest_id: 5,
                    status: "pending".into(),
                    error: None,
                }],
            },
        );
        let running = tier3_run_check(
            "running",
            &AliasStoreState {
                tentative_manifest_id: Some(6),
                tier3_runs: vec![Tier3Run {
                    analyzer_id: "demo-analyzer".into(),
                    manifest_id: 6,
                    status: "running".into(),
                    error: None,
                }],
            },
        );
        let failed = tier3_run_check(
            "fail",
            &AliasStoreState {
                tentative_manifest_id: Some(3),
                tier3_runs: vec![Tier3Run {
                    analyzer_id: "demo-analyzer".into(),
                    manifest_id: 3,
                    status: "failed".into(),
                    error: Some("boom".into()),
                }],
            },
        );
        let missing = tier3_run_check(
            "missing",
            &AliasStoreState {
                tentative_manifest_id: Some(4),
                tier3_runs: Vec::new(),
            },
        );

        assert_eq!(succeeded.status, DoctorStatus::Pass);
        assert_eq!(skipped.status, DoctorStatus::Pass);
        assert!(
            skipped
                .detail
                .as_deref()
                .unwrap()
                .contains("ContentModified")
        );
        assert_eq!(pending.status, DoctorStatus::Pass);
        assert!(pending.detail.as_deref().unwrap().contains("queued"));
        assert!(pending.remediation.is_none());
        assert_eq!(running.status, DoctorStatus::Pass);
        assert!(running.detail.as_deref().unwrap().contains("in progress"));
        assert!(running.remediation.is_none());
        assert_eq!(failed.status, DoctorStatus::Warn);
        assert!(
            failed
                .remediation
                .as_deref()
                .unwrap()
                .contains("manifest 3")
        );
        assert_eq!(missing.status, DoctorStatus::Warn);
        assert!(
            missing
                .remediation
                .as_deref()
                .unwrap()
                .contains("reindex-repo")
        );
    }

    #[test]
    fn tier3_run_check_reports_python_success_when_rust_skips() {
        let check = tier3_run_check(
            "py",
            &AliasStoreState {
                tentative_manifest_id: Some(9),
                tier3_runs: vec![
                    Tier3Run {
                        analyzer_id: "pyright-lsp".into(),
                        manifest_id: 9,
                        status: "succeeded".into(),
                        error: None,
                    },
                    Tier3Run {
                        analyzer_id: RUST_ANALYZER_ID.into(),
                        manifest_id: 9,
                        status: "skipped".into(),
                        error: Some("no matching files".into()),
                    },
                ],
            },
        );

        assert_eq!(check.status, DoctorStatus::Pass);
        let detail = check.detail.as_deref().unwrap();
        assert!(detail.contains("pyright-lsp=succeeded"));
        assert!(detail.contains("rust-analyzer-lsp=skipped (no matching files)"));
    }

    #[tokio::test]
    async fn doctor_dispatch_reports_live_watcher_tentative_anchor_and_tier3_success() {
        let fixture = DoctorFixture::new();
        fixture.seed_alias("demo", true, Some("succeeded"), None);
        fixture
            .watch_manager
            .watch_alias("demo".into(), fixture.repo_path("demo"))
            .unwrap();

        let report = fixture.run_doctor().await;

        let watcher = find_check(&report, "repo `demo` watcher active");
        assert_eq!(watcher.status, DoctorStatus::Pass);
        let tentative = find_check(&report, "repo `demo` tentative snapshot present");
        assert_eq!(tentative.status, DoctorStatus::Pass);
        let tier3 = find_check(&report, "repo `demo` Tier-3 analyzer status");
        assert_eq!(tier3.status, DoctorStatus::Pass);
        assert!(tier3.detail.as_deref().unwrap().contains("succeeded"));
    }

    #[tokio::test]
    async fn doctor_dispatch_reports_per_analyzer_tier3_status_when_multiple_runs_present() {
        let fixture = DoctorFixture::new();
        fixture.seed_alias("demo", true, None, None);
        fixture.seed_workspace_run("demo", "pyright-lsp", "succeeded", None);
        fixture.seed_workspace_run(
            "demo",
            RUST_ANALYZER_ID,
            "skipped",
            Some("no matching files"),
        );

        let report = fixture.run_doctor().await;

        let tier3 = find_check(&report, "repo `demo` Tier-3 analyzer status");
        assert_eq!(tier3.status, DoctorStatus::Pass);
        let detail = tier3.detail.as_deref().unwrap();
        assert!(detail.contains("pyright-lsp=succeeded"));
        assert!(detail.contains("rust-analyzer-lsp=skipped"));
    }

    #[tokio::test]
    async fn doctor_dispatch_reports_missing_watcher_and_tentative_with_remediation() {
        let fixture = DoctorFixture::new();
        fixture.seed_alias("demo", false, None, None);

        let report = fixture.run_doctor().await;

        let watcher = find_check(&report, "repo `demo` watcher active");
        assert_eq!(watcher.status, DoctorStatus::Warn);
        assert!(
            watcher
                .detail
                .as_deref()
                .unwrap()
                .starts_with("not watching")
        );
        assert!(
            watcher
                .remediation
                .as_deref()
                .unwrap()
                .contains("register-repo --alias demo")
        );
        let tentative = find_check(&report, "repo `demo` tentative snapshot present");
        assert_eq!(tentative.status, DoctorStatus::Warn);
        assert_eq!(
            tentative.detail.as_deref(),
            Some("no tentative anchor yet (reads will fall back to HEAD)")
        );
        assert!(
            tentative
                .remediation
                .as_deref()
                .unwrap()
                .contains("reindex-repo")
        );
    }

    struct DoctorFixture {
        _tmp: tempfile::TempDir,
        cas_data_dir: Arc<CasDataDir>,
        watch_manager: Arc<WatchManager>,
        repos_root: PathBuf,
    }

    impl DoctorFixture {
        fn new() -> Self {
            let tmp = tempfile::tempdir().unwrap();
            let cas_data_dir = Arc::new(CasDataDir::with_root(tmp.path().join("data")));
            cas_data_dir.ensure().unwrap();
            let watch_manager = Arc::new(WatchManager::with_backend(
                cas_data_dir.clone(),
                WatchBackend::Poll,
            ));
            let repos_root = tmp.path().join("repos");
            std::fs::create_dir_all(&repos_root).unwrap();
            Self {
                _tmp: tmp,
                cas_data_dir,
                watch_manager,
                repos_root,
            }
        }

        fn repo_path(&self, alias: &str) -> PathBuf {
            self.repos_root.join(alias)
        }

        fn seed_alias(
            &self,
            alias: &str,
            with_tentative: bool,
            tier3_status: Option<&str>,
            tier3_error: Option<&str>,
        ) {
            let repo_path = self.repo_path(alias);
            std::fs::create_dir_all(&repo_path).unwrap();
            let repo_hash = format!("{alias}-hash");
            let mut index = registry::open(&self.cas_data_dir.index_db_path()).unwrap();
            {
                let tx = index.transaction().unwrap();
                registry::upsert(&tx, alias, &repo_path.to_string_lossy(), &repo_hash, 0).unwrap();
                tx.commit().unwrap();
            }

            let store_path = self.cas_data_dir.store_db_path(&repo_hash);
            let store = cas_store::open(&store_path).unwrap();
            store
                .execute(
                    "INSERT INTO worktrees (path, registered_at_ns) VALUES (?1, 0)",
                    params![repo_path.to_string_lossy().as_ref()],
                )
                .unwrap();
            let worktree_id = store.last_insert_rowid();
            store
                .execute(
                    "INSERT INTO manifests (manifest_id, kind, built_at_ns)
                     VALUES (1, 'tentative', 0)",
                    [],
                )
                .unwrap();
            if with_tentative {
                store
                    .execute(
                        "INSERT INTO anchors (anchor_name, manifest_id, last_updated_ns)
                         VALUES (?1, 1, 0)",
                        params![format!("tentative/{worktree_id}")],
                    )
                    .unwrap();
            }
            if let Some(status) = tier3_status {
                self.seed_workspace_run(alias, RUST_ANALYZER_ID, status, tier3_error);
            }
        }

        fn seed_workspace_run(
            &self,
            alias: &str,
            analyzer_id: &str,
            status: &str,
            error: Option<&str>,
        ) {
            let store_path = self.cas_data_dir.store_db_path(&format!("{alias}-hash"));
            let store = cas_store::open(&store_path).unwrap();
            store
                .execute(
                    "INSERT INTO workspace_analysis_runs
                       (manifest_id, analyzer_id, analyzer_revision, config_hash,
                        status, started_at_ns, finished_at_ns, error)
                     VALUES (1, ?1, 1, 'cfg', ?2, 0, 1, ?3)",
                    params![analyzer_id, status, error],
                )
                .unwrap();
        }

        async fn run_doctor(&self) -> DoctorReport {
            let ctx = CtlCtx {
                cas_data_dir: self.cas_data_dir.clone(),
                shutdown: Arc::new(Notify::new()),
                watch_manager: Some(self.watch_manager.clone()),
                version: "test",
                started_at: Instant::now(),
            };
            let value = Doctor.dispatch(&ctx, Value::Null).await.unwrap();
            serde_json::from_value(value).unwrap()
        }
    }

    fn find_check<'a>(report: &'a DoctorReport, name: &str) -> &'a DoctorCheck {
        report
            .checks
            .iter()
            .find(|check| check.name == name)
            .unwrap_or_else(|| panic!("missing check `{name}` in {:#?}", report.checks))
    }
}
