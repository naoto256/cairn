//! `doctor` — environment / dependency / registry sanity checks.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use cairn_proto::control::{DoctorCheck, DoctorReport, DoctorStatus};
use linkme::distributed_slice;
use rusqlite::{OptionalExtension, params};
use serde_json::Value;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx};
use crate::Result;
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::lsp_discovery::{
    discover_lsp_binary, discover_lsp_binary_candidates, discover_sourcekit_lsp,
};
use crate::manifest::ManifestId;
use crate::paths::CasDataDir;
use crate::workspace_analyzer::{all_workspace_analyzers, expected_analyzers_for_manifest};

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
            .map_err(|e| crate::Error::internal_task_panic("doctor", e))?;

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
                .map_err(|e| crate::Error::internal_task_panic("doctor", e))?;
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
                "Run `cairn ctl repo remove {}` to drop the alias entry (on-disk data is kept for any other aliases at the same path), or restore the directory at {}.",
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
                        "Run `cairn ctl repo remove {}` then `cairn ctl repo register --alias {} {}` to re-establish the FS watcher. Restarting the daemon is an alternative that re-installs every alias's watcher in one shot.",
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
    expected_tier3_analyzer_ids: Vec<String>,
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
    let (tier3_runs, expected_tier3_analyzer_ids) = match tentative_manifest_id {
        Some(manifest_id) => {
            let mut expected_tier3_analyzer_ids =
                expected_analyzers_for_manifest(&conn, ManifestId(manifest_id))?
                    .into_iter()
                    .map(|analyzer| analyzer.id().to_string())
                    .collect::<Vec<_>>();
            expected_tier3_analyzer_ids.sort();

            let mut stmt = conn.prepare(
                "SELECT analyzer_id, manifest_id, status, error FROM workspace_analysis_runs
                 WHERE manifest_id = ?1
                 ORDER BY analyzer_id",
            )?;
            let tier3_runs = stmt
                .query_map(params![manifest_id], |r| {
                    Ok(Tier3Run {
                        analyzer_id: r.get(0)?,
                        manifest_id: r.get(1)?,
                        status: r.get(2)?,
                        error: r.get(3)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            (tier3_runs, expected_tier3_analyzer_ids)
        }
        None => (Vec::new(), Vec::new()),
    };
    Ok(AliasStoreState {
        tentative_manifest_id,
        tier3_runs,
        expected_tier3_analyzer_ids,
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
                        "Run `cairn ctl repo reindex {}` to build the tentative snapshot.",
                        probe.alias
                    )),
                ),
            },
            Err(error) => doctor_check(
                format!("repo `{}` tentative snapshot present", probe.alias),
                DoctorStatus::Fail,
                Some(error.clone()),
                Some(format!(
                    "Run `cairn ctl repo remove {}` then re-register, or restore the CAS file at {}.",
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
        return Some(root);
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
                    "Run `cairn ctl repo remove {}` then re-register, or restore the CAS file at {}.",
                    probe.alias,
                    probe.store_path.display()
                )),
            ),
        })
        .collect()
}

fn tier3_run_check(alias: &str, state: &AliasStoreState) -> DoctorCheck {
    if state.tier3_runs.is_empty() {
        let missing = missing_tier3_analyzer_ids(state);
        if !missing.is_empty() {
            return doctor_check(
                format!("repo `{alias}` Tier-3 analyzer status"),
                DoctorStatus::Warn,
                Some(tier3_runs_detail(state)),
                Some(format!(
                    "Trigger a reindex with `cairn ctl repo reindex {alias}` to record the current workspace analyzer set."
                )),
            );
        }

        return doctor_check(
            format!("repo `{alias}` Tier-3 analyzer status"),
            DoctorStatus::Warn,
            Some("no Tier-3 run recorded for this alias".into()),
            Some(format!(
                "Trigger a reindex with `cairn ctl repo reindex {alias}` or wait for the next file edit to drive a watcher tick."
            )),
        );
    }

    let detail = tier3_runs_detail(state);
    if state
        .tier3_runs
        .iter()
        .any(|run| matches!(run.status.as_str(), "queued" | "running"))
    {
        return doctor_check(
            format!("repo `{alias}` Tier-3 analyzer status"),
            DoctorStatus::Warn,
            Some(format!("{detail}; indexing in progress")),
            Some(format!(
                "Track progress with `cairn ctl jobs list --alias {alias}`."
            )),
        );
    }

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
                "Check daemon logs near manifest {}; transient failures usually recover on the next watcher tick. If persistent, try `cairn ctl repo reindex {alias}`.",
                run.manifest_id
            )),
        );
    }

    if let Some(run) = state
        .tier3_runs
        .iter()
        .find(|run| matches!(run.status.as_str(), "timed_out" | "cancelled"))
    {
        return doctor_check(
            format!("repo `{alias}` Tier-3 analyzer status"),
            DoctorStatus::Warn,
            Some(format!("{detail}; {} is {}", run.analyzer_id, run.status)),
            Some(format!(
                "Trigger a reindex with `cairn ctl repo reindex {alias}` when ready."
            )),
        );
    }

    if let Some(run) = state.tier3_runs.iter().find(|run| {
        !matches!(
            run.status.as_str(),
            "succeeded" | "skipped" | "queued" | "running" | "cancelled" | "timed_out"
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
                "Trigger a reindex with `cairn ctl repo reindex {alias}` and check daemon logs if the status persists."
            )),
        );
    }

    let missing = missing_tier3_analyzer_ids(state);
    if !missing.is_empty() {
        return doctor_check(
            format!("repo `{alias}` Tier-3 analyzer status"),
            DoctorStatus::Warn,
            Some(detail),
            Some(format!(
                "Trigger a reindex with `cairn ctl repo reindex {alias}` to record the current workspace analyzer set."
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

fn tier3_runs_detail(state: &AliasStoreState) -> String {
    let manifest_id = state
        .tier3_runs
        .iter()
        .map(|run| run.manifest_id)
        .min()
        .or(state.tentative_manifest_id)
        .unwrap_or_default();
    let mut statuses = state
        .tier3_runs
        .iter()
        .map(|run| {
            let status = tier3_status_label(run);
            format!("{}={status}", run.analyzer_id)
        })
        .collect::<Vec<_>>();
    statuses.extend(
        missing_tier3_analyzer_ids(state)
            .into_iter()
            .map(|analyzer_id| format!("{analyzer_id}=not yet recorded (run reindex)")),
    );
    let statuses = statuses.join(", ");
    format!("Tier-3 analyzer runs at manifest {manifest_id}: {statuses}")
}

fn missing_tier3_analyzer_ids(state: &AliasStoreState) -> Vec<String> {
    let recorded = state
        .tier3_runs
        .iter()
        .map(|run| run.analyzer_id.as_str())
        .collect::<HashSet<_>>();
    state
        .expected_tier3_analyzer_ids
        .iter()
        .filter(|analyzer_id| !recorded.contains(analyzer_id.as_str()))
        .cloned()
        .collect()
}

fn tier3_status_label(run: &Tier3Run) -> String {
    match (run.status.as_str(), run.error.as_deref()) {
        ("succeeded", _) => "succeeded".into(),
        ("skipped", Some(error)) => format!("skipped ({error})"),
        ("skipped", None) => "skipped".into(),
        ("queued", _) => "queued".into(),
        ("running", _) => "in progress".into(),
        ("timed_out", Some(error)) => format!("timed out ({error})"),
        ("timed_out", None) => "timed out".into(),
        ("cancelled", _) => "cancelled".into(),
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

        assert_eq!(
            dotnet_sdk_root(Some(root.clone()), std::iter::empty()),
            Some(root)
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
        assert!(remediation.contains("repo remove gone"));
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
                .contains("repo register --alias demo")
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
                    expected_tier3_analyzer_ids: Vec::new(),
                }),
            },
            AliasStoreProbe {
                alias: "missing".into(),
                store_path: PathBuf::from("/tmp/missing/store.db"),
                result: Ok(AliasStoreState {
                    tentative_manifest_id: None,
                    tier3_runs: Vec::new(),
                    expected_tier3_analyzer_ids: Vec::new(),
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
                .contains("repo reindex")
        );
        assert_eq!(checks[2].status, DoctorStatus::Fail);
        assert_eq!(checks[2].detail.as_deref(), Some("not a database"));
        assert!(
            checks[2]
                .remediation
                .as_deref()
                .unwrap()
                .contains("repo remove")
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
                expected_tier3_analyzer_ids: Vec::new(),
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
                expected_tier3_analyzer_ids: Vec::new(),
            },
        );
        let pending = tier3_run_check(
            "queued",
            &AliasStoreState {
                tentative_manifest_id: Some(5),
                tier3_runs: vec![Tier3Run {
                    analyzer_id: "demo-analyzer".into(),
                    manifest_id: 5,
                    status: "queued".into(),
                    error: None,
                }],
                expected_tier3_analyzer_ids: Vec::new(),
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
                expected_tier3_analyzer_ids: Vec::new(),
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
                expected_tier3_analyzer_ids: Vec::new(),
            },
        );
        let missing = tier3_run_check(
            "missing",
            &AliasStoreState {
                tentative_manifest_id: Some(4),
                tier3_runs: Vec::new(),
                expected_tier3_analyzer_ids: Vec::new(),
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
        assert_eq!(pending.status, DoctorStatus::Warn);
        assert!(pending.detail.as_deref().unwrap().contains("queued"));
        assert!(
            pending
                .remediation
                .as_deref()
                .unwrap()
                .contains("jobs list")
        );
        assert_eq!(running.status, DoctorStatus::Warn);
        assert!(running.detail.as_deref().unwrap().contains("in progress"));
        assert!(
            running
                .remediation
                .as_deref()
                .unwrap()
                .contains("jobs list")
        );
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
                .contains("repo reindex")
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
                expected_tier3_analyzer_ids: Vec::new(),
            },
        );

        assert_eq!(check.status, DoctorStatus::Pass);
        let detail = check.detail.as_deref().unwrap();
        assert!(detail.contains("pyright-lsp=succeeded"));
        assert!(detail.contains("rust-analyzer-lsp=skipped (no matching files)"));
    }

    #[test]
    fn tier3_run_check_reports_expected_analyzer_without_run_record() {
        let check = tier3_run_check(
            "stale",
            &AliasStoreState {
                tentative_manifest_id: Some(10),
                tier3_runs: vec![Tier3Run {
                    analyzer_id: "old-analyzer".into(),
                    manifest_id: 10,
                    status: "succeeded".into(),
                    error: None,
                }],
                expected_tier3_analyzer_ids: vec!["new-analyzer".into(), "old-analyzer".into()],
            },
        );

        assert_eq!(check.status, DoctorStatus::Warn);
        let detail = check.detail.as_deref().unwrap();
        assert!(detail.contains("old-analyzer=succeeded"));
        assert!(detail.contains("new-analyzer=not yet recorded (run reindex)"));
        assert!(
            check
                .remediation
                .as_deref()
                .unwrap()
                .contains("repo reindex stale")
        );
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
    async fn doctor_dispatch_reports_registered_workspace_analyzer_without_run_record() {
        let fixture = DoctorFixture::new();
        fixture.seed_alias("demo", true, None, None);
        fixture.seed_manifest_blob("demo", "sha-fake", "fake-parser");

        let report = fixture.run_doctor().await;

        let tier3 = find_check(&report, "repo `demo` Tier-3 analyzer status");
        assert_eq!(tier3.status, DoctorStatus::Warn);
        assert!(
            tier3
                .detail
                .as_deref()
                .unwrap()
                .contains("fake-workspace=not yet recorded (run reindex)")
        );
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
                .contains("repo register --alias demo")
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
                .contains("repo reindex")
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

        fn seed_manifest_blob(&self, alias: &str, blob_sha: &str, parser_id: &str) {
            let store_path = self.cas_data_dir.store_db_path(&format!("{alias}-hash"));
            let store = cas_store::open(&store_path).unwrap();
            store
                .execute(
                    "INSERT INTO blobs
                       (blob_sha, parser_id, parser_revision, parsed_at_ns)
                     VALUES (?1, ?2, 1, 0)",
                    params![blob_sha, parser_id],
                )
                .unwrap();
            store
                .execute(
                    "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
                     VALUES (1, ?1, ?2)",
                    params![format!("src/{blob_sha}.fake"), blob_sha],
                )
                .unwrap();
        }

        async fn run_doctor(&self) -> DoctorReport {
            let ctx = CtlCtx {
                cas_data_dir: self.cas_data_dir.clone(),
                shutdown: Arc::new(Notify::new()),
                watch_manager: Some(self.watch_manager.clone()),
                job_manager: None,
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
