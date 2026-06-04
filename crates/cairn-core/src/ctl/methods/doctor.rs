//! `doctor` — environment / dependency / registry sanity checks.

use cairn_proto::control::{DoctorCheck, DoctorReport, DoctorStatus};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx};
use crate::Result;
use crate::cas::registry as cas_registry;
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
        checks.push(DoctorCheck {
            name: "language backends linked".into(),
            status: if backend_names.is_empty() {
                DoctorStatus::Fail
            } else {
                DoctorStatus::Pass
            },
            detail: Some(format!(
                "{} backend(s): {}",
                backend_names.len(),
                backend_names.join(", ")
            )),
        });
        checks.push(backend_registration_coherence_check(
            &backend_names,
            &workspace_analyzer_ids(),
        ));

        let cas_root = ctx.cas_data_dir.root().to_path_buf();
        let writable = std::fs::metadata(&cas_root)
            .map(|m| !m.permissions().readonly())
            .unwrap_or(false);
        checks.push(DoctorCheck {
            name: "data directory".into(),
            status: if writable {
                DoctorStatus::Pass
            } else {
                DoctorStatus::Fail
            },
            detail: Some(cas_root.to_string_lossy().to_string()),
        });

        let cas_data_dir = ctx.cas_data_dir.clone();
        let aliases_result =
            tokio::task::spawn_blocking(move || -> Result<Vec<cas_registry::AliasEntry>> {
                let index = cas_registry::open(&cas_data_dir.index_db_path())?;
                cas_registry::list_all(&index)
            })
            .await
            .map_err(|e| crate::Error::InvalidArgument(format!("doctor task panicked: {e}")))?;

        match aliases_result {
            Ok(entries) if entries.is_empty() => checks.push(DoctorCheck {
                name: "registered repositories".into(),
                status: DoctorStatus::Warn,
                detail: Some("no repos registered yet".into()),
            }),
            Ok(entries) => {
                for entry in entries {
                    let exists = std::path::Path::new(&entry.root_path).is_dir();
                    checks.push(DoctorCheck {
                        name: format!("repo `{}` root present", entry.alias),
                        status: if exists {
                            DoctorStatus::Pass
                        } else {
                            DoctorStatus::Fail
                        },
                        detail: Some(entry.root_path),
                    });
                }
            }
            Err(e) => checks.push(DoctorCheck {
                name: "alias index readable".into(),
                status: DoctorStatus::Fail,
                detail: Some(e.to_string()),
            }),
        }

        Ok(serde_json::to_value(DoctorReport { checks }).unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static REGISTER: fn() -> Box<dyn ControlMethod> = || Box::new(Doctor);

fn workspace_analyzer_ids() -> Vec<&'static str> {
    all_workspace_analyzers()
        .iter()
        .map(|analyzer| analyzer.id())
        .collect()
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
        return DoctorCheck {
            name: "backend registration coherence".into(),
            status: DoctorStatus::Pass,
            detail: Some(format!(
                "{} runtime backend crate(s) registered",
                EXPECTED_BACKEND_CRATES.len()
            )),
        };
    }

    DoctorCheck {
        name: "backend registration coherence".into(),
        status: DoctorStatus::Warn,
        detail: Some(
            missing
                .into_iter()
                .map(|expected| {
                    format!(
                        "{} is declared for runtime linking but `{}` is missing from {} - likely missing `{}` in cairn-cli/src/main.rs",
                        expected.crate_name,
                        expected.runtime_id,
                        expected.registry.label(),
                        expected.import_hint
                    )
                })
                .collect::<Vec<_>>()
                .join("; "),
        ),
    }
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
        let language_backends = ["rust", "python", "markdown", "typescript", "go"];
        let workspace_analyzers = ["rust-analyzer-lsp"];

        let check = backend_registration_coherence_check(&language_backends, &workspace_analyzers);

        assert_eq!(check.status, DoctorStatus::Pass);
    }

    #[test]
    fn backend_registration_coherence_warns_for_missing_runtime_entry() {
        let language_backends = ["rust", "python", "markdown", "go"];
        let workspace_analyzers = ["rust-analyzer-lsp"];

        let check = backend_registration_coherence_check(&language_backends, &workspace_analyzers);

        assert_eq!(check.status, DoctorStatus::Warn);
        let detail = check.detail.expect("warning detail");
        assert!(detail.contains("cairn-lang-typescript"));
        assert!(detail.contains("LANGUAGE_BACKENDS"));
        assert!(detail.contains("use cairn_lang_typescript as _;"));
    }
}
