//! `doctor` — environment / dependency / registry sanity checks.

use cairn_proto::control::{DoctorCheck, DoctorReport, DoctorStatus};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx};
use crate::{Result, registry_db};

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

        let data_root = ctx.storage.data_dir.root().to_path_buf();
        let writable = std::fs::metadata(&data_root)
            .map(|m| !m.permissions().readonly())
            .unwrap_or(false);
        checks.push(DoctorCheck {
            name: "data directory".into(),
            status: if writable {
                DoctorStatus::Pass
            } else {
                DoctorStatus::Fail
            },
            detail: Some(data_root.to_string_lossy().to_string()),
        });

        let repos_result = ctx
            .storage
            .with_registry(|conn| registry_db::list_repos(conn))
            .await;
        match repos_result {
            Ok(repos) => {
                if repos.is_empty() {
                    checks.push(DoctorCheck {
                        name: "registered repositories".into(),
                        status: DoctorStatus::Warn,
                        detail: Some("no repos registered yet".into()),
                    });
                } else {
                    for repo in repos {
                        let exists = std::path::Path::new(&repo.root_path).is_dir();
                        checks.push(DoctorCheck {
                            name: format!("repo `{}` root present", repo.alias),
                            status: if exists {
                                DoctorStatus::Pass
                            } else {
                                DoctorStatus::Fail
                            },
                            detail: Some(repo.root_path),
                        });
                    }
                }
            }
            Err(e) => checks.push(DoctorCheck {
                name: "registry readable".into(),
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
