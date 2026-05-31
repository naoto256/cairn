//! `doctor` — environment / dependency / registry sanity checks.

use cairn_proto::control::{DoctorCheck, DoctorReport, DoctorStatus};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx};
use crate::Result;
use crate::cas::registry as cas_registry;

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
