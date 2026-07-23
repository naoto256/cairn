//! Concrete control-socket methods.
//!
//! Each sub-module owns one admin verb end-to-end. Adding a new verb
//! is a single-file change; the dispatcher in [`super::CtlHandler`]
//! picks it up automatically via the [`super::CONTROL_METHODS`]
//! distributed slice.

use cairn_proto::control::{Ack, QueuedAnalyzerJobReceipt};
use serde_json::Value;

use crate::jobs::QueuedAnalyzerJob;

mod doctor;
mod jobs;
mod prune;
mod register_repo;
mod reindex_repo;
mod remove_repo;
mod shutdown;
mod status;

pub(super) fn ack_with_queued_analyzer_jobs(ack: Ack, jobs: &[QueuedAnalyzerJob]) -> Value {
    let receipts = jobs
        .iter()
        .map(|job| QueuedAnalyzerJobReceipt {
            job_id: job.job_id,
            analyzer_id: job.analyzer_id.clone(),
            state: job.state.clone(),
        })
        .collect::<Vec<_>>();
    let mut value = serde_json::to_value(ack).unwrap();
    if let Value::Object(obj) = &mut value {
        obj.insert("jobs".into(), serde_json::to_value(receipts).unwrap());
    }
    value
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn queued_analyzer_job_receipts_serialize_large_ids_as_decimal_strings() {
        let response = ack_with_queued_analyzer_jobs(
            Ack::with_alias("cairn"),
            &[QueuedAnalyzerJob {
                job_id: 1_784_679_083_389_822_001,
                analyzer_id: "rust-analyzer-lsp".into(),
                state: "queued".into(),
            }],
        );

        assert_eq!(
            response["jobs"][0],
            json!({
                "job_id": "1784679083389822001",
                "analyzer_id": "rust-analyzer-lsp",
                "state": "queued"
            })
        );
    }
}
