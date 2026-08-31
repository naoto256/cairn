//! Minimal LSP subprocess client for workspace analyzers.
//!
//! PR2 keeps this deliberately small: enough JSON-RPC framing,
//! lifecycle, timeout, and `textDocument/definition` support for the
//! rust-analyzer integration planned in PR3, without pulling in the
//! full `lsp-types` surface yet.

mod client;
mod error;
mod process_sweep;
// `pool` stays a public module (not curated re-exports): tier-3
// language crates configure spawning through its `LspSpawnSpec`,
// `AvailabilityStrategy`, and `ReadinessStrategy` types directly.
pub mod pool;
mod reader;
mod transport;
mod types;

pub use client::LspClient;
pub use error::{CONTENT_MODIFIED_ERROR_CODE, Error, ExitStatusDetail, Result};
pub use types::{Location, Position, Range, Url};

/// Initialize daemon-scoped ownership markers and sweep marked processes left
/// by an earlier daemon instance. The daemon calls this once, after acquiring
/// its lock and before any pooled LSP process can spawn.
///
/// This hidden facade is not required by standalone [`LspClient`] users;
/// standalone clients and availability probes deliberately remain unmarked.
#[doc(hidden)]
pub fn initialize_daemon_process_owner(data_root: &std::path::Path) -> Result<()> {
    process_sweep::initialize_daemon_process_owner(data_root)
}

pub(crate) use process_sweep::{SweepDiagnostics, sweep_diagnostics as orphan_cleanup_diagnostics};

#[cfg(test)]
mod tests;
