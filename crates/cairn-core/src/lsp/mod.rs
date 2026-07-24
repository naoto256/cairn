//! Minimal LSP subprocess client for workspace analyzers.
//!
//! PR2 keeps this deliberately small: enough JSON-RPC framing,
//! lifecycle, timeout, and `textDocument/definition` support for the
//! rust-analyzer integration planned in PR3, without pulling in the
//! full `lsp-types` surface yet.

mod client;
mod error;
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

#[cfg(test)]
mod tests;
