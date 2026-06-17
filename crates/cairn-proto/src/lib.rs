//! `cairn-proto` — wire types shared between the daemon, its consumers,
//! the management CLI, and external out-of-tree callers.
//!
//! Layout:
//! - [`jsonrpc`] — the JSON-RPC 2.0 envelope (Request / Response /
//!   RequestId / ResponseError / error codes). Shared by every
//!   JSON-RPC-framed surface.
//! - [`methods`] — protocol-neutral payload shapes for the cairn data
//!   API (`get_outline`, `find_symbols`, `list_repos`, plus the
//!   `register_repo` / `reindex` admin verbs that ride the control
//!   socket). These ride inside MCP `tools/call`, plain JSON-RPC, and
//!   any future LSP request alike.
//! - [`control`] — management messages spoken over `control.sock`,
//!   used by `cairn ctl` and the MCP front-end's admin-verb path.
//!
//! Both protocols ride newline-delimited JSON for simplicity. MCP-specific
//! types are intentionally *not* in this crate — they live next to the
//! one binary that speaks MCP (`cairn serve`), under
//! `cairn/src/cmd/mcp.rs`. Out-of-tree consumers
//! (cairn-graph, cairn-audit, future LSP front-end) reach for the
//! protocol-neutral surfaces in [`methods`] and [`control`] and never
//! need to depend on MCP.

#![forbid(unsafe_code)]

pub mod common;
pub mod control;
pub mod jsonrpc;
pub mod methods;
pub mod version;

pub use common::{
    AnalyzerState, Completeness, LanguageEnrichment, MissingTier, PartialReason, Position, Range,
    ReasonCode, RefKind, SourceTier, SymbolKind, Tier3AnalyzerStatus, Tier3RepoStatus, Tier3Status,
    Tier3StatusBody, Timing, TypeRole,
};
