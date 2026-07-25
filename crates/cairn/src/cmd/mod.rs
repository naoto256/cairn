//! `cairn` subcommand handlers. Each module owns one of the
//! top-level subcommands and exposes a `run(Args)` entry point.
//!
//! The shared helpers stay crate-private: [`rpc_client`] wraps the
//! newline-delimited JSON-RPC round trip against the daemon's UDS,
//! and [`version_guard`] warns/aborts on daemon/CLI version drift
//! before either front-end (CLI or MCP) starts issuing real
//! requests.

pub mod ctl;
pub mod daemon;
pub mod mcp;
pub mod query;
mod rpc_client;
mod version_guard;
