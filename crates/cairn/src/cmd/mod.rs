//! `cairn` subcommand handlers. Each module owns one of the
//! top-level subcommands and exposes a `run(Args)` entry point.

pub mod ctl;
pub mod daemon;
pub mod mcp;
pub mod query;
mod version_guard;
