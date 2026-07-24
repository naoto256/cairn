//! Concrete control-socket methods.
//!
//! Each sub-module owns one admin verb end-to-end. Adding a new verb
//! is a single-file change; the dispatcher in [`super::CtlHandler`]
//! picks it up automatically via the [`super::CONTROL_METHODS`]
//! distributed slice.
//!
//! Sub-modules are kept `mod`-private: every `ControlMethod` is
//! registered into the linker-time slice via
//! `#[distributed_slice(CONTROL_METHODS)]`, so callers never need
//! to name them directly — the trait object is what escapes.

mod doctor;
mod jobs;
mod prune;
mod register_repo;
mod reindex_repo;
mod remove_repo;
mod shutdown;
mod status;
