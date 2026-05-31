//! Concrete control-socket methods.
//!
//! Each sub-module owns one admin verb end-to-end. Adding a new verb
//! is a single-file change; the dispatcher in [`super::CtlHandler`]
//! picks it up automatically via the [`super::CONTROL_METHODS`]
//! distributed slice.

mod doctor;
mod register_repo;
mod reindex_repo;
mod remove_repo;
mod shutdown;
mod status;
