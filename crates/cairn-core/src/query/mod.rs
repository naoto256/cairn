//! Query layer over the CAS store.
//!
//! Resolves an anchor to a `manifest_id`, then joins indexed facts against
//! `manifest_entries` filtered by that manifest so each query is scoped to one
//! snapshot's visible blobs.

mod find_impls;
mod find_imports;
mod find_references;
mod find_symbols;
mod get_outline;
mod get_symbol_source;

pub use find_impls::{
    FindSubtypesArgs, FindSupertypesArgs, ImplHit, find_subtypes, find_supertypes,
};
pub use find_imports::{FindImportsArgs, ImportHit, find_imports};
pub use find_references::{FindReferencesArgs, ReferenceHit, find_references};
pub use find_symbols::{FindSymbolsArgs, SymbolHit, find_symbols};
pub use get_outline::{OutlineFilter, OutlineItem, get_outline, get_outline_under_path};
pub use get_symbol_source::{SymbolSourceRow, get_symbol_source_row};

#[cfg(test)]
mod tests;
