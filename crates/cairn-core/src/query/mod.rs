//! Query layer over the CAS store.
//!
//! Resolves an anchor to a `manifest_id`, then joins indexed facts against
//! `manifest_entries` filtered by that manifest so each query is scoped to one
//! snapshot's visible blobs.
//!
//! Two query shapes coexist:
//!
//! * **Resolution-aware** — `find_impls`, `find_imports`, and
//!   `find_references` LEFT JOIN the Tier-1/Tier-2 fact tables (`refs`,
//!   `imports`, `implementations`) against `resolutions` rows written by
//!   Tier-2-direct / Tier-2.5 / Tier-3 passes. A `best_resolution` CTE
//!   picks the highest-ranked row per site via
//!   [`crate::workspace_analyzer::source_rank_case_sql`] and the queries
//!   surface a `kind_source` provenance string (either the resolution
//!   `source` when covered, or the sentinel `KIND_SOURCE_FACT`
//!   (`"tier2-fact"`) when only the fact row is available).
//! * **Declaration reads** — `get_outline` and `get_symbol_source`
//!   read directly from fact tables. `find_symbols` does the same for
//!   ordinary lookups; only `include_inherited` consults resolution
//!   authority to decide which owners' declarations join the result.

mod find_impls;
mod find_imports;
mod find_references;
mod find_symbols;
mod get_outline;
mod get_symbol_source;

pub use find_impls::{
    FindSubtypesArgs, FindSupertypesArgs, ImplHit, KIND_SOURCE_FACT, find_subtypes, find_supertypes,
};
pub use find_imports::{FindImportsArgs, ImportHit, find_imports};
pub use find_references::{FindReferencesArgs, ReferenceHit, find_references};
pub(crate) use find_symbols::find_symbols_with_status;
pub use find_symbols::{FindSymbolsArgs, SymbolHit, find_symbols};
pub use get_outline::{OutlineFilter, OutlineItem, get_outline, get_outline_under_path};
pub use get_symbol_source::{SymbolSourceRow, get_symbol_source_row, get_symbol_source_rows};

#[cfg(test)]
mod tests;
