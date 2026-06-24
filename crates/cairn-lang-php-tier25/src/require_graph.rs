//! `use Foo\Bar;` → workspace file resolution.
//!
//! Tier-2.5 only resolves PHP `use` imports whose target lies inside
//! the workspace — vendor / stdlib `use`s stay unresolved (Tier-3
//! territory). We resolve by looking the imported fully-qualified
//! class name up in the workspace `ConstIndex`. This is the PSR-4
//! best-effort path: we don't need composer.json, since if the
//! workspace defines class `\App\Models\Widget`, we know which file
//! it lives in.

use std::collections::HashMap;

use crate::const_resolver::{ConstIndex, FileConstFacts};

#[derive(Debug, Clone)]
pub struct RequireEdge {
    pub site_byte_start: u32,
    pub site_byte_end: u32,
    pub target_path: Option<String>,
    pub target_qualified: Option<String>,
}

#[derive(Debug, Default)]
pub struct RequireGraph {
    edges: HashMap<String, Vec<RequireEdge>>,
}

impl RequireGraph {
    pub fn build(per_file: &[(String, Vec<u8>, FileConstFacts)], const_index: &ConstIndex) -> Self {
        let mut edges: HashMap<String, Vec<RequireEdge>> = HashMap::new();
        for (path, _, facts) in per_file {
            let mut list = Vec::new();
            for u in &facts.use_imports {
                // Exact lookup — `use App\Models\Widget;` resolves to
                // whatever file defined `App\Models\Widget`. If the
                // workspace has no such symbol, we record the site
                // with target=None and the row's `target_path` /
                // `target_qualified` stay null, matching how Ruby's
                // require-graph records misses.
                let target = const_index.get(&u.qualified);
                list.push(RequireEdge {
                    site_byte_start: u.byte_start,
                    site_byte_end: u.byte_end,
                    target_path: target.map(|t| t.path.clone()),
                    target_qualified: target.map(|t| t.qualified.clone()),
                });
            }
            edges.insert(path.clone(), list);
        }
        Self { edges }
    }

    pub fn edges_for(&self, path: &str) -> &[RequireEdge] {
        self.edges.get(path).map(Vec::as_slice).unwrap_or(&[])
    }
}
