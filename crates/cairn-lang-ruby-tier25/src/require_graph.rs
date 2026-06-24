//! `require` / `require_relative` / `autoload` file-path resolution.
//!
//! Tier-2.5 only resolves paths whose target lies inside the workspace —
//! stdlib / gem `require`s stay unresolved (Tier-3 territory).

use std::collections::HashSet;
use std::path::PathBuf;

use crate::const_resolver::{FileConstFacts, RequireKind};

#[derive(Debug, Clone)]
pub struct RequireEdge {
    pub site_byte_start: u32,
    pub site_byte_end: u32,
    pub target_path: Option<String>,
    pub target_qualified: Option<String>,
}

#[derive(Debug, Default)]
pub struct RequireGraph {
    edges: std::collections::HashMap<String, Vec<RequireEdge>>,
}

impl RequireGraph {
    pub fn build(per_file: &[(String, Vec<u8>, FileConstFacts)], file_paths: &[String]) -> Self {
        let workspace: HashSet<&str> = file_paths.iter().map(String::as_str).collect();
        let mut edges = std::collections::HashMap::new();
        for (path, _, facts) in per_file {
            let mut list = Vec::new();
            for req in &facts.requires {
                let resolved = match req.kind {
                    RequireKind::RequireRelative => {
                        resolve_relative(path, &req.literal, file_paths)
                    }
                    RequireKind::Require => resolve_workspace_require(&req.literal, &workspace),
                };
                list.push(RequireEdge {
                    site_byte_start: req.byte_start,
                    site_byte_end: req.byte_end,
                    // Use the path as the qualified key — symbols don't
                    // have a 1-1 import-binding name in Ruby. Persist
                    // tolerates this by looking the symbol up by the
                    // first symbol in the target blob; if none, the row
                    // still records site→file with target_symbol_id=NULL.
                    target_qualified: resolved.clone(),
                    target_path: resolved,
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

/// `require_relative "../foo/bar"` from `dir/file.rb` → `dir/../foo/bar.rb`,
/// returned only when the resolved path exists in `file_paths`.
pub fn resolve_relative(from: &str, literal: &str, file_paths: &[String]) -> Option<String> {
    let from_path = PathBuf::from(from);
    let base = from_path.parent().map(PathBuf::from).unwrap_or_default();
    let joined = base.join(literal);
    let normalized = normalize_path(&joined);
    [format!("{normalized}.rb"), normalized.clone()]
        .into_iter()
        .find(|cand| file_paths.contains(cand))
}

fn resolve_workspace_require(literal: &str, workspace: &HashSet<&str>) -> Option<String> {
    let candidates = [
        format!("{literal}.rb"),
        format!("lib/{literal}.rb"),
        format!("app/{literal}.rb"),
    ];
    candidates
        .into_iter()
        .find(|cand| workspace.contains(cand.as_str()))
}

fn normalize_path(path: &std::path::Path) -> String {
    let mut out: Vec<String> = Vec::new();
    for comp in path.components() {
        use std::path::Component;
        match comp {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
            Component::Normal(s) => out.push(s.to_string_lossy().into_owned()),
        }
    }
    out.join("/")
}
