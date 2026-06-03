//! Workspace-level analyzer boundary.
//!
//! Per-language [`cairn_lang_api::Analyzer`] implementations operate
//! on one source blob at a time. LSP-class analyzers such as
//! rust-analyzer need a wider view: a repository root, a manifest, and
//! the set of files visible in that snapshot. This module defines that
//! boundary without starting any external analyzer process yet.

use std::path::{Path, PathBuf};

use linkme::distributed_slice;

use crate::Result;
use crate::manifest::ManifestId;

/// Linker-time registry of workspace analyzers.
///
/// Future analyzer crates or modules contribute constructors with
/// `#[distributed_slice(WORKSPACE_ANALYZERS)]`, mirroring the language
/// backend and JSON-RPC method registries.
#[allow(unsafe_code)]
#[distributed_slice]
pub static WORKSPACE_ANALYZERS: [fn() -> Box<dyn WorkspaceAnalyzer>] = [..];

/// Collect every registered workspace analyzer.
#[must_use]
pub fn all_workspace_analyzers() -> Vec<Box<dyn WorkspaceAnalyzer>> {
    WORKSPACE_ANALYZERS.iter().map(|ctor| ctor()).collect()
}

/// Analyzer that can derive facts from a repository snapshot.
pub trait WorkspaceAnalyzer: Send + Sync {
    /// Stable analyzer identifier, e.g. `"rust-analyzer-lsp"`.
    fn id(&self) -> &'static str;

    /// Monotonic revision for this analyzer's output.
    fn revision(&self) -> u32;

    /// Short language tag this analyzer enriches, e.g. `"rust"`.
    fn language(&self) -> &'static str;

    /// Analyze one manifest worth of files rooted at `repo_root`.
    ///
    /// PR1 only establishes the boundary. Later PRs will add concrete
    /// fields to [`WorkspaceFacts`] and wire a long-lived analyzer
    /// service behind this call.
    fn analyze_workspace(
        &self,
        repo_root: &Path,
        manifest_id: ManifestId,
        files: &[WorkspaceFile],
    ) -> Result<WorkspaceFacts>;
}

/// One file visible to a [`WorkspaceAnalyzer`] within a manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceFile {
    /// Path relative to the registered repository root.
    pub path: String,
    /// Blob SHA recorded by the manifest for this path.
    pub blob_sha: String,
    /// Absolute path when the file is materialized in the worktree.
    pub worktree_path: Option<PathBuf>,
}

/// Placeholder fact bundle for workspace analyzers.
///
/// PR3 will add resolved reference facts here. Keeping the type empty
/// in PR1 lets downstream code depend on the boundary without
/// committing to the rust-analyzer fact shape too early.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WorkspaceFacts;

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeWorkspaceAnalyzer;

    impl WorkspaceAnalyzer for FakeWorkspaceAnalyzer {
        fn id(&self) -> &'static str {
            "fake-workspace"
        }

        fn revision(&self) -> u32 {
            7
        }

        fn language(&self) -> &'static str {
            "fake"
        }

        fn analyze_workspace(
            &self,
            _repo_root: &Path,
            _manifest_id: ManifestId,
            _files: &[WorkspaceFile],
        ) -> Result<WorkspaceFacts> {
            Ok(WorkspaceFacts)
        }
    }

    #[allow(unsafe_code)]
    #[distributed_slice(WORKSPACE_ANALYZERS)]
    static FAKE_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
        || Box::new(FakeWorkspaceAnalyzer);

    #[test]
    fn discovers_registered_workspace_analyzer() {
        let analyzers = all_workspace_analyzers();
        let fake = analyzers
            .iter()
            .find(|a| a.id() == "fake-workspace")
            .expect("fake workspace analyzer should be registered");

        assert_eq!(fake.revision(), 7);
        assert_eq!(fake.language(), "fake");
    }

    #[test]
    fn workspace_analyzer_boundary_accepts_manifest_context() {
        let analyzer = FakeWorkspaceAnalyzer;
        let files = [WorkspaceFile {
            path: "src/lib.rs".into(),
            blob_sha: "sha1".into(),
            worktree_path: Some(PathBuf::from("/tmp/repo/src/lib.rs")),
        }];

        let facts = analyzer
            .analyze_workspace(Path::new("/tmp/repo"), ManifestId(42), &files)
            .unwrap();

        assert_eq!(facts, WorkspaceFacts);
    }
}
