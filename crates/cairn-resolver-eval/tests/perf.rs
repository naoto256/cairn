//! Record-only resolver performance benchmark.
//!
//! Run with:
//! `cargo test -p cairn-resolver-eval --test perf -- --ignored --nocapture`
//!
//! Set `CAIRN_EVAL_LSP=1` to include the local clangd multi-kind pass.
//! Results are informational: this test is ignored in CI and has no threshold.

use std::collections::BTreeSet;
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use cairn_core::lsp::Position;
use cairn_core::lsp::pool::{AvailabilityStrategy, LspSpawnSpec, ReadinessStrategy};
use cairn_core::workspace_analyzer::{
    AnalyzerProgress, DefinitionRetryPolicy, DefinitionSite, LspDefinitionCollector,
    LspMultiKindDefinitionPass, WorkspaceFile, run_lsp_multi_kind_definition_pass,
};
use cairn_proto::RefKind;
use cairn_resolver_eval::cases::all_cases;
use cairn_resolver_eval::{GoldenCase, RegisteredFixture, Tool, register_fixture};
use serde::Serialize;
use serde_json::json;

const SAMPLE_COUNT: usize = 5;
const LANGUAGES: &[&str] = &[
    "rust",
    "python",
    "typescript",
    "java",
    "php",
    "kotlin",
    "swift",
    "csharp",
    "javascript",
    "ruby",
];
const TIER25_LANGUAGES: &[&str] = &[
    "python",
    "php",
    "kotlin",
    "swift",
    "csharp",
    "javascript",
    "ruby",
];

#[derive(Debug, Serialize)]
struct PerfReport {
    samples: usize,
    languages: Vec<LanguagePerf>,
    lsp_pool: Option<LspPoolPerf>,
}

#[derive(Debug, Serialize)]
struct LanguagePerf {
    language: String,
    register_median_ms: f64,
    find_subtypes: Option<QueryPerf>,
    find_references: Option<QueryPerf>,
}

#[derive(Debug, Serialize)]
struct QueryPerf {
    cases: usize,
    tier25_median_ms: f64,
    tier2_median_ms: f64,
    tier25_to_tier2_ratio: Option<f64>,
}

#[derive(Debug, Serialize)]
struct LspPoolPerf {
    binary: String,
    samples: usize,
    multi_kind_median_ms: f64,
    collectors: usize,
    files: usize,
    did_open_count_per_pass: usize,
    did_open_count_total: usize,
}

#[derive(Debug, Clone, Copy)]
enum QueryGroup {
    FindSubtypes,
    FindReferences,
}

impl QueryGroup {
    fn includes(self, tool: Tool) -> bool {
        match self {
            Self::FindSubtypes => tool == Tool::FindSubtypes,
            Self::FindReferences => matches!(tool, Tool::FindCallers | Tool::FindCallees),
        }
    }
}

#[test]
#[ignore = "record-only benchmark; run explicitly with --ignored --nocapture"]
fn resolver_perf_record() -> Result<()> {
    let all = all_cases();
    let actual_languages: BTreeSet<_> = all.iter().map(|case| case.language).collect();
    let configured_languages: BTreeSet<_> = LANGUAGES.iter().copied().collect();
    if actual_languages != configured_languages {
        bail!(
            "benchmark language list drifted: cases={actual_languages:?}, configured={configured_languages:?}"
        );
    }

    let mut languages = Vec::with_capacity(LANGUAGES.len());
    for language in LANGUAGES {
        let cases: Vec<_> = all
            .iter()
            .filter(|case| case.language == *language)
            .collect();
        languages.push(measure_language(language, &cases)?);
    }

    let lsp_pool = if std::env::var("CAIRN_EVAL_LSP").as_deref() == Ok("1") {
        Some(measure_lsp_pool()?)
    } else {
        println!("\nLSP pool: skipped (set CAIRN_EVAL_LSP=1 to run locally)");
        None
    };

    let report = PerfReport {
        samples: SAMPLE_COUNT,
        languages,
        lsp_pool,
    };
    print_markdown(&report);
    write_json(&report)?;
    Ok(())
}

fn measure_language(language: &str, cases: &[&GoldenCase]) -> Result<LanguagePerf> {
    let subtype_cases = select_cases(cases, QueryGroup::FindSubtypes);
    let reference_cases = select_cases(cases, QueryGroup::FindReferences);
    let has_tier25 = TIER25_LANGUAGES.contains(&language);

    let mut register_samples = Vec::with_capacity(SAMPLE_COUNT);
    let mut subtype_tier25 = Vec::new();
    let mut subtype_tier2 = Vec::new();
    let mut references_tier25 = Vec::new();
    let mut references_tier2 = Vec::new();

    for _ in 0..SAMPLE_COUNT {
        let fixture = register_fixture(language)
            .with_context(|| format!("register {language} benchmark fixture"))?;
        register_samples.push(fixture.register_elapsed());

        if has_tier25 {
            measure_if_present(&fixture, &subtype_cases, &mut subtype_tier25)?;
            measure_if_present(&fixture, &reference_cases, &mut references_tier25)?;

            let deleted = fixture.delete_tier25_resolutions()?;
            if deleted == 0 {
                bail!("{language} benchmark deleted zero Tier-2.5 resolutions");
            }

            measure_if_present(&fixture, &subtype_cases, &mut subtype_tier2)?;
            measure_if_present(&fixture, &reference_cases, &mut references_tier2)?;
        }
    }

    Ok(LanguagePerf {
        language: language.to_string(),
        register_median_ms: duration_ms(median(&mut register_samples)),
        find_subtypes: query_perf(&subtype_cases, &mut subtype_tier25, &mut subtype_tier2),
        find_references: query_perf(
            &reference_cases,
            &mut references_tier25,
            &mut references_tier2,
        ),
    })
}

fn select_cases<'a>(cases: &'a [&'a GoldenCase], group: QueryGroup) -> Vec<&'a GoldenCase> {
    cases
        .iter()
        .copied()
        .filter(|case| group.includes(case.tool))
        .collect()
}

fn measure_if_present(
    fixture: &RegisteredFixture,
    cases: &[&GoldenCase],
    samples: &mut Vec<Duration>,
) -> Result<()> {
    if cases.is_empty() {
        return Ok(());
    }
    let started = Instant::now();
    let mut hit_count = 0usize;
    for case in cases {
        hit_count = hit_count.saturating_add(fixture.run_query(case)?.len());
    }
    black_box(hit_count);
    samples.push(started.elapsed());
    Ok(())
}

fn query_perf(
    cases: &[&GoldenCase],
    tier25_samples: &mut [Duration],
    tier2_samples: &mut [Duration],
) -> Option<QueryPerf> {
    if cases.is_empty() || tier25_samples.is_empty() || tier2_samples.is_empty() {
        return None;
    }
    let tier25 = duration_ms(median(tier25_samples));
    let tier2 = duration_ms(median(tier2_samples));
    Some(QueryPerf {
        cases: cases.len(),
        tier25_median_ms: tier25,
        tier2_median_ms: tier2,
        tier25_to_tier2_ratio: finite_ratio(tier25, tier2),
    })
}

fn finite_ratio(numerator: f64, denominator: f64) -> Option<f64> {
    let ratio = numerator / denominator;
    ratio.is_finite().then_some(ratio)
}

fn median(samples: &mut [Duration]) -> Duration {
    assert_eq!(samples.len(), SAMPLE_COUNT, "median requires N=5 samples");
    samples.sort_unstable();
    samples[SAMPLE_COUNT / 2]
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn print_markdown(report: &PerfReport) {
    println!(
        "\n| language | register ms | subtypes T2.5 ms | subtypes T2 ms | ratio | references T2.5 ms | references T2 ms | ratio |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|");
    for language in &report.languages {
        println!(
            "| {} | {:.3} | {} | {} | {} | {} | {} | {} |",
            language.language,
            language.register_median_ms,
            metric_value(language.find_subtypes.as_ref(), |m| m.tier25_median_ms),
            metric_value(language.find_subtypes.as_ref(), |m| m.tier2_median_ms),
            optional_metric_value(language.find_subtypes.as_ref(), |m| {
                m.tier25_to_tier2_ratio
            }),
            metric_value(language.find_references.as_ref(), |m| m.tier25_median_ms),
            metric_value(language.find_references.as_ref(), |m| m.tier2_median_ms),
            optional_metric_value(language.find_references.as_ref(), |m| {
                m.tier25_to_tier2_ratio
            }),
        );
    }
    if let Some(lsp) = &report.lsp_pool {
        println!(
            "\n| LSP binary | multi-kind ms | collectors | files | didOpen/pass | didOpen total |"
        );
        println!("|---|---:|---:|---:|---:|---:|");
        println!(
            "| {} | {:.3} | {} | {} | {} | {} |",
            lsp.binary,
            lsp.multi_kind_median_ms,
            lsp.collectors,
            lsp.files,
            lsp.did_open_count_per_pass,
            lsp.did_open_count_total,
        );
    }
}

fn metric_value(metric: Option<&QueryPerf>, value: impl FnOnce(&QueryPerf) -> f64) -> String {
    metric
        .map(|metric| format!("{:.3}", value(metric)))
        .unwrap_or_else(|| "-".to_string())
}

fn optional_metric_value(
    metric: Option<&QueryPerf>,
    value: impl FnOnce(&QueryPerf) -> Option<f64>,
) -> String {
    metric
        .and_then(value)
        .map(|value| format!("{value:.3}"))
        .unwrap_or_else(|| "-".to_string())
}

fn write_json(report: &PerfReport) -> Result<()> {
    let target = target_dir().join("eval-perf.json");
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(report).context("serialize perf report")?;
    fs::write(&target, bytes).with_context(|| format!("write {}", target.display()))?;
    println!("\nwrote {}", target.display());
    Ok(())
}

fn target_dir() -> PathBuf {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("resolver-eval crate must live below workspace/crates");
    match std::env::var_os("CARGO_TARGET_DIR") {
        Some(path) if Path::new(&path).is_absolute() => PathBuf::from(path),
        Some(path) => workspace.join(path),
        None => workspace.join("target"),
    }
}

fn measure_lsp_pool() -> Result<LspPoolPerf> {
    let fixture = LspFixture::new()?;
    let did_open_count_per_pass = fixture.did_open_count()?;
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);

    for _ in 0..SAMPLE_COUNT {
        let started = Instant::now();
        let facts = run_lsp_multi_kind_definition_pass(
            fixture.pass(),
            fixture.root(),
            fixture.files(),
            &AnalyzerProgress::default(),
        )?;
        black_box(facts.resolved_refs.len());
        samples.push(started.elapsed());
    }

    Ok(LspPoolPerf {
        binary: fixture.binary.display().to_string(),
        samples: SAMPLE_COUNT,
        multi_kind_median_ms: duration_ms(median(&mut samples)),
        collectors: 2,
        files: fixture.files.len(),
        did_open_count_per_pass,
        did_open_count_total: did_open_count_per_pass * SAMPLE_COUNT,
    })
}

struct LspFixture {
    _tempdir: tempfile::TempDir,
    root: PathBuf,
    files: Vec<WorkspaceFile>,
    binary: PathBuf,
}

impl LspFixture {
    fn new() -> Result<Self> {
        let tempdir = tempfile::tempdir().context("create LSP perf fixture")?;
        let root = tempdir.path().to_path_buf();
        let sources = [
            ("api.h", "int helper(int value);\n"),
            (
                "helper.c",
                "#include \"api.h\"\nint helper(int value) { return value + 1; }\n",
            ),
            (
                "main.c",
                "#include \"api.h\"\nint main(void) { return helper(41); }\n",
            ),
        ];
        let mut files = Vec::with_capacity(sources.len());
        for (path, source) in sources {
            let worktree_path = root.join(path);
            fs::write(&worktree_path, source)
                .with_context(|| format!("write {}", worktree_path.display()))?;
            files.push(WorkspaceFile {
                path: path.to_string(),
                blob_sha: format!("eval-{path}"),
                worktree_path: Some(worktree_path),
                source_bytes: None,
            });
        }

        let binary = std::env::var_os("CAIRN_EVAL_LSP_BINARY")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("clangd"));
        Ok(Self {
            _tempdir: tempdir,
            root,
            files,
            binary,
        })
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn files(&self) -> &[WorkspaceFile] {
        &self.files
    }

    fn pass(&self) -> LspMultiKindDefinitionPass {
        LspMultiKindDefinitionPass {
            analyzer_id: "resolver-eval-clangd",
            pool_analyzer_id: None,
            language: "c",
            spawn_spec: LspSpawnSpec {
                binary: self.binary.clone(),
                workspace_root: self.root.clone(),
                config_hash: "resolver-eval-clangd-v1".to_string(),
                request_timeout: Duration::from_secs(20),
                availability: AvailabilityStrategy::VersionFlag,
                readiness: ReadinessStrategy::InitializeResponseOnly,
                language_id: "c",
                launch_args: Vec::new(),
                env: Vec::new(),
                initialization_options: json!({}),
            },
            retry: DefinitionRetryPolicy::default(),
            collectors: collectors(),
            suppress_definition_targets_at_requested_sites: false,
        }
    }

    fn did_open_count(&self) -> Result<usize> {
        let collectors = collectors();
        let mut count = 0;
        for file in &self.files {
            let path = file
                .worktree_path
                .as_ref()
                .context("LSP perf file missing worktree path")?;
            let source = fs::read(path).with_context(|| format!("read {}", path.display()))?;
            let mut has_sites = false;
            for collector in &collectors {
                if !(collector.collect_definition_sites)(&source)?.is_empty() {
                    has_sites = true;
                }
            }
            count += usize::from(has_sites);
        }
        Ok(count)
    }
}

fn collectors() -> Vec<LspDefinitionCollector> {
    vec![
        LspDefinitionCollector {
            ref_kind: RefKind::Call,
            collect_definition_sites: collect_helper_sites,
        },
        LspDefinitionCollector {
            ref_kind: RefKind::Import,
            collect_definition_sites: collect_header_sites,
        },
    ]
}

fn collect_helper_sites(source: &[u8]) -> cairn_core::Result<Vec<DefinitionSite>> {
    Ok(collect_token_sites(source, b"helper"))
}

fn collect_header_sites(source: &[u8]) -> cairn_core::Result<Vec<DefinitionSite>> {
    Ok(collect_token_sites(source, b"api.h"))
}

fn collect_token_sites(source: &[u8], token: &[u8]) -> Vec<DefinitionSite> {
    let mut sites = Vec::new();
    let mut offset = 0;
    while let Some(relative) = source[offset..]
        .windows(token.len())
        .position(|window| window == token)
    {
        let byte_start = offset + relative;
        let line_start = source[..byte_start]
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |newline| newline + 1);
        let line = source[..byte_start]
            .iter()
            .filter(|byte| **byte == b'\n')
            .count() as u32;
        sites.push(DefinitionSite {
            position: Position {
                line,
                character: (byte_start - line_start) as u32,
            },
            byte_start,
            byte_end: byte_start + token.len(),
        });
        offset = byte_start + token.len();
    }
    sites
}

#[test]
fn median_selects_middle_of_five_samples() {
    let mut samples = [
        Duration::from_millis(50),
        Duration::from_millis(10),
        Duration::from_millis(40),
        Duration::from_millis(20),
        Duration::from_millis(30),
    ];
    assert_eq!(median(&mut samples), Duration::from_millis(30));
}

#[test]
fn non_finite_perf_ratio_serializes_as_null() {
    let perf = QueryPerf {
        cases: 1,
        tier25_median_ms: 0.0,
        tier2_median_ms: 0.0,
        tier25_to_tier2_ratio: finite_ratio(0.0, 0.0),
    };

    assert_eq!(perf.tier25_to_tier2_ratio, None);
    assert_eq!(
        serde_json::to_value(perf).unwrap()["tier25_to_tier2_ratio"],
        json!(null)
    );
}

#[test]
fn token_sites_use_zero_based_lsp_positions() {
    let sites = collect_token_sites(b"first\n  helper();\n", b"helper");
    assert_eq!(sites.len(), 1);
    assert_eq!(
        sites[0].position,
        Position {
            line: 1,
            character: 2
        }
    );
    assert_eq!(sites[0].byte_start, 8);
    assert_eq!(sites[0].byte_end, 14);
}
