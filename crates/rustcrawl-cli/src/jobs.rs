//! Local crawl job history.
//!
//! The CLI stores completed interactive/script runs as JSON Lines under
//! `.rustcrawl/jobs.jsonl` by default. It is intentionally local and simple:
//! enough for Crawl Deck without pretending to be a scheduler.

use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

use anyhow::Context;
use chrono::{DateTime, Utc};
use rustcrawl_core::{CrawlSummary, DEFAULT_CONCURRENCY};
use serde::{Deserialize, Serialize};

use crate::cli::Cli;
use crate::progress::NewJobSpec;

const JOB_DIR: &str = ".rustcrawl";
const JOB_FILE: &str = "jobs.jsonl";
const RECENT_LIMIT: usize = 25;

fn default_concurrency() -> usize {
    DEFAULT_CONCURRENCY
}

fn default_respect_crawl_delay() -> bool {
    true
}

/// A completed crawl run recorded by the local CLI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JobRecord {
    /// Monotonic-ish local identifier, currently based on UTC nanoseconds.
    pub id: String,
    /// When the run completed.
    pub finished_at: DateTime<Utc>,
    /// Seeds passed on the command line.
    pub seeds: Vec<String>,
    /// Sitemap seeds passed on the command line.
    pub sitemaps: Vec<String>,
    /// Max page limit for the run.
    pub max_pages: Option<usize>,
    /// Max depth for the run.
    pub max_depth: Option<u32>,
    /// Configured concurrency.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Crawl scope used for this run.
    #[serde(default)]
    pub scope: Option<String>,
    /// Per host delay used for this run.
    #[serde(default)]
    pub delay: Option<String>,
    /// Whether robots.txt Crawl-delay could raise the configured per host delay.
    #[serde(default = "default_respect_crawl_delay")]
    pub respect_crawl_delay: bool,
    /// Output file path, if any.
    pub output: Option<String>,
    /// Pages fetched successfully.
    pub pages_fetched: u64,
    /// Pages that failed.
    pub pages_failed: u64,
    /// URLs accepted into the crawl frontier.
    pub urls_discovered: u64,
    /// Bytes downloaded.
    pub bytes_downloaded: u64,
    /// Wall-clock runtime in seconds.
    pub duration_secs: f64,
}

impl JobRecord {
    /// Fetched pages per second for this run.
    pub(crate) fn pages_per_sec(&self) -> f64 {
        if self.duration_secs > 0.0 {
            self.pages_fetched as f64 / self.duration_secs
        } else {
            0.0
        }
    }

    /// Success rate across fetched + failed page attempts.
    pub(crate) fn success_rate(&self) -> f64 {
        let total = self.pages_fetched + self.pages_failed;
        if total > 0 {
            self.pages_fetched as f64 / total as f64
        } else {
            1.0
        }
    }

    /// Short target label for terminal display.
    pub(crate) fn target_label(&self) -> String {
        self.seeds
            .first()
            .or_else(|| self.sitemaps.first())
            .cloned()
            .unwrap_or_else(|| "(no target)".to_string())
    }
}

/// Crawl settings captured before a run. Converted into a [`JobRecord`] once
/// the run completes and has a real summary.
#[derive(Debug, Clone)]
pub(crate) struct JobTemplate {
    seeds: Vec<String>,
    sitemaps: Vec<String>,
    max_pages: Option<usize>,
    max_depth: Option<u32>,
    concurrency: usize,
    scope: Option<String>,
    delay: Option<String>,
    respect_crawl_delay: bool,
    output: Option<String>,
}

impl JobTemplate {
    /// Capture job settings from CLI arguments.
    pub(crate) fn from_cli(cli: &Cli) -> Self {
        Self {
            seeds: cli.seeds.clone(),
            sitemaps: cli.sitemap.clone(),
            max_pages: cli.max_pages,
            max_depth: cli.depth,
            concurrency: cli.concurrency,
            scope: Some(format!("{:?}", cli.scope).to_ascii_lowercase()),
            delay: Some(humantime::format_duration(cli.delay).to_string()),
            respect_crawl_delay: !cli.ignore_robots && !cli.ignore_crawl_delay,
            output: cli.output.as_ref().map(|p| p.display().to_string()),
        }
    }

    /// Capture job settings from a dashboard form.
    pub(crate) fn from_new_job(spec: &NewJobSpec) -> Self {
        Self {
            seeds: vec![spec.target.clone()],
            sitemaps: Vec::new(),
            max_pages: spec.max_pages,
            max_depth: spec.depth,
            concurrency: spec.concurrency,
            scope: Some(spec.scope.clone()),
            delay: Some(spec.delay.clone()),
            respect_crawl_delay: spec.respect_crawl_delay,
            output: spec.output.clone(),
        }
    }

    /// Add completion stats and produce a persisted job record.
    pub(crate) fn into_record(self, summary: &CrawlSummary) -> JobRecord {
        let now = Utc::now();
        JobRecord {
            id: format!("{}", now.timestamp_nanos_opt().unwrap_or_default()),
            finished_at: now,
            seeds: self.seeds,
            sitemaps: self.sitemaps,
            max_pages: self.max_pages,
            max_depth: self.max_depth,
            concurrency: self.concurrency,
            scope: self.scope,
            delay: self.delay,
            respect_crawl_delay: self.respect_crawl_delay,
            output: self.output,
            pages_fetched: summary.pages_fetched,
            pages_failed: summary.pages_failed,
            urls_discovered: summary.urls_discovered,
            bytes_downloaded: summary.bytes_downloaded,
            duration_secs: summary.duration.as_secs_f64(),
        }
    }
}

/// In-memory snapshot used by the dashboard.
#[derive(Debug, Clone, Default)]
pub(crate) struct JobHistory {
    /// Most recent jobs, oldest to newest.
    pub recent: Vec<JobRecord>,
    /// Aggregate stats across all saved jobs.
    pub stats: JobStats,
}

/// Aggregate stats across saved jobs.
#[derive(Debug, Clone, Default)]
pub(crate) struct JobStats {
    /// Number of saved jobs.
    pub total_jobs: u64,
    /// Average wall-clock runtime in seconds.
    pub avg_runtime_secs: f64,
    /// Average fetched pages per second.
    pub avg_pages_per_sec: f64,
    /// Average success rate.
    pub avg_success_rate: f64,
    /// Total successfully fetched pages.
    pub total_pages_fetched: u64,
    /// Total failed pages.
    pub total_pages_failed: u64,
}

/// JSONL-backed local job store.
#[derive(Debug, Clone)]
pub(crate) struct JobStore {
    path: PathBuf,
}

impl JobStore {
    /// Use the default local store path: `.rustcrawl/jobs.jsonl`.
    pub(crate) fn local() -> Self {
        Self {
            path: PathBuf::from(JOB_DIR).join(JOB_FILE),
        }
    }

    /// Append a completed run to the history file.
    pub(crate) fn append(&self, record: &JobRecord) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "could not create job history directory {}",
                    parent.display()
                )
            })?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("could not open job history {}", self.path.display()))?;
        serde_json::to_writer(&mut file, record)?;
        file.write_all(b"\n")?;
        Ok(())
    }

    /// Remove all saved job history. Missing history is treated as already
    /// clear.
    pub(crate) fn clear(&self) -> anyhow::Result<()> {
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err)
                .with_context(|| format!("could not clear job history {}", self.path.display())),
        }
    }

    /// Load recent jobs and aggregate stats. Malformed lines are skipped so one
    /// bad manual edit does not break the control center.
    pub(crate) fn load(&self) -> JobHistory {
        let Ok(file) = OpenOptions::new().read(true).open(&self.path) else {
            return JobHistory::default();
        };

        let mut all = Vec::new();
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(record) = serde_json::from_str::<JobRecord>(&line) {
                all.push(record);
            }
        }

        let stats = JobStats::from_records(&all);
        let start = all.len().saturating_sub(RECENT_LIMIT);
        JobHistory {
            recent: all[start..].to_vec(),
            stats,
        }
    }
}

impl JobStats {
    fn from_records(records: &[JobRecord]) -> Self {
        if records.is_empty() {
            return Self::default();
        }

        let total_jobs = records.len() as u64;
        let total_pages_fetched = records.iter().map(|r| r.pages_fetched).sum();
        let total_pages_failed = records.iter().map(|r| r.pages_failed).sum();
        let avg_runtime_secs =
            records.iter().map(|r| r.duration_secs).sum::<f64>() / total_jobs as f64;
        let avg_pages_per_sec =
            records.iter().map(JobRecord::pages_per_sec).sum::<f64>() / total_jobs as f64;
        let avg_success_rate =
            records.iter().map(JobRecord::success_rate).sum::<f64>() / total_jobs as f64;

        Self {
            total_jobs,
            avg_runtime_secs,
            avg_pages_per_sec,
            avg_success_rate,
            total_pages_fetched,
            total_pages_failed,
        }
    }
}
