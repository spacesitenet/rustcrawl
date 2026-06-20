//! Command-line argument definitions and their mapping to a [`CrawlConfig`].

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context};
use clap::{Parser, ValueEnum};
use rustcrawl_core::{CrawlConfig, Scope, DEFAULT_CONCURRENCY, MAX_CONCURRENCY};
use url::Url;

/// A fast, efficient web crawler.
///
/// Crawls one or more seed URLs breadth-first, staying within a configurable
/// scope, and writes one JSON object per page (JSON Lines) to stdout or a file.
#[derive(Debug, Parser)]
#[command(name = "rustcrawl", version, about, long_about = None)]
pub(crate) struct Cli {
    /// Seed URLs to start crawling from.
    #[arg(value_name = "URL")]
    pub seeds: Vec<String>,

    /// Seed additionally from a sitemap.xml URL (may be repeated).
    #[arg(long, value_name = "URL")]
    pub sitemap: Vec<String>,

    /// Maximum link depth to follow (seeds are depth 0). Unlimited if unset.
    #[arg(short = 'd', long)]
    pub depth: Option<u32>,

    /// Stop after fetching this many pages. Unlimited if unset.
    #[arg(short = 'n', long = "max-pages", value_name = "N")]
    pub max_pages: Option<usize>,

    /// Number of concurrent in-flight requests across all hosts.
    #[arg(short = 'c', long, default_value_t = DEFAULT_CONCURRENCY, value_parser = parse_concurrency)]
    pub concurrency: usize,

    /// Minimum delay between requests to the same host (e.g. `250ms`, `1s`).
    #[arg(long, default_value = "250ms", value_parser = parse_duration)]
    pub delay: Duration,

    /// Per-request timeout (e.g. `30s`).
    #[arg(long, default_value = "30s", value_parser = parse_duration)]
    pub timeout: Duration,

    /// How far link-following may stray from the seeds.
    #[arg(long, value_enum, default_value_t = ScopeArg::Domain)]
    pub scope: ScopeArg,

    /// Override the User-Agent header.
    #[arg(long, value_name = "STRING")]
    pub user_agent: Option<String>,

    /// Do not fetch or obey robots.txt. Use only when you have permission.
    #[arg(long)]
    pub ignore_robots: bool,

    /// Obey robots.txt allow/deny rules but ignore its Crawl-delay value.
    #[arg(long = "ignore-crawl-delay")]
    pub ignore_crawl_delay: bool,

    /// Only crawl URLs matching this regex (may be repeated).
    #[arg(long = "include", value_name = "REGEX")]
    pub include: Vec<String>,

    /// Never crawl URLs matching this regex (may be repeated).
    #[arg(long = "exclude", value_name = "REGEX")]
    pub exclude: Vec<String>,

    /// Write JSON Lines output to this file instead of stdout.
    #[arg(short = 'o', long, value_name = "FILE")]
    pub output: Option<PathBuf>,

    /// Retry attempts for transient network failures and temporary HTTP statuses.
    #[arg(long, default_value_t = 2)]
    pub retries: u32,

    /// Maximum response body size in bytes; larger bodies are skipped.
    #[arg(long = "max-body", value_name = "BYTES", default_value_t = 8 * 1024 * 1024)]
    pub max_body: usize,

    /// Suppress the terminal dashboard and only print the final summary.
    #[arg(short = 'q', long)]
    pub quiet: bool,

    /// Do not record this run in the local job history.
    #[arg(long = "no-save")]
    pub no_save: bool,

    /// Clear local job history and exit.
    #[arg(long = "clear-jobs")]
    pub clear_jobs: bool,

    /// Increase log verbosity (-v info, -vv debug, -vvv trace).
    #[arg(short = 'v', long, action = clap::ArgAction::Count)]
    pub verbose: u8,
}

/// CLI mirror of [`Scope`].
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ScopeArg {
    /// Only the exact seed host(s).
    Host,
    /// The seed's registrable domain, including subdomains (default).
    Domain,
    /// Anywhere on the web.
    Any,
}

impl From<ScopeArg> for Scope {
    fn from(value: ScopeArg) -> Self {
        match value {
            ScopeArg::Host => Scope::Host,
            ScopeArg::Domain => Scope::Domain,
            ScopeArg::Any => Scope::Any,
        }
    }
}

impl Cli {
    /// Whether this invocation has enough input to start a crawl.
    pub(crate) fn has_crawl_target(&self) -> bool {
        !self.seeds.is_empty() || !self.sitemap.is_empty()
    }

    /// Parsed, validated sitemap URLs.
    pub(crate) fn sitemap_urls(&self) -> anyhow::Result<Vec<Url>> {
        self.sitemap
            .iter()
            .map(|s| Url::parse(s).with_context(|| format!("invalid sitemap URL: {s}")))
            .collect()
    }

    /// Translate CLI arguments into a validated [`CrawlConfig`].
    ///
    /// When no seed URLs are given but a sitemap is, the sitemap's origin root
    /// is used as a seed so that the default domain scope is anchored sensibly.
    pub(crate) fn to_config(&self) -> anyhow::Result<CrawlConfig> {
        let mut builder = CrawlConfig::builder()
            .max_depth(self.depth)
            .max_pages(self.max_pages)
            .concurrency(self.concurrency)
            .per_host_delay(self.delay)
            .request_timeout(self.timeout)
            .scope(self.scope.into())
            .respect_robots(!self.ignore_robots)
            .respect_crawl_delay(!self.ignore_robots && !self.ignore_crawl_delay)
            .max_retries(self.retries)
            .max_body_bytes(self.max_body);

        if let Some(ua) = &self.user_agent {
            builder = builder.user_agent(ua.clone());
        }
        for pattern in &self.include {
            builder = builder.include(pattern.clone());
        }
        for pattern in &self.exclude {
            builder = builder.exclude(pattern.clone());
        }

        let mut had_seed = false;
        for seed in &self.seeds {
            builder = builder
                .add_seed(seed)
                .with_context(|| format!("invalid seed URL: {seed}"))?;
            had_seed = true;
        }

        if !had_seed {
            let sitemaps = self.sitemap_urls()?;
            if sitemaps.is_empty() {
                bail!("provide at least one seed URL or a --sitemap");
            }
            for sm in &sitemaps {
                if let Some(root) = origin_root(sm) {
                    builder = builder.seed_url(root);
                }
            }
        }

        builder.build().map_err(Into::into)
    }
}

fn origin_root(url: &Url) -> Option<Url> {
    let mut root = url.clone();
    root.set_path("/");
    root.set_query(None);
    root.set_fragment(None);
    Some(root)
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    humantime::parse_duration(s).map_err(|e| format!("invalid duration {s:?}: {e}"))
}

fn parse_concurrency(s: &str) -> Result<usize, String> {
    let concurrency = s
        .parse::<usize>()
        .map_err(|_| format!("invalid concurrency {s:?}: expected a positive integer"))?;
    if concurrency == 0 {
        return Err("concurrency must be at least 1".into());
    }
    if concurrency > MAX_CONCURRENCY {
        return Err(format!("concurrency must be at most {MAX_CONCURRENCY}"));
    }
    Ok(concurrency)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_uses_default_concurrency() {
        let cli = Cli::try_parse_from(["rustcrawl", "https://example.com/"]).unwrap();
        assert_eq!(cli.concurrency, DEFAULT_CONCURRENCY);
    }

    #[test]
    fn cli_rejects_zero_concurrency() {
        let result =
            Cli::try_parse_from(["rustcrawl", "https://example.com/", "--concurrency", "0"]);
        assert!(result.is_err());
    }

    #[test]
    fn cli_rejects_excessive_concurrency() {
        let too_many = (MAX_CONCURRENCY + 1).to_string();
        let result = Cli::try_parse_from([
            "rustcrawl",
            "https://example.com/",
            "--concurrency",
            too_many.as_str(),
        ]);
        assert!(result.is_err());
    }
}
