//! Crawl configuration and its builder.

use std::time::Duration;

use url::Url;

use crate::error::{CrawlError, Result};

/// Default number of concurrent in-flight requests across the crawl.
pub const DEFAULT_CONCURRENCY: usize = 16;

/// Upper bound for the configured worker pool.
///
/// This protects embedders and CLI users from accidentally spawning an
/// unbounded number of Tokio tasks. Higher fan-out should be introduced with a
/// deliberate architecture change, not a typo in a command-line flag.
pub const MAX_CONCURRENCY: usize = 1024;

/// How aggressively the crawler is allowed to wander away from its seeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Scope {
    /// Only follow links on the exact same host as the seed
    /// (`docs.example.com` will not crawl `example.com`).
    Host,
    /// Follow links on the same registrable domain, including subdomains
    /// (`example.com` and `docs.example.com` are both in scope). This is the
    /// default and the most common choice for crawling a single site.
    #[default]
    Domain,
    /// Follow links anywhere. Combine with `--max-pages` and patterns unless
    /// you really mean to crawl the open web.
    Any,
}

/// Immutable configuration for a single crawl.
///
/// Construct one with [`CrawlConfig::builder`]; the builder validates inputs
/// and applies sensible, polite defaults.
#[derive(Debug, Clone)]
pub struct CrawlConfig {
    /// Starting URLs. Always crawled first, at depth `0`.
    pub seeds: Vec<Url>,
    /// Maximum link depth to follow. `None` means unlimited.
    pub max_depth: Option<u32>,
    /// Maximum number of pages to fetch. `None` means unlimited.
    pub max_pages: Option<usize>,
    /// Number of concurrent in-flight requests across all hosts.
    ///
    /// Must be in the range `1..=`[`MAX_CONCURRENCY`]. Per-host politeness is
    /// still enforced separately by [`Self::per_host_delay`].
    pub concurrency: usize,
    /// Minimum delay between two requests to the *same* host.
    pub per_host_delay: Duration,
    /// Per-request timeout.
    pub request_timeout: Duration,
    /// `User-Agent` header sent with every request.
    pub user_agent: String,
    /// Whether to fetch and obey `robots.txt`.
    pub respect_robots: bool,
    /// Whether an advertised `Crawl-delay` in `robots.txt` may raise
    /// [`Self::per_host_delay`].
    pub respect_crawl_delay: bool,
    /// How far link-following may stray from the seeds.
    pub scope: Scope,
    /// If set, only URLs matching at least one pattern are crawled.
    pub include: Vec<regex::Regex>,
    /// URLs matching any of these patterns are never crawled.
    pub exclude: Vec<regex::Regex>,
    /// Number of retry attempts for transient transport failures.
    pub max_retries: u32,
    /// Hard cap on response body size; larger bodies are skipped.
    pub max_body_bytes: usize,
}

impl CrawlConfig {
    /// Start building a configuration.
    pub fn builder() -> CrawlConfigBuilder {
        CrawlConfigBuilder::default()
    }
}

/// Builder for [`CrawlConfig`].
#[derive(Debug, Clone)]
pub struct CrawlConfigBuilder {
    seeds: Vec<Url>,
    max_depth: Option<u32>,
    max_pages: Option<usize>,
    concurrency: usize,
    per_host_delay: Duration,
    request_timeout: Duration,
    user_agent: String,
    respect_robots: bool,
    respect_crawl_delay: bool,
    scope: Scope,
    include: Vec<String>,
    exclude: Vec<String>,
    max_retries: u32,
    max_body_bytes: usize,
}

impl Default for CrawlConfigBuilder {
    fn default() -> Self {
        Self {
            seeds: Vec::new(),
            max_depth: None,
            max_pages: None,
            concurrency: DEFAULT_CONCURRENCY,
            per_host_delay: Duration::from_millis(250),
            request_timeout: Duration::from_secs(30),
            user_agent: default_user_agent(),
            respect_robots: true,
            respect_crawl_delay: true,
            scope: Scope::default(),
            include: Vec::new(),
            exclude: Vec::new(),
            max_retries: 2,
            max_body_bytes: 8 * 1024 * 1024,
        }
    }
}

impl CrawlConfigBuilder {
    /// Add a seed URL from a string.
    pub fn add_seed(mut self, seed: impl AsRef<str>) -> Result<Self> {
        let raw = seed.as_ref();
        let url = Url::parse(raw).map_err(|source| CrawlError::Url {
            url: raw.to_string(),
            source,
        })?;
        self.seeds.push(url);
        Ok(self)
    }

    /// Add an already-parsed seed URL.
    pub fn seed_url(mut self, url: Url) -> Self {
        self.seeds.push(url);
        self
    }

    /// Set the maximum link depth (`None` = unlimited).
    pub fn max_depth(mut self, depth: Option<u32>) -> Self {
        self.max_depth = depth;
        self
    }

    /// Set the maximum number of pages to fetch (`None` = unlimited).
    pub fn max_pages(mut self, pages: Option<usize>) -> Self {
        self.max_pages = pages;
        self
    }

    /// Set the number of concurrent in-flight requests.
    ///
    /// The value is validated by [`Self::build`].
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Set the minimum delay between requests to the same host.
    pub fn per_host_delay(mut self, delay: Duration) -> Self {
        self.per_host_delay = delay;
        self
    }

    /// Set the per-request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Override the `User-Agent` header.
    pub fn user_agent(mut self, ua: impl Into<String>) -> Self {
        self.user_agent = ua.into();
        self
    }

    /// Toggle obeying `robots.txt`.
    pub fn respect_robots(mut self, respect: bool) -> Self {
        self.respect_robots = respect;
        self
    }

    /// Toggle honoring an advertised `Crawl-delay`.
    pub fn respect_crawl_delay(mut self, respect: bool) -> Self {
        self.respect_crawl_delay = respect;
        self
    }

    /// Set the crawl scope.
    pub fn scope(mut self, scope: Scope) -> Self {
        self.scope = scope;
        self
    }

    /// Add an include pattern (compiled in [`Self::build`]).
    pub fn include(mut self, pattern: impl Into<String>) -> Self {
        self.include.push(pattern.into());
        self
    }

    /// Add an exclude pattern (compiled in [`Self::build`]).
    pub fn exclude(mut self, pattern: impl Into<String>) -> Self {
        self.exclude.push(pattern.into());
        self
    }

    /// Set the retry count for transient failures.
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// Set the maximum response body size in bytes.
    pub fn max_body_bytes(mut self, bytes: usize) -> Self {
        self.max_body_bytes = bytes;
        self
    }

    /// Validate inputs and produce an immutable [`CrawlConfig`].
    pub fn build(self) -> Result<CrawlConfig> {
        if self.seeds.is_empty() {
            return Err(CrawlError::Config(
                "at least one seed URL is required".into(),
            ));
        }
        if self.concurrency == 0 {
            return Err(CrawlError::Config("concurrency must be at least 1".into()));
        }
        if self.concurrency > MAX_CONCURRENCY {
            return Err(CrawlError::Config(format!(
                "concurrency must be at most {MAX_CONCURRENCY}"
            )));
        }
        for seed in &self.seeds {
            if !matches!(seed.scheme(), "http" | "https") {
                return Err(CrawlError::Config(format!(
                    "seed {seed} must use http or https"
                )));
            }
        }

        let include = compile_patterns(self.include)?;
        let exclude = compile_patterns(self.exclude)?;

        Ok(CrawlConfig {
            seeds: self.seeds,
            max_depth: self.max_depth,
            max_pages: self.max_pages,
            concurrency: self.concurrency,
            per_host_delay: self.per_host_delay,
            request_timeout: self.request_timeout,
            user_agent: self.user_agent,
            respect_robots: self.respect_robots,
            respect_crawl_delay: self.respect_crawl_delay,
            scope: self.scope,
            include,
            exclude,
            max_retries: self.max_retries,
            max_body_bytes: self.max_body_bytes,
        })
    }
}

fn compile_patterns(patterns: Vec<String>) -> Result<Vec<regex::Regex>> {
    patterns
        .into_iter()
        .map(|p| regex::Regex::new(&p).map_err(|source| CrawlError::Pattern { pattern: p, source }))
        .collect()
}

/// The default `User-Agent`, advertising the project and version so site
/// operators can identify (and contact about) the crawler.
pub fn default_user_agent() -> String {
    format!(
        "rustcrawl/{} (+https://github.com/rustcrawl/rustcrawl)",
        env!("CARGO_PKG_VERSION")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_requires_at_least_one_seed() {
        assert!(CrawlConfig::builder().build().is_err());
    }

    #[test]
    fn build_rejects_zero_concurrency() {
        let result = CrawlConfig::builder()
            .add_seed("https://example.com/")
            .unwrap()
            .concurrency(0)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn build_rejects_non_http_seed() {
        let result = CrawlConfig::builder()
            .add_seed("ftp://example.com/")
            .unwrap()
            .build();
        assert!(matches!(result, Err(CrawlError::Config(_))));
    }

    #[test]
    fn add_seed_rejects_unparseable_url() {
        let result = CrawlConfig::builder().add_seed("not a url");
        assert!(matches!(result, Err(CrawlError::Url { .. })));
    }

    #[test]
    fn build_rejects_invalid_include_regex() {
        let result = CrawlConfig::builder()
            .add_seed("https://example.com/")
            .unwrap()
            .include("(")
            .build();
        assert!(matches!(result, Err(CrawlError::Pattern { .. })));
    }

    #[test]
    fn build_applies_sensible_defaults() {
        let config = CrawlConfig::builder()
            .add_seed("https://example.com/")
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(config.concurrency, DEFAULT_CONCURRENCY);
        assert_eq!(config.scope, Scope::Domain);
        assert!(config.respect_robots);
        assert_eq!(config.seeds.len(), 1);
    }

    #[test]
    fn build_rejects_excessive_concurrency() {
        let result = CrawlConfig::builder()
            .add_seed("https://example.com/")
            .unwrap()
            .concurrency(MAX_CONCURRENCY + 1)
            .build();
        assert!(matches!(result, Err(CrawlError::Config(_))));
    }
}
