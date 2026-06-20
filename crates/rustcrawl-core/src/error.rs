//! Error types for the crawler engine.

use thiserror::Error;

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, CrawlError>;

/// Errors that can occur while configuring or running a crawl.
///
/// Per-page failures (a single URL that timed out or returned a 500) are *not*
/// represented here; those are reported as [`crate::page::PageError`] so that a
/// crawl can continue past individual failures. [`CrawlError`] is reserved for
/// problems that affect the crawl as a whole.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CrawlError {
    /// The configuration was rejected (e.g. no seeds, zero concurrency).
    #[error("invalid configuration: {0}")]
    Config(String),

    /// A seed or pattern could not be parsed.
    #[error("invalid URL {url:?}: {source}")]
    Url {
        /// The offending input.
        url: String,
        /// The underlying parse error.
        source: url::ParseError,
    },

    /// An invalid include/exclude regular expression was supplied.
    #[error("invalid pattern {pattern:?}: {source}")]
    Pattern {
        /// The offending pattern.
        pattern: String,
        /// The underlying regex error.
        source: regex::Error,
    },

    /// The HTTP client could not be constructed.
    #[error("failed to build HTTP client: {0}")]
    Client(#[source] reqwest::Error),

    /// A sink failed in a way that should abort the crawl.
    #[error("output sink error: {0}")]
    Sink(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// An I/O error not tied to a specific page.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl CrawlError {
    /// Wrap an arbitrary sink error.
    pub fn sink<E>(err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        CrawlError::Sink(Box::new(err))
    }
}
