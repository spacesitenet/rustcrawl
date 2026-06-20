//! Data types that flow through the crawl pipeline.

use serde::{Deserialize, Serialize};
use url::Url;

/// A unit of work in the [`crate::frontier::Frontier`]: a URL to fetch plus the
/// bookkeeping needed to enforce depth limits and trace provenance.
#[derive(Debug, Clone)]
pub struct CrawlTask {
    /// The (already normalized) URL to fetch.
    pub url: Url,
    /// Distance from the nearest seed. Seeds have depth `0`.
    pub depth: u32,
    /// The page this URL was discovered on, if any.
    pub referrer: Option<Url>,
}

impl CrawlTask {
    /// Create a seed task (depth `0`, no referrer).
    pub fn seed(url: Url) -> Self {
        Self {
            url,
            depth: 0,
            referrer: None,
        }
    }

    /// Create a child task discovered while crawling `referrer`.
    pub fn child(url: Url, depth: u32, referrer: Url) -> Self {
        Self {
            url,
            depth,
            referrer: Some(referrer),
        }
    }
}

/// A page that was successfully fetched.
///
/// This is the primary record handed to a [`crate::sink::Sink`]. It is
/// `Serialize`/`Deserialize` so it can be written as JSON, stored, or shipped
/// to a downstream index without further conversion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrawledPage {
    /// The URL that was requested.
    pub url: String,
    /// The final URL after following redirects.
    pub final_url: String,
    /// HTTP status code of the final response.
    pub status: u16,
    /// Depth at which this page was crawled.
    pub depth: u32,
    /// The page that linked here, if any.
    pub referrer: Option<String>,
    /// `Content-Type` header, if present.
    pub content_type: Option<String>,
    /// Document `<title>`, if the body was HTML and a title was found.
    pub title: Option<String>,
    /// Size of the response body in bytes.
    pub content_length: usize,
    /// Absolute, in-scope links discovered on the page.
    pub links: Vec<String>,
    /// When the response finished downloading (RFC 3339, UTC).
    pub fetched_at: chrono::DateTime<chrono::Utc>,
    /// Wall-clock time spent fetching the page, in milliseconds.
    pub elapsed_ms: u64,
}

/// A per-page failure. Recorded and counted, but never aborts the crawl.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageError {
    /// The URL that failed.
    pub url: String,
    /// The page that linked here, if any.
    pub referrer: Option<String>,
    /// A human-readable description of what went wrong.
    pub error: String,
    /// A coarse classification useful for metrics and retry decisions.
    pub kind: PageErrorKind,
}

/// Coarse classification of a [`PageError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PageErrorKind {
    /// Blocked by `robots.txt`.
    RobotsDenied,
    /// Connection/timeout/transport-level failure.
    Transport,
    /// Server responded with a 4xx/5xx status.
    HttpStatus,
    /// Response was larger than the configured limit.
    TooLarge,
    /// Content type was not crawlable (e.g. a binary download).
    UnsupportedContentType,
    /// Anything else.
    Other,
}
