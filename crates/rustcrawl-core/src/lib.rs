//! # rustcrawl-core
//!
//! The crawling engine that powers the `rustcrawl` CLI. It is designed to be
//! embedded in other applications (indexers, portals, archival tools), so the
//! engine is decoupled from any particular form of output or user interface.
//!
//! ## Architecture
//!
//! A crawl is driven by a small number of cooperating pieces:
//!
//! - [`Frontier`] — the politeness-aware URL queue. It owns deduplication,
//!   per-host scheduling (rate limiting), depth tracking, and the global page
//!   budget. It is the single source of truth for "what should be fetched next".
//! - [`Fetcher`] — a thin wrapper over an HTTP client that adds retries,
//!   timeouts, and response-size limits.
//! - [`RobotsCache`] — fetches and caches `robots.txt` per host and answers
//!   allow/deny questions (and surfaces any advertised crawl-delay).
//! - [`parser`] — extracts links and titles from HTML.
//! - [`Sink`] — where crawled pages go. Implement this trait to plug the
//!   crawler into a downstream index or storage system.
//! - [`Engine`] — wires the above together and runs a pool of async workers.
//!
//! ## Example
//!
//! ```no_run
//! use rustcrawl_core::{sink::JsonlSink, CrawlConfig, Engine, Result};
//! use std::sync::Arc;
//!
//! # async fn run() -> Result<()> {
//! let config = CrawlConfig::builder()
//!     .add_seed("https://example.com")?
//!     .max_pages(Some(100))
//!     .concurrency(8)
//!     .build()?;
//!
//! let sink = Arc::new(JsonlSink::stdout());
//! let summary = Engine::new(config, sink)?.run().await?;
//! println!("crawled {} pages", summary.pages_fetched);
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod engine;
pub mod error;
pub mod fetcher;
pub mod filter;
pub mod frontier;
pub mod normalize;
pub mod page;
pub mod parser;
pub mod robots;
pub mod sink;
pub mod sitemap;
pub mod stats;

pub use config::{CrawlConfig, CrawlConfigBuilder, Scope, DEFAULT_CONCURRENCY, MAX_CONCURRENCY};
pub use engine::{CrawlControl, CrawlEvent, CrawlSummary, Engine};
pub use error::{CrawlError, Result};
pub use fetcher::{FetchResponse, Fetcher};
pub use filter::UrlFilter;
pub use frontier::Frontier;
pub use page::{CrawlTask, CrawledPage, PageError};
pub use robots::RobotsCache;
pub use sink::Sink;
pub use stats::{Stats, StatsSnapshot};
