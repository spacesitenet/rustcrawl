//! Output sinks: where crawled pages go.
//!
//! [`Sink`] is the crawler's output boundary. The engine never assumes anything
//! about storage beyond this trait, so embedding `rustcrawl` in an indexer or
//! storage pipeline is a matter of implementing one method.

use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::error::{CrawlError, Result};
use crate::page::{CrawledPage, PageError};

/// A destination for crawl results.
///
/// Implementations must be cheap to share across worker tasks (`Send + Sync`).
/// All methods take `&self`; use interior mutability for buffering or batching.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Handle one successfully crawled page.
    async fn page(&self, page: &CrawledPage) -> Result<()>;

    /// Handle one per-page failure. The default ignores it (the engine still
    /// logs and counts every failure).
    async fn error(&self, _error: &PageError) -> Result<()> {
        Ok(())
    }

    /// Flush and release resources at the end of the crawl.
    async fn finish(&self) -> Result<()> {
        Ok(())
    }
}

/// Writes each page as one JSON object per line ([JSON Lines]).
///
/// This is the default CLI output and a convenient interchange format for
/// downstream tooling.
///
/// [JSON Lines]: https://jsonlines.org/
pub struct JsonlSink {
    writer: Mutex<BufWriter<Box<dyn Write + Send>>>,
}

impl JsonlSink {
    /// Build a sink over any writer.
    pub fn new(writer: impl Write + Send + 'static) -> Self {
        Self {
            writer: Mutex::new(BufWriter::new(Box::new(writer))),
        }
    }

    /// Write JSON Lines to standard output.
    pub fn stdout() -> Self {
        Self::new(io::stdout())
    }

    /// Write JSON Lines to a file, creating or truncating it.
    pub fn to_file(path: impl AsRef<Path>) -> Result<Self> {
        let file = std::fs::File::create(path)?;
        Ok(Self::new(file))
    }
}

#[async_trait]
impl Sink for JsonlSink {
    async fn page(&self, page: &CrawledPage) -> Result<()> {
        let line = serde_json::to_string(page).map_err(CrawlError::sink)?;
        let mut guard = self.writer.lock().expect("sink mutex poisoned");
        guard.write_all(line.as_bytes())?;
        guard.write_all(b"\n")?;
        Ok(())
    }

    async fn finish(&self) -> Result<()> {
        self.writer.lock().expect("sink mutex poisoned").flush()?;
        Ok(())
    }
}

/// Discards everything. Useful for benchmarking the engine or running a crawl
/// purely for its side effects (e.g. warming a cache).
#[derive(Debug, Default, Clone, Copy)]
pub struct NullSink;

#[async_trait]
impl Sink for NullSink {
    async fn page(&self, _page: &CrawledPage) -> Result<()> {
        Ok(())
    }
}
