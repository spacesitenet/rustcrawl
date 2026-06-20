//! The crawl engine: a pool of async workers driving the [`Frontier`].

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Notify;
use url::Url;

use crate::config::CrawlConfig;
use crate::error::{CrawlError, Result};
use crate::fetcher::Fetcher;
use crate::filter::UrlFilter;
use crate::frontier::{Frontier, Lease};
use crate::page::{CrawlTask, CrawledPage, PageError, PageErrorKind};
use crate::parser;
use crate::robots::RobotsCache;
use crate::sink::Sink;
use crate::sitemap;
use crate::stats::Stats;

/// How long an idle worker waits before re-checking the frontier, bounding the
/// latency of detecting that the crawl is finished.
const IDLE_POLL: Duration = Duration::from_millis(50);

/// Maximum number of crawl events buffered for an observer.
///
/// Events are telemetry, not control-plane state. If a UI or embedder falls
/// behind, the engine drops excess events rather than letting observability
/// allocate unbounded memory or slow down crawling.
const EVENT_BUFFER: usize = 4096;

/// A best-effort telemetry event emitted by the crawl engine.
///
/// Consumers such as terminal dashboards can use these for live logs, but the
/// event stream is intentionally not part of crawl correctness. Dropping the
/// receiver stops delivery, and a slow receiver may miss events when the bounded
/// buffer is full.
#[derive(Debug, Clone)]
pub enum CrawlEvent {
    /// A page was fetched successfully.
    Page {
        /// The requested URL.
        url: String,
        /// Final HTTP status.
        status: u16,
        /// Depth of the page.
        depth: u32,
        /// Newly enqueued in-scope links found on the page.
        new_links: usize,
    },
    /// A URL failed to be crawled.
    Failed {
        /// The URL that failed.
        url: String,
        /// Failure classification.
        kind: PageErrorKind,
        /// Human-readable detail.
        error: String,
    },
}

/// Totals reported when a crawl finishes.
#[derive(Debug, Clone)]
pub struct CrawlSummary {
    /// Pages fetched successfully.
    pub pages_fetched: u64,
    /// URLs that failed.
    pub pages_failed: u64,
    /// URLs accepted into the frontier.
    pub urls_discovered: u64,
    /// Total bytes downloaded.
    pub bytes_downloaded: u64,
    /// Wall-clock duration of the crawl.
    pub duration: Duration,
}

/// A cheap, clonable handle used to stop a crawl early (e.g. on Ctrl-C). The
/// engine stops leasing new work and exits once in-flight requests settle.
#[derive(Clone)]
pub struct Shutdown {
    shared: Arc<Shared>,
}

impl Shutdown {
    /// Request a graceful stop.
    pub fn trigger(&self) {
        self.shared.shutdown.store(true, Ordering::SeqCst);
        self.shared.notify.notify_waiters();
    }
}

/// A cheap, clonable handle for interactive crawl control.
///
/// This is used by terminal UIs and embedders that want interactive controls:
/// pause leasing new URLs, resume, or request a graceful stop.
#[derive(Clone)]
pub struct CrawlControl {
    shared: Arc<Shared>,
}

impl CrawlControl {
    /// Request a graceful stop. In-flight requests are allowed to finish.
    pub fn stop(&self) {
        self.shared.shutdown.store(true, Ordering::SeqCst);
        self.shared.notify.notify_waiters();
    }

    /// Pause the crawl. Workers stop leasing new URLs; in-flight requests keep
    /// running until they finish.
    pub fn pause(&self) {
        self.shared.paused.store(true, Ordering::SeqCst);
        self.shared.notify.notify_waiters();
    }

    /// Resume a paused crawl.
    pub fn resume(&self) {
        self.shared.paused.store(false, Ordering::SeqCst);
        self.shared.notify.notify_waiters();
    }

    /// Toggle between paused and running.
    pub fn toggle_pause(&self) {
        self.shared.paused.fetch_xor(true, Ordering::SeqCst);
        self.shared.notify.notify_waiters();
    }

    /// Whether the crawl has been paused.
    pub fn is_paused(&self) -> bool {
        self.shared.paused.load(Ordering::SeqCst)
    }

    /// Whether a graceful stop has been requested.
    pub fn is_stopping(&self) -> bool {
        self.shared.shutdown.load(Ordering::SeqCst)
    }
}

/// State shared by every worker. Kept private so the public surface stays small.
struct Shared {
    config: CrawlConfig,
    fetcher: Fetcher,
    robots: RobotsCache,
    filter: UrlFilter,
    sink: Arc<dyn Sink>,
    stats: Arc<Stats>,
    frontier: Mutex<Frontier>,
    notify: Notify,
    paused: AtomicBool,
    shutdown: AtomicBool,
    error_slot: Mutex<Option<CrawlError>>,
    reporter: Option<Sender<CrawlEvent>>,
}

/// The crawler. Construct with [`Engine::new`], optionally seed from a sitemap,
/// then [`run`](Engine::run).
pub struct Engine {
    shared: Arc<Shared>,
}

impl Engine {
    /// Build an engine for `config`, delivering results to `sink`.
    pub fn new(config: CrawlConfig, sink: Arc<dyn Sink>) -> Result<Self> {
        Self::build(config, sink, None)
    }

    /// Like [`Engine::new`], but also returns a bounded receiver of
    /// [`CrawlEvent`]s for live progress reporting.
    ///
    /// Event delivery is best effort: a slow observer may miss events, but it
    /// cannot apply backpressure to the crawler or grow memory without bound.
    pub fn with_events(
        config: CrawlConfig,
        sink: Arc<dyn Sink>,
    ) -> Result<(Self, Receiver<CrawlEvent>)> {
        let (tx, rx) = mpsc::channel(EVENT_BUFFER);
        let engine = Self::build(config, sink, Some(tx))?;
        Ok((engine, rx))
    }

    fn build(
        config: CrawlConfig,
        sink: Arc<dyn Sink>,
        reporter: Option<Sender<CrawlEvent>>,
    ) -> Result<Self> {
        let fetcher = Fetcher::from_config(&config)?;
        let robots = RobotsCache::new(fetcher.client().clone(), config.user_agent.clone());
        let filter = UrlFilter::from_config(&config);
        let frontier = Frontier::new(&config);

        let shared = Arc::new(Shared {
            config,
            fetcher,
            robots,
            filter,
            sink,
            stats: Arc::new(Stats::new()),
            frontier: Mutex::new(frontier),
            notify: Notify::new(),
            paused: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            error_slot: Mutex::new(None),
            reporter,
        });

        Ok(Self { shared })
    }

    /// Shared, live metrics. Safe to read concurrently while the crawl runs.
    pub fn stats(&self) -> Arc<Stats> {
        self.shared.stats.clone()
    }

    /// A handle for requesting graceful shutdown.
    pub fn shutdown_handle(&self) -> Shutdown {
        Shutdown {
            shared: self.shared.clone(),
        }
    }

    /// A handle for interactive pause/resume/stop controls.
    pub fn control_handle(&self) -> CrawlControl {
        CrawlControl {
            shared: self.shared.clone(),
        }
    }

    /// Discover additional seed URLs from a sitemap before crawling. In-scope
    /// URLs are added to the frontier at depth `0`.
    pub async fn seed_from_sitemap(&self, sitemap_url: &Url) -> usize {
        let urls = sitemap::fetch_urls(self.shared.fetcher.client(), sitemap_url, 50).await;
        let mut added = 0;
        let mut frontier = self.shared.frontier.lock().expect("frontier poisoned");
        for url in urls {
            if self.shared.filter.allows(&url) && frontier.add(CrawlTask::seed(url)) {
                added += 1;
            }
        }
        self.shared.stats.add_discovered(added as u64);
        added
    }

    /// Run the crawl to completion (or until shutdown) and return a summary.
    pub async fn run(&self) -> Result<CrawlSummary> {
        let started = Instant::now();
        self.seed();

        let mut handles = Vec::with_capacity(self.shared.config.concurrency);
        for _ in 0..self.shared.config.concurrency {
            let shared = self.shared.clone();
            handles.push(tokio::spawn(async move { worker(shared).await }));
        }
        for handle in handles {
            // A panicking worker should not silently shrink the pool unnoticed,
            // but it also must not abort the whole process; log and continue.
            if let Err(err) = handle.await {
                tracing::error!(%err, "crawl worker panicked");
            }
        }

        self.shared.sink.finish().await?;

        if let Some(err) = self
            .shared
            .error_slot
            .lock()
            .expect("error slot poisoned")
            .take()
        {
            return Err(err);
        }

        let snap = self.shared.stats.snapshot();
        Ok(CrawlSummary {
            pages_fetched: snap.fetched,
            pages_failed: snap.failed,
            urls_discovered: snap.discovered,
            bytes_downloaded: snap.bytes,
            duration: started.elapsed(),
        })
    }

    fn seed(&self) {
        let mut frontier = self.shared.frontier.lock().expect("frontier poisoned");
        let mut added = 0;
        for seed in &self.shared.config.seeds {
            if frontier.add(CrawlTask::seed(seed.clone())) {
                added += 1;
            }
        }
        self.shared.stats.add_discovered(added);
    }
}

/// One worker's main loop.
async fn worker(shared: Arc<Shared>) {
    loop {
        if shared.shutdown.load(Ordering::SeqCst) {
            break;
        }

        if shared.paused.load(Ordering::SeqCst) {
            wait_or_notified(&shared, IDLE_POLL).await;
            continue;
        }

        let lease = {
            let mut frontier = shared.frontier.lock().expect("frontier poisoned");
            frontier.lease(Instant::now())
        };

        match lease {
            Lease::Ready(task) => {
                shared.stats.inc_in_flight();
                let _guard = LeaseGuard::new(shared.clone());
                shared.process(task).await;
            }
            Lease::Wait(delay) => {
                wait_or_notified(&shared, delay).await;
            }
            Lease::Idle => {
                wait_or_notified(&shared, IDLE_POLL).await;
            }
            Lease::Done => {
                shared.notify.notify_waiters();
                break;
            }
        }
    }
}

/// Returns a leased task to the frontier even if page processing panics.
///
/// The frontier uses an `active` count to decide when the crawl is drained. This
/// guard keeps that invariant paired with the worker's in-flight metric so a
/// failed worker cannot leave the crawl permanently idle.
struct LeaseGuard {
    shared: Arc<Shared>,
}

impl LeaseGuard {
    fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        self.shared.stats.dec_in_flight();
        {
            let mut frontier = self.shared.frontier.lock().expect("frontier poisoned");
            frontier.complete();
        }
        self.shared.notify.notify_waiters();
    }
}

/// Sleep up to `delay`, waking early if another worker makes progress.
async fn wait_or_notified(shared: &Shared, delay: Duration) {
    tokio::select! {
        _ = tokio::time::sleep(delay) => {}
        _ = shared.notify.notified() => {}
    }
}

impl Shared {
    async fn process(&self, task: CrawlTask) {
        let url = task.url.clone();

        if self.config.respect_robots && !self.robots.allowed(&url).await {
            self.record_failure(&task, PageErrorKind::RobotsDenied, "blocked by robots.txt")
                .await;
            return;
        }

        if self.config.respect_robots && self.config.respect_crawl_delay {
            if let Some(delay) = self.robots.crawl_delay(&url).await {
                self.frontier
                    .lock()
                    .expect("frontier poisoned")
                    .set_host_delay(&url, delay);
            }
        }

        let start = Instant::now();
        match self.fetcher.fetch(&url).await {
            Ok(response) => {
                let elapsed_ms = start.elapsed().as_millis() as u64;
                let body_len = response.body.len();
                let is_html = response
                    .content_type
                    .as_deref()
                    .map(|ct| ct.contains("html"))
                    .unwrap_or(false);

                let parsed = if is_html {
                    let base = response.final_url.clone();
                    let body = String::from_utf8_lossy(&response.body).into_owned();
                    tokio::task::spawn_blocking(move || parser::parse_html(&base, &body))
                        .await
                        .unwrap_or_default()
                } else {
                    parser::ParsedHtml::default()
                };

                let in_scope = self.enqueue_links(&task, &response.final_url, parsed.links);

                self.stats.inc_fetched(body_len as u64);
                let page = CrawledPage {
                    url: url.to_string(),
                    final_url: response.final_url.to_string(),
                    status: response.status.as_u16(),
                    depth: task.depth,
                    referrer: task.referrer.as_ref().map(Url::to_string),
                    content_type: response.content_type,
                    title: parsed.title,
                    content_length: body_len,
                    links: in_scope.links,
                    fetched_at: chrono::Utc::now(),
                    elapsed_ms,
                };

                if let Err(err) = self.sink.page(&page).await {
                    self.fail_crawl(err);
                    return;
                }

                self.emit(CrawlEvent::Page {
                    url: page.url,
                    status: page.status,
                    depth: page.depth,
                    new_links: in_scope.newly_enqueued,
                });
            }
            Err(err) => {
                self.record_failure(&task, err.kind, err.message).await;
            }
        }
    }

    /// Filter, enqueue, and tally the links found on a page.
    fn enqueue_links(&self, task: &CrawlTask, base: &Url, links: Vec<Url>) -> EnqueueOutcome {
        let mut in_scope_links = Vec::new();
        let mut newly_enqueued = 0u64;
        let mut frontier = self.frontier.lock().expect("frontier poisoned");
        for link in links {
            if !self.filter.allows(&link) {
                continue;
            }
            in_scope_links.push(link.to_string());
            let child = CrawlTask::child(link, task.depth + 1, base.clone());
            if frontier.add(child) {
                newly_enqueued += 1;
            }
        }
        drop(frontier);
        if newly_enqueued > 0 {
            self.stats.add_discovered(newly_enqueued);
            self.notify.notify_waiters();
        }
        EnqueueOutcome {
            links: in_scope_links,
            newly_enqueued: newly_enqueued as usize,
        }
    }

    async fn record_failure(
        &self,
        task: &CrawlTask,
        kind: PageErrorKind,
        message: impl Into<String>,
    ) {
        let message = message.into();
        self.stats.inc_failed();
        tracing::debug!(url = %task.url, ?kind, %message, "page failed");

        let error = PageError {
            url: task.url.to_string(),
            referrer: task.referrer.as_ref().map(Url::to_string),
            error: message.clone(),
            kind,
        };
        // The sink's error hook is best-effort: a rejected error record is
        // logged, never propagated, so one bad record cannot abort the crawl.
        if let Err(err) = self.sink.error(&error).await {
            tracing::warn!(%err, "sink rejected error record");
        }
        self.emit(CrawlEvent::Failed {
            url: error.url,
            kind,
            error: message,
        });
    }

    /// Record a fatal error and request shutdown.
    fn fail_crawl(&self, err: CrawlError) {
        tracing::error!(%err, "aborting crawl");
        let mut slot = self.error_slot.lock().expect("error slot poisoned");
        if slot.is_none() {
            *slot = Some(err);
        }
        self.shutdown.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    fn emit(&self, event: CrawlEvent) {
        if let Some(tx) = &self.reporter {
            // Telemetry is intentionally lossy: crawl progress must not depend
            // on a dashboard or embedder draining events quickly enough.
            let _ = tx.try_send(event);
        }
    }
}

struct EnqueueOutcome {
    links: Vec<String>,
    newly_enqueued: usize,
}
