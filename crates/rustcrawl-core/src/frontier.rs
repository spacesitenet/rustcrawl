//! The URL frontier: deduplication, per-host scheduling, and crawl budgets.
//!
//! The frontier is deliberately *not* thread-safe on its own; the [`Engine`]
//! wraps it in a `Mutex` and a `Notify`. Keeping it a plain data structure makes
//! its invariants easy to reason about and unit-test.
//!
//! ## Politeness model
//!
//! URLs are bucketed into one queue per host. Each host has an `next_allowed`
//! timestamp; a host's task can only be leased once that time has passed, after
//! which the timestamp is pushed forward by the host's delay. This guarantees a
//! minimum spacing between requests to the same host while letting different
//! hosts proceed in parallel.
//!
//! [`Engine`]: crate::engine::Engine

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use url::Url;

use crate::config::CrawlConfig;
use crate::normalize::normalize;
use crate::page::CrawlTask;

/// The result of asking the frontier for the next unit of work.
#[derive(Debug)]
pub enum Lease {
    /// A task is ready to be fetched now.
    Ready(CrawlTask),
    /// Work exists, but every ready host is rate-limited. Wait at most this
    /// long, then ask again.
    Wait(Duration),
    /// No queued work, but tasks are still in flight that may enqueue more.
    Idle,
    /// The crawl is finished: nothing queued, nothing in flight (or the page
    /// budget is exhausted).
    Done,
}

/// Politeness-aware, de-duplicating URL queue with crawl budgets.
#[derive(Debug)]
pub struct Frontier {
    queues: HashMap<String, VecDeque<CrawlTask>>,
    next_allowed: HashMap<String, Instant>,
    host_delay: HashMap<String, Duration>,
    seen: std::collections::HashSet<String>,
    base_delay: Duration,
    max_depth: Option<u32>,
    max_pages: Option<usize>,
    queued: usize,
    active: usize,
    dispatched: usize,
}

impl Frontier {
    /// Create an empty frontier from configuration.
    pub fn new(config: &CrawlConfig) -> Self {
        Self {
            queues: HashMap::new(),
            next_allowed: HashMap::new(),
            host_delay: HashMap::new(),
            seen: std::collections::HashSet::new(),
            base_delay: config.per_host_delay,
            max_depth: config.max_depth,
            max_pages: config.max_pages,
            queued: 0,
            active: 0,
            dispatched: 0,
        }
    }

    /// Add a task, returning `true` if it was newly enqueued.
    ///
    /// The URL is normalized for deduplication. Tasks beyond [`max_depth`],
    /// already-seen URLs, and URLs without a host are dropped.
    ///
    /// [`max_depth`]: CrawlConfig::max_depth
    pub fn add(&mut self, mut task: CrawlTask) -> bool {
        if let Some(max) = self.max_depth {
            if task.depth > max {
                return false;
            }
        }

        task.url = normalize(task.url);
        let key = match host_key(&task.url) {
            Some(k) => k,
            None => return false,
        };

        if !self.seen.insert(task.url.as_str().to_owned()) {
            return false;
        }

        self.queues.entry(key).or_default().push_back(task);
        self.queued += 1;
        true
    }

    /// Raise a host's delay to at least `delay` (e.g. from a `robots.txt`
    /// `Crawl-delay`). Never lowers the configured base delay.
    pub fn set_host_delay(&mut self, url: &Url, delay: Duration) {
        if let Some(key) = host_key(url) {
            let entry = self.host_delay.entry(key).or_insert(self.base_delay);
            if delay > *entry {
                *entry = delay;
            }
        }
    }

    /// Lease the next ready task, or report why none is available.
    pub fn lease(&mut self, now: Instant) -> Lease {
        if self.budget_exhausted() {
            return Lease::Done;
        }

        let Some((host, allowed_at)) = self.earliest_ready_host(now) else {
            return if self.active == 0 {
                Lease::Done
            } else {
                Lease::Idle
            };
        };

        if allowed_at > now {
            return Lease::Wait(allowed_at - now);
        }

        let delay = self.delay_for(&host);
        let queue = self
            .queues
            .get_mut(&host)
            .expect("host present in earliest_ready_host");
        let task = queue
            .pop_front()
            .expect("earliest_ready_host returns only non-empty queues");
        if queue.is_empty() {
            self.queues.remove(&host);
        }

        self.next_allowed.insert(host, now + delay);
        self.queued -= 1;
        self.active += 1;
        self.dispatched += 1;
        Lease::Ready(task)
    }

    /// Mark a leased task as finished. Must be paired with each [`Lease::Ready`].
    pub fn complete(&mut self) {
        debug_assert!(self.active > 0, "complete() without an outstanding lease");
        self.active = self.active.saturating_sub(1);
    }

    /// Number of tasks waiting to be leased.
    pub fn queued(&self) -> usize {
        self.queued
    }

    /// Number of leased-but-not-completed tasks.
    pub fn active(&self) -> usize {
        self.active
    }

    fn budget_exhausted(&self) -> bool {
        matches!(self.max_pages, Some(max) if self.dispatched >= max)
    }

    fn delay_for(&self, host: &str) -> Duration {
        self.host_delay
            .get(host)
            .copied()
            .unwrap_or(self.base_delay)
    }

    /// Find the host with a non-empty queue whose `next_allowed` is earliest.
    fn earliest_ready_host(&self, now: Instant) -> Option<(String, Instant)> {
        let mut best: Option<(&String, Instant)> = None;
        for host in self.queues.keys() {
            let allowed = self.next_allowed.get(host).copied().unwrap_or(now);
            match best {
                Some((_, best_at)) if allowed >= best_at => {}
                _ => best = Some((host, allowed)),
            }
        }
        best.map(|(h, t)| (h.clone(), t))
    }
}

fn host_key(url: &Url) -> Option<String> {
    url.host_str().map(|h| h.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> CrawlConfig {
        CrawlConfig::builder()
            .add_seed("https://a.com/")
            .unwrap()
            .per_host_delay(Duration::from_millis(100))
            .build()
            .unwrap()
    }

    fn task(s: &str) -> CrawlTask {
        CrawlTask::seed(Url::parse(s).unwrap())
    }

    #[test]
    fn dedupes_normalized_urls() {
        let mut f = Frontier::new(&cfg());
        assert!(f.add(task("https://a.com/x#frag")));
        assert!(!f.add(task("https://a.com/x")));
        assert_eq!(f.queued(), 1);
    }

    #[test]
    fn enforces_per_host_delay() {
        let mut f = Frontier::new(&cfg());
        f.add(task("https://a.com/1"));
        f.add(task("https://a.com/2"));
        let now = Instant::now();

        assert!(matches!(f.lease(now), Lease::Ready(_)));
        // Second same-host task must wait for the delay.
        match f.lease(now) {
            Lease::Wait(d) => assert!(d <= Duration::from_millis(100)),
            other => panic!("expected Wait, got {other:?}"),
        }
        // After the delay it becomes available.
        assert!(matches!(
            f.lease(now + Duration::from_millis(100)),
            Lease::Ready(_)
        ));
    }

    #[test]
    fn different_hosts_run_in_parallel() {
        let mut f = Frontier::new(&cfg());
        f.add(task("https://a.com/1"));
        f.add(task("https://b.com/1"));
        let now = Instant::now();
        assert!(matches!(f.lease(now), Lease::Ready(_)));
        assert!(matches!(f.lease(now), Lease::Ready(_)));
    }

    #[test]
    fn done_when_drained() {
        let mut f = Frontier::new(&cfg());
        f.add(task("https://a.com/1"));
        let now = Instant::now();
        assert!(matches!(f.lease(now), Lease::Ready(_)));
        assert!(matches!(f.lease(now), Lease::Idle));
        f.complete();
        assert!(matches!(f.lease(now), Lease::Done));
    }

    #[test]
    fn respects_max_depth() {
        let config = CrawlConfig::builder()
            .add_seed("https://a.com/")
            .unwrap()
            .max_depth(Some(1))
            .build()
            .unwrap();
        let mut f = Frontier::new(&config);
        assert!(!f.add(CrawlTask::child(
            Url::parse("https://a.com/deep").unwrap(),
            2,
            Url::parse("https://a.com/").unwrap()
        )));
    }
}
