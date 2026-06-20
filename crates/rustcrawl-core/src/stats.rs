//! Live, lock-free crawl metrics.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Atomic counters shared between the engine and any observer (such as the CLI
/// progress display). Cheap to read concurrently while the crawl runs.
#[derive(Debug)]
pub struct Stats {
    /// Pages successfully fetched.
    pub fetched: AtomicU64,
    /// Pages that failed (transport error, bad status, blocked, ...).
    pub failed: AtomicU64,
    /// URLs accepted into the frontier (after dedup and scope filtering).
    pub discovered: AtomicU64,
    /// Requests currently in flight.
    pub in_flight: AtomicU64,
    /// Total bytes downloaded.
    pub bytes: AtomicU64,
    start: Instant,
}

impl Default for Stats {
    fn default() -> Self {
        Self::new()
    }
}

impl Stats {
    /// Create a fresh set of counters with the clock started.
    pub fn new() -> Self {
        Self {
            fetched: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            discovered: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            start: Instant::now(),
        }
    }

    pub(crate) fn inc_fetched(&self, body_bytes: u64) {
        self.fetched.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(body_bytes, Ordering::Relaxed);
    }

    pub(crate) fn inc_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add_discovered(&self, n: u64) {
        self.discovered.fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn inc_in_flight(&self) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn dec_in_flight(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    /// Take a consistent-enough point-in-time snapshot for display.
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            fetched: self.fetched.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            discovered: self.discovered.load(Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            elapsed_secs: self.start.elapsed().as_secs_f64(),
        }
    }
}

/// An immutable copy of [`Stats`] taken at one instant.
#[derive(Debug, Clone, Copy)]
pub struct StatsSnapshot {
    /// Pages successfully fetched.
    pub fetched: u64,
    /// Pages that failed.
    pub failed: u64,
    /// URLs accepted into the frontier.
    pub discovered: u64,
    /// Requests currently in flight.
    pub in_flight: u64,
    /// Total bytes downloaded.
    pub bytes: u64,
    /// Seconds since the crawl started.
    pub elapsed_secs: f64,
}

impl StatsSnapshot {
    /// Average pages fetched per second over the life of the crawl.
    pub fn pages_per_sec(&self) -> f64 {
        if self.elapsed_secs > 0.0 {
            self.fetched as f64 / self.elapsed_secs
        } else {
            0.0
        }
    }
}
