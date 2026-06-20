//! Per-host `robots.txt` fetching, caching, and evaluation.
//!
//! The cache is keyed by *origin* (scheme + host + port), which is exactly the
//! granularity at which `robots.txt` applies. A host whose `robots.txt` is
//! missing, empty, or unreachable is treated as "allow everything" — the same
//! lenient behavior most crawlers adopt.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use texting_robots::Robot;
use tokio::sync::Mutex;
use url::Url;

/// What we remember about one origin's `robots.txt`.
struct RobotsEntry {
    /// `None` means "no rules apply" (allow all).
    robot: Option<Robot>,
    /// Advertised `Crawl-delay` for our user agent, if any.
    delay: Option<Duration>,
    /// Sitemaps declared in the file (absolute URLs).
    sitemaps: Vec<String>,
}

/// A concurrency-safe cache of parsed `robots.txt` files.
pub struct RobotsCache {
    client: Client,
    user_agent: String,
    cache: Mutex<HashMap<String, Arc<RobotsEntry>>>,
}

impl RobotsCache {
    /// Create a cache that fetches with `client` and evaluates rules for
    /// `user_agent`.
    pub fn new(client: Client, user_agent: impl Into<String>) -> Self {
        Self {
            client,
            user_agent: user_agent.into(),
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Whether `url` may be fetched according to its origin's `robots.txt`.
    pub async fn allowed(&self, url: &Url) -> bool {
        let entry = self.entry_for(url).await;
        match &entry.robot {
            Some(robot) => robot.allowed(url.as_str()),
            None => true,
        }
    }

    /// The advertised crawl-delay for `url`'s origin, if any.
    pub async fn crawl_delay(&self, url: &Url) -> Option<Duration> {
        self.entry_for(url).await.delay
    }

    /// Sitemaps declared in `url`'s origin `robots.txt`.
    pub async fn sitemaps(&self, url: &Url) -> Vec<String> {
        self.entry_for(url).await.sitemaps.clone()
    }

    async fn entry_for(&self, url: &Url) -> Arc<RobotsEntry> {
        let key = origin_key(url);
        if let Some(entry) = self.cache.lock().await.get(&key).cloned() {
            return entry;
        }

        // Fetch outside the lock; a benign race may fetch twice but never
        // corrupts the cache.
        let entry = Arc::new(self.fetch_entry(url).await);
        self.cache.lock().await.insert(key, entry.clone());
        entry
    }

    async fn fetch_entry(&self, url: &Url) -> RobotsEntry {
        let robots_url = match url.join("/robots.txt") {
            Ok(u) => u,
            Err(_) => return RobotsEntry::allow_all(),
        };

        let body = match self.client.get(robots_url).send().await {
            Ok(resp) if resp.status().is_success() => resp.bytes().await.ok(),
            // 4xx (including 404) and unreachable hosts → allow all.
            _ => None,
        };

        let Some(body) = body else {
            return RobotsEntry::allow_all();
        };

        match Robot::new(&self.user_agent, &body) {
            Ok(robot) => RobotsEntry {
                delay: robot
                    .delay
                    .and_then(|secs| Duration::try_from_secs_f32(secs).ok()),
                sitemaps: robot.sitemaps.clone(),
                robot: Some(robot),
            },
            Err(_) => RobotsEntry::allow_all(),
        }
    }
}

impl RobotsEntry {
    fn allow_all() -> Self {
        Self {
            robot: None,
            delay: None,
            sitemaps: Vec::new(),
        }
    }
}

/// Origin key (`scheme://host:port`) used to index the cache.
fn origin_key(url: &Url) -> String {
    let scheme = url.scheme();
    let host = url.host_str().unwrap_or_default();
    match url.port_or_known_default() {
        Some(port) => format!("{scheme}://{host}:{port}"),
        None => format!("{scheme}://{host}"),
    }
}
