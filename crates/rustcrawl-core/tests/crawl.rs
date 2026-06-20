//! End-to-end crawl tests against a local, controlled site (see `support`).
//!
//! These exercise the engine as a whole: scope filtering, deduplication, depth
//! limits, robots.txt enforcement, redirect handling, non-HTML responses, and
//! the page budget — all without touching the network.

mod support;

use std::time::Duration;

use rustcrawl_core::page::PageErrorKind;
use rustcrawl_core::{CrawlConfig, CrawlConfigBuilder, Engine, Scope};
use support::{CollectSink, TestServer};

/// A polite-but-fast base config pointed at the test server.
fn base(root: &str) -> CrawlConfigBuilder {
    CrawlConfig::builder()
        .add_seed(root)
        .expect("valid seed")
        .scope(Scope::Domain)
        .per_host_delay(Duration::ZERO)
        .respect_crawl_delay(false)
        .request_timeout(Duration::from_secs(5))
        .concurrency(4)
}

#[tokio::test]
async fn crawls_site_and_respects_robots() {
    let server = TestServer::start().await;
    let sink = CollectSink::new();

    let config = base(&server.root()).build().unwrap();
    let summary = Engine::new(config, sink.clone())
        .unwrap()
        .run()
        .await
        .unwrap();

    let fetched = sink.fetched_paths();
    for path in ["/", "/a", "/b", "/a1", "/b1", "/redirect", "/image.png"] {
        assert!(
            fetched.contains(path),
            "expected {path} fetched; got {fetched:?}"
        );
    }

    // robots.txt disallows /secret: it is never requested and is recorded as a
    // failure rather than a page.
    assert!(
        !fetched.contains("/secret"),
        "robots.txt should block /secret"
    );
    assert_eq!(server.hits("/secret"), 0, "/secret must never be requested");
    assert!(sink
        .errors()
        .iter()
        .any(|e| e.kind == PageErrorKind::RobotsDenied && e.url.ends_with("/secret")));

    // Redirect target is captured as the final URL.
    assert!(
        sink.final_paths().contains("/c"),
        "redirect should resolve to /c"
    );

    // Off-domain and non-navigational links are dropped.
    assert!(!fetched.iter().any(|p| p.contains("example.com")));

    // Deduplication: home is fetched once despite several inbound links.
    assert_eq!(server.hits("/"), 1);

    assert_eq!(summary.pages_fetched, 7);
    assert_eq!(summary.pages_failed, 1);
}

#[tokio::test]
async fn non_html_pages_have_no_links_or_title() {
    let server = TestServer::start().await;
    let sink = CollectSink::new();

    let config = base(&server.root()).build().unwrap();
    Engine::new(config, sink.clone())
        .unwrap()
        .run()
        .await
        .unwrap();

    let image = sink
        .pages()
        .into_iter()
        .find(|p| p.url.ends_with("/image.png"))
        .expect("image.png should be crawled");

    assert_eq!(image.status, 200);
    assert!(image.title.is_none());
    assert!(image.links.is_empty());
    assert_eq!(image.content_type.as_deref(), Some("image/png"));
}

#[tokio::test]
async fn respects_max_depth() {
    let server = TestServer::start().await;
    let sink = CollectSink::new();

    let config = base(&server.root()).max_depth(Some(1)).build().unwrap();
    Engine::new(config, sink.clone())
        .unwrap()
        .run()
        .await
        .unwrap();

    let fetched = sink.fetched_paths();
    for path in ["/", "/a", "/b", "/redirect", "/image.png"] {
        assert!(fetched.contains(path), "expected {path} at depth <= 1");
    }
    // Depth-2 pages must not be reached.
    assert!(!fetched.contains("/a1"), "/a1 is depth 2");
    assert!(!fetched.contains("/b1"), "/b1 is depth 2");
}

#[tokio::test]
async fn ignore_robots_fetches_disallowed() {
    let server = TestServer::start().await;
    let sink = CollectSink::new();

    let config = base(&server.root()).respect_robots(false).build().unwrap();
    let summary = Engine::new(config, sink.clone())
        .unwrap()
        .run()
        .await
        .unwrap();

    assert!(sink.fetched_paths().contains("/secret"));
    assert!(
        sink.errors().is_empty(),
        "no failures expected: {:?}",
        sink.errors()
    );
    assert_eq!(summary.pages_failed, 0);
    assert_eq!(summary.pages_fetched, 8);
}

#[tokio::test]
async fn honors_max_pages_budget() {
    let server = TestServer::start().await;
    let sink = CollectSink::new();

    let config = base(&server.root())
        .max_pages(Some(3))
        .concurrency(2)
        .build()
        .unwrap();
    let summary = Engine::new(config, sink.clone())
        .unwrap()
        .run()
        .await
        .unwrap();

    assert!(summary.pages_fetched >= 1);
    assert!(
        summary.pages_fetched <= 3,
        "budget exceeded: {}",
        summary.pages_fetched
    );
    assert_eq!(sink.pages().len() as u64, summary.pages_fetched);
    // Exactly the budgeted number of URLs are dispatched (a page or a failure).
    assert_eq!(summary.pages_fetched + summary.pages_failed, 3);
}
