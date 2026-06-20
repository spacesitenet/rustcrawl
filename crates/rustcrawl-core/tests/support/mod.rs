//! Shared test scaffolding: a minimal local HTTP server that serves a small,
//! controlled website, plus a sink that collects results for assertions.
//!
//! Keeping this self-contained (no mock-server dependency) means the
//! integration tests are hermetic and fast: every byte the crawler sees is
//! defined right here.

#![allow(dead_code)]
#![allow(unreachable_pub)]

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rustcrawl_core::page::{CrawledPage, PageError};
use rustcrawl_core::{Result, Sink};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use url::Url;

/// A running test server. The site graph it serves:
///
/// ```text
///   /  ──► /a ──► /a1
///   │  └─► /b ──► /b1
///   │  ├─► /secret      (disallowed by robots.txt)
///   │  ├─► /redirect ─302─► /c
///   │  ├─► /image.png   (non-HTML)
///   │  └─► https://example.com/ext  (off-domain, must be skipped)
/// ```
pub struct TestServer {
    pub addr: SocketAddr,
    hits: Arc<Mutex<HashMap<String, usize>>>,
}

impl TestServer {
    /// Bind to an ephemeral port and start serving in the background.
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let hits = Arc::new(Mutex::new(HashMap::new()));

        let hits_bg = hits.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let hits = hits_bg.clone();
                tokio::spawn(async move { handle(stream, hits).await });
            }
        });

        Self { addr, hits }
    }

    /// The site root, e.g. `http://127.0.0.1:54321/`.
    pub fn root(&self) -> String {
        format!("http://{}/", self.addr)
    }

    /// How many times a given path was requested (includes `/robots.txt`).
    pub fn hits(&self, path: &str) -> usize {
        self.hits.lock().unwrap().get(path).copied().unwrap_or(0)
    }
}

async fn handle(mut stream: TcpStream, hits: Arc<Mutex<HashMap<String, usize>>>) {
    let mut buf = [0u8; 2048];
    let n = stream.read(&mut buf).await.unwrap_or(0);
    if n == 0 {
        return;
    }
    let request = String::from_utf8_lossy(&buf[..n]);
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .map(|p| p.split(['?', '#']).next().unwrap_or(p).to_string())
        .unwrap_or_else(|| "/".to_string());

    *hits.lock().unwrap().entry(path.clone()).or_insert(0) += 1;

    let route = route(&path);
    let mut head = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        route.status,
        route.content_type,
        route.body.len()
    );
    if let Some(location) = route.location {
        head.push_str(&format!("Location: {location}\r\n"));
    }
    head.push_str("\r\n");

    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(&route.body).await;
    let _ = stream.shutdown().await;
}

struct Route {
    status: &'static str,
    content_type: &'static str,
    body: Vec<u8>,
    location: Option<&'static str>,
}

fn html(status: &'static str, body: &str) -> Route {
    Route {
        status,
        content_type: "text/html; charset=utf-8",
        body: body.as_bytes().to_vec(),
        location: None,
    }
}

fn route(path: &str) -> Route {
    match path {
        "/" => html(
            "200 OK",
            r##"<!doctype html><html><head><title>Home</title></head><body>
                <a href="/a">a</a>
                <a href="/a">a duplicate</a>
                <a href="/a#section">a fragment</a>
                <a href="/b">b</a>
                <a href="/secret">secret</a>
                <a href="/redirect">redirect</a>
                <a href="/image.png">image</a>
                <a href="https://example.com/ext">external</a>
                <a href="mailto:hi@example.com">mail</a>
            </body></html>"##,
        ),
        "/a" => html(
            "200 OK",
            r#"<html><head><title>A</title></head><body>
                <a href="/a1">a1</a>
                <a href="/">home</a>
            </body></html>"#,
        ),
        "/b" => html(
            "200 OK",
            r#"<html><head><title>B</title></head><body><a href="/b1">b1</a></body></html>"#,
        ),
        "/a1" => html(
            "200 OK",
            r#"<html><head><title>A1</title></head><body>leaf</body></html>"#,
        ),
        "/b1" => html(
            "200 OK",
            r#"<html><head><title>B1</title></head><body>leaf</body></html>"#,
        ),
        "/c" => html(
            "200 OK",
            r#"<html><head><title>C</title></head><body>leaf</body></html>"#,
        ),
        "/secret" => html(
            "200 OK",
            r#"<html><head><title>Secret</title></head><body>classified</body></html>"#,
        ),
        "/redirect" => Route {
            status: "302 Found",
            content_type: "text/html; charset=utf-8",
            body: Vec::new(),
            location: Some("/c"),
        },
        "/image.png" => Route {
            status: "200 OK",
            content_type: "image/png",
            body: vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 0],
            location: None,
        },
        "/robots.txt" => Route {
            status: "200 OK",
            content_type: "text/plain",
            body: b"User-agent: *\nDisallow: /secret\n".to_vec(),
            location: None,
        },
        _ => html("404 Not Found", "<html><body>not found</body></html>"),
    }
}

/// A [`Sink`] that records everything in memory for later assertions.
#[derive(Clone, Default)]
pub struct CollectSink {
    pages: Arc<Mutex<Vec<CrawledPage>>>,
    errors: Arc<Mutex<Vec<PageError>>>,
}

impl CollectSink {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Snapshot of all successfully crawled pages.
    pub fn pages(&self) -> Vec<CrawledPage> {
        self.pages.lock().unwrap().clone()
    }

    /// Snapshot of all recorded per-page errors.
    pub fn errors(&self) -> Vec<PageError> {
        self.errors.lock().unwrap().clone()
    }

    /// The set of *requested* URL paths among fetched pages.
    pub fn fetched_paths(&self) -> HashSet<String> {
        self.pages()
            .iter()
            .filter_map(|p| path_of(&p.url))
            .collect()
    }

    /// The set of *final* URL paths (after redirects) among fetched pages.
    pub fn final_paths(&self) -> HashSet<String> {
        self.pages()
            .iter()
            .filter_map(|p| path_of(&p.final_url))
            .collect()
    }
}

#[async_trait]
impl Sink for CollectSink {
    async fn page(&self, page: &CrawledPage) -> Result<()> {
        self.pages.lock().unwrap().push(page.clone());
        Ok(())
    }

    async fn error(&self, error: &PageError) -> Result<()> {
        self.errors.lock().unwrap().push(error.clone());
        Ok(())
    }
}

fn path_of(url: &str) -> Option<String> {
    Url::parse(url).ok().map(|u| u.path().to_string())
}
