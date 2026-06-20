use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use rustcrawl_core::frontier::Lease;
use rustcrawl_core::normalize::normalize;
use rustcrawl_core::parser::parse_html;
use rustcrawl_core::{CrawlConfig, CrawlTask, Frontier};
use url::Url;

fn bench_normalize(c: &mut Criterion) {
    let urls = [
        "HTTPS://Example.COM:443/path/page.html#section",
        "http://example.com:80/?",
        "https://www.example.com/search?q=rust&sort=desc#top",
        "https://cdn.example.com/assets/app.js?v=123",
    ]
    .into_iter()
    .map(|url| Url::parse(url).expect("benchmark URL should parse"))
    .collect::<Vec<_>>();

    let mut index = 0usize;
    c.bench_function("normalize/mixed_urls", |b| {
        b.iter(|| {
            let url = urls[index % urls.len()].clone();
            index = index.wrapping_add(1);
            black_box(normalize(black_box(url)));
        });
    });
}

fn bench_frontier(c: &mut Criterion) {
    let mut group = c.benchmark_group("frontier");

    for count in [100usize, 1_000, 10_000] {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("add_unique", count),
            &count,
            |b, &count| {
                b.iter_batched(
                    frontier_config,
                    |config| {
                        let mut frontier = Frontier::new(&config);
                        for task in crawl_tasks(count, 64) {
                            black_box(frontier.add(black_box(task)));
                        }
                        black_box(frontier.queued());
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("lease_and_complete", count),
            &count,
            |b, &count| {
                b.iter_batched(
                    || {
                        let config = frontier_config();
                        let mut frontier = Frontier::new(&config);
                        for task in crawl_tasks(count, 64) {
                            assert!(frontier.add(task));
                        }
                        frontier
                    },
                    |mut frontier| {
                        let now = Instant::now();
                        let mut leased = 0usize;
                        loop {
                            match frontier.lease(now) {
                                Lease::Ready(task) => {
                                    black_box(task);
                                    leased += 1;
                                    frontier.complete();
                                }
                                Lease::Done => break,
                                Lease::Idle | Lease::Wait(_) => {
                                    panic!("zero-delay benchmark frontier should not idle or wait")
                                }
                            }
                        }
                        black_box(leased);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_parse_html(c: &mut Criterion) {
    let base =
        Url::parse("https://example.com/root/index.html").expect("benchmark URL should parse");
    let mut group = c.benchmark_group("parser");

    for links in [25usize, 250, 1_000] {
        let html = synthetic_html(links);
        group.throughput(Throughput::Elements(links as u64));
        group.bench_with_input(
            BenchmarkId::new("parse_html_links", links),
            &html,
            |b, html| {
                b.iter(|| black_box(parse_html(black_box(&base), black_box(html))));
            },
        );
    }

    group.finish();
}

fn frontier_config() -> CrawlConfig {
    CrawlConfig::builder()
        .add_seed("https://example.com/")
        .expect("benchmark seed should parse")
        .per_host_delay(Duration::ZERO)
        .scope(rustcrawl_core::Scope::Any)
        .build()
        .expect("benchmark config should build")
}

fn crawl_tasks(count: usize, host_count: usize) -> impl Iterator<Item = CrawlTask> {
    (0..count).map(move |i| {
        let url = Url::parse(&format!(
            "https://host{}.example.com/path/page-{}?q={}",
            i % host_count,
            i,
            i % 17
        ))
        .expect("benchmark URL should parse");
        CrawlTask::seed(url)
    })
}

fn synthetic_html(link_count: usize) -> String {
    let mut html = String::from("<!doctype html><html><head><title>Benchmark</title></head><body>");
    for i in 0..link_count {
        html.push_str(&format!(
            r#"<a href="/section/{}/page-{}?q={}">link {}</a>"#,
            i % 20,
            i,
            i % 11,
            i
        ));
    }
    html.push_str("</body></html>");
    html
}

criterion_group!(benches, bench_normalize, bench_frontier, bench_parse_html);
criterion_main!(benches);
