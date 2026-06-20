use std::hint::black_box;
use std::time::{Duration, Instant};

use rustcrawl_core::frontier::Lease;
use rustcrawl_core::normalize::normalize;
use rustcrawl_core::parser::parse_html;
use rustcrawl_core::{CrawlConfig, CrawlTask, Frontier};
use url::Url;

fn main() {
    println!("rustcrawl-core hot path benchmarks");
    println!("lower is better; throughput is approximate\n");

    bench_normalize();
    bench_frontier();
    bench_parse_html();
}

fn bench_normalize() {
    let urls = [
        "HTTPS://Example.COM:443/path/page.html#section",
        "http://example.com:80/?",
        "https://www.example.com/search?q=rust&sort=desc#top",
        "https://cdn.example.com/assets/app.js?v=123",
    ]
    .into_iter()
    .map(|url| Url::parse(url).expect("benchmark URL should parse"))
    .collect::<Vec<_>>();

    bench("normalize/mixed_urls", 2_000_000, || {
        let mut index = 0usize;
        move || {
            let url = urls[index % urls.len()].clone();
            index = index.wrapping_add(1);
            black_box(normalize(black_box(url)));
        }
    });
}

fn bench_frontier() {
    for count in [100usize, 1_000, 10_000] {
        bench_items(
            &format!("frontier/add_unique/{count}"),
            iterations_for(count),
            count,
            || {
                let config = frontier_config();
                move || {
                    let mut frontier = Frontier::new(&config);
                    for task in crawl_tasks(count, 64) {
                        black_box(frontier.add(black_box(task)));
                    }
                    black_box(frontier.queued());
                }
            },
        );

        bench_items(
            &format!("frontier/lease_and_complete/{count}"),
            iterations_for(count),
            count,
            || {
                move || {
                    let config = frontier_config();
                    let mut frontier = Frontier::new(&config);
                    for task in crawl_tasks(count, 64) {
                        assert!(frontier.add(task));
                    }

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
                }
            },
        );
    }
}

fn bench_parse_html() {
    let base =
        Url::parse("https://example.com/root/index.html").expect("benchmark URL should parse");

    for links in [25usize, 250, 1_000] {
        let html = synthetic_html(links);
        bench_items(
            &format!("parser/parse_html_links/{links}"),
            iterations_for(links),
            links,
            || {
                let base = base.clone();
                let html = html.clone();
                move || {
                    black_box(parse_html(black_box(&base), black_box(&html)));
                }
            },
        );
    }
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

fn iterations_for(items_per_iter: usize) -> u64 {
    (100_000 / items_per_iter.max(1)).clamp(20, 2_000) as u64
}

fn bench<F, G>(name: &str, iterations: u64, setup: F)
where
    F: FnOnce() -> G,
    G: FnMut(),
{
    let mut run = setup();
    let started = Instant::now();
    for _ in 0..iterations {
        run();
    }
    let elapsed = started.elapsed();
    let ns_per_iter = elapsed.as_nanos() as f64 / iterations as f64;
    println!("{name:<36} {:>12.2} ns/iter", ns_per_iter);
}

fn bench_items<F, G>(name: &str, iterations: u64, items_per_iter: usize, setup: F)
where
    F: FnOnce() -> G,
    G: FnMut(),
{
    let mut run = setup();
    let started = Instant::now();
    for _ in 0..iterations {
        run();
    }
    let elapsed = started.elapsed();
    let total_items = iterations as f64 * items_per_iter as f64;
    let ns_per_item = elapsed.as_nanos() as f64 / total_items;
    let items_per_sec = total_items / elapsed.as_secs_f64();
    println!(
        "{name:<36} {:>12.2} ns/item  {:>10.0} items/s",
        ns_per_item, items_per_sec
    );
}
