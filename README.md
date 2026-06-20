# rustcrawl

A fast, efficient, and embeddable web crawler written in Rust.

`rustcrawl` is a command line spider built on a small, clean core
library. It crawls in breadth first order from one or more seed URLs, respects
`robots.txt` and per host rate limits by default, and streams every page it
finds as [JSON Lines](https://jsonlines.org/), ready to pipe into an indexer,
a database, or `jq`.

The engine (`rustcrawl core`) is deliberately decoupled from the CLI so it can
be reused as the crawling layer of a larger system (a search index, an
archiver, a site auditor).

## Features

* **Async, concurrent engine** built on `tokio` with a configurable worker pool.
* **Politeness by default**: obeys `robots.txt` (including `Crawl delay`) and
  enforces a minimum delay between requests to the same host, while crawling
  different hosts in parallel.
* **Smart scoping**: stay on the same host, the same registrable domain
  (Public Suffix List aware), or roam the open web, plus include and exclude
  regex filters.
* **Robust fetching**: timeouts, bounded retries with backoff for transient
  network failures and temporary HTTP statuses, redirect following, and a hard
  cap on response body size.
* **Deduplication** via URL normalization, so the same page is never queued
  twice.
* **Sitemap seeding** from `sitemap.xml` (and sitemap indexes).
* **Crawl Deck terminal control center**: live request logs,
  request rate graph, success rate, stats, pause and resume, stop, and rerun.
  Machine readable JSON Lines stay on stdout so scripts still work.
* **Pluggable output**: implement one trait (`Sink`) to send crawled pages
  anywhere.

## Install

Requires a recent stable Rust toolchain ([rustup](https://rustup.rs/)).

```bash
git clone https://github.com/rustcrawl/rustcrawl
cd rustcrawl
```

### Run from source (local development)

If you just cloned the repo, `rustcrawl` is not on your PATH yet.
During development, run it through Cargo:

```bash
cargo run -p rustcrawl-cli -- https://example.com -n 50 -o pages.jsonl
```

Everything after `--` is passed to the crawler.

If you prefer running the built binary directly:

```bash
cargo build -p rustcrawl-cli
# Windows (PowerShell, debug build):
.\target\debug\rustcrawl.exe https://example.com -n 50

cargo build --release -p rustcrawl-cli
# Linux/macOS:
./target/release/rustcrawl https://example.com -n 50
# Windows (PowerShell, release build):
.\target\release\rustcrawl.exe https://example.com -n 50
```

To install `rustcrawl` on your PATH for daily use:

```bash
cargo install --path crates/rustcrawl-cli
# then, in any directory:
rustcrawl https://example.com -n 50
```

On Windows, ensure `%USERPROFILE%\.cargo\bin` is on your PATH (rustup usually
adds this). Open a **new terminal** after installing.

### Troubleshooting

**`cargo` or `rustcrawl` is not recognized**

Rust installs to `%USERPROFILE%\.cargo\bin`. If a terminal was open *before*
rustup ran, it won't see that path until you restart it.

PowerShell (current session only):

```powershell
$env:Path += ";$env:USERPROFILE\.cargo\bin"
cargo --version
```

Or close and reopen your terminal (or Cursor) and try again.

**Windows build errors about `dlltool.exe` or `link.exe`**

If the MSVC linker is missing, either install
[Visual Studio Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
with the "Desktop development with C++" workload, or use the GNU toolchain +
MinGW:

```powershell
rustup default stable-x86_64-pc-windows-gnu
# MinGW-w64 must provide dlltool/gcc on PATH (e.g. C:\Users\...\mingw64\bin)
```

### Verify it works

```bash
cargo run -p rustcrawl-cli -- --help
cargo test
```

## Usage

Crawl a site, staying within its domain, and write results to a file:

```powershell
.\target\debug\rustcrawl.exe https://example.com --max-pages 500 -o pages.jsonl
```

When run in an interactive terminal, `rustcrawl` opens a full screen dashboard.
Run it with no target to open the control center without starting a crawl:

```powershell
.\target\debug\rustcrawl.exe
```

Dashboard controls:

* `n`: create a new crawl job from the dashboard
* `c`: clear saved recent jobs when the control center is open
* Up / Down: select a recent job
* `e`: edit the selected recent job and run it again
* `p` / space: pause or resume leasing new URLs
* `s`: gracefully stop after in flight requests finish
* `r`: rerun the same crawl after a run finishes
* `q` / `Esc`: exit the dashboard

The new job form supports the high level per run settings you usually need while
iterating: target URL, max pages, depth, concurrency, scope, per host delay,
optional output file, and whether the job should be saved to local history.

For script friendly output with no dashboard, use `--quiet`:

```powershell
.\target\debug\rustcrawl.exe https://example.com -n 50 --quiet -o pages.jsonl
```

Skip local job history for throwaway runs:

```powershell
.\target\debug\rustcrawl.exe https://example.com -n 50 --quiet --no-save
```

Clear saved recent jobs from the command line:

```powershell
.\target\debug\rustcrawl.exe --clear-jobs
```

Pipe results straight into `jq` (progress is printed to stderr, data to stdout):

```bash
rustcrawl https://example.com -n 50 | jq -r '.url'
```

Seed from a sitemap and follow links two levels deep:

```bash
rustcrawl --sitemap https://example.com/sitemap.xml --depth 2
```

Tune concurrency and politeness, and restrict to a section of a site:

```bash
rustcrawl https://docs.example.com \
  --concurrency 32 \
  --delay 100ms \
  --include '/guide/' \
  --exclude '\.(png|jpg|pdf)$'
```

### Common options

| Flag | Description | Default |
|------|-------------|---------|
| `<URL>...` | Seed URLs | none |
| `--sitemap <URL>` | Also seed from a sitemap (repeatable) | none |
| `-d, --depth <N>` | Maximum link depth | unlimited |
| `-n, --max-pages <N>` | Stop after N pages | unlimited |
| `-c, --concurrency <N>` | Concurrent in flight requests across all hosts, 1 to 1024 | 16 |
| `--delay <DUR>` | Min delay per host (e.g. `250ms`, `1s`) | 250ms |
| `--ignore-crawl-delay` | Ignore `Crawl-delay` while still obeying robots allow and deny rules | off |
| `--timeout <DUR>` | Per-request timeout | 30s |
| `--retries <N>` | Retries for transient network failures and temporary HTTP statuses | 2 |
| `--scope <host\|domain\|any>` | How far to roam | domain |
| `--include <REGEX>` | Only crawl matching URLs (repeatable) | none |
| `--exclude <REGEX>` | Skip matching URLs (repeatable) | none |
| `--user-agent <STRING>` | Override the User-Agent | project UA |
| `--ignore-robots` | Do not obey `robots.txt` | off |
| `-o, --output <FILE>` | Write JSON Lines to a file | stdout |
| `-q, --quiet` | Disable the dashboard; final summary only | off |
| `--no-save` | Do not write this run to local job history | off |
| `--clear-jobs` | Clear local job history and exit | off |
| `-v` | Increase log verbosity (`-v`, `-vv`) | warn |

Run `rustcrawl --help` for the full list.

### Output

Each crawled page is one JSON object:

```json
{
  "url": "https://example.com/",
  "final_url": "https://example.com/",
  "status": 200,
  "depth": 0,
  "referrer": null,
  "content_type": "text/html; charset=utf-8",
  "title": "Example Domain",
  "content_length": 1256,
  "links": ["https://example.com/about"],
  "fetched_at": "2026-01-01T00:00:00Z",
  "elapsed_ms": 42
}
```

## Architecture

```
                ┌────────────────────────── rustcrawl-cli ──────────────────────────┐
                │  clap args ─► CrawlConfig      Crawl Deck (ratatui, stderr)        │
                └───────────────┬───────────────────────────▲───────────────────────┘
                                │                            │ events / stats
                ┌───────────────▼────────────── rustcrawl-core ──────────────────────┐
                │                                                                     │
                │   Engine  ──spawns──►  worker pool (tokio tasks)                    │
                │     │                       │                                       │
                │     ▼                       ▼                                       │
                │  Frontier            Fetcher ─► RobotsCache                         │
                │  (dedup,             (retries,    (robots.txt                       │
                │   per host            timeouts,    cache + delay)                   │
                │   scheduling,         size cap)        │                            │
                │   budgets)                 │           ▼                            │
                │     ▲                      ▼        parser (links, title)           │
                │     └──── enqueue in-scope links ◄── UrlFilter (scope + regex)      │
                │                            │                                        │
                │                            ▼                                        │
                │                          Sink  ─►  JSON Lines / your storage        │
                └───────────────────────────────────────────────────────────────────┘
```

The pieces are intentionally small and independently testable:

* **`Frontier`**, the politeness aware URL queue: deduplication, per host
  scheduling, depth tracking, and the page budget.
* **`Fetcher`**, HTTP with retries, timeouts, and a response size ceiling.
* **`RobotsCache`**, per host `robots.txt`, including `Crawl delay`.
* **`parser` / `sitemap`**, link and title extraction plus sitemap parsing.
* **`UrlFilter`**, scope and include or exclude rules.
* **`Sink`**, the output seam; implement it to plug into anything.
* **`Engine`**, wires it all together and runs the worker pool.

## Using the engine as a library

```rust,no_run
use rustcrawl_core::{sink::JsonlSink, CrawlConfig, Engine, Result};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let config = CrawlConfig::builder()
        .add_seed("https://example.com")?
        .max_pages(Some(100))
        .concurrency(8)
        .build()?;

    let summary = Engine::new(config, Arc::new(JsonlSink::stdout()))?
        .run()
        .await?;

    eprintln!("crawled {} pages", summary.pages_fetched);
    Ok(())
}
```

## Contributing

Contributions are very welcome, see [CONTRIBUTING.md](CONTRIBUTING.md).

If you want to help, this project is intentionally open to practical
improvements from real crawling and search workflows. Good contributions include:

* Better crawl correctness and safety (URL normalization edge cases, robots
  behavior, dedup quality, stronger defaults).
* Better Crawl Deck UX (new control views, clearer metrics, better job form
  ergonomics, keyboard workflow improvements).
* Better reliability and performance (frontier efficiency, fetch throughput,
  retry behavior, memory pressure control, large crawl stability).
* Better developer experience (cleaner local setup, easier Windows support,
  packaging, release automation, onboarding docs).
* Better interoperability (new `Sink` implementations for common data stores,
  index pipelines, and analytics stacks).
* Better test depth (integration scenarios, regression suites, failure mode
  tests, reproducible fixtures).

What we want most: small, focused PRs with clear behavior changes, tests for
new logic, and defaults that keep the crawler polite and safe. If you are not
sure where to start, open an issue with your idea and we can shape it together.

## Responsible Use and Disclaimer

`rustcrawl` is a general purpose crawler framework and terminal control center.
You are solely responsible for how you use it.

By using this software, you agree to all of the following:

1. You will comply with all applicable laws, regulations, contracts, and site
   terms of service in your jurisdiction.
2. You will respect `robots.txt`, rate limits, and operational safety practices
   for systems you crawl.
3. You will only crawl content you are authorized to access and process.
4. You are responsible for any legal, operational, or financial consequences of
   your usage, including traffic impact and data handling.

This project is provided for legitimate engineering and research workflows.
It is provided **as is**, without warranty of any kind, express or implied.
The maintainers and contributors assume no responsibility or liability for
misuse, damages, claims, or losses resulting from use of this software.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.
