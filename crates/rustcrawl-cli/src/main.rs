//! `rustcrawl` — command-line entry point.

mod cli;
mod jobs;
mod progress;

use std::io::IsTerminal;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use rustcrawl_core::sink::{JsonlSink, NullSink};
use rustcrawl_core::{CrawlConfig, CrawlSummary, Engine, Scope, Sink};
use tracing_subscriber::EnvFilter;
use url::Url;

use crate::cli::{Cli, ScopeArg};
use crate::jobs::{JobStore, JobTemplate};
use crate::progress::{DashboardAction, NewJobSpec};

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let show_ui = !cli.quiet && std::io::stderr().is_terminal();
    let job_store = JobStore::local();

    if cli.clear_jobs {
        job_store.clear()?;
        eprintln!("cleared local job history");
        return Ok(());
    }

    if !cli.has_crawl_target() {
        if show_ui {
            let mut action = progress::open_control_center(job_store.load()).await;
            loop {
                match action {
                    DashboardAction::Start(spec) => {
                        let request = RunRequest::Dashboard(spec);
                        action = run_interactive_job(&request, &job_store).await?;
                    }
                    DashboardAction::ClearJobs => {
                        job_store.clear()?;
                        action = progress::open_control_center(job_store.load()).await;
                    }
                    DashboardAction::Rerun | DashboardAction::Exit => break,
                }
            }
            return Ok(());
        }
        anyhow::bail!("provide at least one seed URL or --sitemap, or run in an interactive terminal to open the control center");
    }

    if show_ui {
        let mut request = RunRequest::Cli(&cli);
        loop {
            let action = run_interactive_job(&request, &job_store).await?;
            match action {
                DashboardAction::Rerun => {}
                DashboardAction::Start(spec) => request = RunRequest::Dashboard(spec),
                DashboardAction::ClearJobs => {
                    job_store.clear()?;
                }
                DashboardAction::Exit => break,
            }
        }
    } else {
        let prepared = PreparedRun::from_cli(&cli)?;
        let engine = Engine::new(prepared.config.clone(), prepared.sink.clone())?;
        install_signal_handler(&engine);
        seed_sitemaps(&engine, &prepared.sitemaps).await;
        let summary = engine.run().await?;
        prepared.save_if_needed(&job_store, &summary)?;
        report(&summary);
    }
    Ok(())
}

#[derive(Clone)]
enum RunRequest<'a> {
    Cli(&'a Cli),
    Dashboard(NewJobSpec),
}

impl RunRequest<'_> {
    fn editable_spec(&self) -> Option<NewJobSpec> {
        match self {
            RunRequest::Cli(cli) => {
                let target = cli
                    .seeds
                    .first()
                    .cloned()
                    .or_else(|| cli.sitemap.first().cloned())?;
                let scope = match cli.scope {
                    ScopeArg::Host => "host",
                    ScopeArg::Domain => "domain",
                    ScopeArg::Any => "any",
                }
                .to_string();
                Some(NewJobSpec {
                    target,
                    max_pages: cli.max_pages,
                    depth: cli.depth,
                    concurrency: cli.concurrency,
                    scope,
                    delay: humantime::format_duration(cli.delay).to_string(),
                    output: cli.output.as_ref().map(|p| p.display().to_string()),
                    save: !cli.no_save,
                })
            }
            RunRequest::Dashboard(spec) => Some(spec.clone()),
        }
    }
}

struct PreparedRun {
    config: CrawlConfig,
    sitemaps: Vec<Url>,
    sink: Arc<dyn Sink>,
    save_record: SaveRecord,
}

enum SaveRecord {
    Skip,
    Template(JobTemplate),
}

impl PreparedRun {
    fn from_cli(cli: &Cli) -> anyhow::Result<Self> {
        let config = cli.to_config()?;
        let sitemaps = cli.sitemap_urls()?;
        let sink = match &cli.output {
            Some(path) => Arc::new(
                JsonlSink::to_file(path)
                    .with_context(|| format!("could not open output file {}", path.display()))?,
            ) as Arc<dyn Sink>,
            None => Arc::new(JsonlSink::stdout()) as Arc<dyn Sink>,
        };
        Ok(Self {
            config,
            sitemaps,
            sink,
            save_record: if cli.no_save {
                SaveRecord::Skip
            } else {
                SaveRecord::Template(JobTemplate::from_cli(cli))
            },
        })
    }

    fn from_dashboard(spec: &NewJobSpec) -> anyhow::Result<Self> {
        let scope = match spec.scope.as_str() {
            "host" => Scope::Host,
            "domain" => Scope::Domain,
            "any" => Scope::Any,
            _ => anyhow::bail!("scope must be host, domain, or any"),
        };
        let delay = humantime::parse_duration(&spec.delay)
            .with_context(|| format!("invalid delay {:?}", spec.delay))?;
        let config = CrawlConfig::builder()
            .add_seed(&spec.target)?
            .max_pages(spec.max_pages)
            .max_depth(spec.depth)
            .concurrency(spec.concurrency)
            .scope(scope)
            .per_host_delay(delay)
            .build()?;
        let sink = match &spec.output {
            Some(path) => Arc::new(
                JsonlSink::to_file(PathBuf::from(path))
                    .with_context(|| format!("could not open output file {path}"))?,
            ) as Arc<dyn Sink>,
            None => Arc::new(NullSink) as Arc<dyn Sink>,
        };
        Ok(Self {
            config,
            sitemaps: Vec::new(),
            sink,
            save_record: if spec.save {
                SaveRecord::Template(JobTemplate::from_new_job(spec))
            } else {
                SaveRecord::Skip
            },
        })
    }

    fn save_if_needed(&self, job_store: &JobStore, summary: &CrawlSummary) -> anyhow::Result<()> {
        match &self.save_record {
            SaveRecord::Skip => Ok(()),
            SaveRecord::Template(template) => {
                let record = template.clone().into_record(summary);
                job_store.append(&record)
            }
        }
    }
}

async fn run_interactive_job(
    request: &RunRequest<'_>,
    job_store: &JobStore,
) -> anyhow::Result<DashboardAction> {
    let prepared = match &request {
        RunRequest::Cli(cli) => PreparedRun::from_cli(cli)?,
        RunRequest::Dashboard(spec) => PreparedRun::from_dashboard(spec)?,
    };

    let (engine, events) = Engine::with_events(prepared.config.clone(), prepared.sink.clone())?;
    let display = progress::spawn(
        engine.stats(),
        events,
        engine.control_handle(),
        job_store.load(),
        request.editable_spec(),
    );

    seed_sitemaps(&engine, &prepared.sitemaps).await;

    let summary = match engine.run().await {
        Ok(summary) => summary,
        Err(err) => {
            display.stop().await;
            return Err(err.into());
        }
    };
    prepared.save_if_needed(job_store, &summary)?;
    report(&summary);

    Ok(display.wait_for_action(summary, job_store.load()).await)
}

async fn seed_sitemaps(engine: &Engine, sitemaps: &[Url]) {
    for sitemap in sitemaps {
        let n = engine.seed_from_sitemap(sitemap).await;
        tracing::info!(%sitemap, seeded = n, "expanded sitemap");
    }
}

fn report(summary: &CrawlSummary) {
    eprintln!(
        "done: {} fetched, {} failed, {} discovered, {} in {:.1}s ({:.0} pages/s)",
        summary.pages_fetched,
        summary.pages_failed,
        summary.urls_discovered,
        progress::human_bytes(summary.bytes_downloaded),
        summary.duration.as_secs_f64(),
        if summary.duration.as_secs_f64() > 0.0 {
            summary.pages_fetched as f64 / summary.duration.as_secs_f64()
        } else {
            0.0
        },
    );
}

/// On Ctrl-C, request a graceful stop; a second Ctrl-C forces an exit.
fn install_signal_handler(engine: &Engine) {
    let shutdown = engine.shutdown_handle();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!("\nstopping… (Ctrl-C again to force quit)");
            shutdown.trigger();
            if tokio::signal::ctrl_c().await.is_ok() {
                std::process::exit(130);
            }
        }
    });
}

fn init_tracing(verbose: u8) {
    let level = match verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    let default = format!("warn,rustcrawl_core={level},rustcrawl_cli={level}");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();
}
