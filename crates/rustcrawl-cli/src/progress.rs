//! Crawl Deck: a terminal dashboard for watching and controlling crawls.
//!
//! The display is intentionally decoupled from the engine: it reads the shared
//! [`Stats`] for aggregate numbers and consumes a stream of [`CrawlEvent`]s to
//! show recent crawl activity. Output goes to stderr, leaving stdout free for
//! machine-readable JSON Lines.

use std::collections::VecDeque;
use std::io::{self, Stderr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Axis, Block, Borders, Chart, Dataset, List, ListItem, Paragraph, Wrap};
use ratatui::{Frame, Terminal};
use rustcrawl_core::{
    CrawlControl, CrawlEvent, CrawlSummary, Stats, StatsSnapshot, DEFAULT_CONCURRENCY,
    MAX_CONCURRENCY,
};
use tokio::sync::mpsc::{self, Receiver, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;

use crate::jobs::{JobHistory, JobRecord};

const MAX_LOG_LINES: usize = 300;
const MAX_SAMPLES: usize = 120;
const SAVE_JOB_FIELD_INDEX: usize = 7;

struct RunInit {
    jobs: JobHistory,
    last_run_spec: Option<NewJobSpec>,
}

/// Spawn the dashboard. Returns a [`ProgressHandle`] used to stop it.
pub(crate) fn spawn(
    stats: Arc<Stats>,
    events: Receiver<CrawlEvent>,
    control: CrawlControl,
    jobs: JobHistory,
    last_run_spec: Option<NewJobSpec>,
) -> ProgressHandle {
    let (stop_tx, stop_rx) = oneshot::channel();
    let (message_tx, message_rx) = mpsc::unbounded_channel();
    let (action_tx, action_rx) = mpsc::unbounded_channel();
    let init = RunInit {
        jobs,
        last_run_spec,
    };
    let task = tokio::spawn(run(
        stats, events, control, init, message_rx, action_tx, stop_rx,
    ));
    ProgressHandle {
        stop: Some(stop_tx),
        messages: message_tx,
        actions: action_rx,
        task,
    }
}

/// Open the dashboard as a job history/control center without starting a crawl.
pub(crate) async fn open_control_center(jobs: JobHistory) -> DashboardAction {
    let mut terminal = match DashboardTerminal::enter() {
        Ok(t) => t,
        Err(err) => {
            eprintln!("failed to start dashboard: {err}");
            return DashboardAction::Exit;
        }
    };

    let (action_tx, mut action_rx) = mpsc::unbounded_channel();
    let mut app = Dashboard::idle(jobs);
    let mut ticker = tokio::time::interval(Duration::from_millis(150));

    loop {
        tokio::select! {
            action = action_rx.recv() => {
                break action.unwrap_or(DashboardAction::Exit);
            }
            _ = ticker.tick() => {
                if let Err(err) = terminal.draw(&app) {
                    eprintln!("dashboard draw error: {err}");
                    break DashboardAction::Exit;
                }
                handle_keys(None, &mut app, &action_tx);
            }
        }
    }
}

/// Controls the lifetime of the spawned progress display.
pub(crate) struct ProgressHandle {
    stop: Option<oneshot::Sender<()>>,
    messages: UnboundedSender<DashboardMessage>,
    actions: UnboundedReceiver<DashboardAction>,
    task: tokio::task::JoinHandle<()>,
}

impl ProgressHandle {
    /// Mark the crawl as complete and keep the dashboard open until the user
    /// chooses to rerun or exit.
    pub(crate) async fn wait_for_action(
        mut self,
        summary: CrawlSummary,
        jobs: JobHistory,
    ) -> DashboardAction {
        let _ = self
            .messages
            .send(DashboardMessage::Finished { summary, jobs });
        let action = self.actions.recv().await.unwrap_or(DashboardAction::Exit);
        self.stop().await;
        action
    }

    /// Stop the display immediately and wait for it to clear the terminal.
    pub(crate) async fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let _ = self.task.await;
    }
}

async fn run(
    stats: Arc<Stats>,
    mut events: Receiver<CrawlEvent>,
    control: CrawlControl,
    init: RunInit,
    mut messages: UnboundedReceiver<DashboardMessage>,
    actions: UnboundedSender<DashboardAction>,
    mut stop: oneshot::Receiver<()>,
) {
    let mut terminal = match DashboardTerminal::enter() {
        Ok(t) => t,
        Err(err) => {
            eprintln!("failed to start dashboard: {err}");
            return;
        }
    };

    let mut app = Dashboard::running(init.jobs, init.last_run_spec);
    let mut ticker = tokio::time::interval(Duration::from_millis(150));

    loop {
        tokio::select! {
            _ = &mut stop => break,
            event = events.recv() => {
                if let Some(ev) = event {
                    app.push_event(ev);
                }
            }
            message = messages.recv() => {
                if let Some(DashboardMessage::Finished { summary, jobs }) = message {
                    app.finish(summary, jobs);
                    if let Err(err) = terminal.draw(&app) {
                        eprintln!("dashboard draw error: {err}");
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                app.tick(stats.snapshot(), control.is_paused(), control.is_stopping());
                if let Err(err) = terminal.draw(&app) {
                    eprintln!("dashboard draw error: {err}");
                    break;
                }
                handle_keys(Some(&control), &mut app, &actions);
            }
        }
    }
    drop(terminal);
}

#[derive(Debug, Clone)]
enum DashboardMessage {
    Finished {
        summary: CrawlSummary,
        jobs: JobHistory,
    },
}

/// What the user wants to do from the dashboard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DashboardAction {
    /// Close the dashboard and return to the shell.
    Exit,
    /// Clear saved job history.
    ClearJobs,
    /// Start a fresh crawl with the same CLI options.
    Rerun,
    /// Start a crawl from the dashboard form.
    Start(NewJobSpec),
}

/// User-entered crawl settings from the dashboard form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NewJobSpec {
    pub target: String,
    pub max_pages: Option<usize>,
    pub depth: Option<u32>,
    pub concurrency: usize,
    pub scope: String,
    pub delay: String,
    pub output: Option<String>,
    pub save: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ViewMode {
    Dashboard,
    NewJob,
}

#[derive(Debug, Clone)]
struct NewJobForm {
    fields: Vec<FormField>,
    active: usize,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct FormField {
    label: &'static str,
    help: &'static str,
    value: String,
}

impl NewJobForm {
    fn new() -> Self {
        Self {
            fields: vec![
                FormField {
                    label: "Target URL",
                    help: "required; http(s) URL or sitemap URL",
                    value: String::new(),
                },
                FormField {
                    label: "Max pages",
                    help: "blank = unlimited",
                    value: "100".to_string(),
                },
                FormField {
                    label: "Depth",
                    help: "blank = unlimited",
                    value: "1".to_string(),
                },
                FormField {
                    label: "Concurrency",
                    help: "parallel requests, 1..1024",
                    value: DEFAULT_CONCURRENCY.to_string(),
                },
                FormField {
                    label: "Scope",
                    help: "host | domain | any",
                    value: "domain".to_string(),
                },
                FormField {
                    label: "Delay",
                    help: "per-host delay, e.g. 250ms or 1s",
                    value: "250ms".to_string(),
                },
                FormField {
                    label: "Output",
                    help: "blank = do not write JSONL from dashboard-created job",
                    value: String::new(),
                },
                FormField {
                    label: "Save job",
                    help: "(blank) | y | n (default y)",
                    value: "y".to_string(),
                },
            ],
            active: 0,
            error: None,
        }
    }

    fn from_spec(spec: NewJobSpec) -> Self {
        let mut form = Self::new();
        form.fields[0].value = spec.target;
        form.fields[1].value = spec.max_pages.map(|v| v.to_string()).unwrap_or_default();
        form.fields[2].value = spec.depth.map(|v| v.to_string()).unwrap_or_default();
        form.fields[3].value = spec.concurrency.to_string();
        form.fields[4].value = spec.scope;
        form.fields[5].value = spec.delay;
        form.fields[6].value = spec.output.unwrap_or_default();
        form.fields[7].value = if spec.save { "y" } else { "n" }.to_string();
        form
    }

    fn active_value_mut(&mut self) -> &mut String {
        &mut self.fields[self.active].value
    }

    fn next(&mut self) {
        self.active = (self.active + 1) % self.fields.len();
    }

    fn prev(&mut self) {
        self.active = if self.active == 0 {
            self.fields.len() - 1
        } else {
            self.active - 1
        };
    }

    fn to_spec(&self) -> Result<NewJobSpec, String> {
        let get = |idx: usize| self.fields[idx].value.trim();
        let target = get(0).to_string();
        if target.is_empty() {
            return Err("target URL is required".into());
        }
        let max_pages = parse_optional::<usize>(get(1), "max pages")?;
        let depth = parse_optional::<u32>(get(2), "depth")?;
        let concurrency = get(3)
            .parse::<usize>()
            .map_err(|_| "concurrency must be a number".to_string())?;
        if concurrency == 0 {
            return Err("concurrency must be at least 1".into());
        }
        if concurrency > MAX_CONCURRENCY {
            return Err(format!("concurrency must be at most {MAX_CONCURRENCY}"));
        }
        let scope = get(4).to_ascii_lowercase();
        if !matches!(scope.as_str(), "host" | "domain" | "any") {
            return Err("scope must be host, domain, or any".into());
        }
        let delay = get(5).to_string();
        if delay.is_empty() {
            return Err("delay is required (try 250ms)".into());
        }
        let output = if get(6).is_empty() {
            None
        } else {
            Some(get(6).to_string())
        };
        let save_raw = get(7).to_ascii_lowercase();
        let save = match save_raw.as_str() {
            "" => true,
            "yes" | "y" | "true" | "1" => true,
            "no" | "n" | "false" | "0" => false,
            _ => return Err("save job must be y/n (default y)".into()),
        };

        Ok(NewJobSpec {
            target,
            max_pages,
            depth,
            concurrency,
            scope,
            delay,
            output,
            save,
        })
    }
}

impl NewJobSpec {
    fn from_job(job: &JobRecord) -> Self {
        Self {
            target: job.target_label(),
            max_pages: job.max_pages,
            depth: job.max_depth,
            concurrency: job.concurrency,
            scope: job.scope.clone().unwrap_or_else(|| "domain".to_string()),
            delay: job.delay.clone().unwrap_or_else(|| "250ms".to_string()),
            output: job.output.clone(),
            save: true,
        }
    }
}

fn parse_optional<T>(value: &str, label: &str) -> Result<Option<T>, String>
where
    T: std::str::FromStr,
{
    if value.trim().is_empty() {
        Ok(None)
    } else {
        value
            .parse::<T>()
            .map(Some)
            .map_err(|_| format!("{label} must be a number or blank"))
    }
}

/// A terminal in alternate-screen/raw mode. Dropping restores the user's shell.
struct DashboardTerminal {
    terminal: Terminal<CrosstermBackend<Stderr>>,
}

impl DashboardTerminal {
    fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        let mut stderr = io::stderr();
        execute!(stderr, EnterAlternateScreen)?;
        let terminal = Terminal::new(CrosstermBackend::new(stderr))?;
        Ok(Self { terminal })
    }

    fn draw(&mut self, app: &Dashboard) -> io::Result<()> {
        self.terminal.draw(|frame| app.render(frame))?;
        Ok(())
    }
}

impl Drop for DashboardTerminal {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

#[derive(Debug)]
struct Dashboard {
    logs: VecDeque<LogLine>,
    samples: VecDeque<Sample>,
    last_snapshot: StatsSnapshot,
    last_sample: Option<(Instant, u64)>,
    status: Status,
    last_key_hint: Option<String>,
    summary: Option<CrawlSummary>,
    jobs: JobHistory,
    last_run_spec: Option<NewJobSpec>,
    mode: ViewMode,
    new_job: NewJobForm,
    selected_job: usize,
}

impl Dashboard {
    fn running(jobs: JobHistory, last_run_spec: Option<NewJobSpec>) -> Self {
        Self {
            logs: VecDeque::with_capacity(MAX_LOG_LINES),
            samples: VecDeque::with_capacity(MAX_SAMPLES),
            last_snapshot: StatsSnapshot {
                fetched: 0,
                failed: 0,
                discovered: 0,
                in_flight: 0,
                bytes: 0,
                elapsed_secs: 0.0,
            },
            last_sample: None,
            status: Status::Running,
            last_key_hint: None,
            summary: None,
            jobs,
            last_run_spec,
            mode: ViewMode::Dashboard,
            new_job: NewJobForm::new(),
            selected_job: 0,
        }
    }

    fn idle(jobs: JobHistory) -> Self {
        let mut dashboard = Self::running(jobs, None);
        dashboard.status = Status::Idle;
        dashboard.last_key_hint =
            Some("control center: n new job, c clear jobs, q/Esc exits".to_string());
        dashboard
    }

    fn push_event(&mut self, event: CrawlEvent) {
        if self.logs.len() == MAX_LOG_LINES {
            self.logs.pop_front();
        }
        self.logs.push_back(LogLine::from_event(event));
    }

    fn tick(&mut self, snapshot: StatsSnapshot, paused: bool, stopping: bool) {
        if self.summary.is_some() {
            self.status = Status::Finished;
            return;
        }

        self.status = if stopping {
            Status::Stopping
        } else if paused {
            Status::Paused
        } else {
            Status::Running
        };

        let now = Instant::now();
        let rate = match self.last_sample {
            Some((last_at, last_fetched)) => {
                let secs = now.duration_since(last_at).as_secs_f64();
                if secs > 0.0 {
                    (snapshot.fetched.saturating_sub(last_fetched) as f64) / secs
                } else {
                    0.0
                }
            }
            None => 0.0,
        };
        self.last_sample = Some((now, snapshot.fetched));

        if self.samples.len() == MAX_SAMPLES {
            self.samples.pop_front();
        }
        self.samples.push_back(Sample {
            x: snapshot.elapsed_secs,
            rate,
        });

        self.last_snapshot = snapshot;
    }

    fn finish(&mut self, summary: CrawlSummary, jobs: JobHistory) {
        self.status = Status::Finished;
        self.last_snapshot = StatsSnapshot {
            fetched: summary.pages_fetched,
            failed: summary.pages_failed,
            discovered: summary.urls_discovered,
            in_flight: 0,
            bytes: summary.bytes_downloaded,
            elapsed_secs: summary.duration.as_secs_f64(),
        };
        self.last_key_hint = Some("finished: r rerun, e edit settings, q/Esc exit".to_string());
        self.summary = Some(summary);
        self.jobs = jobs;
        self.clamp_selected_job();
    }

    fn selected_job(&self) -> Option<&JobRecord> {
        self.jobs.recent.iter().rev().nth(self.selected_job)
    }

    fn select_next_job(&mut self) {
        if self.jobs.recent.is_empty() {
            return;
        }
        self.selected_job = (self.selected_job + 1).min(self.jobs.recent.len() - 1);
    }

    fn select_prev_job(&mut self) {
        self.selected_job = self.selected_job.saturating_sub(1);
    }

    fn clamp_selected_job(&mut self) {
        if self.jobs.recent.is_empty() {
            self.selected_job = 0;
        } else {
            self.selected_job = self.selected_job.min(self.jobs.recent.len() - 1);
        }
    }

    fn render(&self, frame: &mut Frame<'_>) {
        let root = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Length(7),
                Constraint::Min(12),
                Constraint::Length(3),
            ])
            .split(frame.area());

        self.render_header(frame, root[0]);
        self.render_stats(frame, root[1]);

        if self.mode == ViewMode::NewJob {
            self.render_new_job(frame, root[2]);
        } else {
            let body = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(42), Constraint::Percentage(58)])
                .split(root[2]);
            let left = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
                .split(body[0]);
            self.render_chart(frame, left[0]);
            self.render_job_stats(frame, left[1]);
            self.render_logs(frame, body[1]);
        }

        self.render_footer(frame, root[3]);
    }

    fn render_header(&self, frame: &mut Frame<'_>, area: Rect) {
        let status_style = match self.status {
            Status::Running => Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
            Status::Idle => Style::default()
                .fg(Color::Blue)
                .add_modifier(Modifier::BOLD),
            Status::Paused => Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
            Status::Stopping => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            Status::Finished => Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        };

        let line = Line::from(vec![
            Span::styled("rustcrawl", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw("  "),
            Span::styled(self.status.as_str(), status_style),
            Span::raw("  "),
            Span::styled("Crawl Deck", Style::default().fg(Color::DarkGray)),
        ]);

        frame.render_widget(
            Paragraph::new(line)
                .block(Block::default().borders(Borders::ALL))
                .alignment(Alignment::Center),
            area,
        );
    }

    fn render_stats(&self, frame: &mut Frame<'_>, area: Rect) {
        let s = self.last_snapshot;
        let total = s.fetched + s.failed;
        let success = if total > 0 {
            s.fetched as f64 / total as f64
        } else {
            1.0
        };
        let current_rate = self.samples.back().map(|sample| sample.rate).unwrap_or(0.0);

        let columns = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(14),
                Constraint::Percentage(14),
                Constraint::Percentage(14),
                Constraint::Percentage(14),
                Constraint::Percentage(14),
                Constraint::Percentage(15),
                Constraint::Percentage(15),
            ])
            .split(area);

        stat_card(
            frame,
            columns[0],
            "Fetched",
            s.fetched.to_string(),
            Color::Green,
        );
        stat_card(
            frame,
            columns[1],
            "Failed",
            s.failed.to_string(),
            Color::Red,
        );
        stat_card(
            frame,
            columns[2],
            "Discovered",
            s.discovered.to_string(),
            Color::Cyan,
        );
        stat_card(
            frame,
            columns[3],
            "In Flight",
            s.in_flight.to_string(),
            Color::Magenta,
        );
        stat_card(
            frame,
            columns[4],
            "Bytes",
            human_bytes(s.bytes),
            Color::Blue,
        );
        stat_card(
            frame,
            columns[5],
            "Req/s",
            format!("{current_rate:.2}"),
            Color::LightGreen,
        );

        stat_card(
            frame,
            columns[6],
            "Success",
            format!("{:.1}%", success * 100.0),
            success_color(success),
        );
    }

    fn render_chart(&self, frame: &mut Frame<'_>, area: Rect) {
        let points: Vec<(f64, f64)> = self.samples.iter().map(|s| (s.x, s.rate)).collect();
        let max_y = points
            .iter()
            .map(|(_, y)| *y)
            .fold(1.0_f64, f64::max)
            .ceil();
        let max_x = points.last().map(|(x, _)| *x).unwrap_or(1.0).max(1.0);
        let min_x = (max_x - 60.0).max(0.0);

        let dataset = Dataset::default()
            .name("req/s")
            .marker(symbols::Marker::Braille)
            .style(Style::default().fg(Color::Green))
            .data(&points);

        let chart = Chart::new(vec![dataset])
            .block(Block::default().title("Request Rate").borders(Borders::ALL))
            .x_axis(
                Axis::default()
                    .title("last 60s")
                    .style(Style::default().fg(Color::DarkGray))
                    .bounds([min_x, max_x]),
            )
            .y_axis(
                Axis::default()
                    .title("pages/s")
                    .style(Style::default().fg(Color::DarkGray))
                    .bounds([0.0, max_y]),
            );

        frame.render_widget(chart, area);
    }

    fn render_logs(&self, frame: &mut Frame<'_>, area: Rect) {
        let panels = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
            .split(area);

        let visible = panels[0].height.saturating_sub(2) as usize;
        let items = self
            .logs
            .iter()
            .rev()
            .take(visible)
            .rev()
            .map(LogLine::as_item)
            .collect::<Vec<_>>();

        frame.render_widget(
            List::new(items).block(
                Block::default()
                    .title("Recent Requests")
                    .borders(Borders::ALL),
            ),
            panels[0],
        );
        self.render_recent_jobs(frame, panels[1]);
    }

    fn render_job_stats(&self, frame: &mut Frame<'_>, area: Rect) {
        let stats = &self.jobs.stats;
        let rows = vec![
            Line::from(vec![
                Span::styled("Saved jobs    ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    stats.total_jobs.to_string(),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
            ]),
            Line::from(vec![
                Span::styled("Avg runtime   ", Style::default().fg(Color::DarkGray)),
                Span::raw(format!("{:.2}s", stats.avg_runtime_secs)),
            ]),
            Line::from(vec![
                Span::styled("Avg pages/s   ", Style::default().fg(Color::DarkGray)),
                Span::raw(format!("{:.2}", stats.avg_pages_per_sec)),
            ]),
            Line::from(vec![
                Span::styled("Avg success   ", Style::default().fg(Color::DarkGray)),
                Span::raw(format!("{:.1}%", stats.avg_success_rate * 100.0)),
            ]),
            Line::from(vec![
                Span::styled("Total fetched ", Style::default().fg(Color::DarkGray)),
                Span::raw(stats.total_pages_fetched.to_string()),
            ]),
            Line::from(vec![
                Span::styled("Total failed  ", Style::default().fg(Color::DarkGray)),
                Span::raw(stats.total_pages_failed.to_string()),
            ]),
        ];

        frame.render_widget(
            Paragraph::new(rows)
                .block(Block::default().title("Job Stats").borders(Borders::ALL))
                .wrap(Wrap { trim: true }),
            area,
        );
    }

    fn render_recent_jobs(&self, frame: &mut Frame<'_>, area: Rect) {
        let visible = area.height.saturating_sub(2) as usize;
        let mut items = self
            .jobs
            .recent
            .iter()
            .rev()
            .take(visible)
            .enumerate()
            .map(|(idx, job)| {
                let selected = idx == self.selected_job;
                let timestamp = job.finished_at.format("%H:%M:%S").to_string();
                let line = Line::from(vec![
                    Span::styled(
                        if selected { ">" } else { " " },
                        Style::default().fg(Color::Yellow),
                    ),
                    Span::raw(" "),
                    Span::styled(timestamp, Style::default().fg(Color::DarkGray)),
                    Span::raw(" "),
                    Span::styled(
                        format!("{:>4}p", job.pages_fetched),
                        Style::default().fg(Color::Green),
                    ),
                    Span::raw(" "),
                    Span::styled(
                        format!("{:>4.1}s", job.duration_secs),
                        Style::default().fg(Color::Yellow),
                    ),
                    Span::raw(" "),
                    Span::raw(truncate(&job.target_label(), 80)),
                ]);
                let style = if selected {
                    Style::default()
                        .fg(Color::White)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                ListItem::new(line).style(style)
            })
            .collect::<Vec<_>>();

        if items.is_empty() {
            items.push(ListItem::new(Line::from(Span::styled(
                "No saved jobs yet",
                Style::default().fg(Color::DarkGray),
            ))));
        }

        frame.render_widget(
            List::new(items).block(
                Block::default()
                    .title("Recent Jobs  Up/Down select  e edit")
                    .borders(Borders::ALL),
            ),
            area,
        );
    }

    fn render_new_job(&self, frame: &mut Frame<'_>, area: Rect) {
        let mut lines = Vec::new();
        lines.push(Line::from(Span::styled(
            "New crawl job",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::from(
            "Enter starts the job | Tab/Down moves | Up moves back | Esc cancels",
        ));
        lines.push(Line::from(""));

        for (idx, field) in self.new_job.fields.iter().enumerate() {
            let active = idx == self.new_job.active;
            let marker = if active { ">" } else { " " };
            let label_style = if active {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            lines.push(Line::from(vec![
                Span::styled(marker, Style::default().fg(Color::Yellow)),
                Span::raw(" "),
                Span::styled(format!("{:<12}", field.label), label_style),
                Span::raw(" "),
                Span::styled(
                    if field.value.is_empty() {
                        "(blank)".to_string()
                    } else {
                        field.value.clone()
                    },
                    if active {
                        Style::default().fg(Color::White)
                    } else {
                        Style::default()
                    },
                ),
                Span::raw("  "),
                Span::styled(field.help, Style::default().fg(Color::DarkGray)),
            ]));
        }

        if let Some(error) = &self.new_job.error {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                error,
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            )));
        }

        frame.render_widget(
            Paragraph::new(lines)
                .block(
                    Block::default()
                        .title("Launch New Job")
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn render_footer(&self, frame: &mut Frame<'_>, area: Rect) {
        let hint = self
            .last_key_hint
            .as_deref()
            .unwrap_or("n new job | Up/Down select job | e edit selected | c clear jobs | q exit");
        frame.render_widget(
            Paragraph::new(hint)
                .block(Block::default().borders(Borders::ALL))
                .wrap(Wrap { trim: true }),
            area,
        );
    }
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    x: f64,
    rate: f64,
}

#[derive(Debug, Clone, Copy)]
enum Status {
    Idle,
    Running,
    Paused,
    Stopping,
    Finished,
}

impl Status {
    fn as_str(self) -> &'static str {
        match self {
            Status::Idle => "CONTROL CENTER",
            Status::Running => "RUNNING",
            Status::Paused => "PAUSED",
            Status::Stopping => "STOPPING",
            Status::Finished => "FINISHED",
        }
    }
}

#[derive(Debug, Clone)]
struct LogLine {
    status: String,
    depth: String,
    links: String,
    url: String,
    style: Style,
}

impl LogLine {
    fn from_event(event: CrawlEvent) -> Self {
        match event {
            CrawlEvent::Page {
                url,
                status,
                depth,
                new_links,
            } => {
                let style = if (200..300).contains(&status) {
                    Style::default().fg(Color::Green)
                } else if (300..400).contains(&status) {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default().fg(Color::Red)
                };
                Self {
                    status: status.to_string(),
                    depth: format!("d{depth}"),
                    links: format!("+{new_links}"),
                    url,
                    style,
                }
            }
            CrawlEvent::Failed { url, kind, error } => Self {
                status: "ERR".to_string(),
                depth: "-".to_string(),
                links: "-".to_string(),
                url: format!("{kind:?}: {url} ({error})"),
                style: Style::default().fg(Color::Red),
            },
        }
    }

    fn as_item(&self) -> ListItem<'_> {
        let line = Line::from(vec![
            Span::styled(format!("{:<4}", self.status), self.style),
            Span::raw(" "),
            Span::styled(
                format!("{:<4}", self.depth),
                Style::default().fg(Color::DarkGray),
            ),
            Span::raw(" "),
            Span::styled(
                format!("{:<6}", self.links),
                Style::default().fg(Color::Cyan),
            ),
            Span::raw(" "),
            Span::raw(self.url.clone()),
        ]);
        ListItem::new(line)
    }
}

fn handle_keys(
    control: Option<&CrawlControl>,
    app: &mut Dashboard,
    actions: &UnboundedSender<DashboardAction>,
) {
    while event::poll(Duration::ZERO).unwrap_or(false) {
        let Ok(Event::Key(key)) = event::read() else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }

        if app.mode == ViewMode::NewJob {
            handle_form_key(key.code, app, actions);
            continue;
        }

        match key.code {
            KeyCode::Char('n') | KeyCode::Char('N')
                if app.summary.is_some() || matches!(app.status, Status::Idle) =>
            {
                app.mode = ViewMode::NewJob;
                app.new_job = NewJobForm::new();
                app.last_key_hint =
                    Some("new job: fill settings, Enter starts, Esc cancels".into());
            }
            KeyCode::Char('e') | KeyCode::Char('E')
                if app.summary.is_some() || matches!(app.status, Status::Idle) =>
            {
                let spec = if app.summary.is_some() {
                    app.last_run_spec
                        .clone()
                        .or_else(|| app.selected_job().map(NewJobSpec::from_job))
                } else {
                    app.selected_job().map(NewJobSpec::from_job)
                };
                if let Some(spec) = spec {
                    app.mode = ViewMode::NewJob;
                    app.new_job = NewJobForm::from_spec(spec);
                    app.last_key_hint =
                        Some("editing run settings: change values, Enter starts".into());
                } else {
                    app.last_key_hint = Some("no run settings available to edit".into());
                }
            }
            KeyCode::Down | KeyCode::Char('j') | KeyCode::Char('J')
                if app.summary.is_some() || matches!(app.status, Status::Idle) =>
            {
                app.select_next_job();
            }
            KeyCode::Up | KeyCode::Char('k') | KeyCode::Char('K')
                if app.summary.is_some() || matches!(app.status, Status::Idle) =>
            {
                app.select_prev_job();
            }
            KeyCode::Char('c') | KeyCode::Char('C') if matches!(app.status, Status::Idle) => {
                let _ = actions.send(DashboardAction::ClearJobs);
                app.last_key_hint = Some("clearing saved jobs".into());
            }
            KeyCode::Char('r') | KeyCode::Char('R') if app.summary.is_some() => {
                let _ = actions.send(DashboardAction::Rerun);
                app.last_key_hint = Some("rerun requested".into());
            }
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(control) = control {
                    control.stop();
                }
                let _ = actions.send(DashboardAction::Exit);
                app.last_key_hint = Some("stop requested".into());
            }
            KeyCode::Char('p') | KeyCode::Char('P') | KeyCode::Char(' ') => {
                if app.summary.is_none() {
                    let Some(control) = control else {
                        continue;
                    };
                    control.toggle_pause();
                    app.last_key_hint = Some(if control.is_paused() {
                        "paused: press p to resume, s/q to stop".to_string()
                    } else {
                        "resumed".to_string()
                    });
                }
            }
            KeyCode::Char('s') | KeyCode::Char('S') => {
                if app.summary.is_none() {
                    let Some(control) = control else {
                        continue;
                    };
                    control.stop();
                    app.last_key_hint =
                        Some("stopping gracefully after in-flight requests finish".into());
                }
            }
            KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => {
                if let Some(control) = control {
                    control.stop();
                }
                let _ = actions.send(DashboardAction::Exit);
                app.last_key_hint = Some(if app.summary.is_some() {
                    "exiting".into()
                } else {
                    "quit requested; stopping gracefully".into()
                });
            }
            _ => {}
        }
    }
}

fn handle_form_key(code: KeyCode, app: &mut Dashboard, actions: &UnboundedSender<DashboardAction>) {
    match code {
        KeyCode::Esc => {
            app.mode = ViewMode::Dashboard;
            app.last_key_hint = Some("new job cancelled".into());
        }
        KeyCode::Tab | KeyCode::Down => app.new_job.next(),
        KeyCode::BackTab | KeyCode::Up => app.new_job.prev(),
        KeyCode::Enter => match app.new_job.to_spec() {
            Ok(spec) => {
                let _ = actions.send(DashboardAction::Start(spec));
                app.last_key_hint = Some("starting new job".into());
            }
            Err(err) => app.new_job.error = Some(err),
        },
        KeyCode::Backspace => {
            app.new_job.active_value_mut().pop();
            app.new_job.error = None;
        }
        KeyCode::Char(c) => {
            // Save field: single-key ergonomic toggles.
            if app.new_job.active == SAVE_JOB_FIELD_INDEX {
                match c.to_ascii_lowercase() {
                    'y' => app.new_job.fields[SAVE_JOB_FIELD_INDEX].value = "y".to_string(),
                    'n' => app.new_job.fields[SAVE_JOB_FIELD_INDEX].value = "n".to_string(),
                    _ => app.new_job.active_value_mut().push(c),
                }
            } else {
                app.new_job.active_value_mut().push(c);
            }
            app.new_job.error = None;
        }
        _ => {}
    }
}

fn stat_card(frame: &mut Frame<'_>, area: Rect, label: &str, value: String, color: Color) {
    let content = vec![
        Line::from(Span::styled(
            value,
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(label, Style::default().fg(Color::DarkGray))),
    ];
    frame.render_widget(
        Paragraph::new(content)
            .alignment(Alignment::Center)
            .block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn success_color(success: f64) -> Color {
    if success >= 0.98 {
        Color::Green
    } else if success >= 0.9 {
        Color::Yellow
    } else {
        Color::Red
    }
}

fn truncate(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

pub(crate) fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}
