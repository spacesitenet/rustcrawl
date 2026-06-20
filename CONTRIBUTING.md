# Contributing to rustcrawl

Thanks for your interest in improving rustcrawl! This project aims to be a
small, well engineered foundation, so contributions that keep the core focused
and the architecture clean are especially appreciated.

## Getting started

```bash
git clone https://github.com/rustcrawl/rustcrawl
cd rustcrawl
cargo build -p rustcrawl-cli
cargo test
```

To run the CLI without installing it globally:

```powershell
.\target\debug\rustcrawl.exe https://example.com -n 10
.\target\debug\rustcrawl.exe
```

The first command starts a crawl. The second opens the dashboard/control center
without starting a crawl, so you can inspect recent local jobs or press `n` to
launch a new one from the terminal UI.

To install the `rustcrawl` command onto your PATH:

```bash
cargo install --path crates/rustcrawl-cli
```

The repository is a Cargo workspace:

* `crates/rustcrawl-core`, the reusable crawling engine (no CLI concerns).
* `crates/rustcrawl-cli`, the `rustcrawl` binary and its terminal UI.

Local runtime state is written under `.rustcrawl/` and ignored by git. Use
`--no-save` for throwaway crawls that should not appear in the job history.

## Before you open a pull request

Please make sure the following pass locally:

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
```

CI runs the same checks.

## Guidelines

* **Keep the core decoupled.** `rustcrawl core` must not depend on anything
  CLI or output specific. New output formats belong behind the `Sink` trait.
* **Keep script mode clean.** Anything dashboard specific must be optional; `--quiet`
  should remain stable for automation and CI usage.
* **Make settings explicit.** New crawl features should have a clear CLI flag and,
  where useful, a dashboard field. Avoid hidden behavior.
* **Be polite by default.** Anything that increases crawl aggressiveness should
  be opt in, never the default.
* **Prefer small, testable units.** The frontier, filter, parser, and
  normalizer all have unit tests; new behavior should too.
* **Document public items.** The core is a library; public APIs should carry
  doc comments.

## Reporting bugs and proposing features

Open an issue describing the problem or idea. For crawling bugs, including the
seed URL, the flags you used, and the observed vs. expected behavior helps a
lot.

## License

By contributing, you agree that your contributions will be dual licensed under
the MIT and Apache-2.0 licenses, as described in the [README](README.md).
