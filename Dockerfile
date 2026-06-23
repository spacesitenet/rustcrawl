# syntax=docker/dockerfile:1
#
# Multi-stage build for the `rustcrawl` CLI.
#
# The image is intentionally headless: with no TTY attached the CLI skips the
# Crawl Deck dashboard, streams one JSON object per page to stdout, and prints a
# one-line summary to stderr. That makes it a clean fit for Kubernetes Jobs and
# CronJobs where logs are collected by the cluster.
#
# Build:   docker build -t rustcrawl:local .
# Run:     docker run --rm rustcrawl:local https://example.com -n 100 --no-save -q

# ---- Build stage -----------------------------------------------------------
FROM rust:1-bookworm AS builder

WORKDIR /app

# Copy the full workspace. BuildKit cache mounts keep the dependency registry
# and target directory warm across builds, so iterative builds stay fast
# without fragile dummy-source caching tricks.
COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release -p rustcrawl-cli \
    && cp target/release/rustcrawl /usr/local/bin/rustcrawl

# ---- Runtime stage ---------------------------------------------------------
# reqwest is built against the system TLS stack (native-tls/OpenSSL), so the
# runtime needs libssl plus root certificates for HTTPS verification.
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Run as an unprivileged, well-known UID so the manifests can assert
# runAsNonRoot without surprises.
RUN useradd --system --create-home --uid 10001 --user-group crawler

COPY --from=builder /usr/local/bin/rustcrawl /usr/local/bin/rustcrawl

USER crawler
WORKDIR /home/crawler

ENTRYPOINT ["rustcrawl"]
CMD ["--help"]
