//! HTTP fetching with retries, timeouts, and a response-size ceiling.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use reqwest::header::{HeaderMap, RETRY_AFTER};
use reqwest::{Client, StatusCode};
use url::Url;

use crate::config::CrawlConfig;
use crate::error::{CrawlError, Result};
use crate::page::PageErrorKind;

const BASE_BACKOFF: Duration = Duration::from_millis(200);
const MAX_BACKOFF: Duration = Duration::from_secs(10);
const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);

/// A successful HTTP response with its body fully (and boundedly) buffered.
#[derive(Debug)]
pub struct FetchResponse {
    /// The URL after following any redirects.
    pub final_url: Url,
    /// HTTP status of the final response.
    pub status: StatusCode,
    /// `Content-Type` header, if present.
    pub content_type: Option<String>,
    /// The response body.
    pub body: Bytes,
}

/// A recoverable, per-request failure. Carries a classification so the engine
/// can decide whether to retry and how to report it.
#[derive(Debug, Clone)]
pub struct FetchError {
    /// Coarse category of the failure.
    pub kind: PageErrorKind,
    /// Human-readable detail.
    pub message: String,
    retryable: bool,
    retry_after: Option<Duration>,
}

impl FetchError {
    fn new(kind: PageErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            retryable: false,
            retry_after: None,
        }
    }

    fn retryable(mut self) -> Self {
        self.retryable = true;
        self
    }

    fn with_retry_after(mut self, delay: Option<Duration>) -> Self {
        self.retry_after = delay;
        self
    }
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

/// Thin, cloneable wrapper around a [`reqwest::Client`].
///
/// Cloning is cheap — the underlying client (and its connection pool) is shared.
#[derive(Debug, Clone)]
pub struct Fetcher {
    client: Client,
    max_retries: u32,
    max_body_bytes: usize,
}

impl Fetcher {
    /// Build a fetcher from a crawl configuration.
    pub fn from_config(config: &CrawlConfig) -> Result<Self> {
        let client = Client::builder()
            .user_agent(config.user_agent.clone())
            .timeout(config.request_timeout)
            .connect_timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .map_err(CrawlError::Client)?;
        Ok(Self {
            client,
            max_retries: config.max_retries,
            max_body_bytes: config.max_body_bytes,
        })
    }

    /// Access the underlying client (used by the robots/sitemap fetchers so
    /// they share the same connection pool and configuration).
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Fetch `url`, retrying transient failures with exponential backoff.
    pub async fn fetch(&self, url: &Url) -> std::result::Result<FetchResponse, FetchError> {
        let mut attempt = 0;
        loop {
            match self.try_fetch(url).await {
                Ok(resp) => return Ok(resp),
                Err(err) if err.is_retryable() && attempt < self.max_retries => {
                    attempt += 1;
                    let delay = err
                        .retry_after
                        .unwrap_or_else(|| backoff_for_attempt(attempt));
                    tracing::debug!(
                        url = %url,
                        attempt,
                        max_retries = self.max_retries,
                        delay_ms = delay.as_millis(),
                        error = %err,
                        "retrying fetch"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn try_fetch(&self, url: &Url) -> std::result::Result<FetchResponse, FetchError> {
        let response = self
            .client
            .get(url.clone())
            .send()
            .await
            .map_err(classify_reqwest_error)?;

        let status = response.status();
        let final_url = response.url().clone();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);

        if let Some(len) = response.content_length() {
            if len as usize > self.max_body_bytes {
                return Err(FetchError::new(
                    PageErrorKind::TooLarge,
                    format!("declared body of {len} bytes exceeds limit"),
                ));
            }
        }

        if status.is_client_error() || status.is_server_error() {
            let retry_after = parse_retry_after(response.headers());
            let mut err = FetchError::new(
                PageErrorKind::HttpStatus,
                format!("server returned {status}"),
            )
            .with_retry_after(retry_after);
            if is_retryable_status(status) {
                err = err.retryable();
            }
            return Err(err);
        }

        let body = self.read_capped(response).await?;
        Ok(FetchResponse {
            final_url,
            status,
            content_type,
            body,
        })
    }

    /// Read the body, aborting if it grows past the configured limit.
    async fn read_capped(
        &self,
        mut response: reqwest::Response,
    ) -> std::result::Result<Bytes, FetchError> {
        let mut buf = BytesMut::new();
        while let Some(chunk) = response.chunk().await.map_err(classify_reqwest_error)? {
            if buf.len() + chunk.len() > self.max_body_bytes {
                return Err(FetchError::new(
                    PageErrorKind::TooLarge,
                    format!("body exceeded {} byte limit", self.max_body_bytes),
                ));
            }
            buf.extend_from_slice(&chunk);
        }
        Ok(buf.freeze())
    }
}

impl FetchError {
    fn is_retryable(&self) -> bool {
        matches!(self.kind, PageErrorKind::Transport) || self.retryable
    }
}

fn is_retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

fn backoff_for_attempt(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(6);
    let millis = (BASE_BACKOFF.as_millis() as u64)
        .saturating_mul(1_u64 << exponent)
        .min(MAX_BACKOFF.as_millis() as u64);
    Duration::from_millis(millis)
}

fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let seconds = headers
        .get(RETRY_AFTER)?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()?;
    Some(Duration::from_secs(seconds).min(MAX_RETRY_AFTER))
}

fn classify_reqwest_error(err: reqwest::Error) -> FetchError {
    let kind = if err.is_timeout() || err.is_connect() || err.is_request() || err.is_body() {
        PageErrorKind::Transport
    } else {
        PageErrorKind::Other
    };
    FetchError::new(kind, err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    #[test]
    fn retries_transient_http_statuses() {
        assert!(is_retryable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retryable_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(is_retryable_status(StatusCode::GATEWAY_TIMEOUT));
        assert!(!is_retryable_status(StatusCode::FORBIDDEN));
        assert!(!is_retryable_status(StatusCode::NOT_FOUND));
    }

    #[test]
    fn backoff_is_exponential_and_capped() {
        assert_eq!(backoff_for_attempt(1), Duration::from_millis(200));
        assert_eq!(backoff_for_attempt(2), Duration::from_millis(400));
        assert_eq!(backoff_for_attempt(3), Duration::from_millis(800));
        assert_eq!(backoff_for_attempt(100), MAX_BACKOFF);
    }

    #[test]
    fn retry_after_seconds_are_capped() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("120"));
        assert_eq!(parse_retry_after(&headers), Some(MAX_RETRY_AFTER));
    }
}
