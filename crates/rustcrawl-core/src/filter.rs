//! Scope enforcement: decides whether a discovered URL is allowed to be queued.

use std::collections::HashSet;

use url::Url;

use crate::config::{CrawlConfig, Scope};

/// Decides whether a URL is in scope for a crawl.
///
/// A URL is allowed when **all** of the following hold:
/// 1. it uses `http` or `https`;
/// 2. it satisfies the configured [`Scope`] relative to the seeds;
/// 3. it matches at least one include pattern (if any are configured);
/// 4. it matches none of the exclude patterns.
#[derive(Debug, Clone)]
pub struct UrlFilter {
    scope: Scope,
    seed_hosts: HashSet<String>,
    seed_domains: HashSet<String>,
    include: Vec<regex::Regex>,
    exclude: Vec<regex::Regex>,
}

impl UrlFilter {
    /// Build a filter from a crawl configuration.
    pub fn from_config(config: &CrawlConfig) -> Self {
        let mut seed_hosts = HashSet::new();
        let mut seed_domains = HashSet::new();
        for seed in &config.seeds {
            if let Some(host) = seed.host_str() {
                seed_hosts.insert(host.to_ascii_lowercase());
                seed_domains.insert(registrable_domain(host));
            }
        }
        Self {
            scope: config.scope,
            seed_hosts,
            seed_domains,
            include: config.include.clone(),
            exclude: config.exclude.clone(),
        }
    }

    /// Returns `true` if `url` may be crawled.
    pub fn allows(&self, url: &Url) -> bool {
        if !matches!(url.scheme(), "http" | "https") {
            return false;
        }
        if !self.in_scope(url) {
            return false;
        }
        let as_str = url.as_str();
        if self.exclude.iter().any(|re| re.is_match(as_str)) {
            return false;
        }
        if !self.include.is_empty() && !self.include.iter().any(|re| re.is_match(as_str)) {
            return false;
        }
        true
    }

    fn in_scope(&self, url: &Url) -> bool {
        let host = match url.host_str() {
            Some(h) => h.to_ascii_lowercase(),
            None => return false,
        };
        match self.scope {
            Scope::Any => true,
            Scope::Host => self.seed_hosts.contains(&host),
            Scope::Domain => self.seed_domains.contains(&registrable_domain(&host)),
        }
    }
}

/// Best-effort registrable domain (eTLD+1) using the Public Suffix List.
///
/// Falls back to the raw host when the suffix list has no opinion (e.g. for an
/// IP address or an internal hostname), which keeps such hosts comparable to
/// themselves under [`Scope::Domain`].
fn registrable_domain(host: &str) -> String {
    psl::domain_str(host)
        .map(|d| d.to_ascii_lowercase())
        .unwrap_or_else(|| host.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    fn filter(seed: &str, scope: Scope) -> UrlFilter {
        let config = CrawlConfig::builder()
            .add_seed(seed)
            .unwrap()
            .scope(scope)
            .build()
            .unwrap();
        UrlFilter::from_config(&config)
    }

    #[test]
    fn domain_scope_includes_subdomains_excludes_others() {
        let f = filter("https://example.com/", Scope::Domain);
        assert!(f.allows(&url("https://example.com/page")));
        assert!(f.allows(&url("https://docs.example.com/page")));
        assert!(!f.allows(&url("https://other.com/page")));
        assert!(!f.allows(&url("https://notexample.com/page")));
    }

    #[test]
    fn host_scope_excludes_subdomains() {
        let f = filter("https://example.com/", Scope::Host);
        assert!(f.allows(&url("https://example.com/page")));
        assert!(!f.allows(&url("https://docs.example.com/page")));
    }

    #[test]
    fn any_scope_allows_other_hosts_but_not_other_schemes() {
        let f = filter("https://example.com/", Scope::Any);
        assert!(f.allows(&url("https://anywhere.example.org/x")));
        assert!(!f.allows(&url("ftp://example.com/x")));
    }

    #[test]
    fn include_and_exclude_patterns_apply() {
        let config = CrawlConfig::builder()
            .add_seed("https://example.com/")
            .unwrap()
            .scope(Scope::Domain)
            .include("/blog/")
            .exclude(r"\.png$")
            .build()
            .unwrap();
        let f = UrlFilter::from_config(&config);

        assert!(f.allows(&url("https://example.com/blog/post")));
        // Not matched by any include pattern.
        assert!(!f.allows(&url("https://example.com/about")));
        // Matched by include but also by exclude -> excluded wins.
        assert!(!f.allows(&url("https://example.com/blog/cover.png")));
    }
}
