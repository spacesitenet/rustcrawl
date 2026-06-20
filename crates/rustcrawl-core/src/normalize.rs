//! URL normalization.
//!
//! Deduplication is only as good as the canonical form we compare against.
//! [`normalize`] applies a conservative set of transformations that are safe
//! for almost all sites: it never touches the path's case or decodes escapes
//! that might be semantically meaningful.

use url::Url;

/// Produce a canonical form of `url` suitable for deduplication.
///
/// The following are applied:
/// - the fragment (`#...`) is dropped — it never reaches the server;
/// - the scheme and host are lowercased (handled by the `url` crate);
/// - a default port for the scheme (`:80`/`:443`) is removed;
/// - an empty path becomes `/`;
/// - a trailing `?` with no query is removed.
///
/// Query strings are preserved as-is: reordering or stripping them is unsafe
/// in general because many sites route on query parameters.
pub fn normalize(mut url: Url) -> Url {
    url.set_fragment(None);

    if url.path().is_empty() {
        url.set_path("/");
    }

    if url.query() == Some("") {
        url.set_query(None);
    }

    if let Some(port) = url.port() {
        let is_default = matches!((url.scheme(), port), ("http", 80) | ("https", 443));
        if is_default {
            // `set_port` only fails for cannot-be-a-base URLs, which we never
            // reach here because the scheme is http(s).
            let _ = url.set_port(None);
        }
    }

    url
}

#[cfg(test)]
mod tests {
    use super::*;

    fn norm(s: &str) -> String {
        normalize(Url::parse(s).unwrap()).to_string()
    }

    #[test]
    fn drops_fragment() {
        assert_eq!(norm("https://e.com/a#frag"), "https://e.com/a");
    }

    #[test]
    fn adds_root_path() {
        assert_eq!(norm("https://e.com"), "https://e.com/");
    }

    #[test]
    fn strips_default_ports() {
        assert_eq!(norm("https://e.com:443/a"), "https://e.com/a");
        assert_eq!(norm("http://e.com:80/a"), "http://e.com/a");
        assert_eq!(norm("https://e.com:8443/a"), "https://e.com:8443/a");
    }

    #[test]
    fn preserves_query() {
        assert_eq!(norm("https://e.com/a?b=1&a=2"), "https://e.com/a?b=1&a=2");
    }
}
