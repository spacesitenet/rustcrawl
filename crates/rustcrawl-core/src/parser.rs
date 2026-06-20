//! HTML parsing: title and link extraction.

use scraper::{Html, Selector};
use url::Url;

/// The crawl-relevant fields extracted from an HTML document.
#[derive(Debug, Default, Clone)]
pub struct ParsedHtml {
    /// Document `<title>`, trimmed, if present and non-empty.
    pub title: Option<String>,
    /// Absolute links discovered in the document, de-duplicated in order.
    pub links: Vec<Url>,
}

/// Parse `html`, resolving links relative to `base`.
///
/// A `<base href>` element, if present and valid, overrides `base` for link
/// resolution, matching browser behavior. Non-navigational schemes
/// (`mailto:`, `javascript:`, `tel:`, `data:`, ...) are discarded; only
/// `http`/`https` links are returned.
pub fn parse_html(base: &Url, html: &str) -> ParsedHtml {
    let document = Html::parse_document(html);

    let title = title_selector()
        .and_then(|sel| document.select(&sel).next())
        .map(|el| el.text().collect::<String>().trim().to_owned())
        .filter(|t| !t.is_empty());

    let resolve_base = document
        .select(&base_selector())
        .next()
        .and_then(|el| el.value().attr("href"))
        .and_then(|href| base.join(href).ok())
        .unwrap_or_else(|| base.clone());

    let mut links = Vec::new();
    let mut seen = std::collections::HashSet::new();
    if let Ok(anchor) = Selector::parse("a[href]") {
        for el in document.select(&anchor) {
            let Some(href) = el.value().attr("href") else {
                continue;
            };
            let Ok(mut joined) = resolve_base.join(href) else {
                continue;
            };
            if !matches!(joined.scheme(), "http" | "https") {
                continue;
            }
            joined.set_fragment(None);
            if seen.insert(joined.as_str().to_owned()) {
                links.push(joined);
            }
        }
    }

    ParsedHtml { title, links }
}

fn title_selector() -> Option<Selector> {
    Selector::parse("title").ok()
}

fn base_selector() -> Selector {
    // `base[href]` is a valid selector; the fallback keeps this infallible.
    Selector::parse("base[href]").unwrap_or_else(|_| Selector::parse("base").unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_title_and_absolute_links() {
        let base = Url::parse("https://example.com/dir/page.html").unwrap();
        let html = r##"
            <html><head><title>  Hello  </title></head>
            <body>
                <a href="/abs">a</a>
                <a href="rel.html">b</a>
                <a href="https://other.com/x">c</a>
                <a href="mailto:hi@example.com">d</a>
                <a href="#frag">e</a>
            </body></html>
        "##;
        let parsed = parse_html(&base, html);
        assert_eq!(parsed.title.as_deref(), Some("Hello"));
        let links: Vec<_> = parsed.links.iter().map(|u| u.as_str()).collect();
        assert!(links.contains(&"https://example.com/abs"));
        assert!(links.contains(&"https://example.com/dir/rel.html"));
        assert!(links.contains(&"https://other.com/x"));
        assert!(!links.iter().any(|l| l.starts_with("mailto")));
    }

    #[test]
    fn honors_base_href() {
        let base = Url::parse("https://example.com/a/b").unwrap();
        let html = r#"<head><base href="https://cdn.example.com/root/"></head>
            <a href="x.html">x</a>"#;
        let parsed = parse_html(&base, html);
        assert_eq!(
            parsed.links[0].as_str(),
            "https://cdn.example.com/root/x.html"
        );
    }
}
