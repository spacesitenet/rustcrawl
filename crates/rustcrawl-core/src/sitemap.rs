//! Minimal sitemap parsing, used to seed a crawl from `sitemap.xml`.
//!
//! Supports both `<urlset>` (a list of pages) and `<sitemapindex>` (a list of
//! other sitemaps). [`fetch_urls`] follows one level of indexes so that the
//! common "index → child sitemaps → URLs" layout works out of the box.

use quick_xml::events::Event;
use quick_xml::Reader;
use reqwest::Client;
use url::Url;

/// `<loc>` entries split by which container they appeared in.
#[derive(Debug, Default, Clone)]
pub struct Sitemap {
    /// Page URLs from a `<urlset>`.
    pub pages: Vec<Url>,
    /// Nested sitemap URLs from a `<sitemapindex>`.
    pub sitemaps: Vec<Url>,
}

/// Parse sitemap XML bytes. Invalid `<loc>` values are skipped.
pub fn parse(bytes: &[u8]) -> Sitemap {
    let mut reader = Reader::from_reader(bytes);
    reader.config_mut().trim_text(true);

    let mut out = Sitemap::default();
    let mut buf = Vec::new();
    let mut in_loc = false;
    // Whether the nearest container is a <sitemap> (index) or a <url>.
    let mut in_sitemap_entry = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match e.local_name().as_ref() {
                b"loc" => in_loc = true,
                b"sitemap" => in_sitemap_entry = true,
                b"url" => in_sitemap_entry = false,
                _ => {}
            },
            Ok(Event::End(e)) => match e.local_name().as_ref() {
                b"loc" => in_loc = false,
                b"sitemap" => in_sitemap_entry = false,
                _ => {}
            },
            Ok(Event::Text(t)) if in_loc => {
                if let Ok(text) = t.unescape() {
                    if let Ok(url) = Url::parse(text.trim()) {
                        if in_sitemap_entry {
                            out.sitemaps.push(url);
                        } else {
                            out.pages.push(url);
                        }
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    out
}

/// Fetch `sitemap_url` and return the page URLs it advertises, following one
/// level of nested sitemap indexes. `max_sitemaps` bounds how many child
/// sitemaps are fetched so a hostile index cannot blow up the crawl.
pub async fn fetch_urls(client: &Client, sitemap_url: &Url, max_sitemaps: usize) -> Vec<Url> {
    let Some(root) = fetch_and_parse(client, sitemap_url).await else {
        return Vec::new();
    };

    let mut pages = root.pages;
    for child in root.sitemaps.into_iter().take(max_sitemaps) {
        if let Some(sm) = fetch_and_parse(client, &child).await {
            pages.extend(sm.pages);
        }
    }
    pages
}

async fn fetch_and_parse(client: &Client, url: &Url) -> Option<Sitemap> {
    let resp = client.get(url.clone()).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let bytes = resp.bytes().await.ok()?;
    Some(parse(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_urlset() {
        let xml = br#"<?xml version="1.0"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
              <url><loc>https://e.com/a</loc></url>
              <url><loc>https://e.com/b</loc></url>
            </urlset>"#;
        let sm = parse(xml);
        assert_eq!(sm.pages.len(), 2);
        assert!(sm.sitemaps.is_empty());
    }

    #[test]
    fn parses_index() {
        let xml = br#"<sitemapindex>
              <sitemap><loc>https://e.com/sm1.xml</loc></sitemap>
            </sitemapindex>"#;
        let sm = parse(xml);
        assert_eq!(sm.sitemaps.len(), 1);
        assert!(sm.pages.is_empty());
    }
}
