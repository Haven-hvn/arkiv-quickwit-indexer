//! Content-type-aware `payload → body` extraction (§11 of the design).
//!
//! Getting an extractor wrong cannot corrupt anything — `body` is
//! `stored: false` in the shipped index config — it only degrades recall
//! for that content type until the extractor is fixed.

use crate::config::{ExtractorStrategy, PayloadExtractorRule};

/// Payloads longer than this are truncated before extraction. Bounds both
/// bridge memory and Quickwit per-doc size; entities are typically small.
const MAX_BODY_BYTES: usize = 1_048_576;

/// Extracts a `body` from raw payload bytes for full-text indexing and
/// storage. The core rule: **if the payload is valid UTF-8, it gets
/// indexed** — regardless of the declared content type. Content-type-aware
/// extractors (JSON flattening, HTML tag stripping, markdown syntax
/// removal) improve token quality when applicable but are not required for
/// the data to be indexed and retained.
///
/// Returns `None` only when the payload is empty or fails UTF-8 decoding,
/// meaning it is truly binary.
pub fn extract_body(
    rules: &[PayloadExtractorRule],
    content_type: &str,
    payload: &[u8],
) -> Option<String> {
    if payload.is_empty() {
        return None;
    }
    let payload = &payload[..payload.len().min(MAX_BODY_BYTES)];
    // If the payload is not valid UTF-8, it's binary — nothing to index.
    let text = match std::str::from_utf8(payload) {
        Ok(text) => text,
        Err(_) => return None,
    };
    if text.trim().is_empty() {
        return None;
    }
    // Content-type-aware extractors enhance token quality (e.g. flatten
    // JSON keys into searchable form, strip HTML noise). But even without
    // a matching extractor the raw UTF-8 text is indexed.
    let strategy = resolve_strategy(rules, content_type);
    let extracted = match strategy {
        ExtractorStrategy::Utf8Body => Some(text.to_string()),
        ExtractorStrategy::Utf8BodyStripMarkdown => Some(strip_markdown(text)),
        ExtractorStrategy::JsonFlatten => {
            // Flattening makes keys and values individually searchable;
            // fall back to raw text if the JSON doesn't parse.
            flatten_json(payload).or_else(|| Some(text.to_string()))
        }
        ExtractorStrategy::HtmlStripTags => Some(strip_tags(text)),
        ExtractorStrategy::TryUtf8 => Some(text.to_string()),
        // `None` strategy formerly skipped binary content — but since we
        // only reach here after passing the UTF-8 gate, respect the
        // operator's intent to suppress indexing for this content type.
        ExtractorStrategy::None => None,
    };
    extracted.and_then(non_empty)
}

/// First matching rule wins: exact `content_type` first, then prefix rules,
/// in config order. No rule → built-in defaults per §11.
fn resolve_strategy(rules: &[PayloadExtractorRule], content_type: &str) -> ExtractorStrategy {
    for rule in rules {
        if let Some(exact) = &rule.content_type {
            if exact == content_type {
                return rule.strategy;
            }
        }
    }
    for rule in rules {
        if let Some(prefix) = &rule.content_type_prefix {
            if content_type.starts_with(prefix.as_str()) {
                return rule.strategy;
            }
        }
    }
    default_strategy(content_type)
}

fn default_strategy(content_type: &str) -> ExtractorStrategy {
    match content_type {
        "text/plain" => ExtractorStrategy::Utf8Body,
        "text/markdown" => ExtractorStrategy::Utf8BodyStripMarkdown,
        "application/json" => ExtractorStrategy::JsonFlatten,
        "text/html" | "application/xml" | "text/xml" => ExtractorStrategy::HtmlStripTags,
        // Everything else: UTF-8 gate already ran, so pass through as-is.
        // image/video/audio payloads that are genuinely binary won't pass
        // the UTF-8 gate anyway; SVG / text-based formats that do pass it
        // should be indexed.
        _ => ExtractorStrategy::TryUtf8,
    }
}


/// Markdown → prose using a real CommonMark parser: keeps text, code and
/// link titles; drops syntax.
fn strip_markdown(markdown: &str) -> String {
    use pulldown_cmark::{Event, Parser};

    let mut prose = String::with_capacity(markdown.len());
    for event in Parser::new(markdown) {
        match event {
            Event::Text(text) | Event::Code(text) => {
                prose.push_str(&text);
                prose.push(' ');
            }
            Event::SoftBreak | Event::HardBreak => prose.push('\n'),
            _ => {}
        }
    }
    prose.trim().to_string()
}

/// Flattens a JSON document into a `"key: value"` token stream so both keys
/// and values are searchable in `body`.
fn flatten_json(payload: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let mut output = String::new();
    flatten_json_value("", &value, &mut output);
    non_empty(output.trim().to_string())
}

fn flatten_json_value(prefix: &str, value: &serde_json::Value, output: &mut String) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map {
                let child_prefix = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten_json_value(&child_prefix, child, output);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                flatten_json_value(prefix, item, output);
            }
        }
        serde_json::Value::Null => {}
        scalar => {
            let rendered = match scalar {
                serde_json::Value::String(text) => text.clone(),
                other => other.to_string(),
            };
            output.push_str(prefix);
            output.push_str(": ");
            output.push_str(&rendered);
            output.push('\n');
        }
    }
}

/// Tag stripper for HTML/XML: keeps text nodes plus `title` and `alt`
/// attribute values; drops markup, scripts, and styles.
fn strip_tags(html: &str) -> String {
    let mut output = String::with_capacity(html.len());
    let mut chars = html.char_indices().peekable();
    let mut skip_until_close: Option<&str> = None;
    while let Some((position, character)) = chars.next() {
        if character != '<' {
            if skip_until_close.is_none() {
                output.push(character);
            }
            continue;
        }
        let rest = &html[position..];
        if let Some(container) = skip_until_close {
            let close_tag = format!("</{container}");
            if rest.to_ascii_lowercase().starts_with(&close_tag) {
                skip_until_close = None;
            }
        } else {
            let lower = rest.to_ascii_lowercase();
            if lower.starts_with("<script") {
                skip_until_close = Some("script");
            } else if lower.starts_with("<style") {
                skip_until_close = Some("style");
            }
            // Pull `title="…"` / `alt="…"` attribute text out of the tag.
            if let Some(tag_end) = rest.find('>') {
                let tag_body = &rest[..tag_end];
                for attribute in ["title=\"", "alt=\""] {
                    if let Some(start) = tag_body.find(attribute) {
                        let value_start = start + attribute.len();
                        if let Some(len) = tag_body[value_start..].find('"') {
                            output.push(' ');
                            output.push_str(&tag_body[value_start..value_start + len]);
                            output.push(' ');
                        }
                    }
                }
            }
        }
        // Consume through the closing '>'.
        for (_, tag_char) in chars.by_ref() {
            if tag_char == '>' {
                break;
            }
        }
        output.push(' ');
    }
    // Collapse whitespace runs.
    let mut collapsed = String::with_capacity(output.len());
    let mut previous_was_space = true;
    for character in output.chars() {
        if character.is_whitespace() {
            if !previous_was_space {
                collapsed.push(' ');
            }
            previous_was_space = true;
        } else {
            collapsed.push(character);
            previous_was_space = false;
        }
    }
    collapsed.trim().to_string()
}

fn non_empty(text: String) -> Option<String> {
    if text.trim().is_empty() { None } else { Some(text) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_text_passes_through() {
        let body = extract_body(&[], "text/plain", b"hello world").unwrap();
        assert_eq!(body, "hello world");
    }

    #[test]
    fn markdown_syntax_is_stripped() {
        let body = extract_body(&[], "text/markdown", b"# Title\n\nSome *bold* [link](http://x)").unwrap();
        assert!(body.contains("Title"));
        assert!(body.contains("bold"));
        assert!(body.contains("link"));
        assert!(!body.contains('#'));
        assert!(!body.contains("http://x"));
    }

    #[test]
    fn json_is_flattened_with_keys_and_values() {
        let body =
            extract_body(&[], "application/json", br#"{"name": "ada", "tags": ["a", "b"], "n": 3}"#)
                .unwrap();
        assert!(body.contains("name: ada"));
        assert!(body.contains("tags: a"));
        assert!(body.contains("tags: b"));
        assert!(body.contains("n: 3"));
    }

    #[test]
    fn html_tags_are_stripped_scripts_dropped() {
        let html = br#"<html><script>evil()</script><p title="Hi">Hello <b>there</b></p></html>"#;
        let body = extract_body(&[], "text/html", html).unwrap();
        assert!(body.contains("Hello"));
        assert!(body.contains("there"));
        assert!(body.contains("Hi"));
        assert!(!body.contains("evil"));
    }

    #[test]
    fn binary_content_not_indexed_when_not_utf8() {
        // PNG header: 0x89 is not valid UTF-8, so extraction returns None.
        assert!(extract_body(&[], "image/png", &[0x89, 0x50, 0x4e, 0x47]).is_none());
        // Invalid multi-byte sequence.
        assert!(extract_body(&[], "application/octet-stream", &[0xc0, 0x80, 0xfe]).is_none());
    }

    #[test]
    fn utf8_payload_indexed_regardless_of_content_type() {
        // Even "binary" content types get indexed if the payload is valid UTF-8.
        assert_eq!(
            extract_body(&[], "application/octet-stream", b"hello").unwrap(),
            "hello"
        );
        assert_eq!(
            extract_body(&[], "image/svg+xml", b"<svg/>").unwrap(),
            "<svg/>"
        );
    }

    #[test]
    fn non_utf8_payload_returns_none() {
        assert!(extract_body(&[], "text/plain", &[0xff, 0xfe, 0x00]).is_none());
        assert!(extract_body(&[], "application/x-custom", &[0xff, 0xfe, 0x00]).is_none());
    }

    #[test]
    fn config_rules_override_defaults() {
        // `None` strategy suppresses indexing even for valid UTF-8.
        let rules = vec![PayloadExtractorRule {
            content_type: Some("text/plain".to_string()),
            content_type_prefix: None,
            strategy: ExtractorStrategy::None,
        }];
        assert!(extract_body(&rules, "text/plain", b"hello").is_none());
    }

    #[test]
    fn prefix_rules_match_families() {
        let rules = vec![PayloadExtractorRule {
            content_type: None,
            content_type_prefix: Some("text/".to_string()),
            strategy: ExtractorStrategy::Utf8BodyStripMarkdown,
        }];
        assert_eq!(
            extract_body(&rules, "text/markdown", b"# heading").unwrap(),
            "heading"
        );
    }
}
