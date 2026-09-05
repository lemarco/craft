//! Path patterns and parameter extraction.

use std::collections::HashMap;

/// One segment of a route path — literal text or a named capture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSegment {
    /// Fixed path segment (`brands`).
    Literal(String),
    /// Named capture (`{brandId}`).
    Param(String),
}

/// Parsed route path such as `/brands/{brandId}/urls`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathPattern {
    template: String,
    segments: Vec<PathSegment>,
}

impl PathPattern {
    /// Parse a path pattern. Leading/trailing slashes are ignored.
    ///
    /// # Panics
    /// In debug builds, if a segment looks like `{name` without a closing `}`.
    #[must_use]
    pub fn new(path: &str) -> Self {
        let trimmed = path.trim().trim_matches('/');
        let template = if trimmed.is_empty() {
            "/".to_string()
        } else {
            format!("/{trimmed}")
        };
        let segments = trimmed
            .split('/')
            .filter(|s| !s.is_empty())
            .map(parse_segment)
            .collect();
        Self { template, segments }
    }

    /// Original path template (`/items/{id}`).
    #[must_use]
    pub fn template(&self) -> &str {
        &self.template
    }

    /// Number of path segments (excluding leading slash).
    #[must_use]
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Whether the pattern has no segments.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Match a request path and extract named parameters.
    ///
    /// Trailing slashes are ignored (`/items/` matches `/items`).
    #[must_use]
    pub fn match_path(&self, request_path: &str) -> Option<PathParams> {
        let normalized = request_path.trim().trim_end_matches('/');
        let normalized = if normalized.is_empty() {
            "/"
        } else {
            normalized
        };

        let req_segments: Vec<&str> = normalized
            .trim_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        if req_segments.len() != self.segments.len() {
            return None;
        }

        let mut params = PathParams::new();
        for (pattern, actual) in self.segments.iter().zip(req_segments) {
            match pattern {
                PathSegment::Literal(expected) if expected == actual => {}
                PathSegment::Literal(_) => return None,
                PathSegment::Param(name) => {
                    params.insert(name.clone(), actual.to_string());
                }
            }
        }
        Some(params)
    }
}

fn parse_segment(raw: &str) -> PathSegment {
    if let Some(inner) = raw.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
        PathSegment::Param(inner.to_string())
    } else {
        PathSegment::Literal(raw.to_string())
    }
}

/// Named path parameters extracted from a matched route.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PathParams {
    values: HashMap<String, String>,
}

impl PathParams {
    /// Empty parameter map.
    #[must_use]
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    fn insert(&mut self, name: String, value: String) {
        self.values.insert(name, value);
    }

    /// Lookup one captured parameter.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    /// Iterator over `(name, value)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.values.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_segments_must_match_exactly() {
        let pattern = PathPattern::new("/brands/list");
        assert!(pattern.match_path("/brands/list").is_some());
        assert!(pattern.match_path("/brands/other").is_none());
    }

    #[test]
    fn captures_named_parameters() {
        let pattern = PathPattern::new("/brands/{brandId}/urls/{urlId}");
        let params = pattern.match_path("/brands/42/urls/7").expect("match");
        assert_eq!(params.get("brandId"), Some("42"));
        assert_eq!(params.get("urlId"), Some("7"));
    }

    #[test]
    fn rejects_wrong_segment_count() {
        let pattern = PathPattern::new("/brands/{id}");
        assert!(pattern.match_path("/brands").is_none());
        assert!(pattern.match_path("/brands/1/extra").is_none());
    }

    #[test]
    fn trailing_slash_matches() {
        let pattern = PathPattern::new("/brands/list");
        assert!(pattern.match_path("/brands/list/").is_some());
    }
}
