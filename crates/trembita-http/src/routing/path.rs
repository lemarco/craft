//! Path patterns and parameter extraction.

use std::collections::HashMap;

/// One segment of a route path — literal text or a named capture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSegment {
    /// Fixed path segment (`brands`).
    Literal(String),
    /// Named capture (`{brandId}`).
    Param(String),
    /// Optional capture (`{version?}`) — zero or one segment.
    OptionalParam(String),
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
    /// Optional segments use `{name?}` (e.g. `/api/{version?}/health` matches `/api/health`
    /// and `/api/v2/health`).
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

        let mut params = PathParams::new();
        match_segments(&self.segments, &req_segments, 0, 0, &mut params)?;
        Some(params)
    }
}

fn match_segments(
    pattern: &[PathSegment],
    req: &[&str],
    pi: usize,
    ri: usize,
    params: &mut PathParams,
) -> Option<()> {
    if pi == pattern.len() {
        return if ri == req.len() { Some(()) } else { None };
    }

    match &pattern[pi] {
        PathSegment::Literal(expected) => {
            if ri >= req.len() || req[ri] != expected {
                return None;
            }
            match_segments(pattern, req, pi + 1, ri + 1, params)
        }
        PathSegment::Param(name) => {
            if ri >= req.len() {
                return None;
            }
            params.insert(name.clone(), req[ri].to_string());
            match_segments(pattern, req, pi + 1, ri + 1, params)
        }
        PathSegment::OptionalParam(name) => {
            if ri < req.len() {
                params.insert(name.clone(), req[ri].to_string());
                if match_segments(pattern, req, pi + 1, ri + 1, params).is_some() {
                    return Some(());
                }
                params.remove(name);
            }
            match_segments(pattern, req, pi + 1, ri, params)
        }
    }
}

fn parse_segment(raw: &str) -> PathSegment {
    if let Some(inner) = raw.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
        if let Some(name) = inner.strip_suffix('?') {
            if name.is_empty() {
                PathSegment::Literal(raw.to_string())
            } else {
                PathSegment::OptionalParam(name.to_string())
            }
        } else {
            PathSegment::Param(inner.to_string())
        }
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

    fn remove(&mut self, name: &str) {
        self.values.remove(name);
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

    #[test]
    fn optional_trailing_segment() {
        let pattern = PathPattern::new("/files/{path?}");
        assert!(pattern.match_path("/files").is_some());
        let with_id = pattern.match_path("/files/readme.txt").expect("match");
        assert_eq!(with_id.get("path"), Some("readme.txt"));
    }

    #[test]
    fn optional_middle_segment() {
        let pattern = PathPattern::new("/api/{version?}/health");
        assert!(pattern.match_path("/api/health").is_some());
        let v2 = pattern.match_path("/api/v2/health").expect("match");
        assert_eq!(v2.get("version"), Some("v2"));
    }
}
