//! Route table comparison for gateway parity tests.

use std::collections::HashMap;

use http::Method;

use super::auth::AuthMode;
use super::table::RouteTable;

/// One declarative route (method + path template + auth mode).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RouteDescriptor {
    /// HTTP method.
    pub method: Method,
    /// Path template (`/items/{id}`).
    pub path: String,
    /// Auth gate applied before the handler.
    pub auth: AuthMode,
}

impl RouteDescriptor {
    /// Stable key for maps and diffs.
    #[must_use]
    pub fn key(&self) -> (Method, String) {
        (self.method.clone(), self.path.clone())
    }
}

/// Result of comparing an actual route table to an expected one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RouteTableDiff {
    /// Routes present in `expected` but missing from `actual`.
    pub missing: Vec<RouteDescriptor>,
    /// Routes present in `actual` but not in `expected`.
    pub extra: Vec<RouteDescriptor>,
    /// Same method+path but different [`AuthMode`].
    pub auth_mismatch: Vec<(RouteDescriptor, RouteDescriptor)>,
}

impl RouteTableDiff {
    /// Whether the two tables match exactly (method, path, auth).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.missing.is_empty() && self.extra.is_empty() && self.auth_mismatch.is_empty()
    }
}

/// Compare `actual` against `expected`.
#[must_use]
pub fn compare_tables(actual: &RouteTable, expected: &RouteTable) -> RouteTableDiff {
    let actual_map: HashMap<_, _> = actual
        .descriptors()
        .into_iter()
        .map(|d| (d.key(), d))
        .collect();
    let expected_map: HashMap<_, _> = expected
        .descriptors()
        .into_iter()
        .map(|d| (d.key(), d))
        .collect();

    let mut missing = Vec::new();
    let mut auth_mismatch = Vec::new();
    for (key, exp) in &expected_map {
        match actual_map.get(key) {
            None => missing.push(exp.clone()),
            Some(act) if act.auth != exp.auth => {
                auth_mismatch.push((act.clone(), exp.clone()));
            }
            Some(_) => {}
        }
    }
    let extra = actual_map
        .into_iter()
        .filter_map(|(key, act)| {
            if expected_map.contains_key(&key) {
                None
            } else {
                Some(act)
            }
        })
        .collect();

    RouteTableDiff {
        missing,
        extra,
        auth_mismatch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::{RequestCtx, Response, RouteTable};

    #[test]
    fn diff_reports_missing_extra_and_auth() {
        let expected = RouteTable::new()
            .get("/health", |_: RequestCtx| async {
                Ok(Response::status(http::StatusCode::OK))
            })
            .post_session("/orders", |_: RequestCtx| async {
                Ok(Response::status(http::StatusCode::OK))
            });
        let actual = RouteTable::new()
            .get("/health", |_: RequestCtx| async {
                Ok(Response::status(http::StatusCode::OK))
            })
            .post("/orders", |_: RequestCtx| async {
                Ok(Response::status(http::StatusCode::OK))
            })
            .get("/extra", |_: RequestCtx| async {
                Ok(Response::status(http::StatusCode::OK))
            });

        let diff = actual.diff(&expected);
        assert_eq!(diff.missing.len(), 0);
        assert_eq!(diff.extra.len(), 1);
        assert_eq!(diff.auth_mismatch.len(), 1);
        assert_eq!(diff.extra[0].path, "/extra");
        assert_eq!(diff.auth_mismatch[0].0.auth, AuthMode::Open);
        assert_eq!(diff.auth_mismatch[0].1.auth, AuthMode::Session);
    }
}
