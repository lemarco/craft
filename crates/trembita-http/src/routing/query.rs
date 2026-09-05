//! Query string parsing and URI helpers.

use std::collections::HashMap;

/// Parse a query string into a map (URL-decoded keys and values).
#[must_use]
pub fn parse_query_string(raw: Option<&str>) -> HashMap<String, String> {
    let Some(raw) = raw else {
        return HashMap::new();
    };
    raw.split('&')
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            Some((percent_decode(k), percent_decode(v)))
        })
        .collect()
}

/// Build an HTTP URI from path and query map.
#[must_use]
pub fn build_uri(path: &str, query: &HashMap<String, String>) -> http::Uri {
    if query.is_empty() {
        return path.parse().unwrap_or_else(|_| http::Uri::from_static("/"));
    }
    let q = query
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    format!("{path}?{q}")
        .parse()
        .unwrap_or_else(|_| path.parse().unwrap_or_else(|_| http::Uri::from_static("/")))
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                if let Ok(byte) = u8::from_str_radix(&input[i + 1..i + 3], 16) {
                    out.push(byte);
                    i += 3;
                    continue;
                }
                out.push(bytes[i]);
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_percent_encoding() {
        let q = parse_query_string(Some("msg=hello%20world&x=1"));
        assert_eq!(q.get("msg"), Some(&"hello world".to_string()));
        assert_eq!(q.get("x"), Some(&"1".to_string()));
    }

    #[test]
    fn decodes_plus_as_space() {
        let q = parse_query_string(Some("q=a+b"));
        assert_eq!(q.get("q"), Some(&"a b".to_string()));
    }
}
