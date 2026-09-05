//! Host header normalization for gateway dispatch.

/// Normalize an HTTP `Host` header to a lowercase hostname without port.
///
/// `API.Example.COM:443` → `api.example.com`, `[::1]:8080` → `::1`.
#[must_use]
pub fn normalize_host(raw: &str) -> String {
    let raw = raw.trim().to_ascii_lowercase();
    if let Some(stripped) = raw.strip_prefix('[')
        && let Some(end) = stripped.find(']')
    {
        return stripped[..end].to_string();
    }
    raw.split(':').next().unwrap_or(&raw).to_string()
}

/// Loopback hostnames recognized by gateway dev fallback.
#[must_use]
pub fn is_local_dev_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_strips_port_and_lowercases() {
        assert_eq!(normalize_host("API.Example.COM:443"), "api.example.com");
        assert_eq!(normalize_host("[::1]:8080"), "::1");
        assert_eq!(normalize_host("localhost"), "localhost");
    }
}
