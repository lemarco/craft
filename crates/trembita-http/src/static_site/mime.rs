//! MIME type guessing for static assets.

/// Guess content type from a file path.
#[must_use]
pub fn from_path(path: &str) -> String {
    mime_guess::from_path(path)
        .first_or_octet_stream()
        .essence_str()
        .to_string()
}
