//! Chat line bookkeeping (in-memory in the capability host state).

/// Append a line; returns total line count after insert.
pub fn append_line(history: &mut Vec<String>, text: String) -> u64 {
    history.push(text);
    history.len() as u64
}

/// Current number of stored lines.
#[must_use]
pub fn line_count(history: &[String]) -> u64 {
    history.len() as u64
}
