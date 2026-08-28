//! Shared helpers for the crafty benchmarks + soak harness (backlog T10).

use crafty_proto::{EntryPayload, LogEntry, LogIndex, Term};

/// Parse a `u64` from the environment, falling back to `default`.
#[must_use]
pub fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Opaque payload bytes for queue enqueue benchmarks/soak.
#[must_use]
pub fn queue_payload(size: usize, seq: u64) -> Vec<u8> {
    let mut buf = vec![0u8; size.max(8)];
    buf[..8].copy_from_slice(&seq.to_le_bytes());
    buf
}

/// A tiny, fast, non-cryptographic PRNG (xorshift64*) so the benches/soak stay
/// dependency-light and deterministic given a seed.
pub struct TinyRng(u64);

impl TinyRng {
    /// Seed the generator (any non-zero state).
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    /// Next pseudo-random `u64`.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

/// Build `n` contiguous command log entries (term 1, indices 1..=n), each
/// carrying `size` opaque payload bytes.
#[must_use]
pub fn command_entries(n: u64, size: usize) -> Vec<LogEntry> {
    (1..=n)
        .map(|i| LogEntry {
            term: Term(1),
            index: LogIndex(i),
            payload: EntryPayload::Command(vec![0xABu8; size]),
        })
        .collect()
}
