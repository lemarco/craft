//! Deterministic PRNG for reproducible election timeouts.
//!
//! The core must be free of ambient randomness so that a given seed replays
//! identically in simulation (ADR 029, ADR 030). This is a tiny xorshift64
//! generator — not cryptographic, only for jitter selection.

/// A small deterministic xorshift64 generator.
#[derive(Debug, Clone)]
pub(crate) struct Rng(u64);

impl Rng {
    /// Create a generator from `seed`. The internal state is forced non-zero
    /// (xorshift has a fixed point at zero).
    pub(crate) fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Uniformly pick a value in the inclusive range `[lo, hi]`.
    pub(crate) fn range(&mut self, lo: u64, hi: u64) -> u64 {
        debug_assert!(hi >= lo, "range hi < lo");
        if hi == lo {
            return lo;
        }
        lo + self.next_u64() % (hi - lo + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_for_same_seed() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        let sa: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
        let sb: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
        assert_ne!(sa, sb);
    }

    #[test]
    fn range_is_within_bounds() {
        let mut r = Rng::new(7);
        for _ in 0..10_000 {
            let v = r.range(10, 20);
            assert!((10..=20).contains(&v));
        }
    }

    #[test]
    fn range_equal_bounds_is_constant() {
        let mut r = Rng::new(9);
        assert_eq!(r.range(5, 5), 5);
    }

    #[test]
    fn zero_seed_still_advances() {
        let mut r = Rng::new(0);
        assert_ne!(r.next_u64(), 0);
    }
}
