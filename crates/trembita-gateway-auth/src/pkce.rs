//! PKCE helpers (RFC 7636) — **S256 default** for production OAuth (B-47).

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use sha2::{Digest, Sha256};

/// PKCE code challenge method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PkceMethod {
    /// `BASE64URL(SHA256(code_verifier))` — required for public clients (OAuth 2.1 default).
    #[default]
    S256,
}

impl PkceMethod {
    /// Parse env value (`S256` only; plain text rejected in production helpers).
    #[must_use]
    pub fn parse_env(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "S256" | "SHA256" => Some(Self::S256),
            _ => None,
        }
    }
}

/// Generated verifier + challenge pair for the authorization request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PkcePair {
    /// Secret sent to the token endpoint (`code_verifier`).
    pub verifier: String,
    /// Public value sent on the authorize redirect (`code_challenge`).
    pub challenge: String,
    /// Challenge method (always [`PkceMethod::S256`] from [`PkcePair::generate_s256`]).
    pub method: PkceMethod,
}

impl PkcePair {
    /// Default production pair — 32 random bytes, S256 challenge.
    ///
    /// # Panics
    /// If OS randomness fails (same as other crypto bootstrap in the workspace).
    #[must_use]
    pub fn generate_s256() -> Self {
        let mut buf = [0u8; 32];
        getrandom::fill(&mut buf).expect("OS randomness for PKCE verifier");
        let verifier = URL_SAFE_NO_PAD.encode(buf);
        let challenge = s256_challenge(&verifier);
        Self {
            verifier,
            challenge,
            method: PkceMethod::S256,
        }
    }

    /// Verify `code_verifier` produces `code_challenge` for `method`.
    #[must_use]
    pub fn verify(method: PkceMethod, verifier: &str, challenge: &str) -> bool {
        if verifier.len() < 43 || verifier.len() > 128 {
            return false;
        }
        match method {
            PkceMethod::S256 => constant_time_eq(challenge, &s256_challenge(verifier)),
        }
    }
}

#[must_use]
fn s256_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b47_pkce_s256_roundtrip() {
        let pair = PkcePair::generate_s256();
        assert!(PkcePair::verify(
            PkceMethod::S256,
            &pair.verifier,
            &pair.challenge
        ));
        assert!(!PkcePair::verify(PkceMethod::S256, &pair.verifier, "wrong"));
    }

    #[test]
    fn b47_pkce_method_parse_env_table() {
        assert_eq!(PkceMethod::parse_env("s256"), Some(PkceMethod::S256));
        assert_eq!(PkceMethod::parse_env("plain"), None);
    }

    #[test]
    fn b47_pkce_rejects_short_verifier() {
        assert!(!PkcePair::verify(
            PkceMethod::S256,
            "short",
            &s256_challenge("short")
        ));
    }
}
