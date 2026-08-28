//! Cluster discovery (discovery): how a node finds a live member to bootstrap a
//! dynamic join against.
//!
//! v1 used a single `JOIN_ADDR`. This module generalizes that to a **seed set**
//! — an ordered list of candidate members — so a joining node stays resilient
//! to any one seed being down or having moved (discovery's noted "seed address
//! stability" risk), and gossips the full peer-address book from whichever seed
//! answers first. Cloud/orchestrated environments (e.g. Kubernetes headless
//! services) resolve their pod DNS into a seed set via [`resolve_dns_seeds`].
//!
//! Discovery only bootstraps *first contact*; the authoritative membership
//! remains the Raft-committed voter set (joint consensus, membership-early), and peer
//! addresses converge through the `/cluster/peers` anti-entropy gossip.

use std::net::SocketAddr;

use craft_proto::NodeId;

/// A candidate cluster member to bootstrap a dynamic join against: a node id
/// plus a currently-believed address. Node ids are required because the wire
/// transport keys connections by id (wire-transport); orchestrated discovery derives
/// them deterministically (e.g. a `StatefulSet` ordinal `craft-2` → `NodeId(3)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Seed {
    /// The seed member's node id.
    pub node_id: NodeId,
    /// A currently-believed address for that member.
    pub addr: SocketAddr,
}

impl Seed {
    /// A seed at `addr` identified as `node_id`.
    #[must_use]
    pub fn new(node_id: NodeId, addr: SocketAddr) -> Self {
        Self { node_id, addr }
    }
}

/// Why discovery could not produce any seeds.
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    /// A DNS name could not be resolved to any address.
    #[error("no addresses resolved for any of: {0}")]
    Unresolved(String),
    /// The underlying resolver failed.
    #[error("resolver error: {0}")]
    Resolver(String),
}

/// Resolve a Kubernetes-style **headless-service** seed set: for each ordinal in
/// `0..replicas`, resolve `{prefix}-{ordinal}.{service}` to an address and pair
/// it with `NodeId(ordinal + 1)` (the one-based node-id convention). Unresolved
/// ordinals are skipped (a pod may not be up yet); the result is every seed that
/// currently resolves, in ordinal order.
///
/// This is the DNS the Kubernetes `StatefulSet` + headless `Service` manifests
/// in `deploy/kubernetes/` expose: a stable per-pod DNS name whose ordinal maps
/// one-to-one to a craft node id.
///
/// # Errors
/// Returns [`DiscoveryError::Unresolved`] only if *no* ordinal resolves (so the
/// caller can retry while the `StatefulSet` is still coming up).
pub async fn resolve_dns_seeds(
    prefix: &str,
    service: &str,
    replicas: u64,
    port: u16,
) -> Result<Vec<Seed>, DiscoveryError> {
    let mut seeds = Vec::new();
    for ordinal in 0..replicas {
        let host = seed_host(prefix, ordinal, service);
        match tokio::net::lookup_host((host.as_str(), port)).await {
            Ok(addrs) => {
                if let Some(addr) = addrs.into_iter().next() {
                    seeds.push(Seed::new(NodeId(ordinal + 1), addr));
                }
            }
            // A not-yet-scheduled pod fails to resolve; skip it and keep going.
            Err(_) => continue,
        }
    }
    if seeds.is_empty() {
        return Err(DiscoveryError::Unresolved(format!(
            "{prefix}-0..{replicas}.{service}"
        )));
    }
    Ok(seeds)
}

/// The per-ordinal DNS name a Kubernetes headless service exposes for a
/// `StatefulSet` pod: `{prefix}-{ordinal}.{service}` (a trailing-empty `service`
/// yields the bare pod name, used in tests).
#[must_use]
pub(crate) fn seed_host(prefix: &str, ordinal: u64, service: &str) -> String {
    if service.is_empty() {
        format!("{prefix}-{ordinal}")
    } else {
        format!("{prefix}-{ordinal}.{service}")
    }
}

/// Order and de-duplicate a seed set for a bootstrap attempt: preserves first
/// occurrence order (callers list preferred seeds first) and drops repeats and
/// any entry pointing at `me` (a node never bootstraps against itself).
#[must_use]
pub fn dedupe_seeds(seeds: impl IntoIterator<Item = Seed>, me: NodeId) -> Vec<Seed> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for seed in seeds {
        if seed.node_id == me {
            continue;
        }
        if seen.insert(seed) {
            out.push(seed);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(p: u16) -> SocketAddr {
        format!("127.0.0.1:{p}").parse().unwrap()
    }

    #[test]
    fn dedupe_preserves_order_and_drops_repeats_and_self() {
        let seeds = [
            Seed::new(NodeId(2), addr(2)),
            Seed::new(NodeId(3), addr(3)),
            Seed::new(NodeId(2), addr(2)), // duplicate
            Seed::new(NodeId(1), addr(1)), // == me, dropped
        ];
        let out = dedupe_seeds(seeds, NodeId(1));
        assert_eq!(
            out,
            vec![Seed::new(NodeId(2), addr(2)), Seed::new(NodeId(3), addr(3))]
        );
    }

    #[test]
    fn dns_host_names_follow_the_statefulset_convention() {
        assert_eq!(
            seed_host("craft", 0, "craft.default.svc"),
            "craft-0.craft.default.svc"
        );
        assert_eq!(
            seed_host("craft", 2, "craft-headless"),
            "craft-2.craft-headless"
        );
        // Empty service → bare pod name (test/util form).
        assert_eq!(seed_host("craft", 1, ""), "craft-1");
    }
}
