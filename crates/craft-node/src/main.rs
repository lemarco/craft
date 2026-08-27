//! `craft-node` — reference runner for a single craft cluster node (backlog
//! J3). It reads its configuration from the environment, builds the mTLS
//! [`Security`], and starts a node over the live QUIC transport with a built-in
//! demo state machine, then runs until `SIGINT`/Ctrl-C.
//!
//! It exists to smoke-test deployments and to serve as a copyable template —
//! real applications embed [`craft::CraftCluster`] in their own binary with
//! their own [`StateMachine`] and actors.
//!
//! ## Environment
//!
//! | Var | Meaning | Default |
//! |-----|---------|---------|
//! | `CRAFT_NODE_ID` | This node's id (`u64`) | *from `POD_NAME` ordinal, else `1`* |
//! | `POD_NAME` | Kubernetes pod name (`craft-0`); its ordinal → `NodeId(ordinal+1)` | *unset* |
//! | `CRAFT_LISTEN` | QUIC listen `addr:port` | `0.0.0.0:7443` |
//! | `CRAFT_ADMIN` | Admin HTTP `addr:port` (`-` to disable) | `0.0.0.0:8080` |
//! | `CRAFT_PEERS` | `id@host:port` list of **all** members (static membership) | *self only* |
//! | `CRAFT_JOIN_SEEDS` | `id@host:port` seed list for a **dynamic** join (discovery) | *none* |
//! | `CRAFT_DISCOVERY` | `dns:<prefix>:<service>:<replicas>:<port>` → resolve seeds | *none* |
//! | `CRAFT_ALLOW_JOIN` | Accept dynamic joins on this node (`1`/`true`) | `false` |
//! | `CRAFT_ALLOW_LEAVE` | Accept cluster leave RPC on this node (`1`/`true`) | `false` |
//! | `CRAFT_NODE_CERT` / `CRAFT_NODE_KEY` / `CRAFT_CA_CERT` | PEM cert chain / key / CA | *dev CA* |
//! | `CRAFT_CERT_ORDINAL_BASE` | Dir with per-ordinal subdirs (`0/tls.crt`, …) for K8s cert-manager | *unset* |
//! | `CRAFT_CERT_WATCH_SECS` | Poll interval for on-disk cert reload (cert-automation) | `60` |
//!
//! With no cert vars set, a throwaway dev CA is minted for a **single-node**
//! cluster (great for `cargo run -p craft-node`). A multi-node cluster needs a
//! shared CA: provide `CRAFT_NODE_CERT`/`CRAFT_NODE_KEY`/`CRAFT_CA_CERT` on
//! every node (mint them with `examples/certs/generate.sh`; see `docs/certs.md`)
//! and either list every member in `CRAFT_PEERS` (static) or point new nodes at
//! `CRAFT_JOIN_SEEDS` / `CRAFT_DISCOVERY` to grow the cluster dynamically. See
//! `deploy/kubernetes/` for a StatefulSet using ordinal-derived ids + DNS.

use std::collections::BTreeMap;
use std::error::Error;
use std::net::{SocketAddr, ToSocketAddrs};

use craft::core::StateMachine;
use craft::discovery::{Seed, resolve_dns_seeds};
use craft::proto::LogIndex;
use craft::{CraftCluster, NodeId, PeerDirectory, PemSecurity, Security, cert_paths_from_env};

/// A minimal built-in state machine: counts applied commands. Real apps supply
/// their own; this keeps the reference runner self-contained.
#[derive(Default)]
struct Demo {
    applied: u64,
}

impl StateMachine for Demo {
    type Command = Vec<u8>;
    type Query = ();
    type Response = u64;
    type Error = std::convert::Infallible;

    fn apply(&mut self, _index: LogIndex, _command: &Vec<u8>) -> Result<u64, Self::Error> {
        self.applied += 1;
        Ok(self.applied)
    }

    fn query(&self, _query: &()) -> Result<u64, Self::Error> {
        Ok(self.applied)
    }

    fn snapshot(&self) -> Result<Vec<u8>, Self::Error> {
        Ok(self.applied.to_le_bytes().to_vec())
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), Self::Error> {
        let mut buf = [0u8; 8];
        let n = snapshot.len().min(8);
        buf[..n].copy_from_slice(&snapshot[..n]);
        self.applied = u64::from_le_bytes(buf);
        Ok(())
    }
}

/// Parsed runtime configuration from the environment.
struct NodeConfig {
    node_id: NodeId,
    listen: SocketAddr,
    admin: Option<SocketAddr>,
    peers: PeerDirectory,
    members: Vec<NodeId>,
    join_seeds: Vec<Seed>,
    /// A `dns:<prefix>:<service>:<replicas>:<port>` discovery spec, resolved
    /// asynchronously in `main` into additional seeds (Kubernetes headless
    /// service; see [`craft::discovery::resolve_dns_seeds`]).
    discovery: Option<DnsSpec>,
    allow_join: bool,
    allow_leave: bool,
    /// Production PEM paths when cert env vars are set (cert-automation hot reload).
    pem_paths: Option<craft::CertPaths>,
    security: Security,
}

/// A parsed `CRAFT_DISCOVERY=dns:<prefix>:<service>:<replicas>:<port>` spec.
struct DnsSpec {
    prefix: String,
    service: String,
    replicas: u64,
    port: u16,
}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

fn env_bool(key: &str) -> bool {
    matches!(
        env(key).as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "on")
    )
}

/// Derive a node id from the environment: explicit `CRAFT_NODE_ID` wins,
/// otherwise a Kubernetes `POD_NAME` ordinal (`craft-0` → `NodeId(1)`), else `1`.
fn node_id_from_env() -> Result<NodeId, Box<dyn Error>> {
    if let Some(raw) = env("CRAFT_NODE_ID") {
        return Ok(NodeId(raw.parse()?));
    }
    if let Some(pod) = env("POD_NAME") {
        let ordinal: u64 = pod
            .rsplit_once('-')
            .and_then(|(_, ord)| ord.parse().ok())
            .ok_or_else(|| format!("POD_NAME {pod:?} has no trailing ordinal (want name-N)"))?;
        return Ok(NodeId(ordinal + 1));
    }
    Ok(NodeId(1))
}

/// StatefulSet pod ordinal from `POD_NAME` (`craft-2` → `2`).
fn pod_ordinal_from_env() -> Result<u64, Box<dyn Error>> {
    let pod = env("POD_NAME")
        .ok_or("CRAFT_CERT_ORDINAL_BASE requires POD_NAME (Kubernetes downward API)")?;
    pod.rsplit_once('-')
        .and_then(|(_, ord)| ord.parse().ok())
        .ok_or_else(|| format!("POD_NAME {pod:?} has no trailing ordinal (want name-N)").into())
}

/// Load mTLS material from explicit PEM paths or cert-manager ordinal mounts.
fn load_security_from_env(
    node_id: NodeId,
    members: &[NodeId],
    joining: bool,
) -> Result<(Security, Option<craft::CertPaths>), Box<dyn Error>> {
    if let Some(base) = env("CRAFT_CERT_ORDINAL_BASE") {
        let ca = env("CRAFT_CA_CERT").ok_or("CRAFT_CERT_ORDINAL_BASE requires CRAFT_CA_CERT")?;
        let ordinal = pod_ordinal_from_env()?;
        let paths = cert_paths_from_env(
            format!("{base}/{ordinal}/tls.crt"),
            format!("{base}/{ordinal}/tls.key"),
            ca,
        );
        let pem = PemSecurity::load(node_id, paths.clone())?;
        return Ok((pem.security, Some(paths)));
    }

    match (
        env("CRAFT_NODE_CERT"),
        env("CRAFT_NODE_KEY"),
        env("CRAFT_CA_CERT"),
    ) {
        (Some(cert), Some(key), Some(ca)) => {
            let paths = cert_paths_from_env(cert, key, ca);
            let pem = PemSecurity::load(node_id, paths.clone())?;
            Ok((pem.security, Some(paths)))
        }
        (None, None, None) => {
            if members.len() > 1 || joining {
                return Err("multi-node clusters need shared certs: set \
                     CRAFT_NODE_CERT/CRAFT_NODE_KEY/CRAFT_CA_CERT on every node \
                     (mint them with examples/certs/generate.sh; see docs/certs.md), \
                     or CRAFT_CERT_ORDINAL_BASE + CRAFT_CA_CERT for cert-manager. \
                     A per-process dev CA only works for a single node."
                    .into());
            }
            let ca = craft::net::tls::ClusterCa::generate()?;
            Ok((Security::dev(&ca, node_id)?, None))
        }
        _ => Err(
            "set all of CRAFT_NODE_CERT, CRAFT_NODE_KEY, CRAFT_CA_CERT together, \
             or CRAFT_CERT_ORDINAL_BASE + CRAFT_CA_CERT + POD_NAME, or none for dev mode"
                .into(),
        ),
    }
}

/// Parse `CRAFT_JOIN_SEEDS` (`id@host:port,...`) into a discovery seed set.
fn parse_seeds(raw: &str) -> Result<Vec<Seed>, Box<dyn Error>> {
    let mut seeds = Vec::new();
    for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (id, addr) = entry
            .split_once('@')
            .ok_or_else(|| format!("bad CRAFT_JOIN_SEEDS entry {entry:?} (want id@host:port)"))?;
        let id: u64 = id
            .parse()
            .map_err(|_| format!("bad node id in {entry:?}"))?;
        seeds.push(Seed::new(NodeId(id), resolve_addr(addr)?));
    }
    Ok(seeds)
}

/// Parse `CRAFT_DISCOVERY=dns:<prefix>:<service>:<replicas>:<port>`.
fn parse_discovery(raw: &str) -> Result<DnsSpec, Box<dyn Error>> {
    let parts: Vec<&str> = raw.split(':').collect();
    match parts.as_slice() {
        ["dns", prefix, service, replicas, port] => Ok(DnsSpec {
            prefix: (*prefix).to_string(),
            service: (*service).to_string(),
            replicas: replicas.parse()?,
            port: port.parse()?,
        }),
        _ => Err(format!(
            "bad CRAFT_DISCOVERY {raw:?} (want dns:<prefix>:<service>:<replicas>:<port>)"
        )
        .into()),
    }
}

/// Resolve `host:port` to a `SocketAddr`, accepting both numeric IPs and DNS
/// names (e.g. docker-compose service names). Retries briefly so a peer whose
/// container is still coming up on the shared network doesn't fail the boot.
fn resolve_addr(hostport: &str) -> Result<SocketAddr, Box<dyn Error>> {
    if let Ok(addr) = hostport.parse::<SocketAddr>() {
        return Ok(addr);
    }
    let mut last = String::new();
    for _ in 0..20 {
        match hostport.to_socket_addrs() {
            Ok(mut addrs) => {
                if let Some(addr) = addrs.next() {
                    return Ok(addr);
                }
                last = format!("no addresses for {hostport:?}");
            }
            Err(e) => last = format!("cannot resolve {hostport:?}: {e}"),
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    Err(last.into())
}

/// Parse `CRAFT_PEERS` (`id@host:port,...`) into an address book + member list.
fn parse_peers(raw: &str) -> Result<(PeerDirectory, Vec<NodeId>), Box<dyn Error>> {
    let mut map = BTreeMap::new();
    for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (id, addr) = entry
            .split_once('@')
            .ok_or_else(|| format!("bad CRAFT_PEERS entry {entry:?} (want id@host:port)"))?;
        let id: u64 = id
            .parse()
            .map_err(|_| format!("bad node id in {entry:?}"))?;
        map.insert(NodeId(id), resolve_addr(addr)?);
    }
    let members = map.keys().copied().collect();
    Ok((map.into_iter().collect(), members))
}

fn config_from_env() -> Result<NodeConfig, Box<dyn Error>> {
    let node_id = node_id_from_env()?;
    let listen: SocketAddr = env("CRAFT_LISTEN")
        .as_deref()
        .unwrap_or("0.0.0.0:7443")
        .parse()?;
    let admin = match env("CRAFT_ADMIN").as_deref() {
        Some("-") => None,
        Some(a) => Some(a.parse()?),
        None => Some("0.0.0.0:8080".parse()?),
    };

    let join_seeds = match env("CRAFT_JOIN_SEEDS") {
        Some(raw) => parse_seeds(&raw)?,
        None => Vec::new(),
    };
    let discovery = match env("CRAFT_DISCOVERY") {
        Some(raw) => Some(parse_discovery(&raw)?),
        None => None,
    };
    let allow_join = env_bool("CRAFT_ALLOW_JOIN");
    let allow_leave = env_bool("CRAFT_ALLOW_LEAVE");

    let (mut peers, mut members) = match env("CRAFT_PEERS") {
        Some(raw) => parse_peers(&raw)?,
        None => (PeerDirectory::new(), Vec::new()),
    };
    // A node that joins dynamically starts knowing only the *current* members
    // (from seeds/discovery) and is added to the voter set once its join
    // commits — it must not pre-list itself as a member (discovery, join-rpc).
    let joining = !join_seeds.is_empty() || discovery.is_some();
    if !joining {
        if !members.contains(&node_id) {
            members.push(node_id);
            members.sort();
        }
        if !peers.contains(node_id) {
            peers.insert(node_id, listen);
        }
    }

    let (security, pem_paths) = load_security_from_env(node_id, &members, joining)?;

    Ok(NodeConfig {
        node_id,
        listen,
        admin,
        peers,
        members,
        join_seeds,
        discovery,
        allow_join,
        allow_leave,
        pem_paths,
        security,
    })
}

/// Parse `CRAFT_CERT_WATCH_SECS` (default 60) for PEM hot reload polling.
fn cert_watch_period_from_env() -> std::time::Duration {
    env("CRAFT_CERT_WATCH_SECS")
        .and_then(|raw| raw.parse::<u64>().ok())
        .map_or(
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs,
        )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cfg = config_from_env()?;
    println!(
        "craft-node v{} (protocol v{}, wire {})",
        craft::VERSION,
        craft::PROTOCOL_VERSION,
        craft::proto::WIRE_CODEC,
    );

    // Assemble the discovery seed set: explicit seeds plus any resolved from a
    // DNS discovery spec (Kubernetes headless service; discovery).
    let mut seeds = cfg.join_seeds.clone();
    if let Some(dns) = &cfg.discovery {
        let resolved = resolve_dns_seeds(&dns.prefix, &dns.service, dns.replicas, dns.port).await?;
        println!("discovered {} seed(s) via DNS", resolved.len());
        seeds.extend(resolved);
    }

    let mut builder =
        CraftCluster::builder(cfg.node_id, Demo::default()).members(cfg.members.clone());
    if let Some(admin) = cfg.admin {
        builder = builder.admin_addr(admin);
    }
    if cfg.allow_join {
        builder = builder.allow_join(true);
    }
    if cfg.allow_leave {
        builder = builder.allow_leave(true);
    }
    if !seeds.is_empty() {
        builder = builder.join_seeds(seeds);
    }

    let cluster = if let Some(paths) = cfg.pem_paths {
        let pem = PemSecurity {
            security: cfg.security,
            paths,
        };
        builder
            .cert_watch(cert_watch_period_from_env())
            .start_quic_pem(pem, cfg.listen, cfg.peers)
            .await?
    } else {
        builder
            .start_quic(cfg.security, cfg.listen, cfg.peers)
            .await?
    };

    println!(
        "node {:?} listening on {} — members {:?}{}",
        cfg.node_id,
        cfg.listen,
        cfg.members,
        cfg.admin
            .map(|a| format!(", admin http://{a}"))
            .unwrap_or_default()
    );
    println!("ready; press Ctrl-C to stop.");

    tokio::signal::ctrl_c().await?;
    println!("\nshutting down…");
    cluster.shutdown();
    Ok(())
}
