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
//! | `CRAFT_NODE_ID` | This node's id (`u64`) | `1` |
//! | `CRAFT_LISTEN` | QUIC listen `addr:port` | `0.0.0.0:7443` |
//! | `CRAFT_ADMIN` | Admin HTTP `addr:port` (`-` to disable) | `0.0.0.0:8080` |
//! | `CRAFT_PEERS` | `id@host:port` list of **all** members | *self only* |
//! | `CRAFT_NODE_CERT` / `CRAFT_NODE_KEY` / `CRAFT_CA_CERT` | PEM cert chain / key / CA | *dev CA* |
//!
//! With no cert vars set, a throwaway dev CA is minted for a **single-node**
//! cluster (great for `cargo run -p craft-node`). A multi-node cluster needs a
//! shared CA: provide `CRAFT_NODE_CERT`/`CRAFT_NODE_KEY`/`CRAFT_CA_CERT` on
//! every node (mint them with `examples/certs/generate.sh`; see `docs/certs.md`)
//! and list every member in `CRAFT_PEERS`.

use std::collections::BTreeMap;
use std::error::Error;
use std::net::{SocketAddr, ToSocketAddrs};

use craft::core::StateMachine;
use craft::net::NodeIdentity;
use craft::proto::LogIndex;
use craft::{CraftCluster, NodeId, PeerDirectory, Security};

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
    security: Security,
}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
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

/// Load an mTLS identity + trust root from PEM files.
fn load_pem_security(
    node_id: NodeId,
    cert_path: &str,
    key_path: &str,
    ca_path: &str,
) -> Result<Security, Box<dyn Error>> {
    let cert_chain = rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(
        cert_path,
    )?))
    .collect::<Result<Vec<_>, _>>()?;
    let key =
        rustls_pemfile::private_key(&mut std::io::BufReader::new(std::fs::File::open(key_path)?))?
            .ok_or_else(|| format!("no private key in {key_path}"))?;
    let ca_certs =
        rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(ca_path)?))
            .collect::<Result<Vec<_>, _>>()?;

    let identity = NodeIdentity::from_der(node_id, cert_chain, key);
    Ok(Security::from_ca_certs(identity, &ca_certs)?)
}

fn config_from_env() -> Result<NodeConfig, Box<dyn Error>> {
    let node_id = NodeId(env("CRAFT_NODE_ID").as_deref().unwrap_or("1").parse()?);
    let listen: SocketAddr = env("CRAFT_LISTEN")
        .as_deref()
        .unwrap_or("0.0.0.0:7443")
        .parse()?;
    let admin = match env("CRAFT_ADMIN").as_deref() {
        Some("-") => None,
        Some(a) => Some(a.parse()?),
        None => Some("0.0.0.0:8080".parse()?),
    };

    let (mut peers, mut members) = match env("CRAFT_PEERS") {
        Some(raw) => parse_peers(&raw)?,
        None => (PeerDirectory::new(), Vec::new()),
    };
    // Always ensure this node is a member and reachable in the book.
    if !members.contains(&node_id) {
        members.push(node_id);
        members.sort();
    }
    if !peers.contains(node_id) {
        peers.insert(node_id, listen);
    }

    let security = match (
        env("CRAFT_NODE_CERT"),
        env("CRAFT_NODE_KEY"),
        env("CRAFT_CA_CERT"),
    ) {
        (Some(cert), Some(key), Some(ca)) => load_pem_security(node_id, &cert, &key, &ca)?,
        (None, None, None) => {
            if members.len() > 1 {
                return Err("multi-node clusters need shared certs: set \
                     CRAFT_NODE_CERT/CRAFT_NODE_KEY/CRAFT_CA_CERT on every node \
                     (mint them with examples/certs/generate.sh; see docs/certs.md). \
                     A per-process dev CA only works for a single node."
                    .into());
            }
            // Single-node dev: mint a throwaway CA and self identity.
            let ca = craft::net::tls::ClusterCa::generate()?;
            Security::dev(&ca, node_id)?
        }
        _ => {
            return Err("set all of CRAFT_NODE_CERT, CRAFT_NODE_KEY, CRAFT_CA_CERT \
                 together, or none for dev mode"
                .into());
        }
    };

    Ok(NodeConfig {
        node_id,
        listen,
        admin,
        peers,
        members,
        security,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cfg = config_from_env()?;
    println!(
        "craft-node v{} (protocol v{})",
        craft::VERSION,
        craft::PROTOCOL_VERSION
    );

    let mut builder =
        CraftCluster::builder(cfg.node_id, Demo::default()).members(cfg.members.clone());
    if let Some(admin) = cfg.admin {
        builder = builder.admin_addr(admin);
    }

    let cluster = builder
        .start_quic(cfg.security, cfg.listen, cfg.peers)
        .await?;

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
