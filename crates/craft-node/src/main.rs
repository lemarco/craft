//! `craft-node` — reference runner for a single craft cluster node (backlog
//! J3). It reads its configuration from the environment, builds the mTLS
//! [`craft::Security`], and starts a node over the live QUIC transport with a built-in
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
//! | `CRAFT_ADMIN_TLS_CERT` / `CRAFT_ADMIN_TLS_KEY` | PEM paths for admin HTTPS (both required) | *plain HTTP* |
//! | `CRAFT_PEERS` | `id@host:port` list of **all** members (static membership) | *self only* |
//! | `CRAFT_JOIN_SEEDS` | `id@host:port` seed list for a **dynamic** join (discovery) | *none* |
//! | `CRAFT_DISCOVERY` | `dns:<prefix>:<service>:<replicas>:<port>` → resolve seeds | *none* |
//! | `CRAFT_ALLOW_JOIN` | Accept dynamic joins on this node (`1`/`true`) | `false` |
//! | `CRAFT_ALLOW_LEAVE` | Accept cluster leave RPC on this node (`1`/`true`) | `false` |
//! | `CRAFT_GRACEFUL_LEAVE` | On shutdown, call [`CraftCluster::leave`] before exit | `false` |
//! | `CRAFT_NODE_CERT` / `CRAFT_NODE_KEY` / `CRAFT_CA_CERT` | PEM cert chain / key / CA | *dev CA* |
//! | `CRAFT_CERT_ORDINAL_BASE` | Dir with per-ordinal subdirs (`0/tls.crt`, …) for K8s cert-manager | *unset* |
//! | `CRAFT_CERT_WATCH_SECS` | Poll interval for on-disk cert reload (cert-automation) | `60` |
//! | `RUST_LOG` / `CRAFT_LOG` | `tracing` filter directives (see [`craft::init_tracing`]) | `warn` |
//! | `CRAFT_LOG_REBALANCE` | Enable `craft::rebalance=debug` rebalance planner logs | *off* |
//!
//! With no cert vars set, a throwaway dev CA is minted for a **single-node**
//! cluster (great for `cargo run -p craft-node`). A multi-node cluster needs a
//! shared CA: provide `CRAFT_NODE_CERT`/`CRAFT_NODE_KEY`/`CRAFT_CA_CERT` on
//! every node (mint them with `examples/certs/generate.sh`; see `docs/certs.md`)
//! and either list every member in `CRAFT_PEERS` (static) or point new nodes at
//! `CRAFT_JOIN_SEEDS` / `CRAFT_DISCOVERY` to grow the cluster dynamically. See
//! `deploy/kubernetes/` for a StatefulSet using ordinal-derived ids + DNS.

mod config;

use std::error::Error;

use craft::core::StateMachine;
use craft::discovery::resolve_dns_seeds;
use craft::proto::LogIndex;
use craft::{CraftCluster, PemSecurity};

use config::{cert_watch_period_from_env, config_from_env};

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    craft::init_tracing();
    let cfg = config_from_env()?;
    println!(
        "craft-node v{} (protocol v{}, wire {})",
        craft::VERSION,
        craft::PROTOCOL_VERSION,
        craft::proto::WIRE_CODEC,
    );

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
    if let Some((cert, key)) = cfg.admin_tls {
        builder = builder.admin_tls(cert, key);
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
    builder = builder.drain_timeout(cfg.drain_timeout);

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
    if cfg.graceful_leave && cfg.members.len() > 1 {
        match cluster.leave().await {
            Ok(membership) => println!("left cluster; remaining voters {:?}", membership.voters),
            Err(e) => eprintln!("graceful leave failed ({e}); shutting down anyway"),
        }
    }
    cluster.shutdown();
    Ok(())
}
