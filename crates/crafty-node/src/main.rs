//! `crafty-node` — reference runner for a single crafty cluster node (backlog
//! J3). It reads its configuration from the environment, builds the mTLS
//! [`crafty::advanced::Security`], and starts a node over the live QUIC transport with a built-in
//! demo state machine, then runs until `SIGINT`/Ctrl-C.
//!
//! It exists to smoke-test deployments and to serve as a copyable template —
//! real applications embed [`crafty::CraftyCluster`] in their own binary with
//! their own [`StateMachine`] and actors.
//!
//! ## Environment
//!
//! | Var | Meaning | Default |
//! |-----|---------|---------|
//! | `CRAFTY_NODE_ID` | This node's id (`u64`) | `1` |
//! | `CRAFTY_LISTEN` | QUIC listen `addr:port` | `0.0.0.0:7443` |
//! | `CRAFTY_ADMIN` | Admin HTTP `addr:port` (`-` to disable) | `0.0.0.0:8080` |
//! | `CRAFTY_ADMIN_TLS_CERT` / `CRAFTY_ADMIN_TLS_KEY` | PEM paths for admin HTTPS (both required) | *plain HTTP* |
//! | `CRAFTY_PEERS` | `id@host:port` list of **all** members (static membership) | *self only* |
//! | `CRAFTY_JOIN_SEEDS` | `id@host:port` seed list for a **dynamic** join (discovery) | *none* |
//! | `CRAFTY_DISCOVERY` | `dns:<prefix>:<service>:<replicas>:<port>` → resolve seeds | *none* |
//! | `CRAFTY_ALLOW_JOIN` | Accept dynamic joins on this node (`1`/`true`) | `false` |
//! | `CRAFTY_ALLOW_LEAVE` | Accept cluster leave RPC on this node (`1`/`true`) | `false` |
//! | `CRAFTY_GRACEFUL_LEAVE` | On shutdown, call [`CraftyCluster::leave`] before exit | `false` |
//! | `CRAFTY_DATA_DIR` | Persistent redb directory (Raft + queue files) | *unset* |
//! | `CRAFTY_JOB_QUEUE` | Durable job queue stream name (requires `CRAFTY_DATA_DIR`) | *unset* |
//! | `CRAFTY_JOB_QUEUE_LEASE_SECS` | Queue lease visibility timeout (seconds) | `60` |
//! | `CRAFTY_NODE_CERT` / `CRAFTY_NODE_KEY` / `CRAFTY_CA_CERT` | PEM cert chain / key / CA | *dev CA* |
//! | `CRAFTY_CERT_WATCH_SECS` | Poll interval for on-disk cert reload (cert-automation) | `60` |
//! | `RUST_LOG` / `CRAFTY_LOG` | `tracing` filter directives (see [`crafty::init_tracing`]) | `warn` |
//! | `CRAFTY_LOG_REBALANCE` | Enable `crafty::rebalance=debug` rebalance planner logs | *off* |
//!
//! With no cert vars set, a throwaway dev CA is minted for a **single-node**
//! cluster (great for `cargo run -p crafty-node`). A multi-node cluster needs a
//! shared CA: provide `CRAFTY_NODE_CERT`/`CRAFTY_NODE_KEY`/`CRAFTY_CA_CERT` on
//! every node (mint them with `dev/certs/generate.sh`; see `docs/certs.md`)
//! and either list every member in `CRAFTY_PEERS` (static) or point new nodes at
//! `CRAFTY_JOIN_SEEDS` / `CRAFTY_DISCOVERY` to grow the cluster dynamically.

mod config;

use std::error::Error;

use crafty::CraftyCluster;
use crafty::advanced::PemSecurity;
use crafty::core::StateMachine;
use crafty::discovery::resolve_dns_seeds;
use crafty::proto::LogIndex;

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
    crafty::init_tracing();
    let cfg = config_from_env()?;
    println!(
        "crafty-node v{} (protocol v{}, wire {})",
        crafty::VERSION,
        crafty::PROTOCOL_VERSION,
        crafty::proto::WIRE_CODEC,
    );

    let mut seeds = cfg.join_seeds.clone();
    if let Some(dns) = &cfg.discovery {
        let resolved = resolve_dns_seeds(&dns.prefix, &dns.service, dns.replicas, dns.port).await?;
        println!("discovered {} seed(s) via DNS", resolved.len());
        seeds.extend(resolved);
    }

    let mut builder =
        CraftyCluster::builder(cfg.node_id, Demo::default()).members(cfg.members.clone());
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
    if let Some(data_dir) = &cfg.data_dir {
        builder = builder.data_dir(data_dir);
    }
    if let Some(stream) = &cfg.job_queue_stream {
        builder = builder.job_queue(stream, cfg.job_queue_lease);
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
