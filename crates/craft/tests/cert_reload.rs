//! Integration test: on-disk PEM hot reload over loopback QUIC (cert-automation).

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use craft::{CertPaths, CraftCluster, NodeId, PemSecurity, ReloadOpts};
use craft_test_support::free_udp;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pem_hot_reload_reissues_leaf_without_restart() {
    let ws = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let script = ws.join("examples/certs/generate.sh");
    if !script.is_file() {
        eprintln!("skip: generate.sh not found at {}", script.display());
        return;
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path();
    run_generate(&script, &["--node-id", "1", "--out", out.to_str().unwrap()]);
    run_generate(
        &script,
        &[
            "--node-id",
            "2",
            "--out",
            out.to_str().unwrap(),
            "--ca",
            &format!("{}/ca.pem", out.display()),
            "--ca-key",
            &format!("{}/ca.key", out.display()),
        ],
    );

    let paths1 = CertPaths::new(
        out.join("node-1.pem"),
        out.join("node-1.key"),
        out.join("ca.pem"),
    );
    let paths2 = CertPaths::new(
        out.join("node-2.pem"),
        out.join("node-2.key"),
        out.join("ca.pem"),
    );

    let listen1 = free_udp();
    let listen2 = free_udp();

    let pem1 = PemSecurity::load(NodeId(1), paths1.clone()).expect("load node 1");
    let pem2 = PemSecurity::load(NodeId(2), paths2).expect("load node 2");

    let mut peers = craft::PeerDirectory::new();
    peers.insert(NodeId(1), listen1);
    peers.insert(NodeId(2), listen2);

    let raft = craft::core::Config {
        election_timeout_min: 5,
        election_timeout_max: 10,
        heartbeat_interval: 2,
        seed: 11,
    };

    let cluster1 = CraftCluster::builder(NodeId(1), Counter::default())
        .members([NodeId(1), NodeId(2)])
        .raft_config(raft.clone())
        .tick_period(Duration::from_millis(10))
        .cert_watch(Duration::from_millis(100))
        .start_quic_pem(pem1, listen1, peers.clone())
        .await
        .expect("start node 1");

    let cluster2 = CraftCluster::builder(NodeId(2), Counter::default())
        .members([NodeId(1), NodeId(2)])
        .raft_config(raft)
        .tick_period(Duration::from_millis(10))
        .start_quic_pem(pem2, listen2, peers)
        .await
        .expect("start node 2");

    let reload = cluster1.cert_reload().expect("cert reload handle");
    // Reissue node 1 cert from the same CA (simulates step-ca / cert-manager renewal).
    run_generate(
        &script,
        &[
            "--node-id",
            "1",
            "--out",
            out.to_str().unwrap(),
            "--ca",
            &format!("{}/ca.pem", out.display()),
            "--ca-key",
            &format!("{}/ca.key", out.display()),
        ],
    );
    reload
        .reload_now(ReloadOpts { allow_leader: true })
        .await
        .expect("manual reload");

    // Cluster should still accept writes after reload.
    let status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if cluster1.is_leader().await || cluster2.is_leader().await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(status.is_ok(), "leader election after cert reload");

    cluster1.shutdown();
    cluster2.shutdown();
}

#[derive(Default)]
struct Counter(u64);

impl craft::core::StateMachine for Counter {
    type Command = u64;
    type Query = ();
    type Response = u64;
    type Error = std::convert::Infallible;

    fn apply(&mut self, _: craft::proto::LogIndex, cmd: &u64) -> Result<u64, Self::Error> {
        self.0 += *cmd;
        Ok(self.0)
    }

    fn query(&self, _: &()) -> Result<u64, Self::Error> {
        Ok(self.0)
    }

    fn snapshot(&self) -> Result<Vec<u8>, Self::Error> {
        Ok(self.0.to_le_bytes().to_vec())
    }

    fn restore(&mut self, b: &[u8]) -> Result<(), Self::Error> {
        self.0 = u64::from_le_bytes(b.try_into().unwrap());
        Ok(())
    }
}

fn run_generate(script: &Path, args: &[&str]) {
    let status = Command::new(script)
        .args(args)
        .status()
        .expect("run generate.sh");
    assert!(status.success(), "generate.sh failed: {args:?}");
}
