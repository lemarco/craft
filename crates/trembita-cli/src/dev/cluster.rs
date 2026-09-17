//! Local multi-node showcase cluster (replaces `./cluster.sh up` for dev).

use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use super::DevError;
use super::local_cluster::{LOCAL_CLUSTER_GATEWAY_SESSION_SECRET, LOCAL_CLUSTER_GATEWAY_TOKEN};
use super::showcases::Showcase;

/// Build release binary + optional showcase client.
pub fn setup(showcase: &Showcase, workspace: &Path) -> Result<(), DevError> {
    let cluster = showcase.cluster_root(workspace);
    let certs = cluster.join("certs");
    fs::create_dir_all(cluster.join("data"))?;
    fs::create_dir_all(cluster.join("logs"))?;

    if showcase.solo_http {
        fs::create_dir_all(cluster.join("logs"))?;
    } else if !certs.join("ca.pem").is_file() {
        eprintln!(">> minting cluster CA + node certs in {}", certs.display());
        let generate = workspace.join("dev/certs/generate.sh");
        run_bash(
            &generate,
            workspace,
            &["--ca-only", "--out", certs.to_str().expect("utf8")],
        )?;
        run_bash(
            &generate,
            workspace,
            &[
                "--node-id",
                "0",
                "--out",
                certs.to_str().expect("utf8"),
                "--ca",
                &certs.join("ca.pem").to_string_lossy(),
                "--ca-key",
                &certs.join("ca.key").to_string_lossy(),
            ],
        )?;
        for id in showcase.cert_ids {
            if *id == 0 {
                continue;
            }
            run_bash(
                &generate,
                workspace,
                &[
                    "--node-id",
                    &id.to_string(),
                    "--out",
                    certs.to_str().expect("utf8"),
                    "--ca",
                    &certs.join("ca.pem").to_string_lossy(),
                    "--ca-key",
                    &certs.join("ca.key").to_string_lossy(),
                ],
            )?;
        }
    } else {
        eprintln!(">> reusing certs in {}", certs.display());
    }

    eprintln!(">> building {} (release)", showcase.binary);
    let manifest = showcase.example_dir(workspace).join("Cargo.toml");
    let status = Command::new("cargo")
        .arg("build")
        .arg("--release")
        .arg("--manifest-path")
        .arg(&manifest)
        .status()
        .map_err(DevError::Io)?;
    if !status.success() {
        return Err(DevError::CommandFailed(format!(
            "cargo build --release (exit {status})"
        )));
    }

    eprintln!(">> building trembita-showcase-client");
    let ws_manifest = workspace.join("Cargo.toml");
    let status = Command::new("cargo")
        .arg("build")
        .arg("-p")
        .arg("trembita-tools")
        .arg("--bin")
        .arg("trembita-showcase-client")
        .arg("--manifest-path")
        .arg(&ws_manifest)
        .status()
        .map_err(DevError::Io)?;
    if !status.success() {
        return Err(DevError::CommandFailed(
            "cargo build trembita-showcase-client".into(),
        ));
    }
    Ok(())
}

/// Stop running showcase processes (`pkill -f <binary>`).
pub fn stop(showcase: &Showcase) -> Result<(), DevError> {
    eprintln!(">> stopping {}", showcase.binary);
    let _ = Command::new("pkill")
        .arg("-f")
        .arg(showcase.binary)
        .status()
        .map_err(DevError::Io)?;
    thread::sleep(Duration::from_millis(500));
    Ok(())
}

/// Start `nodes` cluster members with shared gateway session env (B-39).
pub fn up_with_shared_gateway_env(
    showcase: &Showcase,
    workspace: &Path,
    nodes: u32,
) -> Result<(), DevError> {
    up_inner(
        showcase,
        workspace,
        nodes,
        true,
        use_staged_elastic_join(nodes),
    )
}

/// Start `nodes` cluster members in the background.
pub fn up(showcase: &Showcase, workspace: &Path, nodes: u32) -> Result<(), DevError> {
    up_inner(showcase, workspace, nodes, false, false)
}

fn up_inner(
    showcase: &Showcase,
    workspace: &Path,
    nodes: u32,
    shared_gateway_env: bool,
    staged_elastic_join: bool,
) -> Result<(), DevError> {
    if nodes == 0 || nodes > 8 {
        return Err(DevError::InvalidNodes(nodes));
    }
    let bin = showcase.release_bin(workspace);
    if !bin.is_file() {
        return Err(DevError::BinaryMissing(bin));
    }
    let cluster = showcase.cluster_root(workspace);
    let certs = cluster.join("certs");
    if showcase.solo_http {
        if nodes != 1 {
            eprintln!(
                "warn: solo showcase {id} ignores --nodes {nodes}",
                id = showcase.id
            );
        }
        stop(showcase)?;
        fs::create_dir_all(cluster.join("logs"))?;
        spawn_solo_http(showcase, workspace, &bin)?;
    } else {
        if !certs.join("ca.pem").is_file() {
            return Err(DevError::SetupRequired);
        }
        stop(showcase)?;
        fs::create_dir_all(cluster.join("logs"))?;
        let waves = cluster_spawn_waves(nodes, staged_elastic_join);
        if staged_elastic_join && nodes >= 4 {
            eprintln!(">> elastic join: seed nodes 1–3, then joiners 4–{nodes}");
        }
        for wave in &waves {
            for &node in wave {
                spawn_node(showcase, workspace, node, &bin, shared_gateway_env)?;
            }
            if staged_elastic_join && nodes >= 4 {
                for &node in wave {
                    let port = ready_port_for_node(showcase.base_port, node);
                    eprintln!(">> waiting for node {node} GET /ready on :{port}");
                    if !wait_ready(port, 120) {
                        return Err(DevError::CommandFailed(format!(
                            "node {node} /ready timeout (see {}/logs/)",
                            cluster.display()
                        )));
                    }
                }
            }
        }
    }

    eprintln!(">> waiting for /health on :{}", showcase.base_port);
    thread::sleep(Duration::from_secs(2));
    if wait_health(showcase.base_port) {
        eprintln!("OK: {} cluster ready ({} nodes)", showcase.id, nodes);
        eprintln!("  trigger: trembita dev trigger {} -- …", showcase.id);
    } else {
        eprintln!(
            "warn: /health not ready yet — check {}/logs/",
            cluster.display()
        );
    }
    Ok(())
}

fn spawn_solo_http(showcase: &Showcase, workspace: &Path, bin: &Path) -> Result<(), DevError> {
    let cluster = showcase.cluster_root(workspace);
    let data_dir = cluster.join("data").join("solo");
    let log = cluster.join("logs").join("solo.log");
    fs::create_dir_all(&data_dir)?;
    let http = format!("127.0.0.1:{}", showcase.base_port);
    let mut cmd = Command::new(bin);
    cmd.env("TREMBITA_HTTP", &http)
        .env("TREMBITA_LISTEN", &http)
        .env("TREMBITA_DATA_DIR", &data_dir);
    if std::env::var("RUST_LOG").is_err() {
        cmd.env("RUST_LOG", "info,trembita=info");
    }
    let log_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .map_err(DevError::Io)?;
    cmd.stdout(Stdio::from(log_file.try_clone().map_err(DevError::Io)?))
        .stderr(Stdio::from(log_file));
    let child = cmd.spawn().map_err(DevError::Io)?;
    eprintln!(
        ">> solo http pid={} http={http} log={}",
        child.id(),
        log.display()
    );
    let _ = workspace;
    Ok(())
}

fn spawn_node(
    showcase: &Showcase,
    workspace: &Path,
    node: u32,
    bin: &Path,
    shared_gateway_env: bool,
) -> Result<(), DevError> {
    let listen = showcase.listen_addr(node);
    let port = listen.split(':').next_back().unwrap_or("443");
    let cluster = showcase.cluster_root(workspace);
    let data_dir = cluster.join("data").join(format!("p{port}"));
    let log = cluster.join("logs").join(format!("node-{node}.log"));
    fs::create_dir_all(&data_dir)?;

    let mut cmd = Command::new(bin);
    cmd.env("TREMBITA_LISTEN", &listen)
        .env("TREMBITA_DATA_DIR", &data_dir)
        .env("TREMBITA_CERT_DIR", cluster.join("certs"))
        .env("TREMBITA_ALLOW_LEAVE", "1")
        .env("TREMBITA_GRACEFUL_LEAVE", "1")
        .env_remove("TREMBITA_HTTP")
        .env_remove("TREMBITA_GATEWAY")
        .env_remove("TREMBITA_NODE_ID")
        .env_remove("TREMBITA_PEERS");

    if node == 1 {
        cmd.env("TREMBITA_ALLOW_JOIN", "1")
            .env_remove("TREMBITA_JOIN_SEEDS");
    } else {
        cmd.env("TREMBITA_JOIN_SEEDS", showcase.join_seed())
            .env_remove("TREMBITA_ALLOW_JOIN");
    }

    if std::env::var("RUST_LOG").is_err() {
        cmd.env(
            "RUST_LOG",
            "info,showcase=debug,trembita=info,trembita_net=warn",
        );
    }
    if shared_gateway_env {
        cmd.env(
            "TREMBITA_GATEWAY_SESSION_SECRET",
            LOCAL_CLUSTER_GATEWAY_SESSION_SECRET,
        )
        .env("GATEWAY_TOKEN", LOCAL_CLUSTER_GATEWAY_TOKEN);
    }

    let log_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .map_err(DevError::Io)?;

    cmd.stdout(Stdio::from(log_file.try_clone().map_err(DevError::Io)?))
        .stderr(Stdio::from(log_file));

    let child = cmd.spawn().map_err(DevError::Io)?;
    eprintln!(
        ">> node {node} pid={} listen={listen} log={}",
        child.id(),
        log.display()
    );
    Ok(())
}

fn wait_health(port: u16) -> bool {
    for _ in 0..12 {
        if curl_health(port) {
            return true;
        }
        thread::sleep(Duration::from_secs(1));
    }
    false
}

/// Poll `GET /ready` until success or `tries` elapsed seconds (B-35 join smoke).
fn wait_ready(port: u16, tries: u32) -> bool {
    for _ in 0..tries {
        if curl_ready(port) {
            return true;
        }
        thread::sleep(Duration::from_secs(1));
    }
    false
}

fn curl_ready(port: u16) -> bool {
    Command::new("curl")
        .args([
            "-sf",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            &format!("http://127.0.0.1:{port}/ready"),
        ])
        .output()
        .ok()
        .is_some_and(|o| o.stdout.starts_with(b"200"))
}

fn curl_health(port: u16) -> bool {
    Command::new("curl")
        .args([
            "-sf",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            &format!("http://127.0.0.1:{port}/health"),
        ])
        .output()
        .ok()
        .is_some_and(|o| o.stdout.starts_with(b"200"))
}

fn run_bash(script: &Path, cwd: &Path, args: &[&str]) -> Result<(), DevError> {
    if !script.is_file() {
        return Err(DevError::MissingScript(script.to_path_buf()));
    }
    let mut cmd = Command::new("bash");
    cmd.arg(script).args(args).current_dir(cwd);
    let status = cmd.status().map_err(DevError::Io)?;
    if status.success() {
        Ok(())
    } else {
        Err(DevError::CommandFailed(format!(
            "{} {:?}",
            script.display(),
            args
        )))
    }
}

/// Staged seed-then-joiner sequencing for `cluster-up --nodes N` when `N >= 4` (B-42).
#[must_use]
pub(crate) fn use_staged_elastic_join(nodes: u32) -> bool {
    nodes >= 4
}

/// HTTP port for `GET /ready` wait during elastic join (node index 1-based).
#[must_use]
pub(crate) fn ready_port_for_node(base_port: u16, node: u32) -> u16 {
    let port = u32::from(base_port) + node.saturating_sub(1);
    u16::try_from(port).unwrap_or(base_port)
}

/// Process spawn order: one wave for normal up; `[1,2,3]` then `[4..=N]` when staging (B-42).
#[must_use]
pub(crate) fn cluster_spawn_waves(nodes: u32, staged_elastic_join: bool) -> Vec<Vec<u32>> {
    if nodes == 0 {
        return Vec::new();
    }
    if staged_elastic_join && nodes >= 4 {
        return vec![vec![1, 2, 3], (4..=nodes).collect()];
    }
    vec![(1..=nodes).collect()]
}

/// Print cluster / port summary.
pub fn status(showcase: &Showcase, workspace: &Path) -> Result<(), DevError> {
    let _ = workspace;
    eprintln!("{} (seed :{}):", showcase.id, showcase.base_port);
    let out = Command::new("pgrep")
        .args(["-af", showcase.binary])
        .output()
        .map_err(DevError::Io)?;
    if out.stdout.is_empty() {
        eprintln!("  (no processes)");
    } else {
        eprint!("{}", String::from_utf8_lossy(&out.stdout));
    }
    Ok(())
}

#[cfg(test)]
mod b42_tests {
    use super::*;

    #[test]
    fn b42_use_staged_elastic_join_scenarios_table() {
        struct Row {
            nodes: u32,
            want: bool,
        }
        let rows = [
            Row {
                nodes: 1,
                want: false,
            },
            Row {
                nodes: 3,
                want: false,
            },
            Row {
                nodes: 4,
                want: true,
            },
            Row {
                nodes: 8,
                want: true,
            },
        ];
        for row in rows {
            assert_eq!(
                use_staged_elastic_join(row.nodes),
                row.want,
                "nodes={}",
                row.nodes
            );
        }
    }

    #[test]
    fn b42_ready_port_for_node_scenarios_table() {
        struct Row {
            base: u16,
            node: u32,
            want: u16,
        }
        let rows = [
            Row {
                base: 8290,
                node: 1,
                want: 8290,
            },
            Row {
                base: 8290,
                node: 4,
                want: 8293,
            },
            Row {
                base: 8090,
                node: 4,
                want: 8093,
            },
        ];
        for row in rows {
            assert_eq!(
                ready_port_for_node(row.base, row.node),
                row.want,
                "base={} node={}",
                row.base,
                row.node
            );
        }
    }

    #[test]
    fn b42_cluster_spawn_waves_scenarios_table() {
        struct Row {
            nodes: u32,
            staged: bool,
            want: &'static [&'static [u32]],
        }
        let rows = [
            Row {
                nodes: 2,
                staged: false,
                want: &[&[1, 2]],
            },
            Row {
                nodes: 3,
                staged: false,
                want: &[&[1, 2, 3]],
            },
            Row {
                nodes: 3,
                staged: true,
                want: &[&[1, 2, 3]],
            },
            Row {
                nodes: 4,
                staged: true,
                want: &[&[1, 2, 3], &[4]],
            },
            Row {
                nodes: 5,
                staged: true,
                want: &[&[1, 2, 3], &[4, 5]],
            },
            Row {
                nodes: 4,
                staged: false,
                want: &[&[1, 2, 3, 4]],
            },
        ];
        for row in rows {
            let got = cluster_spawn_waves(row.nodes, row.staged);
            assert_eq!(
                got.len(),
                row.want.len(),
                "nodes={} staged={}",
                row.nodes,
                row.staged
            );
            for (i, wave) in row.want.iter().enumerate() {
                assert_eq!(
                    got[i], *wave,
                    "nodes={} staged={} wave={i}",
                    row.nodes, row.staged
                );
            }
        }
    }
}
