//! Local multi-node showcase cluster (replaces `./cluster.sh up` for dev).

use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use super::DevError;
use super::showcases::Showcase;

/// Build release binary + optional showcase client.
pub fn setup(showcase: &Showcase, workspace: &Path) -> Result<(), DevError> {
    let cluster = showcase.cluster_root(workspace);
    let certs = cluster.join("certs");
    fs::create_dir_all(cluster.join("data"))?;
    fs::create_dir_all(cluster.join("logs"))?;

    if !certs.join("ca.pem").is_file() {
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

/// Start `nodes` cluster members in the background.
pub fn up(showcase: &Showcase, workspace: &Path, nodes: u32) -> Result<(), DevError> {
    if nodes == 0 || nodes > 8 {
        return Err(DevError::InvalidNodes(nodes));
    }
    let bin = showcase.release_bin(workspace);
    if !bin.is_file() {
        return Err(DevError::BinaryMissing(bin));
    }
    let cluster = showcase.cluster_root(workspace);
    let certs = cluster.join("certs");
    if !certs.join("ca.pem").is_file() {
        return Err(DevError::SetupRequired);
    }

    stop(showcase)?;
    fs::create_dir_all(cluster.join("logs"))?;

    for node in 1..=nodes {
        spawn_node(showcase, workspace, node, &bin)?;
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

fn spawn_node(
    showcase: &Showcase,
    workspace: &Path,
    node: u32,
    bin: &Path,
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
