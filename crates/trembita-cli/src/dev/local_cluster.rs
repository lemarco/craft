//! Local 3-node showcase cluster (B-39): shared gateway session secret + optional LB smoke.

use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};

use super::DevError;
use super::cluster;
use super::showcases::Showcase;

/// Default showcase for [`cluster_up`](super::dev_cluster_up) (`realtime` — login + `/me`).
pub const DEFAULT_CLUSTER_UP_SHOWCASE: &str = "realtime";

/// Same on every node when exercising cluster session cookies locally ([`env.md`](../../../../docs/env.md)).
pub const LOCAL_CLUSTER_GATEWAY_SESSION_SECRET: &str = "trembita-local-3node-dev-secret";

/// Matches [`examples/realtime/trigger-http.sh`](../../../../examples/realtime/trigger-http.sh).
pub const LOCAL_CLUSTER_GATEWAY_TOKEN: &str = "dev-secret";

/// TCP port for optional nginx round-robin in [`dev/local-3node`](../../../../dev/local-3node/README.md).
pub const LOCAL_CLUSTER_LB_PORT: u16 = 18_290;

/// Start 3-node cluster with shared gateway env; optional local nginx LB.
pub fn cluster_up(
    showcase: &Showcase,
    workspace: &Path,
    nodes: u32,
    run_setup: bool,
    with_lb: bool,
) -> Result<(), DevError> {
    if run_setup {
        cluster::setup(showcase, workspace)?;
    }
    cluster::up_with_shared_gateway_env(showcase, workspace, nodes)?;
    print_smoke_hints(showcase);
    if with_lb {
        lb_up(workspace, showcase)?;
        print_lb_hints(showcase);
    }
    Ok(())
}

/// `docker compose` nginx LB (optional; requires Docker).
pub fn lb_up(workspace: &Path, showcase: &Showcase) -> Result<(), DevError> {
    let dir = workspace.join("dev/local-3node");
    let compose = dir.join("docker-compose.yml");
    if !compose.is_file() {
        return Err(DevError::MissingScript(compose));
    }
    write_lb_nginx(&dir, showcase)?;
    let base = u32::from(showcase.base_port);
    let status = Command::new("docker")
        .args(["compose", "-f"])
        .arg(&compose)
        .env("LOCAL_CLUSTER_LB_PORT", LOCAL_CLUSTER_LB_PORT.to_string())
        .env("LOCAL_CLUSTER_NODE1_PORT", base.to_string())
        .env("LOCAL_CLUSTER_NODE2_PORT", (base + 1).to_string())
        .env("LOCAL_CLUSTER_NODE3_PORT", (base + 2).to_string())
        .args(["up", "-d"])
        .current_dir(workspace)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(DevError::Io)?;
    if status.success() {
        Ok(())
    } else {
        Err(DevError::CommandFailed(
            "docker compose up (local cluster LB)".into(),
        ))
    }
}

/// Stop nginx LB container.
pub fn lb_down(workspace: &Path) -> Result<(), DevError> {
    let compose = workspace.join("dev/local-3node/docker-compose.yml");
    if !compose.is_file() {
        return Err(DevError::MissingScript(compose));
    }
    let _ = Command::new("docker")
        .args(["compose", "-f"])
        .arg(&compose)
        .args(["down"])
        .current_dir(workspace)
        .status()
        .map_err(DevError::Io)?;
    Ok(())
}

fn print_smoke_hints(showcase: &Showcase) {
    let p1 = showcase.base_port;
    let p2 = p1 + 1;
    eprintln!();
    eprintln!("Local 3-node cluster smoke (shared TREMBITA_GATEWAY_SESSION_SECRET):");
    eprintln!("  curl -sf http://127.0.0.1:{p1}/ready | jq .");
    if showcase.id == "realtime" {
        eprintln!("  ./scripts/local-cluster.sh session-smoke");
        eprintln!("  # or: login :{p1} → GET /me on :{p2} with cookie");
    } else {
        eprintln!(
            "  ./scripts/local-cluster.sh lb-smoke   # /ready spread (set --showcase {id} if needed)",
            id = showcase.id
        );
    }
    eprintln!(
        "  ./scripts/local-cluster.sh lb-up      # optional nginx on :{}",
        LOCAL_CLUSTER_LB_PORT
    );
}

fn write_lb_nginx(dir: &Path, showcase: &Showcase) -> Result<(), DevError> {
    let template_path = dir.join("nginx.conf.template");
    let out = dir.join("nginx.generated.conf");
    let template = fs::read_to_string(&template_path).map_err(DevError::Io)?;
    let rendered = render_lb_nginx_ports(&template, showcase.base_port);
    fs::write(&out, rendered).map_err(DevError::Io)?;
    Ok(())
}

/// Substitute backend ports in [`dev/local-3node/nginx.conf.template`](../../../../dev/local-3node/nginx.conf.template).
#[must_use]
pub(crate) fn render_lb_nginx_ports(template: &str, base_port: u16) -> String {
    let base = u32::from(base_port);
    template
        .replace("${LOCAL_CLUSTER_NODE1_PORT}", &base.to_string())
        .replace("${LOCAL_CLUSTER_NODE2_PORT}", &(base + 1).to_string())
        .replace("${LOCAL_CLUSTER_NODE3_PORT}", &(base + 2).to_string())
}

fn print_lb_hints(showcase: &Showcase) {
    print_lb_hints_public(showcase);
}

/// Hints after LB is up (CLI + script).
pub fn print_lb_hints_public(showcase: &Showcase) {
    let lb = LOCAL_CLUSTER_LB_PORT;
    eprintln!("LB listening on http://127.0.0.1:{lb}/");
    eprintln!("  curl -sf http://127.0.0.1:{lb}/ready   # round-robin backends");
    if showcase.id == "realtime" {
        eprintln!("  ./scripts/local-cluster.sh session-smoke --lb");
    }
}

#[cfg(test)]
mod b39_tests {
    use super::*;
    use crate::dev::showcases::find;

    #[test]
    fn b39_local_cluster_secret_and_token_match_script_defaults() {
        assert_eq!(
            LOCAL_CLUSTER_GATEWAY_SESSION_SECRET,
            "trembita-local-3node-dev-secret"
        );
        assert_eq!(LOCAL_CLUSTER_GATEWAY_TOKEN, "dev-secret");
        assert_eq!(LOCAL_CLUSTER_LB_PORT, 18_290);
    }

    #[test]
    fn b39_lb_nginx_render_scenarios_table() {
        struct Row {
            base: u16,
            want: [&'static str; 3],
        }
        let template = "p1=${LOCAL_CLUSTER_NODE1_PORT} p2=${LOCAL_CLUSTER_NODE2_PORT} p3=${LOCAL_CLUSTER_NODE3_PORT}";
        let rows = [
            Row {
                base: 8290,
                want: ["8290", "8291", "8292"],
            },
            Row {
                base: 8090,
                want: ["8090", "8091", "8092"],
            },
            Row {
                base: 8190,
                want: ["8190", "8191", "8192"],
            },
        ];
        for row in rows {
            let got = render_lb_nginx_ports(template, row.base);
            for (i, port) in row.want.iter().enumerate() {
                assert!(got.contains(port), "base={} slot={i}", row.base);
            }
            assert!(
                !got.contains("${LOCAL_CLUSTER_"),
                "base={}: unexpanded placeholders",
                row.base
            );
        }
    }

    #[test]
    fn b39_realtime_showcase_three_node_addrs_and_join_seed() {
        let s = find("realtime").expect("realtime");
        assert_eq!(s.listen_addr(1), "127.0.0.1:8290");
        assert_eq!(s.listen_addr(2), "127.0.0.1:8291");
        assert_eq!(s.listen_addr(3), "127.0.0.1:8292");
        assert_eq!(s.join_seed(), "1@127.0.0.1:8290");
    }

    #[test]
    fn b39_repo_nginx_template_renders_realtime_backends() {
        use crate::dev::workspace::workspace_root;

        let root = workspace_root().expect("repo");
        let template = fs::read_to_string(root.join("dev/local-3node/nginx.conf.template"))
            .expect("nginx template");
        let rendered = render_lb_nginx_ports(&template, 8290);
        assert!(rendered.contains("host.docker.internal:8290"));
        assert!(rendered.contains("host.docker.internal:8291"));
        assert!(rendered.contains("host.docker.internal:8292"));
    }
}
