//! Local showcase cluster (B-39): shared gateway session secret + optional LB smoke (B-42 elastic join).

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

/// Default node count for elastic local proof (seed + 3 joiners, then 4th joiner — mirrors B-34).
pub const LOCAL_CLUSTER_ELASTIC_NODES: u32 = 4;

/// Start cluster with shared gateway env; optional local nginx LB.
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
    print_smoke_hints(showcase, nodes);
    if with_lb {
        let backends = detect_lb_backends(showcase, nodes);
        lb_up(workspace, showcase, backends)?;
        print_lb_hints(showcase);
    }
    Ok(())
}

/// `docker compose` nginx LB (optional; requires Docker).
pub fn lb_up(workspace: &Path, showcase: &Showcase, backends: u32) -> Result<(), DevError> {
    let dir = workspace.join("dev/local-3node");
    let compose = dir.join("docker-compose.yml");
    if !compose.is_file() {
        return Err(DevError::MissingScript(compose));
    }
    let backends = clamp_lb_backends(backends);
    write_lb_nginx(&dir, showcase, backends)?;
    let base = u32::from(showcase.base_port);
    let status = Command::new("docker")
        .args(["compose", "-f"])
        .arg(&compose)
        .env("LOCAL_CLUSTER_LB_PORT", LOCAL_CLUSTER_LB_PORT.to_string())
        .env("LOCAL_CLUSTER_NODE1_PORT", base.to_string())
        .env("LOCAL_CLUSTER_NODE2_PORT", (base + 1).to_string())
        .env("LOCAL_CLUSTER_NODE3_PORT", (base + 2).to_string())
        .env("LOCAL_CLUSTER_NODE4_PORT", (base + 3).to_string())
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

/// Probe `/ready` on node ports (up to `max_nodes`) for LB upstream count.
#[must_use]
pub fn detect_lb_backends(showcase: &Showcase, max_nodes: u32) -> u32 {
    let cap = clamp_lb_backends(max_nodes);
    for n in (3..=cap).rev() {
        let port = u32::from(showcase.base_port) + n - 1;
        if let Ok(port) = u16::try_from(port)
            && curl_ready(port)
        {
            return n;
        }
    }
    3
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

fn print_smoke_hints(showcase: &Showcase, nodes: u32) {
    let p1 = showcase.base_port;
    let p2 = p1 + 1;
    eprintln!();
    if nodes >= LOCAL_CLUSTER_ELASTIC_NODES {
        eprintln!("Local elastic cluster smoke (B-42 — mirrors e2e/elastic_lb.sh on localhost):");
        eprintln!("  ./scripts/local-cluster.sh elastic-smoke");
        eprintln!("  ./scripts/local-cluster.sh cap-smoke      # GET /e2e/whoami via LB");
    } else {
        eprintln!("Local 3-node cluster smoke (shared TREMBITA_GATEWAY_SESSION_SECRET):");
    }
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

fn write_lb_nginx(dir: &Path, showcase: &Showcase, backends: u32) -> Result<(), DevError> {
    let template_path = dir.join("nginx.conf.template");
    let out = dir.join("nginx.generated.conf");
    let template = fs::read_to_string(&template_path).map_err(DevError::Io)?;
    let rendered = render_lb_nginx_ports(&template, showcase.base_port, backends);
    fs::write(&out, rendered).map_err(DevError::Io)?;
    Ok(())
}

/// Nginx upstream count for local LB (always 3 or 4 backends).
#[must_use]
pub(crate) fn clamp_lb_backends(backends: u32) -> u32 {
    backends.clamp(3, 4)
}

/// Substitute backend ports in [`dev/local-3node/nginx.conf.template`](../../../../dev/local-3node/nginx.conf.template).
#[must_use]
pub(crate) fn render_lb_nginx_ports(template: &str, base_port: u16, backends: u32) -> String {
    let backends = clamp_lb_backends(backends);
    let base = u32::from(base_port);
    let node4 = if backends >= 4 {
        format!("    server host.docker.internal:{};\n", base + 3)
    } else {
        String::new()
    };
    template
        .replace("${LOCAL_CLUSTER_NODE1_PORT}", &base.to_string())
        .replace("${LOCAL_CLUSTER_NODE2_PORT}", &(base + 1).to_string())
        .replace("${LOCAL_CLUSTER_NODE3_PORT}", &(base + 2).to_string())
        .replace("${LOCAL_CLUSTER_NODE4_SERVER}", &node4)
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
        eprintln!("  ./scripts/local-cluster.sh cap-smoke");
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
            backends: u32,
            want: [&'static str; 3],
            want_fourth: bool,
        }
        let template = "p1=${LOCAL_CLUSTER_NODE1_PORT} p2=${LOCAL_CLUSTER_NODE2_PORT} p3=${LOCAL_CLUSTER_NODE3_PORT}\n${LOCAL_CLUSTER_NODE4_SERVER}";
        let rows = [
            Row {
                base: 8290,
                backends: 3,
                want: ["8290", "8291", "8292"],
                want_fourth: false,
            },
            Row {
                base: 8290,
                backends: 4,
                want: ["8290", "8291", "8292"],
                want_fourth: true,
            },
            Row {
                base: 8090,
                backends: 3,
                want: ["8090", "8091", "8092"],
                want_fourth: false,
            },
            Row {
                base: 8190,
                backends: 4,
                want: ["8190", "8191", "8192"],
                want_fourth: true,
            },
        ];
        for row in rows {
            let got = render_lb_nginx_ports(template, row.base, row.backends);
            for (i, port) in row.want.iter().enumerate() {
                assert!(got.contains(port), "base={} slot={i}", row.base);
            }
            assert!(
                !got.contains("${LOCAL_CLUSTER_"),
                "base={}: unexpanded placeholders",
                row.base
            );
            let has_fourth = got.contains(&format!("host.docker.internal:{}", row.base + 3));
            assert_eq!(
                has_fourth, row.want_fourth,
                "base={} backends={}",
                row.base, row.backends
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
        let rendered = render_lb_nginx_ports(&template, 8290, 3);
        assert!(rendered.contains("host.docker.internal:8290"));
        assert!(rendered.contains("host.docker.internal:8291"));
        assert!(rendered.contains("host.docker.internal:8292"));
        assert!(!rendered.contains("host.docker.internal:8293"));
    }

    #[test]
    fn b42_repo_nginx_template_renders_fourth_backend_when_elastic() {
        use crate::dev::workspace::workspace_root;

        let root = workspace_root().expect("repo");
        let template = fs::read_to_string(root.join("dev/local-3node/nginx.conf.template"))
            .expect("nginx template");
        let rendered = render_lb_nginx_ports(&template, 8290, 4);
        assert!(rendered.contains("host.docker.internal:8293"));
    }
}

#[cfg(test)]
mod b42_tests {
    use super::*;
    use crate::dev::showcases::find;

    #[test]
    fn b42_elastic_node_count_default_is_four() {
        assert_eq!(LOCAL_CLUSTER_ELASTIC_NODES, 4);
    }

    #[test]
    fn b42_realtime_fourth_listen_addr_matches_elastic_ports() {
        let s = find("realtime").expect("realtime");
        assert_eq!(s.listen_addr(4), "127.0.0.1:8293");
    }

    /// B-42 — fourth node port aligns with [`scripts/local-cluster.sh`](../../../../scripts/local-cluster.sh) base ports.
    #[test]
    fn b42_showcase_fourth_listen_addr_scenarios_table() {
        struct Row {
            id: &'static str,
            port: u16,
        }
        let rows = [
            Row {
                id: "realtime",
                port: 8293,
            },
            Row {
                id: "background-jobs",
                port: 8093,
            },
            Row {
                id: "stateful-workers",
                port: 8193,
            },
            Row {
                id: "workflows",
                port: 8493,
            },
        ];
        for row in rows {
            let s = find(row.id).unwrap_or_else(|| panic!("showcase {}", row.id));
            assert_eq!(
                s.listen_addr(4),
                format!("127.0.0.1:{}", row.port),
                "{}",
                row.id
            );
        }
    }

    #[test]
    fn b42_clamp_lb_backends_scenarios_table() {
        struct Row {
            in_backends: u32,
            want: u32,
        }
        let rows = [
            Row {
                in_backends: 0,
                want: 3,
            },
            Row {
                in_backends: 2,
                want: 3,
            },
            Row {
                in_backends: 3,
                want: 3,
            },
            Row {
                in_backends: 4,
                want: 4,
            },
            Row {
                in_backends: 8,
                want: 4,
            },
        ];
        for row in rows {
            assert_eq!(
                clamp_lb_backends(row.in_backends),
                row.want,
                "backends={}",
                row.in_backends
            );
        }
    }

    #[test]
    fn b42_workflows_fourth_nginx_upstream_port() {
        let template = "${LOCAL_CLUSTER_NODE4_SERVER}";
        let got = render_lb_nginx_ports(template, 8490, 4);
        assert!(got.contains("host.docker.internal:8493"));
    }

    #[test]
    fn b42_ready_port_matches_showcase_listen_addr() {
        use crate::dev::cluster::ready_port_for_node;

        let s = find("realtime").expect("realtime");
        for node in 1..=4 {
            assert_eq!(
                ready_port_for_node(s.base_port, node),
                s.listen_addr(node)
                    .split(':')
                    .next_back()
                    .unwrap()
                    .parse::<u16>()
                    .expect("port")
            );
        }
    }
}
