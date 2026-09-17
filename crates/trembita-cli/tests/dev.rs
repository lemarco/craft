//! Tests for `trembita dev` showcase registry and workspace discovery (debug CLI only).

#![cfg(debug_assertions)]

use trembita_cli::{
    DEFAULT_CLUSTER_UP_SHOWCASE, DevError, LOCAL_CLUSTER_ELASTIC_NODES,
    LOCAL_CLUSTER_GATEWAY_SESSION_SECRET, LOCAL_CLUSTER_LB_PORT, dev_cluster_up, dev_trigger,
    find_showcase, showcase_ids, workspace_root,
};

#[test]
fn b39_local_cluster_gateway_secret_meets_env_min_length() {
    assert!(LOCAL_CLUSTER_GATEWAY_SESSION_SECRET.len() >= 16);
}

#[test]
fn b39_cluster_up_defaults_to_realtime() {
    assert_eq!(DEFAULT_CLUSTER_UP_SHOWCASE, "realtime");
    let s = find_showcase(DEFAULT_CLUSTER_UP_SHOWCASE).expect("realtime");
    assert_eq!(s.base_port, 8290);
}

#[test]
fn b39_cluster_up_rejects_invalid_node_count() {
    let err = dev_cluster_up(Some("realtime"), 0, false, false).unwrap_err();
    assert!(matches!(err, DevError::InvalidNodes(0)));
    let err = dev_cluster_up(Some("realtime"), 9, false, false).unwrap_err();
    assert!(matches!(err, DevError::InvalidNodes(9)));
}

#[test]
fn b39_local_3node_packaging_files_exist_in_repo() {
    let root = workspace_root().expect("repo");
    assert!(root.join("dev/local-3node/docker-compose.yml").is_file());
    assert!(root.join("dev/local-3node/nginx.conf.template").is_file());
    assert!(root.join("scripts/local-cluster.sh").is_file());
    let compose = std::fs::read_to_string(root.join("dev/local-3node/docker-compose.yml")).unwrap();
    assert!(compose.contains("LOCAL_CLUSTER_LB_PORT"));
    assert!(compose.contains("nginx.generated.conf"));
    let template =
        std::fs::read_to_string(root.join("dev/local-3node/nginx.conf.template")).unwrap();
    assert!(template.contains("${LOCAL_CLUSTER_NODE1_PORT}"));
    assert!(template.contains("${LOCAL_CLUSTER_NODE4_SERVER}"));
    assert_eq!(LOCAL_CLUSTER_LB_PORT, 18_290);
}

#[test]
fn b42_elastic_default_four_nodes_constant() {
    assert_eq!(LOCAL_CLUSTER_ELASTIC_NODES, 4);
}

/// B-42 — script phases documented in header and wired in `case` dispatch.
#[test]
fn b42_local_cluster_script_elastic_phases_exist() {
    let root = workspace_root().expect("repo");
    let script = std::fs::read_to_string(root.join("scripts/local-cluster.sh")).unwrap();
    for needle in [
        "elastic-up",
        "elastic-smoke",
        "cap-smoke",
        "lb_min_distinct",
        "/e2e/whoami",
        "--nodes 4",
    ] {
        assert!(
            script.contains(needle),
            "local-cluster.sh missing B-42 phase: {needle}"
        );
    }
}

#[test]
fn b42_elastic_lb_e2e_script_uses_same_whoami_path_as_local_cap_smoke() {
    let root = workspace_root().expect("repo");
    let e2e = std::fs::read_to_string(root.join("e2e/elastic_lb.sh")).unwrap();
    let local = std::fs::read_to_string(root.join("scripts/local-cluster.sh")).unwrap();
    assert!(e2e.contains("/e2e/whoami"));
    assert!(local.contains("/e2e/whoami"));
}

/// B-42 — `--nodes 4` is valid before release binary / setup checks.
#[test]
fn b42_cluster_up_four_nodes_rejects_invalid_nodes_not_first() {
    let err = dev_cluster_up(Some("realtime"), 4, false, false).unwrap_err();
    assert!(
        !matches!(err, DevError::InvalidNodes(_)),
        "expected setup/binary error, got {err:?}"
    );
}

/// B-42 — LB smoke thresholds aligned with [`e2e/elastic_lb.sh`](../../e2e/elastic_lb.sh).
#[test]
fn b42_lb_smoke_distinct_thresholds_match_e2e_elastic_script() {
    let root = workspace_root().expect("repo");
    let e2e = std::fs::read_to_string(root.join("e2e/elastic_lb.sh")).unwrap();
    let local = std::fs::read_to_string(root.join("scripts/local-cluster.sh")).unwrap();
    assert!(
        e2e.contains("distinct") && e2e.contains("-lt 3"),
        "e2e LB expects ≥3 distinct node_id"
    );
    assert!(local.contains("lb_min_distinct"));
    assert!(local.contains('3') && local.contains("cap_distinct") && local.contains("-lt 2"));
}

#[test]
fn b42_realtime_example_wires_e2e_pool_manifest() {
    let root = workspace_root().expect("repo");
    let main_rs = std::fs::read_to_string(root.join("examples/realtime/src/main.rs")).unwrap();
    assert!(main_rs.contains("capabilities_manifest()"));
    assert!(main_rs.contains("mount_whoami"));
}

#[test]
fn b42_cluster_showcases_include_node_four_cert_id() {
    for id in [
        "realtime",
        "background-jobs",
        "stateful-workers",
        "workflows",
    ] {
        let s = find_showcase(id).unwrap_or_else(|| panic!("{id}"));
        assert!(
            s.cert_ids.contains(&4),
            "{id}: cert id 4 required for elastic join"
        );
    }
}

#[test]
fn b42_script_elastic_up_forces_four_nodes() {
    let root = workspace_root().expect("repo");
    let script = std::fs::read_to_string(root.join("scripts/local-cluster.sh")).unwrap();
    assert!(script.contains("elastic_up()"));
    assert!(script.contains("NODES=4 up"));
}

#[test]
fn b42_e2e_and_local_cap_smoke_min_distinct_documented() {
    let root = workspace_root().expect("repo");
    let e2e = std::fs::read_to_string(root.join("e2e/elastic_lb.sh")).unwrap();
    let local = std::fs::read_to_string(root.join("scripts/local-cluster.sh")).unwrap();
    assert!(e2e.contains("cap_distinct") && e2e.contains("-lt 2"));
    assert!(local.contains("cap_distinct") && local.contains("-lt 2"));
}

#[test]
fn b42_cli_main_cluster_up_mentions_elastic_nodes_four() {
    let root = workspace_root().expect("repo");
    let main_rs = std::fs::read_to_string(root.join("crates/trembita-cli/src/main.rs")).unwrap();
    assert!(main_rs.contains("--nodes 4"));
    assert!(main_rs.contains("B-42"));
}

#[test]
fn showcase_registry_includes_stateful_workers() {
    let s = find_showcase("stateful-workers").expect("showcase");
    assert_eq!(s.base_port, 8190);
    assert_eq!(s.listen_addr(2), "127.0.0.1:8191");
}

#[test]
fn showcase_ids_non_empty() {
    assert!(showcase_ids().contains("background-jobs"));
}

#[test]
fn workspace_root_from_env_or_cwd() {
    let root = workspace_root().expect("trembita repo");
    assert!(root.join("examples/stateful-workers").is_dir());
}

/// B-50 — CI heavy lane wraps B-42 local-cluster elastic smoke.
#[test]
fn b50_ci_local_elastic_smoke_script_wires_local_cluster_phases() {
    let root = workspace_root().expect("repo");
    let script = std::fs::read_to_string(root.join("scripts/ci-local-elastic-smoke.sh")).unwrap();
    for needle in [
        "B-50",
        "local-cluster.sh",
        "elastic-up",
        "lb-up",
        "elastic-smoke",
        "docker info",
    ] {
        assert!(
            script.contains(needle),
            "ci-local-elastic-smoke.sh missing: {needle}"
        );
    }
}

#[test]
fn b50_gitlab_ci_declares_local_elastic_smoke_job() {
    let root = workspace_root().expect("repo");
    let ci = std::fs::read_to_string(root.join(".gitlab-ci.yml")).unwrap();
    assert!(ci.contains("local-elastic-smoke:"));
    assert!(ci.contains("scripts/ci-local-elastic-smoke.sh"));
}

#[test]
fn trigger_requires_script_or_fails_gracefully() {
    let root = workspace_root().expect("repo");
    let s = find_showcase("stateful-workers").unwrap();
    let script = s.example_dir(&root).join("trigger.sh");
    assert!(script.is_file());
    if std::env::var("TREMBITA_DEV_INTEGRATION").is_ok() {
        dev_trigger("stateful-workers", &["9999".to_string()]).expect("trigger");
    }
}
