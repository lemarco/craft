//! B-45 — repo `deploy/` production pack smoke (templates present + key contracts).

use std::path::PathBuf;

fn repo_deploy_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy")
        .canonicalize()
        .expect("repo deploy/ directory")
}

fn read_deploy(rel: &str) -> String {
    let path = repo_deploy_dir().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read deploy/{rel}: {e}"))
}

#[test]
fn b45_deploy_pack_artifacts_exist() {
    let root = repo_deploy_dir();
    for rel in [
        "README.md",
        "systemd/trembita.service",
        "env/seed.env.example",
        "env/joiner.env.example",
        "env/matrix.md",
        "nginx/upstream.conf.example",
        "rolling-upgrade-systemd.md",
        "backup-restore.md",
    ] {
        assert!(root.join(rel).is_file(), "missing deploy/{rel}");
    }
}

#[test]
fn b45_systemd_unit_mentions_drain_and_restart() {
    let unit = read_deploy("systemd/trembita.service");
    assert!(
        unit.contains("Restart=always"),
        "systemd must restart after upgrade exit"
    );
    assert!(
        unit.contains("TimeoutStopSec="),
        "systemd stop timeout must cover drain"
    );
    assert!(
        unit.contains("EnvironmentFile="),
        "env file indirection for seed/joiner"
    );
}

#[test]
fn b45_seed_env_omits_join_seeds_joiner_requires_them() {
    let seed = read_deploy("env/seed.env.example");
    let joiner = read_deploy("env/joiner.env.example");
    assert!(
        !seed.contains("TREMBITA_JOIN_SEEDS=1@"),
        "seed example must not set join seeds"
    );
    assert!(seed.contains("TREMBITA_ALLOW_JOIN=1"));
    assert!(joiner.contains("TREMBITA_JOIN_SEEDS="));
    assert!(
        !joiner.contains("TREMBITA_ALLOW_JOIN=1"),
        "joiner example should not enable seed-only allow_join"
    );
}

#[test]
fn b45_matrix_documents_product_footguns() {
    let matrix = read_deploy("env/matrix.md");
    assert!(matrix.contains("TREMBITA_NODE_ID"));
    assert!(matrix.contains("Do not set"));
    assert!(matrix.contains("node-0.pem"));
}

#[test]
fn b48_backup_script_and_runbook_linked_from_deploy() {
    let root = repo_deploy_dir()
        .join("..")
        .canonicalize()
        .expect("repo root");
    let script = root.join("scripts/backup-data-dir.sh");
    assert!(script.is_file(), "missing scripts/backup-data-dir.sh");
    let readme = read_deploy("README.md");
    assert!(
        readme.contains("backup-restore.md"),
        "deploy/README must link backup-restore.md"
    );
    let runbook = std::fs::read_to_string(root.join("docs/ops/backup-restore.md"))
        .expect("docs/ops/backup-restore.md");
    assert!(
        runbook.contains("node-id"),
        "runbook must document node-id in data_dir"
    );
    assert!(
        runbook.contains("does **not** do automatically"),
        "runbook must list non-automatic scope"
    );
}

#[test]
fn b48_deploy_backup_cheat_sheet_mentions_stop() {
    let cheat = read_deploy("backup-restore.md");
    assert!(cheat.contains("systemctl stop"));
    assert!(cheat.contains("backup-data-dir.sh"));
}

fn repo_docs_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs")
        .canonicalize()
        .expect("repo docs/ directory")
}

#[test]
fn b53_backlog_wave_b33_b41_archive_exists() {
    let path = repo_docs_dir().join("archive/backlog-wave-b33-b41.md");
    let text = std::fs::read_to_string(&path).expect("B-33…B-41 archive");
    for id in [
        "**B-33**", "**B-34**", "**B-35**", "**B-36**", "**B-37**", "**B-38**", "**B-39**",
        "**B-40**", "**B-41**",
    ] {
        assert!(text.contains(id), "archive missing epic row {id}");
    }
    assert!(
        !text.contains("**B-42**"),
        "B-33…B-41 archive must not include B-42"
    );
}

#[test]
fn b53_testing_coverage_links_b33_b41_archive() {
    let backlog = std::fs::read_to_string(repo_docs_dir().join("backlog.md")).unwrap();
    let coverage = std::fs::read_to_string(repo_docs_dir().join("testing-coverage.md")).unwrap();
    assert!(backlog.contains("archive/backlog-wave-b33-b41.md"));
    assert!(coverage.contains("## Shipped backlog B-33–B-41"));
    assert!(coverage.contains("archive/backlog-wave-b33-b41.md"));
    let status = std::fs::read_to_string(repo_docs_dir().join("status.md")).unwrap();
    assert!(
        status.contains("testing-coverage.md#shipped-backlog-b-33b41"),
        "status must link to B-33…B-41 coverage section"
    );
}
