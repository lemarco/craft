//! Tests for `trembita dev` showcase registry and workspace discovery.

use trembita_cli::{dev_trigger, find_showcase, showcase_ids, workspace_root};

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
