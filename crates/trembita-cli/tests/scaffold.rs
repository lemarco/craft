//! Integration tests for project scaffolding.

use std::path::Path;

use tempfile::tempdir;
use trembita_cli::{AppFeature, NewProjectOpts, scaffold_project};

#[test]
fn scaffolds_default_layout() {
    let dir = tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "demo-app".into(),
        output: dir.path().to_path_buf(),
        features: AppFeature::defaults(),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
    };
    let root = scaffold_project(&opts).unwrap();
    assert!(root.join("src/main.rs").is_file());
    assert!(root.join("src/app.rs").is_file());
    assert!(root.join("src/config.rs").is_file());
    assert!(root.join("src/consumers/sample.rs").is_file());
    assert!(root.join("src/domain/mod.rs").is_file());
    assert!(root.join("deploy/.env.example").is_file());
    let cargo = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    assert!(cargo.contains("name = \"demo-app\""));
    assert!(cargo.contains("default = [\"gateway\", \"jobs\", \"telemetry\"]"));
}

#[test]
fn rejects_invalid_name() {
    let dir = tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "Bad_Name".into(),
        output: dir.path().to_path_buf(),
        features: AppFeature::defaults(),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
    };
    assert!(scaffold_project(&opts).is_err());
}

#[test]
fn optional_modules_for_extra_features() {
    let dir = tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "full-app".into(),
        output: dir.path().to_path_buf(),
        features: vec![
            AppFeature::Jobs,
            AppFeature::Gateway,
            AppFeature::Actors,
            AppFeature::Workflows,
        ],
        trembita_version: "0.3.2".into(),
        trembita_path: Some(Path::new("/tmp/trembita").to_path_buf()),
    };
    let root = scaffold_project(&opts).unwrap();
    assert!(root.join("src/actors/mod.rs").is_file());
    assert!(root.join("src/workflows/mod.rs").is_file());
    assert!(root.join("src/http/mod.rs").is_file());
    let cargo = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    assert!(cargo.contains("path = \"/tmp/trembita/crates/trembita\""));
}

#[test]
fn postgres_adapters_forward_through_trembita_features() {
    let dir = tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "backlog-app".into(),
        output: dir.path().to_path_buf(),
        features: vec![
            AppFeature::Jobs,
            AppFeature::Gateway,
            AppFeature::ExternalBacklog,
            AppFeature::DomainOutbox,
        ],
        trembita_version: "0.3.2".into(),
        trembita_path: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let cargo = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    assert!(cargo.contains(
        "features = [\"dev-certs\", \"http-jobs\", \"external-backlog\", \"domain-outbox\"]"
    ));
    assert!(cargo.contains("external-backlog = [\"trembita/external-backlog\"]"));
    assert!(cargo.contains("domain-outbox = [\"trembita/domain-outbox\"]"));
    assert!(!cargo.contains("trembita-backlog-postgres"));
    assert!(!cargo.contains("trembita-events-postgres"));
}
