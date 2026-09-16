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
        template: None,
    };
    let root = scaffold_project(&opts).unwrap();
    assert!(root.join("src/main.rs").is_file());
    assert!(root.join("src/app.rs").is_file());
    assert!(root.join("src/manifest.rs").is_file());
    let app = std::fs::read_to_string(root.join("src/app.rs")).unwrap();
    assert!(app.contains("TrembitaApp::from_config"));
    assert!(app.contains(".run()"));
    assert!(!app.contains("RunOpts::for_manifest"));
    assert!(app.contains("let manifest = manifest::build()"));
    assert!(app.contains(".manifest(manifest)"));
    assert!(
        app.contains(".without_actors_api()"),
        "greenfield scaffold disables /actors/* unless WorkerOpts::http_cast"
    );
    let product = std::fs::read_to_string(root.join("src/http/product.rs")).unwrap();
    assert!(product.contains("cap_invoke::<Ping>"));
    assert!(root.join("src/config.rs").is_file());
    assert!(root.join("src/consumers/sample.rs").is_file());
    assert!(root.join("src/domain/mod.rs").is_file());
    assert!(root.join("src/capabilities/ping.rs").is_file());
    let manifest = std::fs::read_to_string(root.join("src/manifest.rs")).unwrap();
    assert!(manifest.contains("CapManifest"));
    assert!(root.join("deploy/.env.example").is_file());
    let readme = std::fs::read_to_string(root.join("README.md")).unwrap();
    assert!(
        !readme.contains("../../docs/"),
        "scaffold README must not use monorepo-relative doc paths"
    );
    assert!(readme.contains("gitlab.com/lemarco/trembita"));
    assert!(readme.starts_with("# demo app\n"));
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
        template: None,
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
        template: None,
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
        template: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let app = std::fs::read_to_string(root.join("src/app.rs")).unwrap();
    assert!(app.contains("TrembitaApp::from_config"));
    assert!(app.contains(".run()"));
    assert!(!app.contains("RunOpts::for_manifest"));
    let cargo = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    assert!(cargo.contains(
        "features = [\"dev-certs\", \"http-jobs\", \"external-backlog\", \"domain-outbox\"]"
    ));
    assert!(cargo.contains("external-backlog = [\"trembita/external-backlog\"]"));
    assert!(cargo.contains("domain-outbox = [\"trembita/domain-outbox\"]"));
    assert!(!cargo.contains("trembita-backlog-postgres"));
    assert!(!cargo.contains("trembita-events-postgres"));
}

#[test]
fn template_realtime_includes_ws_and_chat_capability() {
    use trembita_cli::{AppTemplate, resolve_scaffold_features};

    let dir = tempdir().unwrap();
    let features = resolve_scaffold_features(Some(AppTemplate::Realtime), "").unwrap();
    let opts = NewProjectOpts {
        name: "live-app".into(),
        output: dir.path().to_path_buf(),
        features,
        template: Some(AppTemplate::Realtime),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
    };
    let root = scaffold_project(&opts).unwrap();
    assert!(root.join("src/capabilities/chat.rs").is_file());
    assert!(!root.join("src/actors/chat.rs").exists());
    let manifest = std::fs::read_to_string(root.join("src/manifest.rs")).unwrap();
    assert!(manifest.contains("chat::manifest()"));
    let app = std::fs::read_to_string(root.join("src/app.rs")).unwrap();
    assert!(app.contains("mount_sticky_websocket"));
    assert!(app.contains("fire_cap"));
    assert!(app.contains("Append"));
    assert!(!root.join("src/consumers/sample.rs").exists());
}

#[test]
fn template_workflows_includes_onboarding_cap_and_named_workflow() {
    use trembita_cli::{AppTemplate, resolve_scaffold_features};

    let dir = tempdir().unwrap();
    let features = resolve_scaffold_features(Some(AppTemplate::Workflows), "").unwrap();
    let opts = NewProjectOpts {
        name: "flow-app".into(),
        output: dir.path().to_path_buf(),
        features,
        template: Some(AppTemplate::Workflows),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
    };
    let root = scaffold_project(&opts).unwrap();
    assert!(root.join("src/capabilities/onboarding.rs").is_file());
    assert!(root.join("src/workflows/onboarding.rs").is_file());
    let manifest = std::fs::read_to_string(root.join("src/manifest.rs")).unwrap();
    assert!(manifest.contains("onboarding::manifest()"));
    assert!(manifest.contains("WorkflowOpts::named(\"onboard\""));
    assert!(!root.join("src/capabilities/ping.rs").exists());
}
