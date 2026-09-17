//! Integration tests for project scaffolding.

use std::path::Path;

use tempfile::tempdir;
use trembita_cli::{AppFeature, NewProjectOpts, scaffold_project, workspace_root};

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
        !app.contains("from_config(cfg)?"),
        "from_config is infallible — do not use ? on the builder"
    );
    assert!(
        app.contains("TrembitaConfigure"),
        "scaffold configures data_dir and gateway via TrembitaConfigure"
    );
    assert!(
        app.contains(".without_actors_api()"),
        "greenfield scaffold must omit /actors/* explicitly"
    );
    let product = std::fs::read_to_string(root.join("src/http/product.rs")).unwrap();
    assert!(product.contains("cap_invoke::<Ping>"));
    assert!(root.join("src/config.rs").is_file());
    assert!(root.join("src/consumers/sample.rs").is_file());
    assert!(root.join("src/domain/mod.rs").is_file());
    assert!(root.join("src/capabilities/ping.rs").is_file());
    let manifest = std::fs::read_to_string(root.join("src/manifest.rs")).unwrap();
    assert!(manifest.contains("// trembita:topics"));
    assert!(manifest.contains("// trembita:workers"));
    assert!(manifest.contains("ping::manifest()"));
    let ping = std::fs::read_to_string(root.join("src/capabilities/ping.rs")).unwrap();
    assert!(ping.contains("cap_handler"));
    assert!(ping.contains("CapManifest"));
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
    let onboarding = std::fs::read_to_string(root.join("src/capabilities/onboarding.rs")).unwrap();
    assert!(onboarding.contains("cap_handler"));
    assert!(onboarding.contains("cap_register_chain!"));
    assert!(root.join("src/workflows/onboarding.rs").is_file());
    let manifest = std::fs::read_to_string(root.join("src/manifest.rs")).unwrap();
    assert!(manifest.contains("onboarding::manifest()"));
    assert!(manifest.contains("WorkflowOpts::named(\"onboard\""));
    assert!(!root.join("src/capabilities/ping.rs").exists());
}

#[test]
fn b38_template_jobs_includes_queued_idempotency_capability() {
    use trembita_cli::{AppTemplate, resolve_scaffold_features};

    let dir = tempdir().unwrap();
    let features = resolve_scaffold_features(Some(AppTemplate::Jobs), "").unwrap();
    let opts = NewProjectOpts {
        name: "jobs-app".into(),
        output: dir.path().to_path_buf(),
        features,
        template: Some(AppTemplate::Jobs),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let task = std::fs::read_to_string(root.join("src/capabilities/task.rs")).unwrap();
    assert!(task.contains("require_store"));
    assert!(task.contains("default_queue_for"));
    assert!(task.contains("store_get"));
    let product = std::fs::read_to_string(root.join("src/http/product.rs")).unwrap();
    assert!(product.contains("cap_enqueue::<RunTask>"));
    let manifest = std::fs::read_to_string(root.join("src/manifest.rs")).unwrap();
    assert!(manifest.contains("task::manifest()"));
}

#[test]
fn b38_profile_jobs_matches_template_jobs_layout() {
    use trembita_cli::{AppTemplate, resolve_scaffold_features};

    let dir = tempdir().unwrap();
    let features = resolve_scaffold_features(Some(AppTemplate::Jobs), "").unwrap();
    let opts = NewProjectOpts {
        name: "jobs-profile".into(),
        output: dir.path().to_path_buf(),
        features,
        template: Some(AppTemplate::Jobs),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
    };
    let root = scaffold_project(&opts).unwrap();
    assert!(root.join("src/capabilities/task.rs").is_file());
    assert!(root.join("src/consumers/sample.rs").is_file());
}

#[test]
fn b38_profile_realtime_includes_session_per_node_wiring() {
    use trembita_cli::{AppTemplate, resolve_scaffold_features};

    let dir = tempdir().unwrap();
    let features = resolve_scaffold_features(Some(AppTemplate::Realtime), "").unwrap();
    let opts = NewProjectOpts {
        name: "rt-profile".into(),
        output: dir.path().to_path_buf(),
        features,
        template: Some(AppTemplate::Realtime),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let chat = std::fs::read_to_string(root.join("src/capabilities/chat.rs")).unwrap();
    assert!(chat.contains(".per_node()"));
    assert!(chat.contains("session"));
}

/// B-52 — jobs template ships queued idempotency reference (`task.rs`).
#[test]
fn b52_jobs_template_task_idempotency_sample() {
    use trembita_cli::{AppTemplate, resolve_scaffold_features};

    let dir = tempdir().unwrap();
    let features = resolve_scaffold_features(Some(AppTemplate::Jobs), "").unwrap();
    let opts = NewProjectOpts {
        name: "b52-jobs".into(),
        output: dir.path().to_path_buf(),
        features,
        template: Some(AppTemplate::Jobs),
        trembita_version: "0.6.3".into(),
        trembita_path: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let task = std::fs::read_to_string(root.join("src/capabilities/task.rs")).unwrap();
    assert!(task.contains("B-52") || task.contains("require_store"));
    assert!(task.contains("store_set"));
}

#[test]
fn b52_topics_template_includes_domain_outbox_stub() {
    use trembita_cli::{AppTemplate, resolve_scaffold_features};

    let dir = tempdir().unwrap();
    let features = resolve_scaffold_features(Some(AppTemplate::Topics), "").unwrap();
    let opts = NewProjectOpts {
        name: "b52-topics".into(),
        output: dir.path().to_path_buf(),
        features,
        template: Some(AppTemplate::Topics),
        trembita_version: "0.6.3".into(),
        trembita_path: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let outbox = std::fs::read_to_string(root.join("src/domain/outbox.rs")).unwrap();
    assert!(outbox.contains("EventOutboxSource"));
    assert!(outbox.contains("TopicOpts::outbox"));
    let domain_mod = std::fs::read_to_string(root.join("src/domain/mod.rs")).unwrap();
    assert!(domain_mod.contains("pub mod outbox"));
}

#[test]
fn b52_api_template_includes_consensus_reference() {
    use trembita_cli::{AppTemplate, resolve_scaffold_features};

    let dir = tempdir().unwrap();
    let features = resolve_scaffold_features(Some(AppTemplate::Api), "").unwrap();
    let opts = NewProjectOpts {
        name: "b52-api".into(),
        output: dir.path().to_path_buf(),
        features,
        template: Some(AppTemplate::Api),
        trembita_version: "0.6.3".into(),
        trembita_path: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let consensus = std::fs::read_to_string(root.join("src/capabilities/consensus.rs")).unwrap();
    assert!(consensus.contains("propose_keyed"));
    assert!(consensus.contains("query_keyed_linearizable"));
}

#[test]
fn b52_domain_patterns_documented_in_capability_dx_and_structural_limits() {
    let root = workspace_root().expect("repo");
    let dx = std::fs::read_to_string(root.join("docs/decisions/capability-dx.md")).unwrap();
    let limits = std::fs::read_to_string(root.join("docs/scenarios/structural-limits.md")).unwrap();
    assert!(dx.contains("Domain DX patterns (B-52)"));
    assert!(limits.contains("Domain patterns (B-52)"));
    assert!(dx.contains("EventOutboxSource"));
    assert!(limits.contains("require_store"));
}

#[test]
fn b38_profile_api_omits_job_consumer() {
    use trembita_cli::{AppTemplate, resolve_scaffold_features};

    let dir = tempdir().unwrap();
    let features = resolve_scaffold_features(Some(AppTemplate::Api), "").unwrap();
    let opts = NewProjectOpts {
        name: "api-app".into(),
        output: dir.path().to_path_buf(),
        features,
        template: Some(AppTemplate::Api),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
    };
    let root = scaffold_project(&opts).unwrap();
    assert!(!root.join("src/consumers/sample.rs").exists());
    assert!(root.join("src/capabilities/ping.rs").is_file());
}
