//! B-31 — integration tests for product scale doctor findings on scaffold projects.

use tempfile::tempdir;
use trembita_cli::{
    AppFeature, AppTemplate, Level, NewProjectOpts, TrembitaProject, run_doctor, run_explain_scale,
    scaffold_project,
};

#[test]
fn scaffold_passes_product_scale_doctor_checks() {
    let dir = tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "b31-scaffold".into(),
        output: dir.path().to_path_buf(),
        features: AppFeature::defaults(),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
        template: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let project = TrembitaProject { root };
    let report = run_doctor(&project, false);
    assert!(
        !report.findings.iter().any(|f| {
            f.level == Level::Error
                && (f.message.contains("B-31") || f.message.contains("instances(1)"))
        }),
        "errors: {:?}",
        report
            .findings
            .iter()
            .filter(|f| f.level == Level::Error)
            .collect::<Vec<_>>()
    );
}

#[test]
fn stateless_instances_one_triggers_b31_warn_on_full_doctor_run() {
    let dir = tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "b31-warn".into(),
        output: dir.path().to_path_buf(),
        features: AppFeature::defaults(),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
        template: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let project = TrembitaProject { root };
    std::fs::write(
        project.capabilities_dir().join("pinned.rs"),
        r#"
use trembita::CapGroup;

#[derive(Default)]
struct PinState;

fn _manifest_snippet() {
    let _ = CapGroup::<PinState>::with_state("pin").instances(1);
}
"#,
    )
    .unwrap();
    let report = run_doctor(&project, false);
    assert!(
        report.findings.iter().any(|f| {
            f.level == Level::Warn && f.message.contains("stateless capability group")
        }),
        "findings: {:?}",
        report.findings
    );
}

#[test]
fn queued_fixed_one_without_key_fails_full_doctor_run() {
    let dir = tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "b31-err".into(),
        output: dir.path().to_path_buf(),
        features: AppFeature::defaults(),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
        template: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let project = TrembitaProject { root };
    std::fs::write(
        project.capabilities_dir().join("bad_queue.rs"),
        r#"
use trembita::{cap_handler, cap_register_chain, CapError, CapGroup, CapManifest};

#[derive(Default)]
pub struct BadState;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Work { pub n: u32 }

#[cap_handler(group = "bad")]
async fn work(_: Work, _: &mut BadState) -> Result<(), CapError> { Ok(()) }

#[must_use]
pub fn manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<BadState>::for_cap::<Work>()
            .instances(1)
            .default_queue_for::<Work>(),
        work_register,
    ))
}
"#,
    )
    .unwrap();
    let report = run_doctor(&project, false);
    assert!(
        report.findings.iter().any(|f| {
            f.level == Level::Error
                && f.message.contains("instances(1)")
                && f.message.contains("queued")
        }),
        "findings: {:?}",
        report.findings
    );
}

#[test]
fn b38_queued_footgun_error_includes_fix_suggestion() {
    let dir = tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "b38-suggest".into(),
        output: dir.path().to_path_buf(),
        features: AppFeature::defaults(),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
        template: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let project = TrembitaProject { root };
    std::fs::write(
        project.capabilities_dir().join("bad_queue.rs"),
        r#"
use trembita::{cap_handler, cap_register_chain, CapError, CapGroup, CapManifest};

#[derive(Default)]
pub struct BadState;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Work { pub n: u32 }

#[cap_handler(group = "bad")]
async fn work(_: Work, _: &mut BadState) -> Result<(), CapError> { Ok(()) }

#[must_use]
pub fn manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<BadState>::for_cap::<Work>()
            .instances(1)
            .default_queue_for::<Work>(),
        work_register,
    ))
}
"#,
    )
    .unwrap();
    let report = run_doctor(&project, false);
    let err = report.findings.iter().find(|f| {
        f.level == Level::Error
            && f.message.contains("instances(1)")
            && f.message.contains("queued")
    });
    assert!(err.is_some(), "{:?}", report.findings);
    assert!(
        err.unwrap()
            .suggestion
            .as_ref()
            .is_some_and(|s| s.contains("PerNode") || s.contains("key")),
        "{:?}",
        report.findings
    );
}

#[test]
fn b38_jobs_template_passes_r4_store_doctor() {
    let dir = tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "b38-jobs-r4".into(),
        output: dir.path().to_path_buf(),
        features: AppTemplate::Jobs.features(),
        template: Some(AppTemplate::Jobs),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let project = TrembitaProject { root };
    let report = run_doctor(&project, false);
    assert!(
        !report.findings.iter().any(|f| {
            f.level == Level::Error
                && f.message.contains("require_store")
                && f.message.contains("task.rs")
        }),
        "{:?}",
        report
            .findings
            .iter()
            .filter(|f| f.level == Level::Error)
            .collect::<Vec<_>>()
    );
}

#[test]
fn b38_explain_scale_on_realtime_scaffold_mentions_session() {
    let dir = tempfile::tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "b38-rt-explain".into(),
        output: dir.path().to_path_buf(),
        features: AppTemplate::Realtime.features(),
        template: Some(AppTemplate::Realtime),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let project = TrembitaProject { root };
    let report = run_explain_scale(&project);
    assert!(!report.has_errors(), "explain-scale: {:?}", report.findings);
    assert!(
        report
            .findings
            .iter()
            .any(|f| f.message.contains("Product scale")),
        "{:?}",
        report.findings
    );
    let chat = std::fs::read_to_string(project.capabilities_dir().join("chat.rs")).unwrap();
    assert!(chat.contains(".per_node()"));
}

#[test]
fn b38_explain_scale_on_jobs_scaffold_lists_queued_guidance() {
    let dir = tempfile::tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "scale-explain".into(),
        output: dir.path().to_path_buf(),
        features: AppTemplate::Jobs.features(),
        template: Some(AppTemplate::Jobs),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let project = TrembitaProject { root };
    let report = run_explain_scale(&project);
    assert!(!report.has_errors(), "explain-scale: {:?}", report.findings);
    assert!(
        report.findings.iter().any(|f| {
            f.message.contains("Queued capabilities") || f.message.contains("Product scale")
        }),
        "findings: {:?}",
        report.findings
    );
}

#[test]
fn b33_scaffold_preflight_has_no_active_join_without_session_secret() {
    let dir = tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "b33-preflight".into(),
        output: dir.path().to_path_buf(),
        features: AppFeature::defaults(),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
        template: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let project = TrembitaProject { root };
    let report = run_doctor(&project, true);
    assert!(
        !report.findings.iter().any(|f| {
            f.level == Level::Error
                && f.message.contains("TREMBITA_JOIN_SEEDS")
                && f.message.contains("GATEWAY_SESSION_SECRET")
        }),
        "preflight must not error when join seeds stay commented in deploy/.env.example: {:?}",
        report
            .findings
            .iter()
            .filter(|f| f.level == Level::Error)
            .collect::<Vec<_>>()
    );
    let env = std::fs::read_to_string(project.root.join("deploy/.env.example")).unwrap();
    assert!(
        env.contains("TREMBITA_JOIN_SEEDS") && env.contains("TREMBITA_GATEWAY_SESSION_SECRET"),
        "deploy/.env.example should document elastic join and gateway session secret"
    );
}
