//! B-31 — integration tests for founder scale doctor findings on scaffold projects.

use tempfile::tempdir;
use trembita_cli::{
    AppFeature, Level, NewProjectOpts, TrembitaProject, run_doctor, scaffold_project,
};

#[test]
fn scaffold_passes_founder_scale_doctor_checks() {
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
