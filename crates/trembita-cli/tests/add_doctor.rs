//! Integration tests for `trembita doctor` and project discovery.

use tempfile::tempdir;
use trembita_cli::{
    AppFeature, Level, NewProjectOpts, TrembitaProject, run_doctor, scaffold_project,
};

#[test]
fn scaffolded_project_passes_doctor() {
    let dir = tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "integration".into(),
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
        !report.has_errors(),
        "doctor errors: {:?}",
        report
            .findings
            .iter()
            .filter(|f| f.level == Level::Error)
            .collect::<Vec<_>>()
    );
}

#[test]
fn doctor_finds_orphan_consumer_file() {
    let dir = tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "orphan-test".into(),
        output: dir.path().to_path_buf(),
        features: AppFeature::defaults(),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
        template: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let project = TrembitaProject { root };

    std::fs::write(
        project.consumers_dir().join("orphan.rs"),
        r#"use trembita::consumer;

#[consumer("orphan")]
async fn handle_orphan(_: &[u8]) -> Result<(), ()> { Ok(()) }
"#,
    )
    .unwrap();

    let report = run_doctor(&project, false);
    assert!(report.has_errors());
}

#[test]
fn project_discover_from_subdirectory() {
    let dir = tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "discover".into(),
        output: dir.path().to_path_buf(),
        features: AppFeature::defaults(),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
        template: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let sub = root.join("src/consumers");
    let project = TrembitaProject::discover(&sub).unwrap();
    assert_eq!(project.root, root);
}
