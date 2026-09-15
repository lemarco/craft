//! Doctor checks for manifest-only capability wiring.

use std::fs;

use tempfile::tempdir;
use trembita_cli::{
    AppFeature, Level, NewProjectOpts, TrembitaProject, run_doctor, scaffold_project,
};

#[test]
fn doctor_errors_when_jobs_registered_in_app_rs() {
    let dir = tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "inline-jobs".into(),
        output: dir.path().to_path_buf(),
        features: AppFeature::defaults(),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let project = TrembitaProject { root };

    let app_path = project.app_rs();
    let mut app = fs::read_to_string(&app_path).unwrap();
    app = app.replace(".manifest(manifest::build())", "");
    app = app.replace(
        ".data_dir(&self.config.data_dir)",
        ".data_dir(&self.config.data_dir)\n            .jobs([])",
    );
    fs::write(&app_path, app).unwrap();

    let report = run_doctor(&project, false);
    assert!(
        report.has_errors(),
        "expected doctor error for inline .jobs() in app.rs"
    );
    assert!(
        report.findings.iter().any(|f| {
            f.level == Level::Error
                && f.message
                    .contains("must not register .jobs/.topics/.workers")
        }),
        "findings: {:?}",
        report
            .findings
            .iter()
            .filter(|f| f.level == Level::Error)
            .collect::<Vec<_>>()
    );
}

#[test]
fn doctor_errors_when_manifest_missing() {
    let dir = tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "no-manifest".into(),
        output: dir.path().to_path_buf(),
        features: AppFeature::defaults(),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
    };
    let root = scaffold_project(&opts).unwrap();
    fs::remove_file(root.join("src/manifest.rs")).unwrap();

    let project = TrembitaProject { root };
    let report = run_doctor(&project, false);
    assert!(report.has_errors());
    assert!(
        report
            .findings
            .iter()
            .any(|f| f.message.contains("manifest.rs"))
    );
}
