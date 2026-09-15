//! Integration tests for `trembita add` and `trembita doctor`.

use tempfile::tempdir;
use trembita_cli::{
    AddActorOpts, AddConsumerOpts, AddHttpSurfaceOpts, AddStaticSiteOpts, AddTopicOpts, AppFeature,
    Level, NewProjectOpts, StaticSiteSource, TrembitaProject, add_actor, add_consumer,
    add_http_surface, add_static_site, add_topic, run_doctor, scaffold_project,
};

#[test]
fn add_and_doctor_integration() {
    let dir = tempdir().unwrap();
    let opts = NewProjectOpts {
        name: "integration".into(),
        output: dir.path().to_path_buf(),
        features: AppFeature::defaults(),
        trembita_version: "0.3.2".into(),
        trembita_path: None,
    };
    let root = scaffold_project(&opts).unwrap();
    let project = TrembitaProject { root };

    add_consumer(
        &project,
        &AddConsumerOpts {
            stream: "emails".into(),
            module: None,
            lease_secs: 60,
        },
    )
    .unwrap();
    add_topic(
        &project,
        &AddTopicOpts {
            topic: "app.events".into(),
        },
    )
    .unwrap();
    add_actor(
        &project,
        &AddActorOpts {
            group: "catalog".into(),
            type_name: None,
        },
    )
    .unwrap();
    add_http_surface(
        &project,
        &AddHttpSurfaceOpts {
            name: "api".into(),
            hosts: vec!["api.example.com".into()],
            module: None,
            session: false,
            cors: false,
        },
    )
    .unwrap();
    add_static_site(
        &project,
        &AddStaticSiteOpts {
            name: "app".into(),
            hosts: vec!["app.example.com".into()],
            source: StaticSiteSource::Filesystem {
                path: "fe/app/dist".into(),
            },
            module: None,
        },
    )
    .unwrap();

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
    };
    let root = scaffold_project(&opts).unwrap();
    let sub = root.join("src/consumers");
    let project = TrembitaProject::discover(&sub).unwrap();
    assert_eq!(project.root, root);
}
