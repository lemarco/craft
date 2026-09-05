//! `trembita add` generators.

use std::fs;

use super::markers::{
    AppRsPatch, PatchError, consumer_type_name, ensure_mod_declaration, module_name, names,
};
use super::project::TrembitaProject;

/// `trembita add` failure.
#[derive(Debug, thiserror::Error)]
pub enum AddError {
    /// Patch / layout error.
    #[error(transparent)]
    Patch(#[from] PatchError),
    /// I/O error.
    #[error("{0}")]
    Io(#[from] std::io::Error),
    /// Invalid identifier.
    #[error("invalid name: {0}")]
    InvalidName(String),
    /// Target file already exists.
    #[error("already exists: {0}")]
    Exists(String),
}

/// Options for `add consumer`.
#[derive(Debug, Clone)]
pub struct AddConsumerOpts {
    /// Job stream name (`emails`, `platform.events`, …).
    pub stream: String,
    /// Rust module file name (default: derived from stream).
    pub module: Option<String>,
    /// Lease duration in seconds.
    pub lease_secs: u64,
}

/// Options for `add topic`.
#[derive(Debug, Clone)]
pub struct AddTopicOpts {
    /// Topic stream name.
    pub topic: String,
}

/// Options for `add actor`.
#[derive(Debug, Clone)]
pub struct AddActorOpts {
    /// Actor group name (routing key).
    pub group: String,
    /// Rust type name (default: `{Group}Worker` in PascalCase).
    pub type_name: Option<String>,
}

/// Add a job consumer: file + `app.rs` patch.
pub fn add_consumer(project: &TrembitaProject, opts: &AddConsumerOpts) -> Result<(), AddError> {
    validate_identifier(&opts.stream)?;
    let module = opts
        .module
        .clone()
        .unwrap_or_else(|| module_name(&opts.stream));
    validate_identifier(&module)?;

    let consumer_path = project.consumers_dir().join(format!("{module}.rs"));
    if consumer_path.exists() {
        return Err(AddError::Exists(consumer_path.display().to_string()));
    }

    fs::create_dir_all(project.consumers_dir())?;
    let handler = format!("handle_{module}");
    let consumer_type = consumer_type_name(&module);
    fs::write(
        &consumer_path,
        format!(
            r#"//! `{stream}` job consumer.

use trembita::consumer;

/// Job stream name.
pub const STREAM: &str = "{stream}";

#[consumer("{stream}")]
async fn {handler}(payload: &[u8]) -> Result<(), String> {{
    let preview = String::from_utf8_lossy(payload);
    tracing::info!(target: "app", stream = STREAM, "job: {{preview}}");
    Ok(())
}}
"#,
            stream = opts.stream,
            handler = handler,
        ),
    )?;

    let mod_rs = project.consumers_dir().join("mod.rs");
    ensure_mod_declaration(&mod_rs, &module)?;

    let mut app = AppRsPatch::load(&project.app_rs())?;
    app.insert_import(&format!(
        "use crate::consumers::{module}::{consumer_type};"
    ))?;

    let job_line = format!(
        r#"JobOpts::new("{stream}")
                    .lease(Duration::from_secs({lease}))
                    .consumer(&{consumer_type})
                    .http_enqueue(true),"#,
        stream = opts.stream,
        lease = opts.lease_secs,
        consumer_type = consumer_type,
    );

    if app.has_marker(names::JOBS) {
        app.insert_before_end(names::JOBS, &format!("    {job_line}"))?;
    } else if app.contains(".jobs(") {
        return Err(AddError::Patch(PatchError::MissingMarker {
            marker: names::JOBS.into(),
        }));
    } else {
        app.insert_block_before_configure(&format!(
            r#"            .jobs([
                // trembita:jobs
                {job_line}
                // trembita:jobs-end
            ])
"#,
        ))?;
    }

    app.save(&project.app_rs())?;
    ensure_main_mod(project, "consumers")?;
    Ok(())
}

/// Add an event topic registration.
pub fn add_topic(project: &TrembitaProject, opts: &AddTopicOpts) -> Result<(), AddError> {
    validate_identifier(&opts.topic.replace('.', "_"))?;

    let mut app = AppRsPatch::load(&project.app_rs())?;
    app.insert_import("use trembita::TopicOpts;")?;

    let topic_line = format!(r#"TopicOpts::topic("{topic}"),"#, topic = opts.topic);

    if app.has_marker(names::TOPICS) {
        app.insert_before_end(names::TOPICS, &format!("    {topic_line}"))?;
    } else if app.contains(".topics(") {
        return Err(AddError::Patch(PatchError::MissingMarker {
            marker: names::TOPICS.into(),
        }));
    } else {
        app.insert_block_before_configure(&format!(
            r#"            .topics([
                // trembita:topics
                {topic_line}
                // trembita:topics-end
            ])
"#,
        ))?;
    }

    app.save(&project.app_rs())?;
    Ok(())
}

/// Add a stateful worker stub + registration.
pub fn add_actor(project: &TrembitaProject, opts: &AddActorOpts) -> Result<(), AddError> {
    validate_identifier(&opts.group)?;
    let module = module_name(&opts.group);
    let type_name = opts.type_name.clone().unwrap_or_else(|| {
        format!(
            "{}Worker",
            module
                .split('_')
                .filter(|s| !s.is_empty())
                .map(|s| {
                    let mut c = s.chars();
                    match c.next() {
                        None => String::new(),
                        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
                    }
                })
                .collect::<String>()
        )
    });

    fs::create_dir_all(project.actors_dir())?;
    let actor_path = project.actors_dir().join(format!("{module}.rs"));
    if actor_path.exists() {
        return Err(AddError::Exists(actor_path.display().to_string()));
    }

    fs::write(
        &actor_path,
        format!(
            r#"//! `{group}` stateful worker group.

use trembita::actor;
use trembita::runtime::{{MessageDecodeError, UserActor}};

/// Stateful worker for group `{group}`.
#[derive(Default)]
pub struct {type_name};

#[actor]
impl UserActor for {type_name} {{
    type Config = ();
    type Message = Vec<u8>;
    type Error = String;

    fn decode_message(payload: &[u8]) -> Result<Self::Message, MessageDecodeError> {{
        Ok(payload.to_vec())
    }}

    async fn handle(&mut self, _msg: Self::Message) -> Result<(), Self::Error> {{
        Ok(())
    }}
}}
"#,
            group = opts.group,
            type_name = type_name,
        ),
    )?;

    let mod_rs = project.actors_dir().join("mod.rs");
    ensure_mod_declaration(&mod_rs, &module)?;

    let mut app = AppRsPatch::load(&project.app_rs())?;
    app.insert_import(&format!(
        "use crate::actors::{module}::{type_name};"
    ))?;
    app.insert_import("use trembita::{WorkerOpts, WorkerScale, workers};")?;

    let worker_line = format!(
        r#"WorkerOpts::<{type_name}>::new("{group}")
                    .config(())
                    .scale(WorkerScale::PerNode(1)),"#,
        type_name = type_name,
        group = opts.group,
    );

    if app.has_marker(names::WORKERS) {
        app.insert_before_end(names::WORKERS, &format!("    {worker_line}"))?;
    } else if app.contains(".workers(") {
        return Err(AddError::Patch(PatchError::MissingMarker {
            marker: names::WORKERS.into(),
        }));
    } else {
        app.insert_block_before_configure(&format!(
            r#"            .workers(workers!(
                // trembita:workers
                {worker_line}
                // trembita:workers-end
            ))
"#,
        ))?;
    }

    app.save(&project.app_rs())?;
    ensure_main_mod(project, "actors")?;
    Ok(())
}

fn validate_identifier(name: &str) -> Result<(), AddError> {
    let ok = !name.is_empty()
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
        && name.chars().next().is_some_and(|c| c.is_ascii_alphabetic());
    if ok {
        Ok(())
    } else {
        Err(AddError::InvalidName(name.to_string()))
    }
}

fn ensure_main_mod(project: &TrembitaProject, module: &str) -> Result<(), AddError> {
    let main_path = project.main_rs();
    let content = fs::read_to_string(&main_path).map_err(PatchError::Io)?;
    let decl = format!("mod {module};");
    if content.lines().any(|l| l.trim() == decl) {
        return Ok(());
    }
    let needle = "mod app;";
    let Some(idx) = content.find(needle) else {
        return Ok(());
    };
    let insert_at = idx + needle.len();
    let mut out = content;
    out.insert_str(insert_at, &format!("\n{decl}"));
    fs::write(main_path, out).map_err(PatchError::Io)?;
    Ok(())
}

/// Validate topic name allows dots.
fn _topic_ok(topic: &str) -> bool {
    !topic.is_empty()
        && topic
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scaffold::{AppFeature, NewProjectOpts, scaffold_project};
    use tempfile::tempdir;

    fn sample_project() -> (tempfile::TempDir, TrembitaProject) {
        let dir = tempdir().unwrap();
        let opts = NewProjectOpts {
            name: "add-test".into(),
            output: dir.path().to_path_buf(),
            features: AppFeature::defaults(),
            trembita_version: "0.3.2".into(),
            trembita_path: None,
        };
        let root = scaffold_project(&opts).unwrap();
        let project = TrembitaProject { root };
        (dir, project)
    }

    #[test]
    fn add_consumer_creates_file_and_patches_app() {
        let (_dir, project) = sample_project();
        add_consumer(
            &project,
            &AddConsumerOpts {
                stream: "emails".into(),
                module: None,
                lease_secs: 120,
            },
        )
        .unwrap();
        assert!(project.consumers_dir().join("emails.rs").is_file());
        let app = fs::read_to_string(project.app_rs()).unwrap();
        assert!(app.contains("JobOpts::new(\"emails\")"));
        assert!(app.contains("HandleEmailsConsumer"));
    }

    #[test]
    fn add_topic_patches_app() {
        let (_dir, project) = sample_project();
        add_topic(
            &project,
            &AddTopicOpts {
                topic: "platform.events".into(),
            },
        )
        .unwrap();
        let app = fs::read_to_string(project.app_rs()).unwrap();
        assert!(app.contains("TopicOpts::topic(\"platform.events\")"));
    }

    #[test]
    fn add_actor_creates_stub() {
        let (_dir, project) = sample_project();
        add_actor(
            &project,
            &AddActorOpts {
                group: "catalog".into(),
                type_name: None,
            },
        )
        .unwrap();
        assert!(project.actors_dir().join("catalog.rs").is_file());
        let app = fs::read_to_string(project.app_rs()).unwrap();
        assert!(app.contains("WorkerOpts::<CatalogWorker>::new(\"catalog\")"));
    }
}
