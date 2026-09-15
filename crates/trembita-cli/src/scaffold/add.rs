//! `trembita add` — patch marker regions in scaffolded apps.

use std::fs;
use std::path::Path;

use super::markers::{consumer_type_name, names};
use super::project::TrembitaProject;

/// What to register.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddKind {
    /// Durable job stream + consumer stub.
    Job,
    /// Event topic name.
    Topic,
}

/// Add a capability to a scaffolded project (manifest markers + optional consumer file).
///
/// # Errors
/// I/O failures or unknown marker regions.
pub fn run_add(project: &TrembitaProject, kind: AddKind, name: &str) -> Result<(), String> {
    validate_name(name)?;
    match kind {
        AddKind::Job => add_job(project, name),
        AddKind::Topic => add_topic(project, name),
    }
}

fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("name must not be empty".into());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
    {
        return Err(format!(
            "invalid name `{name}` — use lowercase letters, digits, `-`, `_`"
        ));
    }
    Ok(())
}

fn add_job(project: &TrembitaProject, stream: &str) -> Result<(), String> {
    let consumer_mod = stream.replace('-', "_");
    let consumer_path = project.consumers_dir().join(format!("{consumer_mod}.rs"));
    if !consumer_path.exists() {
        fs::create_dir_all(project.consumers_dir()).map_err(|e| e.to_string())?;
        let type_name = consumer_type_name(&consumer_mod);
        let body = format!(
            r"//! `{stream}` job consumer.

use trembita::consumer;

pub const STREAM: &str = "{stream}";

#[consumer("{stream}")]
async fn handle_{consumer_mod}(payload: &[u8]) -> Result<(), String> {{
    let preview = String::from_utf8_lossy(payload);
    tracing::info!(target: "app", stream = STREAM, %preview, "job");
    Ok(())
}}

trembita::macros::consumer!({{
    pub struct {type_name};
    {type_name} => handle_{consumer_mod};
}});
"
        );
        // consumer macro generates type - simplify without double macro
        let body = format!(
            r"//! `{stream}` job consumer.

use trembita::consumer;

pub const STREAM: &str = "{stream}";

#[consumer("{stream}")]
async fn handle_{consumer_mod}(payload: &[u8]) -> Result<(), String> {{
    let preview = String::from_utf8_lossy(payload);
    tracing::info!(target: "app", stream = STREAM, %preview, "job");
    Ok(())
}}
"
        );
        fs::write(&consumer_path, body).map_err(|e| e.to_string())?;
        patch_consumers_mod(project, &consumer_mod)?;
    }

    let type_name = consumer_type_name(&consumer_mod);
    let entry = format!(
        "JobOpts::product(\"{stream}\", &{type_name}),\n            "
    );
    insert_before_marker(
        &project.manifest_rs(),
        names::JOBS,
        &format!("// {0}-end", names::JOBS),
        &entry,
    )?;
    Ok(())
}

fn add_topic(project: &TrembitaProject, topic: &str) -> Result<(), String> {
    let entry = format!("TopicOpts::topic(\"{topic}\"),\n            ");
    insert_before_marker(
        &project.manifest_rs(),
        names::TOPICS,
        &format!("// {0}-end", names::TOPICS),
        &entry,
    )?;
    Ok(())
}

fn patch_consumers_mod(project: &TrembitaProject, module: &str) -> Result<(), String> {
    let mod_path = project.consumers_dir().join("mod.rs");
    let mut content = fs::read_to_string(&mod_path).map_err(|e| e.to_string())?;
    let decl = format!("pub mod {module};");
    if content.contains(&decl) {
        return Ok(());
    }
    content.push_str(&format!("\n{decl}\n"));
    fs::write(mod_path, content).map_err(|e| e.to_string())
}

fn insert_before_marker(
    path: &Path,
    region: &str,
    end_marker: &str,
    insert: &str,
) -> Result<(), String> {
    let mut content = fs::read_to_string(path).map_err(|e| e.to_string())?;
    let start = format!("// {region}");
    if !content.contains(&start) {
        return Err(format!("missing marker `{start}` in {}", path.display()));
    }
    if !content.contains(end_marker) {
        return Err(format!("missing marker `{end_marker}` in {}", path.display()));
    }
    if content.contains(&format!("JobOpts::new(\"{insert}") ) {
        // weak dup check skipped
    }
    content = content.replacen(end_marker, &format!("{insert}{end_marker}"), 1);
    fs::write(path, content).map_err(|e| e.to_string())
}
