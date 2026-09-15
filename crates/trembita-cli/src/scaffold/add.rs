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
        let body = format!(
            "//! `{stream}` job consumer.\n\n\
use trembita::consumer;\n\n\
pub const STREAM: &str = \"{stream}\";\n\n\
#[consumer(\"{stream}\")]\n\
async fn handle_{consumer_mod}(payload: &[u8]) -> Result<(), String> {{\n\
    let preview = String::from_utf8_lossy(payload);\n\
    tracing::info!(target: \"app\", stream = STREAM, %preview, \"job\");\n\
    Ok(())\n\
}}\n"
        );
        fs::write(&consumer_path, body).map_err(|e| e.to_string())?;
        patch_consumers_mod(project, &consumer_mod)?;
    }

    let type_name = consumer_type_name(&consumer_mod);
    let entry = format!(
        "JobOpts::product(\"{stream}\", &crate::consumers::{consumer_mod}::{type_name}),\n            "
    );
    insert_before_marker(
        &project.manifest_rs(),
        names::JOBS,
        &format!("// {}-end", names::JOBS),
        &entry,
        &format!("JobOpts::product(\"{stream}\""),
    )?;
    Ok(())
}

fn add_topic(project: &TrembitaProject, topic: &str) -> Result<(), String> {
    let entry = format!("TopicOpts::topic(\"{topic}\"),\n            ");
    insert_before_marker(
        &project.manifest_rs(),
        names::TOPICS,
        &format!("// {}-end", names::TOPICS),
        &entry,
        &format!("TopicOpts::topic(\"{topic}\")"),
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
    dup_needle: &str,
) -> Result<(), String> {
    let mut content = fs::read_to_string(path).map_err(|e| e.to_string())?;
    let start = format!("// {region}");
    if !content.contains(&start) {
        return Err(format!(
            "missing marker `{start}` in {} — enable the feature in Cargo.toml or add the region",
            path.display()
        ));
    }
    if !content.contains(end_marker) {
        return Err(format!(
            "missing marker `{end_marker}` in {}",
            path.display()
        ));
    }
    if content.contains(dup_needle) {
        return Err(format!("`{dup_needle}` already registered in manifest"));
    }
    content = content.replacen(end_marker, &format!("{insert}{end_marker}"), 1);
    fs::write(path, content).map_err(|e| e.to_string())
}
