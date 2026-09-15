//! `trembita add` generators.

use std::fs;

use super::markers::{
    AppRsPatch, PatchError, consumer_type_name, ensure_main_module, ensure_mod_declaration,
    module_name, names,
};
use super::project::TrembitaProject;
use super::registry::CapabilityRegistry;
use super::templates::{http_jobs_rs, http_ops_rs};

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

/// Options for `add workflow`.
#[derive(Debug, Clone)]
pub struct AddWorkflowOpts {
    /// Saga id prefix (e.g. `onboard` matches `onboard-42`).
    pub prefix: String,
    /// Rust module file name (default: derived from `prefix`).
    pub module: Option<String>,
}

/// Options for `add actor`.
#[derive(Debug, Clone)]
pub struct AddActorOpts {
    /// Actor group name (routing key).
    pub group: String,
    /// Rust type name (default: `{Group}Worker` in `PascalCase`).
    pub type_name: Option<String>,
}

/// Static asset source for `add static-site`.
#[derive(Debug, Clone)]
pub enum StaticSiteSource {
    /// Compile-time bytes via `include_dir!`.
    Embedded {
        /// Path relative to project root (e.g. `fe/app/dist`).
        path: String,
    },
    /// Serve from a directory on disk.
    Filesystem {
        /// Absolute or project-relative path.
        path: String,
    },
}

/// Options for `add http-surface`.
#[derive(Debug, Clone)]
pub struct AddHttpSurfaceOpts {
    /// Surface label (module name by default).
    pub name: String,
    /// Hostnames routed to this surface.
    pub hosts: Vec<String>,
    /// Rust module file name (default: derived from `name`).
    pub module: Option<String>,
    /// Wire `SessionGate` on the surface.
    pub session: bool,
    /// Attach `CorsPolicy::credentials` for browser clients.
    pub cors: bool,
}

/// Options for `add static-site`.
#[derive(Debug, Clone)]
pub struct AddStaticSiteOpts {
    /// Site label (module `{name}_static` by default).
    pub name: String,
    /// Hostnames serving the SPA.
    pub hosts: Vec<String>,
    /// Asset source (default: filesystem `fe/{name}/dist`).
    pub source: StaticSiteSource,
    /// Rust module file name (default: `{name}_static`).
    pub module: Option<String>,
}

/// Add a job consumer: file + `manifest.rs` patch.
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

    let mut registry = CapabilityRegistry::load(project)?;
    registry
        .patch_mut()
        .insert_import(&format!("use crate::consumers::{module}::{consumer_type};"))?;

    let job_line = format!(
        r#"JobOpts::new("{stream}")
                .lease(Duration::from_secs({lease}))
                .consumer(&{consumer_type})
                .http_enqueue(true),"#,
        stream = opts.stream,
        lease = opts.lease_secs,
        consumer_type = consumer_type,
    );

    registry.register_job_line(&job_line)?;
    registry.save()?;
    ensure_main_module(project, "consumers").map_err(AddError::Patch)?;
    Ok(())
}

/// Add an event topic registration.
pub fn add_topic(project: &TrembitaProject, opts: &AddTopicOpts) -> Result<(), AddError> {
    validate_identifier(&opts.topic.replace('.', "_"))?;

    let mut registry = CapabilityRegistry::load(project)?;
    registry
        .patch_mut()
        .insert_import("use trembita::TopicOpts;")?;

    let topic_line = format!(r#"TopicOpts::topic("{topic}"),"#, topic = opts.topic);

    registry.register_topic_line(&topic_line)?;
    registry.save()?;
    Ok(())
}

/// Add a saga workflow stub + `manifest.rs` registration.
pub fn add_workflow(project: &TrembitaProject, opts: &AddWorkflowOpts) -> Result<(), AddError> {
    validate_identifier(&opts.prefix)?;
    let module = opts
        .module
        .clone()
        .unwrap_or_else(|| module_name(&opts.prefix));
    validate_identifier(&module)?;

    fs::create_dir_all(project.workflows_dir())?;
    let workflow_path = project.workflows_dir().join(format!("{module}.rs"));
    if workflow_path.exists() {
        return Err(AddError::Exists(workflow_path.display().to_string()));
    }

    fs::write(
        &workflow_path,
        format!(
            r#"//! `{prefix}` saga workflow.

use std::sync::Arc;

use trembita::TrembitaApp;
use trembita::WorkflowBuilder;
use trembita::client::{{SagaError, SagaOutcome, SagaPlan}};
use trembita::journal_workflow;

/// Saga id prefix for this workflow.
pub const PREFIX: &str = "{prefix}";

/// Build saga plan for `{prefix}` workflows.
#[must_use]
pub fn plan_for(saga_id: &str) -> SagaPlan {{
    WorkflowBuilder::new(PREFIX)
        .step("noop", saga_id, b"ok")
        .build()
        .expect("valid workflow plan")
}}

/// Run plan on the cluster journal.
pub async fn run_plan(app: Arc<TrembitaApp>, plan: SagaPlan) -> Result<SagaOutcome, SagaError> {{
    journal_workflow(app, plan).await
}}
"#,
            prefix = opts.prefix,
        ),
    )?;

    let mod_rs = project.workflows_dir().join("mod.rs");
    ensure_mod_declaration(&mod_rs, &module)?;

    let mut registry = CapabilityRegistry::load(project)?;
    registry
        .patch_mut()
        .insert_import("use trembita::WorkflowOpts;")?;

    let workflow_line = format!(
        r#"WorkflowOpts::named("{prefix}", crate::workflows::{module}::plan_for, crate::workflows::{module}::run_plan),"#,
        prefix = opts.prefix,
        module = module,
    );

    registry.register_workflow_line(&workflow_line)?;
    registry.save()?;
    ensure_main_module(project, "workflows").map_err(AddError::Patch)?;
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
            r"//! `{group}` stateful worker group.

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
",
            group = opts.group,
            type_name = type_name,
        ),
    )?;

    let mod_rs = project.actors_dir().join("mod.rs");
    ensure_mod_declaration(&mod_rs, &module)?;

    let mut registry = CapabilityRegistry::load(project)?;
    registry
        .patch_mut()
        .insert_import(&format!("use crate::actors::{module}::{type_name};"))?;
    registry
        .patch_mut()
        .insert_import("use trembita::{WorkerOpts, WorkerScale, workers};")?;

    let worker_line = format!(
        r#"WorkerOpts::<{type_name}>::new("{group}")
                .config(())
                .scale(WorkerScale::PerNode(1)),"#,
        type_name = type_name,
        group = opts.group,
    );

    registry.register_worker_line(&worker_line)?;
    registry.save()?;
    ensure_main_module(project, "actors").map_err(AddError::Patch)?;
    Ok(())
}

/// Add a custom HTTP surface (`src/http/` + gateway `.surface()`).
pub fn add_http_surface(
    project: &TrembitaProject,
    opts: &AddHttpSurfaceOpts,
) -> Result<(), AddError> {
    validate_identifier(&opts.name)?;
    validate_hosts(&opts.hosts)?;
    let module = opts
        .module
        .clone()
        .unwrap_or_else(|| module_name(&opts.name));
    validate_identifier(&module)?;

    fs::create_dir_all(project.http_dir())?;
    let http_path = project.http_dir().join(format!("{module}.rs"));
    if http_path.exists() {
        return Err(AddError::Exists(http_path.display().to_string()));
    }

    ensure_cargo_dependency(project, "http", "1")?;

    fs::write(
        &http_path,
        format!(
            r#"//! `{name}` HTTP surface — custom [`RouteTable`](trembita::RouteTable).

use trembita::{{RequestCtx, Response, RouteTable}};

/// Routes for hosts: {hosts_comment}.
#[must_use]
pub fn route_table() -> RouteTable {{
    RouteTable::new().get("/health", |_ctx: RequestCtx| async move {{
        Ok(Response::text(http::StatusCode::OK, "{name} ok"))
    }})
}}
"#,
            name = opts.name,
            hosts_comment = opts.hosts.join(", "),
        ),
    )?;

    wire_http_surface(project, &module, &opts.hosts, opts.session, opts.cors)?;
    Ok(())
}

/// Add operational HTTP routes (`src/http/ops.rs` + merge in gateway surfaces).
pub fn add_ops_routes(project: &TrembitaProject) -> Result<(), AddError> {
    ensure_cargo_feature(project, "gateway")?;
    write_http_module(project, "ops", http_ops_rs())?;
    wire_builtin_http_routes(project, "ops")?;
    Ok(())
}

/// Add job operator HTTP routes (`src/http/jobs.rs` + merge in gateway surfaces).
pub fn add_jobs_routes(project: &TrembitaProject) -> Result<(), AddError> {
    ensure_cargo_feature(project, "gateway")?;
    ensure_cargo_feature(project, "jobs")?;
    write_http_module(project, "jobs", http_jobs_rs())?;
    wire_builtin_http_routes(project, "jobs")?;
    Ok(())
}

/// Add a static SPA surface (`StaticSite` + gateway `.surface()`).
pub fn add_static_site(
    project: &TrembitaProject,
    opts: &AddStaticSiteOpts,
) -> Result<(), AddError> {
    validate_identifier(&opts.name)?;
    validate_hosts(&opts.hosts)?;
    let module = opts.module.clone().unwrap_or_else(|| {
        let base = module_name(&opts.name);
        if base.ends_with("_static") {
            base
        } else {
            format!("{base}_static")
        }
    });
    validate_identifier(&module)?;

    fs::create_dir_all(project.http_dir())?;
    let http_path = project.http_dir().join(format!("{module}.rs"));
    if http_path.exists() {
        return Err(AddError::Exists(http_path.display().to_string()));
    }

    let source_body = match &opts.source {
        StaticSiteSource::Embedded { path } => {
            ensure_cargo_dependency(project, "include_dir", "0.7")?;
            format!(
                r#"static ASSETS: include_dir::Dir = include_dir::include_dir!("$CARGO_MANIFEST_DIR/{path}");

StaticSite::new(StaticSource::embedded(embedded_from_dir(&ASSETS)))"#,
                path = path.trim_start_matches("./")
            )
        }
        StaticSiteSource::Filesystem { path } => {
            if path.starts_with('/') {
                format!(r#"StaticSite::new(StaticSource::filesystem("{path}"))"#)
            } else {
                format!(
                    r#"StaticSite::new(StaticSource::filesystem(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("{path}"),
    ))"#,
                    path = path.trim_start_matches("./")
                )
            }
        }
    };

    fs::write(
        &http_path,
        format!(
            r"//! `{name}` static site (SPA).

use trembita::{{RouteTable, StaticSite, StaticSource, embedded_from_dir}};

/// Route table serving the `{name}` SPA on: {hosts_comment}.
#[must_use]
pub fn route_table() -> RouteTable {{
    {source_body}
        .spa_fallback(true)
        .route_table()
}}
",
            name = opts.name,
            hosts_comment = opts.hosts.join(", "),
            source_body = source_body,
        ),
    )?;

    wire_http_surface(project, &module, &opts.hosts, false, false)?;
    Ok(())
}

fn ensure_gateway_surfaces_block(app: &mut AppRsPatch) -> Result<(), PatchError> {
    if app.contains(".gateway(") || app.has_marker(names::SURFACES) {
        return Ok(());
    }
    app.insert_import("use trembita::{DefaultGatewayApis, Gateway, GatewayOpts, TrembitaApp};")?;
    app.insert_block_before_configure(
        r"
            .gateway(
                GatewayOpts::from_env()?.surfaces(|state| {
                    // trembita:surfaces
                    TrembitaApp::default_surfaces(
                        state,
                        false,
                        DefaultGatewayApis {
                            jobs: true,
                            ops: true,
                            ..DefaultGatewayApis::default()
                        },
                    )
                    // trembita:surfaces-end
                }),
            )
",
    )?;
    Ok(())
}

fn wire_http_surface(
    project: &TrembitaProject,
    module: &str,
    hosts: &[String],
    session: bool,
    cors: bool,
) -> Result<(), AddError> {
    let mod_rs = project.http_dir().join("mod.rs");
    ensure_mod_declaration(&mod_rs, module)?;

    let mut app = AppRsPatch::load(&project.app_rs())?;
    ensure_gateway_surfaces_block(&mut app).map_err(AddError::Patch)?;
    app.insert_import("use trembita::Gateway;")?;
    if session {
        app.insert_import("use trembita::{HttpError, SessionGate};")?;
    }
    if cors {
        app.insert_import("use trembita::CorsPolicy;")?;
    }

    let hosts_list = hosts
        .iter()
        .map(|h| format!("\"{h}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let cors_line = if cors {
        format!(
            "\n                                    .cors(CorsPolicy::credentials([{hosts_list}]))"
        )
    } else {
        String::new()
    };
    let session_line = if session {
        r#"
                                    .session(SessionGate::validate("session", |token| async move {
                                        if token.is_empty() {
                                            Err(HttpError::Unauthorized("empty session".into()))
                                        } else {
                                            Ok(())
                                        }
                                    }))"#
            .to_string()
    } else {
        String::new()
    };
    app.insert_surface(&format!(
        r"
                            .surface(|s| {{
                                s.hosts([{hosts_list}]){cors_line}{session_line}
                                    .routes(http::{module}::route_table())
                            }})",
    ))?;

    app.save(&project.app_rs())?;
    ensure_main_module(project, "http").map_err(AddError::Patch)?;
    Ok(())
}

fn write_http_module(project: &TrembitaProject, module: &str, body: &str) -> Result<(), AddError> {
    fs::create_dir_all(project.http_dir())?;
    let path = project.http_dir().join(format!("{module}.rs"));
    if path.exists() {
        return Err(AddError::Exists(path.display().to_string()));
    }
    fs::write(&path, body)?;
    ensure_http_mod_rs(project, module)?;
    ensure_main_module(project, "http").map_err(AddError::Patch)?;
    Ok(())
}

fn ensure_http_mod_rs(project: &TrembitaProject, module: &str) -> Result<(), AddError> {
    let mod_rs = project.http_dir().join("mod.rs");
    if !mod_rs.is_file() {
        fs::write(
            &mod_rs,
            format!(
                r"//! HTTP route modules — merge in `GatewayOpts::surfaces()` (`app.rs`).

pub mod {module};
"
            ),
        )?;
        return Ok(());
    }
    ensure_mod_declaration(&mod_rs, module)?;
    Ok(())
}

fn wire_builtin_http_routes(project: &TrembitaProject, module: &str) -> Result<(), AddError> {
    let route_call = format!("http::{module}::route_table(&state)");
    let mut app = AppRsPatch::load(&project.app_rs())?;
    if app.contains(&route_call) {
        return Err(AddError::Patch(PatchError::Duplicate(format!(
            "{route_call} already wired in app.rs"
        ))));
    }
    ensure_gateway_surfaces_block(&mut app).map_err(AddError::Patch)?;

    if app.contains("TrembitaApp::default_surfaces") {
        let merge_line = format!(".merge_routes(http::{module}::route_table(&state))");
        if !app.contains(&merge_line) {
            app.insert_before_end(names::SURFACES, &merge_line)?;
        }
        app.save(&project.app_rs())?;
        return Ok(());
    }

    app.insert_import("use trembita::Gateway;")?;

    if module == "ops" {
        if app.contains("http::jobs::route_table(&state)") {
            app.replace_once(
                "http::jobs::route_table(&state)",
                "http::jobs::route_table(&state).merge(http::ops::route_table(&state))",
            )?;
        } else if app.contains(".dev_fallback(") && !app.contains(".dev_fallback(http::") {
            app.replace_once(
                ".dev_fallback(",
                ".dev_fallback(http::ops::route_table(&state)",
            )?;
        } else {
            app.ensure_surfaces_block()?;
            app.insert_before_end(
                names::SURFACES,
                "Gateway::new(false).dev_fallback(http::ops::route_table(&state))",
            )?;
        }
    } else if app.contains("http::ops::route_table(&state)") {
        app.replace_once(
            "http::ops::route_table(&state)",
            "http::jobs::route_table(&state).merge(http::ops::route_table(&state))",
        )?;
    } else if app.contains(".dev_fallback(") && !app.contains(".dev_fallback(http::") {
        app.replace_once(
            ".dev_fallback(",
            ".dev_fallback(http::jobs::route_table(&state)",
        )?;
    } else {
        app.ensure_surfaces_block()?;
        app.insert_before_end(
            names::SURFACES,
            "Gateway::new(false).dev_fallback(http::jobs::route_table(&state))",
        )?;
    }

    app.save(&project.app_rs())?;
    Ok(())
}

fn ensure_cargo_feature(project: &TrembitaProject, feature: &str) -> Result<(), AddError> {
    let path = project.cargo_toml();
    let content = fs::read_to_string(&path)?;
    let decl = format!("{feature} =");
    if content
        .lines()
        .any(|line| line.trim_start().starts_with(&decl))
    {
        return Ok(());
    }
    let anchor = "[features]";
    let Some(idx) = content.find(anchor) else {
        return Err(AddError::InvalidName(
            "Cargo.toml has no [features] section".into(),
        ));
    };
    let insert_at = idx + anchor.len();
    let mut out = content;
    out.insert_str(insert_at, &format!("\n{feature} = []\n"));
    fs::write(path, out)?;
    Ok(())
}

fn ensure_cargo_dependency(
    project: &TrembitaProject,
    name: &str,
    spec: &str,
) -> Result<bool, AddError> {
    let path = project.cargo_toml();
    let content = fs::read_to_string(&path)?;
    if content
        .lines()
        .any(|line| line.trim_start().starts_with(&format!("{name} =")))
    {
        return Ok(false);
    }
    let anchor = "[dependencies]";
    let Some(idx) = content.find(anchor) else {
        return Err(AddError::InvalidName(
            "Cargo.toml has no [dependencies] section".into(),
        ));
    };
    let insert_at = idx + anchor.len();
    let mut out = content;
    out.insert_str(insert_at, &format!("\n{name} = \"{spec}\""));
    fs::write(path, out)?;
    Ok(true)
}

fn validate_hosts(hosts: &[String]) -> Result<(), AddError> {
    if hosts.is_empty() {
        return Err(AddError::InvalidName(
            "at least one --hosts value required".into(),
        ));
    }
    for host in hosts {
        let ok = !host.is_empty()
            && host
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_');
        if ok {
            continue;
        }
        return Err(AddError::InvalidName(format!("host: {host}")));
    }
    Ok(())
}

fn validate_identifier(name: &str) -> Result<(), AddError> {
    let ok = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
        && name.chars().next().is_some_and(|c| c.is_ascii_alphabetic());
    if ok {
        Ok(())
    } else {
        Err(AddError::InvalidName(name.to_string()))
    }
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
        let manifest = fs::read_to_string(project.manifest_rs()).unwrap();
        assert!(manifest.contains("JobOpts::new(\"emails\")"));
        assert!(manifest.contains("HandleEmailsConsumer"));
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
        let manifest = fs::read_to_string(project.manifest_rs()).unwrap();
        assert!(manifest.contains("TopicOpts::topic(\"platform.events\")"));
    }

    #[test]
    fn add_workflow_creates_stub() {
        let (_dir, project) = sample_project();
        add_workflow(
            &project,
            &AddWorkflowOpts {
                prefix: "onboard".into(),
                module: None,
            },
        )
        .unwrap();
        assert!(project.workflows_dir().join("onboard.rs").is_file());
        let manifest = fs::read_to_string(project.manifest_rs()).unwrap();
        assert!(manifest.contains("WorkflowOpts::named("));
        assert!(manifest.contains("workflows::onboard::plan_for"));
        let main_rs = fs::read_to_string(project.main_rs()).unwrap();
        assert!(main_rs.contains("mod workflows;"));
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
        let manifest = fs::read_to_string(project.manifest_rs()).unwrap();
        assert!(manifest.contains("WorkerOpts::<CatalogWorker>::new(\"catalog\")"));
    }

    #[test]
    fn add_http_surface_creates_routes_and_surface() {
        let (_dir, project) = sample_project();
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
        assert!(project.http_dir().join("api.rs").is_file());
        let app = fs::read_to_string(project.app_rs()).unwrap();
        assert!(app.contains("http::api::route_table()"));
        assert!(app.contains("\"api.example.com\""));
    }

    #[test]
    fn add_static_site_filesystem() {
        let (_dir, project) = sample_project();
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
        assert!(project.http_dir().join("app_static.rs").is_file());
        let app = fs::read_to_string(project.app_rs()).unwrap();
        assert!(app.contains("http::app_static::route_table()"));
    }

    #[test]
    fn add_ops_routes_brownfield() {
        let (_dir, project) = sample_project();
        fs::remove_file(project.http_dir().join("ops.rs")).unwrap();
        let app_path = project.app_rs();
        let app = fs::read_to_string(&app_path).unwrap();
        let app = app.replace(".merge(http::ops::route_table(&state))", "");
        fs::write(&app_path, app).unwrap();
        add_ops_routes(&project).unwrap();
        assert!(project.http_dir().join("ops.rs").is_file());
        let app = fs::read_to_string(&app_path).unwrap();
        assert!(app.contains("http::ops::route_table(&state)"));
    }

    #[test]
    fn add_jobs_routes_after_ops_only() {
        let dir = tempdir().unwrap();
        let opts = NewProjectOpts {
            name: "ops-only".into(),
            output: dir.path().to_path_buf(),
            features: vec![AppFeature::Gateway],
            trembita_version: "0.3.2".into(),
            trembita_path: None,
        };
        let root = scaffold_project(&opts).unwrap();
        let project = TrembitaProject { root };
        assert!(!project.http_dir().join("jobs.rs").exists());
        add_jobs_routes(&project).unwrap();
        assert!(project.http_dir().join("jobs.rs").is_file());
        let app = fs::read_to_string(project.app_rs()).unwrap();
        assert!(app.contains("http::jobs::route_table(&state)"));
    }
}
