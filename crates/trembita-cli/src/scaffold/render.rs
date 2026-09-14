//! Template rendering for scaffolded projects.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use super::features::AppFeature;
use super::new::NewProjectOpts;

/// `include_str!` paths anchored at [`CARGO_MANIFEST_DIR`](https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-set-for-crates) (`crates/trembita-cli`).
macro_rules! app_tpl {
    ($rel:literal) => {
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/templates/trembita-app/",
            $rel
        ))
    };
}

/// Error while writing scaffold files.
#[derive(Debug, thiserror::Error)]
pub enum ScaffoldError {
    /// Target directory already exists.
    #[error("target already exists: {0}")]
    Exists(PathBuf),
    /// Invalid project name.
    #[error("invalid project name: {0}")]
    InvalidName(String),
    /// I/O failure.
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

/// Variables substituted in static templates.
struct Vars {
    name: String,
    title: String,
    trembita_version: String,
    trembita_dep: String,
    trembita_doc_base: String,
}

impl Vars {
    fn from_opts(opts: &NewProjectOpts) -> Self {
        Self {
            name: opts.name.clone(),
            title: opts.name.replace('-', " "),
            trembita_version: opts.trembita_version.clone(),
            trembita_dep: opts.trembita_dependency_line(),
            trembita_doc_base: "https://gitlab.com/lemarco/trembita/-/blob/main".into(),
        }
    }

    fn apply(&self, raw: &str) -> String {
        raw.replace("{{PROJECT_NAME}}", &self.name)
            .replace("{{PROJECT_TITLE}}", &self.title)
            .replace("{{TREMBITA_VERSION}}", &self.trembita_version)
            .replace("{{TREMBITA_DEP}}", &self.trembita_dep)
            .replace("{{TREMBITA_DOC_BASE}}", &self.trembita_doc_base)
    }
}

/// Validate and create a new project tree.
pub fn scaffold_project(opts: &NewProjectOpts) -> Result<PathBuf, ScaffoldError> {
    validate_name(&opts.name)?;
    let root = opts.output.join(&opts.name);
    if root.exists() {
        return Err(ScaffoldError::Exists(root));
    }

    let vars = Vars::from_opts(opts);
    let features: HashSet<_> = opts.features.iter().copied().collect();

    fs::create_dir_all(root.join("src/consumers"))?;
    fs::create_dir_all(root.join("src/domain"))?;
    fs::create_dir_all(root.join("deploy"))?;

    if features.contains(&AppFeature::Actors) {
        fs::create_dir_all(root.join("src/actors"))?;
    }
    if features.contains(&AppFeature::Gateway) {
        fs::create_dir_all(root.join("src/http"))?;
    }
    if features.contains(&AppFeature::Workflows) {
        fs::create_dir_all(root.join("src/workflows"))?;
    }

    write_file(&root.join("Cargo.toml"), &generate_cargo_toml(opts))?;
    write_file(
        &root.join("README.md"),
        &vars.apply(app_tpl!("README.md.tpl")),
    )?;
    write_file(
        &root.join("deploy/docker-compose.yml"),
        &vars.apply(app_tpl!("deploy/docker-compose.yml.tpl")),
    )?;
    write_file(
        &root.join("deploy/.env.example"),
        &vars.apply(app_tpl!("deploy/env.example.tpl")),
    )?;
    write_file(
        &root.join("src/main.rs"),
        &generate_main_rs(opts, &features),
    )?;
    write_file(&root.join("src/app.rs"), &generate_app_rs(opts, &features))?;
    write_file(
        &root.join("src/config.rs"),
        &vars.apply(app_tpl!("src/config.rs.tpl")),
    )?;
    write_file(
        &root.join("src/consumers/mod.rs"),
        &vars.apply(app_tpl!("src/consumers/mod.rs.tpl")),
    )?;
    write_file(
        &root.join("src/consumers/sample.rs"),
        &vars.apply(app_tpl!("src/consumers/sample.rs.tpl")),
    )?;
    write_file(
        &root.join("src/domain/mod.rs"),
        &vars.apply(app_tpl!("src/domain/mod.rs.tpl")),
    )?;

    if features.contains(&AppFeature::Actors) {
        write_file(
            &root.join("src/actors/mod.rs"),
            &vars.apply(app_tpl!("src/actors/mod.rs.tpl")),
        )?;
    }
    if features.contains(&AppFeature::Gateway) {
        write_file(
            &root.join("src/http/mod.rs"),
            &generate_http_mod_rs(&features),
        )?;
        write_file(
            &root.join("src/http/ops.rs"),
            &vars.apply(app_tpl!("src/http/ops.rs.tpl")),
        )?;
        if features.contains(&AppFeature::Jobs) {
            write_file(
                &root.join("src/http/jobs.rs"),
                &vars.apply(app_tpl!("src/http/jobs.rs.tpl")),
            )?;
        }
    }
    if features.contains(&AppFeature::Workflows) {
        write_file(
            &root.join("src/workflows/mod.rs"),
            &vars.apply(app_tpl!("src/workflows/mod.rs.tpl")),
        )?;
    }

    Ok(root)
}

fn generate_http_mod_rs(features: &HashSet<AppFeature>) -> String {
    let mut mods = String::from("pub mod ops;\n");
    if features.contains(&AppFeature::Jobs) {
        mods.push_str("pub mod jobs;\n");
    }
    format!(
        r"//! Custom HTTP surfaces — wired via `GatewayOpts::surfaces()` in `app.rs`.
//!
//! Add surfaces with:
//! ```bash
//! trembita add http-surface api --hosts api.example.com
//! ```
//!
//! Each module exports `route_table() -> RouteTable`.

{mods}"
    )
}

fn validate_name(name: &str) -> Result<(), ScaffoldError> {
    let ok = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
        && name.chars().next().is_some_and(|c| c.is_ascii_lowercase());
    if ok {
        Ok(())
    } else {
        Err(ScaffoldError::InvalidName(name.to_string()))
    }
}

fn write_file(path: &Path, contents: &str) -> Result<(), ScaffoldError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, contents)?;
    Ok(())
}

fn generate_cargo_toml(opts: &NewProjectOpts) -> String {
    let mut features = opts.features.clone();
    features.sort_by_key(|f| f.cargo_name());

    let default_features: Vec<_> = features
        .iter()
        .map(|f| format!("\"{}\"", f.cargo_name()))
        .collect();

    let mut feature_lines = String::new();
    for f in &features {
        let deps = match f {
            AppFeature::ExternalBacklog => "trembita/external-backlog".to_string(),
            AppFeature::DomainOutbox => "trembita/domain-outbox".to_string(),
            _ => String::new(),
        };
        if deps.is_empty() {
            feature_lines.push_str(&format!("{} = []\n", f.cargo_name()));
        } else {
            feature_lines.push_str(&format!("{} = [\"{deps}\"]\n", f.cargo_name()));
        }
    }

    let mut extra_deps = opts.optional_adapter_lines();
    if opts.features.contains(&AppFeature::Gateway) {
        extra_deps.push_str("http = \"1\"\n");
    }
    if let Some(runtime) = opts.trembita_runtime_dependency_line() {
        extra_deps.push_str(&runtime);
        extra_deps.push('\n');
    }

    format!(
        r#"[package]
name = "{name}"
version = "0.1.0"
edition = "2024"
publish = false

[dependencies]
{trembita_dep}
tokio = {{ version = "1", features = ["rt-multi-thread", "macros", "signal"] }}
tracing = "0.1"
{extra_deps}
[features]
default = [{default_features}]
{feature_lines}"#,
        name = opts.name,
        trembita_dep = opts.trembita_dependency_line(),
        extra_deps = extra_deps,
        default_features = default_features.join(", "),
        feature_lines = feature_lines,
    )
}

fn generate_main_rs(opts: &NewProjectOpts, features: &HashSet<AppFeature>) -> String {
    let mut mods = vec![
        "mod app;".to_string(),
        "mod config;".to_string(),
        "mod consumers;".to_string(),
        "mod domain;".to_string(),
    ];
    if features.contains(&AppFeature::Actors) {
        mods.push("mod actors;".to_string());
    }
    if features.contains(&AppFeature::Gateway) {
        mods.push("mod http;".to_string());
    }
    if features.contains(&AppFeature::Workflows) {
        mods.push("mod workflows;".to_string());
    }

    let tracing = if features.contains(&AppFeature::Telemetry) {
        r#"    #[cfg(feature = "telemetry")]
    {
        use trembita_runtime::{TracingOpts, init_tracing_with_otlp};
        init_tracing_with_otlp(TracingOpts::from_env(env!("CARGO_PKG_NAME")));
    }
    #[cfg(not(feature = "telemetry"))]
    trembita::init_tracing();"#
            .to_string()
    } else {
        "    trembita::init_tracing();".to_string()
    };

    format!(
        r"//! {name} — trembita product app.

{mods}

use app::App;
use config::AppConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {{
{tracing}

    App::new(AppConfig::from_env()).run().await
}}
",
        name = opts.name,
        mods = mods.join("\n"),
        tracing = tracing,
    )
}

fn generate_app_rs(opts: &NewProjectOpts, features: &HashSet<AppFeature>) -> String {
    let mut imports = vec![
        "use std::sync::Arc;".to_string(),
        "use std::time::Duration;".to_string(),
        "use crate::config::AppConfig;".to_string(),
    ];
    let mut body = String::new();

    if features.contains(&AppFeature::Jobs) {
        imports.push(
            "use crate::consumers::sample::{HandleSampleConsumer, STREAM as SAMPLE_STREAM};"
                .to_string(),
        );
        imports.push(
            "use trembita::{IdempotencyOpts, InMemoryStore, JobOpts, RunOpts, TrembitaApp, TrembitaConfigure};".to_string(),
        );
    } else {
        imports.push("use trembita::{RunOpts, TrembitaApp, TrembitaConfigure};".to_string());
    }

    if features.contains(&AppFeature::Gateway) {
        imports.push(
            "use trembita::{Gateway, GatewayBearerIdentity, GatewayOpts, TrembitaGatewayState};"
                .to_string(),
        );
        imports.push("use crate::http;".to_string());
    }

    if features.contains(&AppFeature::Topics) {
        imports.push("use trembita::TopicOpts;".to_string());
    }
    if features.contains(&AppFeature::Actors) {
        imports.push("use trembita::{WorkerOpts, WorkerScale, workers};".to_string());
    }

    let imports_block = format!(
        "// trembita:imports\n{}\n// trembita:imports-end",
        imports.join("\n")
    );

    let mut builder = String::from("        TrembitaApp::builder()\n");
    builder.push_str("            .data_dir(&self.config.data_dir)\n");

    if features.contains(&AppFeature::Jobs) {
        body.push_str(
            r"        let idem_store = Arc::new(InMemoryStore::new());

",
        );
        builder.push_str(
            r#"            .jobs([
                // trembita:jobs
                JobOpts::new(SAMPLE_STREAM)
                .lease(Duration::from_secs(300))
                .default_max_attempts(5)
                .idempotency(IdempotencyOpts::by_dedup_key(
                    Arc::clone(&idem_store) as Arc<dyn trembita::actor_store::ActorStateStore>,
                    "job:",
                ))
                .consumer(&HandleSampleConsumer)
                .http_enqueue(true),
                // trembita:jobs-end
            ])
"#,
        );
    }

    if features.contains(&AppFeature::Topics) {
        builder.push_str(
            r#"            .topics([
                // trembita:topics
                TopicOpts::topic("app.events"),
                // trembita:topics-end
            ])
"#,
        );
    } else {
        builder.push_str(
            r"            // trembita:topics
            // trembita:topics-end
",
        );
    }

    if features.contains(&AppFeature::Actors) {
        builder.push_str(
            r"            .workers(workers!(
                // trembita:workers
                // trembita:workers-end
            ))
",
        );
    } else {
        builder.push_str(
            r"            // trembita:workers
            // trembita:workers-end
",
        );
    }

    if features.contains(&AppFeature::Workflows) {
        builder.push_str(
            r"            // Register workflows in src/workflows/ and wire .workflows([...]) here.
",
        );
    }

    if features.contains(&AppFeature::Gateway) {
        if features.contains(&AppFeature::Jobs) {
            builder.push_str(
                r"            .gateway(
                GatewayOpts::new(self.config.http_addr)
                    .identity(GatewayBearerIdentity::from_env())
                    .surfaces(|state| {
                        // trembita:surfaces
                        Gateway::new(false).dev_fallback(
                            http::jobs::route_table(&state)
                                .merge(http::ops::route_table(&state)),
                        )
                        // trembita:surfaces-end
                    }),
            )
",
            );
        } else {
            builder.push_str(
                r"            .gateway(
                GatewayOpts::new(self.config.http_addr)
                    .identity(GatewayBearerIdentity::from_env())
                    .surfaces(|state| {
                        // trembita:surfaces
                        Gateway::new(false)
                            .dev_fallback(http::ops::route_table(&state))
                        // trembita:surfaces-end
                    }),
            )
",
            );
        }
    }

    builder.push_str(
        r"            .configure(TrembitaConfigure {
                ..TrembitaConfigure::default()
            })
",
    );

    let run_opts = if features.contains(&AppFeature::Jobs) {
        "RunOpts::default().with_wait_queue(SAMPLE_STREAM)"
    } else {
        "RunOpts::default()"
    };
    builder.push_str(&format!("            .run({run_opts})\n"));
    builder.push_str("            .await\n");

    format!(
        r"//! {name} — [`TrembitaApp`](trembita::TrembitaApp) wiring.

{imports}

/// Application handle — owns config and runs the cluster.
pub struct App {{
    config: AppConfig,
}}

impl App {{
    /// Build from typed configuration.
    #[must_use]
    pub fn new(config: AppConfig) -> Self {{
        Self {{ config }}
    }}

    /// Start the trembita cluster and block until shutdown.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {{
{body}{builder}    }}
}}
",
        name = opts.name,
        imports = imports_block,
        body = body,
        builder = builder,
    )
}
