//! Template rendering for scaffolded projects.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use super::features::AppFeature;
use super::new::NewProjectOpts;
use super::template::AppTemplate;

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
    fs::create_dir_all(root.join("src/capabilities"))?;
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
    write_file(
        &root.join("src/manifest.rs"),
        &generate_manifest_rs(opts, &features),
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
    if features.contains(&AppFeature::Jobs) {
        write_file(
            &root.join("src/consumers/sample.rs"),
            &vars.apply(app_tpl!("src/consumers/sample.rs.tpl")),
        )?;
    }
    write_file(
        &root.join("src/domain/mod.rs"),
        &vars.apply(app_tpl!("src/domain/mod.rs.tpl")),
    )?;
    let capabilities_mod = match opts.template {
        Some(AppTemplate::Realtime) => generate_realtime_capabilities_mod(),
        Some(AppTemplate::Workflows) => {
            "//! Capability groups — saga side effects.\n\npub mod onboarding;\n".to_string()
        }
        _ => vars.apply(app_tpl!("src/capabilities/mod.rs.tpl")),
    };
    write_file(&root.join("src/capabilities/mod.rs"), &capabilities_mod)?;
    match opts.template {
        Some(AppTemplate::Realtime) => {
            write_file(
                &root.join("src/capabilities/chat.rs"),
                &generate_realtime_chat_capability(),
            )?;
        }
        Some(AppTemplate::Workflows) => {
            write_file(
                &root.join("src/capabilities/onboarding.rs"),
                &vars.apply(app_tpl!("src/capabilities/onboarding.rs.tpl")),
            )?;
        }
        _ => {
            write_file(
                &root.join("src/capabilities/ping.rs"),
                &vars.apply(app_tpl!("src/capabilities/ping.rs.tpl")),
            )?;
        }
    }

    if features.contains(&AppFeature::Actors) {
        write_file(
            &root.join("src/actors/mod.rs"),
            &generate_actors_mod_rs(opts),
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
        let product_rs = if opts.template == Some(AppTemplate::Realtime) {
            generate_realtime_product_rs()
        } else {
            vars.apply(app_tpl!("src/http/product.rs.tpl"))
        };
        write_file(&root.join("src/http/product.rs"), &product_rs)?;
    }
    if features.contains(&AppFeature::Workflows) {
        let workflows_mod = if opts.template == Some(AppTemplate::Workflows) {
            "//! Saga workflows — register via `.workflows([...])` in `src/manifest.rs`.\n\npub mod onboarding;\n".to_string()
        } else {
            vars.apply(app_tpl!("src/workflows/mod.rs.tpl"))
        };
        write_file(&root.join("src/workflows/mod.rs"), &workflows_mod)?;
        if opts.template == Some(AppTemplate::Workflows) {
            write_file(
                &root.join("src/workflows/onboarding.rs"),
                &vars.apply(app_tpl!("src/workflows/onboarding.rs.tpl")),
            )?;
        }
    }

    Ok(root)
}

fn generate_http_mod_rs(features: &HashSet<AppFeature>) -> String {
    let mut mods = String::from("pub mod ops;\npub mod product;\n");
    if features.contains(&AppFeature::Jobs) {
        mods.push_str("pub mod jobs;\n");
    }
    format!(
        r"//! HTTP route modules — ops/jobs tables are defaults via [`TrembitaApp::from_config`](trembita::TrembitaApp::from_config).
//!
//! Edit [`product::route_table`](product::route_table) for app-specific routes (`.gateway_routes()` in `app.rs`).
//! Host split: [`Gateway::surface_hosts`](trembita::Gateway::surface_hosts) in a custom `.gateway(GatewayOpts::from_env()?.surfaces(...))`.

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
        "mod manifest;".to_string(),
    ];
    mods.push("mod capabilities;".to_string());
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {{
{tracing}

    App::new(config::from_env()?).run().await
}}
",
        name = opts.name,
        mods = mods.join("\n"),
        tracing = tracing,
    )
}

fn generate_manifest_rs(opts: &NewProjectOpts, features: &HashSet<AppFeature>) -> String {
    let mut imports = vec!["use trembita::AppManifest;".to_string()];

    if features.contains(&AppFeature::Jobs) {
        imports.push("use std::sync::Arc;".to_string());
        imports.push(
            "use crate::consumers::sample::{HandleSampleConsumer, STREAM as SAMPLE_STREAM};"
                .to_string(),
        );
        imports.push(
            "use trembita::{IdempotencyOpts, InMemoryStore, JobOpts, JobsPreset};".to_string(),
        );
    }
    if features.contains(&AppFeature::Topics) {
        imports.push("use trembita::TopicOpts;".to_string());
    }
    match opts.template {
        Some(AppTemplate::Realtime) => {
            imports.push("use crate::capabilities::chat;".to_string());
        }
        Some(AppTemplate::Workflows) => {
            imports.push("use crate::capabilities::onboarding;".to_string());
            imports.push(
                "use crate::workflows::onboarding::{build_plan, run_onboarding_plan};".to_string(),
            );
        }
        _ => {
            imports.push("use crate::capabilities::ping::{PingState, ping_op};".to_string());
            imports.push("use trembita::{CapGroup, CapManifest};".to_string());
        }
    }

    let imports_block = format!(
        "// trembita:imports\n{}\n// trembita:imports-end",
        imports.join("\n")
    );

    let mut body = String::new();
    let mut chain = String::from("    AppManifest::new()");

    if features.contains(&AppFeature::Jobs) {
        body.push_str("    let idem_store = Arc::new(InMemoryStore::new());\n\n");
        chain.push_str(
            r#"
        .jobs([
            // trembita:jobs
            JobsPreset::idempotent_stream(
                    SAMPLE_STREAM,
                    &HandleSampleConsumer,
                    Arc::clone(&idem_store) as Arc<dyn trembita::actor_store::ActorStateStore>,
                    "job:",
                ),
            // trembita:jobs-end
        ])"#,
        );
    }

    if features.contains(&AppFeature::Topics) {
        chain.push_str(
            "
        .topics([
            // trembita:topics
            TopicOpts::topic(\"app.events\"),
            // trembita:topics-end
        ])",
        );
    }

    match opts.template {
        Some(AppTemplate::Realtime) => {
            chain.push_str(
                r#"
        .capabilities(
            // trembita:capabilities
            chat::manifest(),
            // trembita:capabilities-end
        )"#,
            );
        }
        Some(AppTemplate::Workflows) => {
            chain.push_str(
                r#"
        .capabilities(
            // trembita:capabilities
            onboarding::manifest(),
            // trembita:capabilities-end
        )"#,
            );
        }
        _ => {
            chain.push_str(
                r#"
        .capabilities(
            // trembita:capabilities
            CapManifest::new().group(
                CapGroup::<PingState>::with_state("app")
                    .instances(1)
                    .op(ping_op()),
            ),
            // trembita:capabilities-end
        )"#,
            );
        }
    }

    if features.contains(&AppFeature::Workflows) {
        imports.push("use trembita::WorkflowOpts;".to_string());
        if opts.template == Some(AppTemplate::Workflows) {
            chain.push_str(
                r#"
        .workflows([
            // trembita:workflows
            WorkflowOpts::named("onboard", build_plan, run_onboarding_plan),
            // trembita:workflows-end
        ])"#,
            );
        } else {
            chain.push_str(
                r"
        .workflows([
            // trembita:workflows
            // trembita:workflows-end
        ])",
            );
        }
    }

    format!(
        r"//! {name} — product capability registry ([`AppManifest`](trembita::AppManifest)).
//!
//! Edit the `// trembita:*` marker regions below when adding capabilities.

{imports}

/// Jobs, topics, workers, and workflows for this app.
#[must_use]
pub fn build() -> AppManifest {{
{body}{chain}
}}
",
        name = opts.name,
        imports = imports_block,
        body = body,
        chain = chain,
    )
}

fn generate_app_rs(opts: &NewProjectOpts, features: &HashSet<AppFeature>) -> String {
    let mut imports = vec![
        "use crate::config::AppConfig;".to_string(),
        "use crate::manifest;".to_string(),
        "use trembita::TrembitaApp;".to_string(),
    ];

    if opts.template == Some(AppTemplate::Realtime) {
        imports.push(
            "use trembita::{AuthMode, RouteTable, TrembitaGatewayState, WsMessage, mount_sticky_websocket, server_stream};".to_string(),
        );
        imports.push("use trembita::futures_util::{SinkExt, StreamExt};".to_string());
        imports.push("use crate::capabilities::chat::Append;".to_string());
        imports.push("use trembita::CapRequest;".to_string());
    }

    if features.contains(&AppFeature::Gateway) {
        imports.push("use crate::http;".to_string());
    }

    let imports_block = format!(
        "// trembita:imports\n{}\n// trembita:imports-end",
        imports.join("\n")
    );

    let mut preamble = String::new();
    if opts.template == Some(AppTemplate::Realtime) {
        preamble.push_str(
            r#"        fn ws_chat_routes(state: TrembitaGatewayState) -> RouteTable {
            mount_sticky_websocket::<Append>(
                RouteTable::new(),
                "/ws",
                AuthMode::Open,
                state,
                None,
                |sticky| {
                    Box::pin(async move {
                        let mut ws = server_stream(sticky.stream).await;
                        let mut handle = sticky.handle;
                        while let Some(Ok(msg)) = ws.next().await {
                            if let WsMessage::Text(text) = msg {
                                let text = text.to_string();
                                if handle
                                    .fire_cap(Append { text: text.clone() })
                                    .await
                                    .is_ok()
                                {
                                    let _ = ws
                                        .send(WsMessage::Text(format!("ok: {text}").into()))
                                        .await;
                                }
                            }
                        }
                    })
                },
            )
        }

"#,
        );
    }

    let mut builder = String::from(
        "        let cfg = self.config;\n        let manifest = manifest::build();\n        TrembitaApp::from_config(cfg)?\n",
    );
    builder.push_str("            .manifest(manifest)\n");

    if features.contains(&AppFeature::Gateway) {
        builder.push_str(
            "            .without_actors_api() // opt in via WorkerOpts::http_cast(true) in manifest\n",
        );
        if opts.template == Some(AppTemplate::Realtime) {
            builder.push_str(
                r"            .gateway_routes(|state| {
                // trembita:gateway-routes
                let mut table = http::product::route_table(state.clone());
                table.merge(ws_chat_routes(state));
                table
                // trembita:gateway-routes-end
            })
",
            );
        } else {
            builder.push_str(
                r"            .gateway_routes(|state| {
                // trembita:gateway-routes
                http::product::route_table(state)
                // trembita:gateway-routes-end
            })
",
            );
        }
    }

    builder.push_str("            .run()\n");
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
{preamble}{builder}    }}
}}
",
        name = opts.name,
        imports = imports_block,
        preamble = preamble,
        builder = builder,
    )
}

fn generate_actors_mod_rs(_opts: &NewProjectOpts) -> String {
    include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/templates/trembita-app/src/actors/mod.rs.tpl"
    ))
    .into()
}

fn generate_realtime_capabilities_mod() -> String {
    "//! Capability groups — sticky session chat in `chat`.\n\npub mod chat;\n".to_string()
}

fn generate_realtime_product_rs() -> String {
    r"//! App-specific HTTP routes — extend with login/session (see `examples/realtime`).

use trembita::{ProductRoutes, TrembitaGatewayState};
use trembita_http::RouteTable;

/// Custom product routes (webhooks, BFF handlers, capability HTTP, …).
#[must_use]
pub fn route_table(_state: TrembitaGatewayState) -> RouteTable {
    ProductRoutes::new()
        // trembita:product-routes
        .build()
}
"
    .to_string()
}

fn generate_realtime_chat_capability() -> String {
    r#"//! Chat — sticky session append on group `chat`.

use serde::{Deserialize, Serialize};
use trembita::{cap_handler, cap_register_chain, CapError, CapGroup, CapManifest};

#[derive(Default)]
struct State {
    history: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AppendAck {
    pub lines: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Append {
    /// Line from WebSocket (or HTTP when wired).
    pub text: String,
}

#[cap_handler(group = "chat")]
async fn append(msg: Append, state: &mut State) -> Result<AppendAck, CapError> {
    state.history.push(msg.text.clone());
    println!("[chat] {}", msg.text);
    Ok(AppendAck {
        lines: state.history.len() as u64,
    })
}

#[must_use]
pub fn manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<State>::for_cap::<Append>().per_node(),
        append_register,
    ))
}
"#
    .to_string()
}
