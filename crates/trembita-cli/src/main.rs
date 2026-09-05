//! trembita framework CLI — scaffold and manage product apps.

#![allow(missing_docs)]

use std::path::PathBuf;
use std::process;

use clap::{Parser, Subcommand, ValueHint};
use trembita_cli::{
    AddActorOpts, AddConsumerOpts, AddHttpSurfaceOpts, AddStaticSiteOpts, AddTopicOpts,
    NewProjectOpts, StaticSiteSource, TrembitaProject, add_actor, add_consumer, add_http_surface,
    add_static_site, add_topic, default_output, doctor_fix, parse_feature_list, run_doctor,
    scaffold_project,
};

#[derive(Parser)]
#[command(
    name = "trembita",
    about = "Trembita framework CLI — scaffold and manage product apps",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create a new trembita product app with the standard layout.
    New {
        /// Project name (lowercase, e.g. my-service).
        name: String,
        /// Parent directory (default: current directory).
        #[arg(long, value_hint = ValueHint::DirPath)]
        output: Option<PathBuf>,
        /// Comma-separated features: jobs, gateway, telemetry, topics, workflows,
        /// actors, external-backlog, domain-outbox.
        #[arg(long, default_value = "jobs,gateway,telemetry")]
        features: String,
        /// Path to a local trembita checkout (uses path dependency instead of crates.io).
        #[arg(long, value_hint = ValueHint::DirPath)]
        trembita_path: Option<PathBuf>,
        /// crates.io version when not using --trembita-path.
        #[arg(long, default_value = env!("CARGO_PKG_VERSION"))]
        trembita_version: String,
    },
    /// Add a component to an existing product app.
    Add {
        #[command(subcommand)]
        target: AddTarget,
        /// Project root (default: discover from cwd).
        #[arg(long, value_hint = ValueHint::DirPath)]
        path: Option<PathBuf>,
    },
    /// Check layout and wiring consistency.
    Doctor {
        /// Project root (default: discover from cwd).
        #[arg(long, value_hint = ValueHint::DirPath)]
        path: Option<PathBuf>,
        /// Apply safe auto-fixes (missing mod declarations).
        #[arg(long)]
        fix: bool,
    },
}

#[derive(Subcommand)]
enum AddTarget {
    /// Register a job consumer (`consumers/` + `.jobs()`).
    Consumer {
        /// Job stream name.
        stream: String,
        /// Rust module name (default: derived from stream).
        #[arg(long)]
        module: Option<String>,
        /// Lease duration in seconds.
        #[arg(long, default_value_t = 300)]
        lease: u64,
    },
    /// Register an event topic (`.topics()`).
    Topic {
        /// Topic name.
        name: String,
    },
    /// Register a stateful worker group (`actors/` + `.workers()`).
    Actor {
        /// Actor group / routing name.
        group: String,
        /// Rust worker type name (default: `{Group}Worker`).
        #[arg(long)]
        type_name: Option<String>,
    },
    /// Register a custom HTTP surface (`src/http/` + gateway `.surface()`).
    HttpSurface {
        /// Surface name (default module name).
        name: String,
        /// Comma-separated hostnames (e.g. `api.example.com,api.internal`).
        #[arg(long)]
        hosts: String,
        /// Rust module file name (default: derived from `name`).
        #[arg(long)]
        module: Option<String>,
    },
    /// Register a static SPA site (`StaticSite` + gateway `.surface()`).
    StaticSite {
        /// Site name (default module `{name}_static`).
        name: String,
        /// Comma-separated hostnames.
        #[arg(long)]
        hosts: String,
        /// Compile-time asset path relative to project root (`include_dir!`).
        #[arg(long, conflicts_with = "filesystem")]
        embedded: Option<String>,
        /// Filesystem asset path (default: `fe/{name}/dist`).
        #[arg(long, conflicts_with = "embedded")]
        filesystem: Option<String>,
        /// Rust module file name (default: `{name}_static`).
        #[arg(long)]
        module: Option<String>,
    },
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::New {
            name,
            output,
            features,
            trembita_path,
            trembita_version,
        } => {
            let features = parse_feature_list(&features)?;
            let output =
                output.unwrap_or_else(|| default_output(&std::env::current_dir().expect("cwd")));
            let opts = NewProjectOpts {
                name,
                output,
                features,
                trembita_version,
                trembita_path,
            };
            let root = scaffold_project(&opts)?;
            eprintln!("Created {}", root.display());
            eprintln!(
                "  cd {} && cargo run",
                root.file_name().unwrap().to_string_lossy()
            );
            eprintln!("  trembita add consumer <stream>  — register more job handlers");
            eprintln!("  trembita add http-surface <name> --hosts … — custom HTTP routes");
            eprintln!("  trembita doctor                 — verify layout and wiring");
        }
        Command::Add { target, path } => {
            let project = resolve_project(path.as_deref())?;
            match target {
                AddTarget::Consumer {
                    stream,
                    module,
                    lease,
                } => {
                    add_consumer(
                        &project,
                        &AddConsumerOpts {
                            stream,
                            module,
                            lease_secs: lease,
                        },
                    )?;
                    eprintln!("Added consumer in {}", project.consumers_dir().display());
                }
                AddTarget::Topic { name } => {
                    add_topic(&project, &AddTopicOpts { topic: name })?;
                    eprintln!("Registered topic in {}", project.app_rs().display());
                }
                AddTarget::Actor { group, type_name } => {
                    add_actor(&project, &AddActorOpts { group, type_name })?;
                    eprintln!("Added actor in {}", project.actors_dir().display());
                }
                AddTarget::HttpSurface {
                    name,
                    hosts,
                    module,
                } => {
                    add_http_surface(
                        &project,
                        &AddHttpSurfaceOpts {
                            name,
                            hosts: parse_hosts(&hosts)?,
                            module,
                        },
                    )?;
                    eprintln!("Added HTTP surface in {}", project.http_dir().display());
                }
                AddTarget::StaticSite {
                    name,
                    hosts,
                    embedded,
                    filesystem,
                    module,
                } => {
                    let source = match (embedded, filesystem) {
                        (Some(path), None) => StaticSiteSource::Embedded { path },
                        (None, Some(path)) => StaticSiteSource::Filesystem { path },
                        (None, None) => StaticSiteSource::Filesystem {
                            path: format!("fe/{name}/dist"),
                        },
                        _ => unreachable!("clap conflicts_with"),
                    };
                    add_static_site(
                        &project,
                        &AddStaticSiteOpts {
                            name: name.clone(),
                            hosts: parse_hosts(&hosts)?,
                            source,
                            module,
                        },
                    )?;
                    eprintln!("Added static site in {}", project.http_dir().display());
                }
            }
        }
        Command::Doctor { path, fix } => {
            let project = resolve_project(path.as_deref())?;
            eprintln!("Checking {} …", project.root.display());
            if fix {
                let fix_report = doctor_fix(&project);
                if fix_report.fixes_applied > 0 {
                    eprintln!("Applied {} fix(es)", fix_report.fixes_applied);
                } else {
                    eprintln!("No auto-fixes needed");
                }
            }
            let report = run_doctor(&project);
            let code = report.print_and_exit_code();
            if code != 0 {
                process::exit(code);
            }
        }
    }
    Ok(())
}

fn resolve_project(
    path: Option<&std::path::Path>,
) -> Result<TrembitaProject, Box<dyn std::error::Error>> {
    let start = path
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().expect("cwd"));
    Ok(TrembitaProject::discover(&start)?)
}

fn parse_hosts(raw: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let hosts: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .map(String::from)
        .collect();
    if hosts.is_empty() {
        return Err("at least one host required (--hosts)".into());
    }
    Ok(hosts)
}
