//! trembita framework CLI — scaffold and manage product apps.

#![allow(missing_docs)]

use std::path::PathBuf;
use std::process;

use clap::{Parser, Subcommand, ValueHint};
use trembita_tools::scaffold::{
    AddActorOpts, AddConsumerOpts, AddTopicOpts, NewProjectOpts, TrembitaProject, add_actor,
    add_consumer, add_topic, default_output, parse_feature_list, run_doctor, scaffold_project,
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
                    add_actor(
                        &project,
                        &AddActorOpts {
                            group,
                            type_name,
                        },
                    )?;
                    eprintln!("Added actor in {}", project.actors_dir().display());
                }
            }
        }
        Command::Doctor { path } => {
            let project = resolve_project(path.as_deref())?;
            eprintln!("Checking {} …", project.root.display());
            let report = run_doctor(&project);
            let code = report.print_and_exit_code();
            if code != 0 {
                process::exit(code);
            }
        }
    }
    Ok(())
}

fn resolve_project(path: Option<&std::path::Path>) -> Result<TrembitaProject, Box<dyn std::error::Error>> {
    let start = path
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().expect("cwd"));
    Ok(TrembitaProject::discover(&start)?)
}
