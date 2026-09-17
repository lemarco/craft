//! trembita framework CLI — scaffold and manage product apps.

#![allow(missing_docs)]

use std::path::PathBuf;
use std::process;

use clap::{Parser, Subcommand, ValueHint};
use trembita_cli::{
    AddKind, AppTemplate, NewProjectOpts, TrembitaProject, default_output,
    resolve_scaffold_features, run_add, run_doctor, run_doctor_fix, run_explain_scale,
    scaffold_project,
};
#[cfg(debug_assertions)]
use trembita_cli::{
    dev_cluster_lb_down, dev_cluster_lb_up, dev_cluster_up, dev_http, dev_setup, dev_status,
    dev_stop, dev_trigger, dev_up, list_showcases,
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
        /// Preset: jobs, realtime, workflows, topics, api (sets default features + manifest stubs).
        #[arg(long, value_parser = parse_template_arg)]
        template: Option<AppTemplate>,
        /// Alias for `--template` (B-38): jobs | realtime | api | workflows | topics.
        #[arg(long, value_parser = parse_template_arg)]
        profile: Option<AppTemplate>,
        /// Comma-separated features (overrides `--template` when non-empty).
        #[arg(long, default_value = "")]
        features: String,
        /// Path to a local trembita checkout (uses path dependency instead of crates.io).
        #[arg(long, value_hint = ValueHint::DirPath)]
        trembita_path: Option<PathBuf>,
        /// crates.io version when not using --trembita-path.
        #[arg(long, default_value = env!("CARGO_PKG_VERSION"))]
        trembita_version: String,
    },
    /// Local showcase clusters (trembita repo, debug CLI only — not in `--release`).
    #[cfg(debug_assertions)]
    Dev {
        #[command(subcommand)]
        command: DevCommand,
    },
    /// Check layout and wiring consistency (read-only; does not modify sources).
    Doctor {
        /// Project root (default: discover from cwd).
        #[arg(long, value_hint = ValueHint::DirPath)]
        path: Option<PathBuf>,
        /// Stricter deploy checks (`deploy/.env`, compose, certs/listen env).
        #[arg(long)]
        preflight: bool,
        /// Apply safe mechanical fixes (e.g. simplify `.run()` in app.rs).
        #[arg(long)]
        fix: bool,
        /// Print product scale narrative + B-31 footguns (no full layout lint).
        #[arg(long)]
        explain_scale: bool,
    },
    /// Register a job stream or topic in `manifest.rs` (scaffold marker regions).
    Add {
        #[command(subcommand)]
        command: AddCommand,
        /// Project root (default: discover from cwd).
        #[arg(long, value_hint = ValueHint::DirPath)]
        path: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum AddCommand {
    /// Durable job stream + consumer stub under `src/consumers/`.
    Job {
        /// Stream name (e.g. `emails`).
        name: String,
    },
    /// Durable event topic.
    Topic {
        /// Topic name (e.g. `app.events`).
        name: String,
    },
}

#[cfg(debug_assertions)]
#[derive(Subcommand)]
enum DevCommand {
    /// List built-in product showcases and default ports.
    List,
    /// Build release binary, mint dev certs, build showcase client.
    Setup {
        /// Showcase id (`background-jobs`, `stateful-workers`, …).
        #[arg(long)]
        showcase: String,
    },
    /// Start a local multi-node cluster in the background.
    Up {
        /// Showcase id.
        #[arg(long)]
        showcase: String,
        /// Number of nodes (1–8).
        #[arg(long, default_value_t = 3)]
        nodes: u32,
        /// Run setup (certs + build) before starting.
        #[arg(long)]
        setup: bool,
    },
    /// Stop showcase processes.
    Stop {
        #[arg(long)]
        showcase: String,
    },
    /// Show running processes for a showcase.
    Status {
        #[arg(long)]
        showcase: String,
    },
    /// Run the showcase `trigger.sh` (same as manual `./trigger.sh`).
    Trigger {
        /// Showcase id.
        showcase: String,
        /// Arguments passed to `trigger.sh` (after `--`).
        /// Prefix with `job`, `topic`, or `workflow` to call the built-in HTTP client instead.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Product HTTP helpers (job / topic / workflow) without `trigger.sh`.
    Http {
        /// Showcase id (default gateway port).
        #[arg(long)]
        showcase: String,
        /// Override gateway host (`127.0.0.1:PORT`, no scheme).
        #[arg(long)]
        gateway: Option<String>,
        /// e.g. `job emails hello`, `topic orders evt`, `workflow run onboard-42`.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Local cluster: shared session secret + smoke hints (B-39; `--nodes 4` = B-42 elastic).
    ClusterUp {
        /// Showcase id (default: `realtime` — login + cluster cookies).
        #[arg(long)]
        showcase: Option<String>,
        /// Number of nodes (1–8). Use `4` for staged elastic join (seed + 3, then 4th joiner).
        #[arg(long, default_value_t = 3)]
        nodes: u32,
        /// Run setup (certs + build) before starting.
        #[arg(long)]
        setup: bool,
        /// Start optional nginx LB on :18290 (requires Docker).
        #[arg(long)]
        lb: bool,
    },
    /// Start nginx LB on :18290 (nodes must already be up; requires Docker).
    ClusterLbUp {
        /// Showcase id (ports for upstream).
        #[arg(long)]
        showcase: Option<String>,
        /// Upstream count (3 or 4). Default: probe `/ready` on node ports.
        #[arg(long)]
        nodes: Option<u32>,
    },
    /// Stop local cluster nginx LB container.
    ClusterLbDown,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        process::exit(1);
    }
}

fn parse_template_arg(s: &str) -> Result<AppTemplate, String> {
    AppTemplate::parse_name(s).ok_or_else(|| {
        format!(
            "unknown template `{s}` (expected: {})",
            AppTemplate::all()
                .iter()
                .map(|t| t.id())
                .collect::<Vec<_>>()
                .join(", ")
        )
    })
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::New {
            name,
            output,
            template,
            profile,
            features,
            trembita_path,
            trembita_version,
        } => {
            let template = profile.or(template);
            let features = resolve_scaffold_features(template, &features)?;
            let output =
                output.unwrap_or_else(|| default_output(&std::env::current_dir().expect("cwd")));
            let trembita_path = trembita_path.map(|p| {
                p.canonicalize()
                    .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default().join(p))
            });
            let opts = NewProjectOpts {
                name,
                output,
                features,
                template,
                trembita_version,
                trembita_path,
            };
            let root = scaffold_project(&opts)?;
            eprintln!("Created {}", root.display());
            eprintln!(
                "  cd {} && cargo run",
                root.file_name().unwrap().to_string_lossy()
            );
            eprintln!("  Edit src/manifest.rs and src/app.rs to register capabilities");
            eprintln!("  trembita doctor — verify layout and wiring");
        }
        #[cfg(debug_assertions)]
        Command::Dev { command } => match command {
            DevCommand::List => list_showcases(),
            DevCommand::Setup { showcase } => dev_setup(&showcase)?,
            DevCommand::Up {
                showcase,
                nodes,
                setup,
            } => dev_up(&showcase, nodes, setup)?,
            DevCommand::Stop { showcase } => dev_stop(&showcase)?,
            DevCommand::Status { showcase } => dev_status(&showcase)?,
            DevCommand::Trigger { showcase, args } => dev_trigger(&showcase, &args)?,
            DevCommand::Http {
                showcase,
                gateway,
                args,
            } => dev_http(Some(&showcase), gateway.as_deref(), &args)?,
            DevCommand::ClusterUp {
                showcase,
                nodes,
                setup,
                lb,
            } => dev_cluster_up(showcase.as_deref(), nodes, setup, lb)?,
            DevCommand::ClusterLbUp { showcase, nodes } => {
                dev_cluster_lb_up(showcase.as_deref(), nodes)?
            }
            DevCommand::ClusterLbDown => dev_cluster_lb_down()?,
        },
        Command::Doctor {
            path,
            preflight,
            fix,
            explain_scale,
        } => {
            let project = resolve_project(path.as_deref())?;
            if fix {
                let actions = run_doctor_fix(&project)?;
                if actions.is_empty() {
                    eprintln!("No automatic fixes applied.");
                } else {
                    for action in &actions {
                        eprintln!("fix: {action}");
                    }
                }
            }
            if explain_scale {
                eprintln!("Scale model for {} …", project.root.display());
                let report = run_explain_scale(&project);
                let code = report.print_and_exit_code();
                if code != 0 {
                    process::exit(code);
                }
                return Ok(());
            }
            eprintln!("Checking {} …", project.root.display());
            if preflight {
                eprintln!("Preflight mode (deploy env / compose / gateway ops)");
            }
            let report = run_doctor(&project, preflight);
            let code = report.print_and_exit_code();
            if code != 0 {
                process::exit(code);
            }
        }
        Command::Add { command, path } => {
            let project = resolve_project(path.as_deref())?;
            match command {
                AddCommand::Job { name } => {
                    run_add(&project, AddKind::Job, &name)?;
                    eprintln!(
                        "Added job stream `{name}` — edit src/consumers/ and src/manifest.rs"
                    );
                }
                AddCommand::Topic { name } => {
                    run_add(&project, AddKind::Topic, &name)?;
                    eprintln!("Added topic `{name}` in src/manifest.rs");
                }
            }
        }
    }
    Ok(())
}

fn resolve_project(
    path: Option<&std::path::Path>,
) -> Result<TrembitaProject, Box<dyn std::error::Error>> {
    let start = path.map_or_else(|| std::env::current_dir().expect("cwd"), PathBuf::from);
    Ok(TrembitaProject::discover(&start)?)
}
