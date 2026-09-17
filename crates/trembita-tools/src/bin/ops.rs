//! CLI for snapshot backup/restore and cluster upgrade operator (`trembita-ops`).
#![allow(missing_docs)] // publish = false — internal ops binary

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use trembita_tools::ops::backup::{OpsError, export_local, import_local, pull_object, push_object};
use trembita_tools::ops::upgrade::{RunUpgradeOpts, UpgradeManifest, UpgradeOpsError, run_upgrade};

#[derive(Parser)]
#[command(name = "trembita-ops", about = "Trembita cluster operational tools")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Snapshot backup and restore.
    Backup {
        #[command(subcommand)]
        action: BackupAction,
    },
    /// B-54 — drive in-cluster rolling upgrade over HTTP (no SSH fleet loop).
    Upgrade {
        #[command(subcommand)]
        action: UpgradeAction,
    },
}

#[derive(Subcommand)]
enum BackupAction {
    /// Create `archive` from a node `data_dir`.
    Export {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        archive: PathBuf,
    },
    /// Restore `data_dir` from `archive`.
    Import {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        archive: PathBuf,
    },
    /// Upload a tarball to object storage (`s3://`, `gs://`, `file://`).
    Push {
        #[arg(long)]
        archive: PathBuf,
        #[arg(long)]
        dest: String,
    },
    /// Download a tarball from object storage.
    Pull {
        #[arg(long)]
        src: String,
        #[arg(long)]
        archive: PathBuf,
    },
}

#[derive(Subcommand)]
enum UpgradeAction {
    /// Preflight, POST desired manifest, poll until fleet_ready, post /ready smoke.
    Run {
        /// Seed or LB URL (`http://host:port`, no trailing path).
        #[arg(long)]
        gateway: String,
        #[arg(long)]
        app_version: String,
        #[arg(long)]
        url: String,
        #[arg(long)]
        sha256_hex: String,
        /// Bearer token (default: `GATEWAY_TOKEN` / `TREMBITA_GATEWAY_TOKEN`).
        #[arg(long)]
        bearer_token: Option<String>,
        /// Max wait for fleet_ready (e.g. `15m`, `900s`).
        #[arg(long, default_value = "900s")]
        timeout: String,
        /// Poll interval for upgrade status (e.g. `5s`).
        #[arg(long, default_value = "5s")]
        poll_interval: String,
        /// Skip `/ready` and ops-summary preflight.
        #[arg(long)]
        skip_preflight: bool,
        /// Skip final `/ready` smoke after fleet_ready.
        #[arg(long)]
        skip_post_smoke: bool,
    },
}

fn parse_duration(label: &str, raw: &str) -> Result<Duration, String> {
    if let Some(secs) = raw.strip_suffix('s') {
        secs.parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|e| format!("{label}: {e}"))
    } else if let Some(mins) = raw.strip_suffix('m') {
        mins.parse::<u64>()
            .map(|m| Duration::from_secs(m.saturating_mul(60)))
            .map_err(|e| format!("{label}: {e}"))
    } else {
        Err(format!("{label}: use Ns or Nm (e.g. 900s, 15m)"))
    }
}

#[tokio::main]
async fn main() -> Result<(), OpsCliError> {
    let cli = Cli::parse();
    match cli.command {
        Command::Backup { action } => match action {
            BackupAction::Export { data_dir, archive } => export_local(&data_dir, &archive)?,
            BackupAction::Import { data_dir, archive } => import_local(&data_dir, &archive)?,
            BackupAction::Push { archive, dest } => push_object(&archive, &dest).await?,
            BackupAction::Pull { src, archive } => pull_object(&src, &archive).await?,
        },
        Command::Upgrade { action } => match action {
            UpgradeAction::Run {
                gateway,
                app_version,
                url,
                sha256_hex,
                bearer_token,
                timeout,
                poll_interval,
                skip_preflight,
                skip_post_smoke,
            } => {
                let mut opts = RunUpgradeOpts::with_gateway_and_manifest(
                    gateway,
                    UpgradeManifest {
                        app_version,
                        url,
                        sha256_hex,
                    },
                );
                opts.bearer_token = bearer_token;
                opts.timeout = parse_duration("timeout", &timeout).map_err(OpsCliError::Usage)?;
                opts.poll_interval =
                    parse_duration("poll-interval", &poll_interval).map_err(OpsCliError::Usage)?;
                opts.preflight = !skip_preflight;
                opts.skip_post_smoke = skip_post_smoke;
                run_upgrade(opts).await?;
            }
        },
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum OpsCliError {
    #[error(transparent)]
    Backup(#[from] OpsError),
    #[error(transparent)]
    Upgrade(#[from] UpgradeOpsError),
    #[error("{0}")]
    Usage(String),
}
