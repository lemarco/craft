use std::path::PathBuf;

use clap::{Parser, Subcommand};
use craft_ops::backup::{OpsError, export_local, import_local, pull_object, push_object};

#[derive(Parser)]
#[command(name = "craft-ops", about = "Craft cluster operational tools")]
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

#[tokio::main]
async fn main() -> Result<(), OpsError> {
    let cli = Cli::parse();
    match cli.command {
        Command::Backup { action } => match action {
            BackupAction::Export { data_dir, archive } => export_local(&data_dir, &archive)?,
            BackupAction::Import { data_dir, archive } => import_local(&data_dir, &archive)?,
            BackupAction::Push { archive, dest } => push_object(&archive, &dest).await?,
            BackupAction::Pull { src, archive } => pull_object(&src, &archive).await?,
        },
    }
    Ok(())
}
