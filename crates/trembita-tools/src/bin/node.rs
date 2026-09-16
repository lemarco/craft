//! `trembita-node`: reference product node from env (empty state machine, ops + optional job queue).

use std::error::Error;

use clap::Parser;
use trembita::discovery::resolve_dns_seeds;
use trembita::{AppManifest, RunOpts, TrembitaApp, TrembitaConfigure};
use trembita_tools::node::config::{config_from_env, parse_seeds};

#[derive(Parser, Debug)]
#[command(name = "trembita-node", about = "Run a Trembita product node from env")]
struct Cli {
    /// Join seed(s): `node_id@host:port` (repeatable; merged with `TREMBITA_JOIN_SEEDS`).
    #[arg(long = "join-seed", value_name = "ID@HOST:PORT")]
    join_seeds: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    trembita::init_tracing();
    let cli = Cli::parse();
    let cfg = config_from_env()?;

    println!(
        "trembita-node v{} (protocol v{}, wire {})",
        trembita::VERSION,
        trembita::PROTOCOL_VERSION,
        trembita::proto::WIRE_CODEC,
    );

    let mut extra_seeds = Vec::new();
    if !cli.join_seeds.is_empty() {
        extra_seeds.extend(parse_seeds(&cli.join_seeds.join(","))?);
    }
    if let Some(dns) = &cfg.discovery {
        let resolved = resolve_dns_seeds(&dns.prefix, &dns.service, dns.replicas, dns.port).await?;
        println!("discovered {} seed(s) via DNS", resolved.len());
        extra_seeds.extend(resolved);
    }

    let app_cfg = cfg.into_app_config(extra_seeds);
    let manifest = AppManifest::new();
    let run_opts = RunOpts::for_manifest(&app_cfg, &manifest);

    let configure = TrembitaConfigure::default().with_local_gateway_apis();

    TrembitaApp::from_config(app_cfg)
        .manifest(manifest)
        .configure(configure)
        .run_with(run_opts)
        .await
}
