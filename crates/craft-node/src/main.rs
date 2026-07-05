//! `craft-node` — reference runner for a single craft cluster node.
//!
//! Wave 4 wires this to `CraftCluster::builder()` driven by env/CLI
//! (`NODE_ID`, `LISTEN_ADDR`, `JOIN_ADDR`, ...). For now it reports build info.

fn main() {
    println!(
        "craft-node v{} (protocol v{})",
        craft::VERSION,
        craft::PROTOCOL_VERSION
    );
    println!("scaffold: cluster runner not yet wired (see docs/backlog.md, Wave 4).");
}
