//! Self-update showcase — run the workspace binary:
//!
//! ```bash
//! cargo run -p trembita-showcase --bin showcase-self-update
//! ```

fn main() {
    eprintln!("Use: cargo run -p trembita-showcase --bin showcase-self-update");
    eprintln!("See examples/self-update/README.md and ./cluster.sh");
    std::process::exit(1);
}
