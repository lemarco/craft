//! Self-update showcase — run the workspace binary:
//!
//! ```bash
//! cargo run -p trembita --bin showcase-self-update --features http-jobs
//! ```

fn main() {
    eprintln!(
        "Use: cargo run -p trembita --bin showcase-self-update --features http-jobs"
    );
    eprintln!("See examples/self-update/README.md and ./cluster.sh");
    std::process::exit(1);
}
