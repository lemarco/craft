//! Built-in product HTTP triggers via `trembita-showcase-client`.

use std::path::{Path, PathBuf};
use std::process::Command;

use super::DevError;
use super::showcases::Showcase;

/// Run `job`, `topic`, or `workflow` against a showcase gateway.
///
/// Args: `job <stream> <payload>`, `topic <name> <payload>`, or
/// `workflow run|resume <saga-id>`.
pub fn run(
    showcase: &Showcase,
    workspace: &Path,
    gateway_override: Option<&str>,
    args: &[String],
) -> Result<(), DevError> {
    if args.is_empty() {
        return Err(DevError::CommandFailed(usage()));
    }
    let client = showcase_client_bin(workspace);
    if !client.is_file() {
        return Err(DevError::CommandFailed(format!(
            "missing {} — run: trembita dev setup --showcase {}",
            client.display(),
            showcase.id
        )));
    }
    let gateway = resolve_gateway(showcase, gateway_override);
    let mut cmd = Command::new(&client);
    match args[0].as_str() {
        "job" => {
            let stream = args.get(1).ok_or_else(|| usage_err())?;
            let payload = args.get(2).ok_or_else(|| usage_err())?;
            cmd.args(["job", &gateway, stream, payload]);
        }
        "topic" => {
            let name = args.get(1).ok_or_else(|| usage_err())?;
            let payload = args.get(2).ok_or_else(|| usage_err())?;
            cmd.args(["topic", &gateway, name, payload]);
        }
        "workflow" => {
            let action = args.get(1).ok_or_else(|| usage_err())?;
            if action != "run" && action != "resume" {
                return Err(usage_err());
            }
            let saga = args.get(2).ok_or_else(|| usage_err())?;
            cmd.args(["workflow", action, &gateway, saga]);
        }
        _ => return Err(DevError::CommandFailed(usage())),
    }
    let status = cmd.status().map_err(DevError::Io)?;
    if status.success() {
        return Ok(());
    }
    Err(DevError::CommandFailed(format!(
        "{} failed (exit {status})",
        client.display()
    )))
}

/// Whether `args` select the built-in HTTP client instead of `trigger.sh`.
#[must_use]
pub fn is_builtin_http_args(args: &[String]) -> bool {
    matches!(
        args.first().map(String::as_str),
        Some("job" | "topic" | "workflow")
    )
}

#[must_use]
fn showcase_client_bin(workspace: &Path) -> PathBuf {
    workspace.join("target/debug/trembita-showcase-client")
}

#[must_use]
fn resolve_gateway(showcase: &Showcase, override_host: Option<&str>) -> String {
    let raw = override_host
        .map(str::to_string)
        .or_else(|| std::env::var("TREMBITA_HTTP").ok())
        .or_else(|| std::env::var("TREMBITA_GATEWAY").ok())
        .unwrap_or_else(|| showcase.listen_addr(1));
    raw.trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string()
}

fn usage() -> String {
    "usage: job <stream> <payload> | topic <name> <payload> | workflow run|resume <saga-id>"
        .to_string()
}

fn usage_err() -> DevError {
    DevError::CommandFailed(usage())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_defaults_to_showcase_listen() {
        let s = super::super::showcases::find("background-jobs").unwrap();
        assert_eq!(resolve_gateway(&s, None), "127.0.0.1:8090");
    }

    #[test]
    fn detects_builtin_prefix() {
        assert!(is_builtin_http_args(&["job".into(), "emails".into()]));
        assert!(!is_builtin_http_args(&["hello".into()]));
    }
}
