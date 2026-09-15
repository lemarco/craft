//! Delegate to showcase `trigger.sh` or built-in HTTP client.

use std::path::Path;
use std::process::Command;

use super::DevError;
use super::showcases::Showcase;

/// Run showcase trigger script with optional args.
pub fn run(showcase: &Showcase, workspace: &Path, args: &[String]) -> Result<(), DevError> {
    let script = showcase.example_dir(workspace).join("trigger.sh");
    if script.is_file() {
        let mut cmd = Command::new("bash");
        cmd.arg(&script)
            .current_dir(script.parent().expect("parent"));
        for arg in args {
            cmd.arg(arg);
        }
        let status = cmd.status().map_err(DevError::Io)?;
        if status.success() {
            return Ok(());
        }
        return Err(DevError::CommandFailed(format!(
            "{} failed (exit {status})",
            script.display()
        )));
    }
    Err(DevError::MissingScript(script))
}
