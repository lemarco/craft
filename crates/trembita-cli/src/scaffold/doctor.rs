//! `trembita doctor` — layout and wiring consistency checks.

use std::fs;
use std::path::Path;

use super::markers::{ensure_main_module, ensure_mod_declaration, names};
use super::project::TrembitaProject;

/// Single finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    /// `error`, `warn`, or `ok`.
    pub level: Level,
    /// Human-readable message.
    pub message: String,
}

/// Finding severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    /// Must fix.
    Error,
    /// Should fix.
    Warn,
    /// Informational pass.
    Ok,
}

/// Doctor report.
#[derive(Debug, Default)]
pub struct DoctorReport {
    /// All findings.
    pub findings: Vec<Finding>,
}

/// Result of `doctor --fix`.
#[derive(Debug, Default)]
pub struct DoctorFixReport {
    /// Number of auto-fixes applied.
    pub fixes_applied: usize,
}

impl DoctorReport {
    fn error(&mut self, message: impl Into<String>) {
        self.findings.push(Finding {
            level: Level::Error,
            message: message.into(),
        });
    }

    fn warn(&mut self, message: impl Into<String>) {
        self.findings.push(Finding {
            level: Level::Warn,
            message: message.into(),
        });
    }

    fn ok(&mut self, message: impl Into<String>) {
        self.findings.push(Finding {
            level: Level::Ok,
            message: message.into(),
        });
    }

    /// True when any error-level finding exists.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.findings.iter().any(|f| f.level == Level::Error)
    }

    /// Print human-readable report to stderr; returns exit code (0 = ok).
    #[must_use]
    pub fn print_and_exit_code(&self) -> i32 {
        for f in &self.findings {
            let tag = match f.level {
                Level::Error => "error",
                Level::Warn => "warn",
                Level::Ok => "ok",
            };
            eprintln!("[{tag}] {}", f.message);
        }
        i32::from(self.has_errors())
    }
}

/// Run all doctor checks on `project`.
#[must_use]
pub fn run_doctor(project: &TrembitaProject) -> DoctorReport {
    let mut report = DoctorReport::default();
    check_layout(project, &mut report);
    if let Ok(app) = fs::read_to_string(project.app_rs()) {
        check_markers(&app, &mut report);
        check_main_rs(project, &mut report);
        check_domain_boundary(project, &mut report);
        check_consumers(project, &app, &mut report);
        check_actors(project, &app, &mut report);
        check_http(project, &app, &mut report);
        check_topics(&app, &mut report);
    } else {
        report.error(format!("missing {}", project.app_rs().display()));
    }
    report
}

/// Apply safe auto-fixes (missing `mod` declarations, `main.rs` modules).
#[must_use]
pub fn doctor_fix(project: &TrembitaProject) -> DoctorFixReport {
    let mut fixes = 0usize;
    fixes += fix_mod_declarations(&project.consumers_dir());
    fixes += fix_mod_declarations(&project.actors_dir());
    fixes += fix_mod_declarations(&project.http_dir());
    if ensure_main_module(project, "consumers").unwrap_or(false) {
        fixes += 1;
    }
    if ensure_main_module(project, "actors").unwrap_or(false) {
        fixes += 1;
    }
    if project.http_dir().is_dir() && ensure_main_module(project, "http").unwrap_or(false) {
        fixes += 1;
    }
    DoctorFixReport {
        fixes_applied: fixes,
    }
}

fn fix_mod_declarations(dir: &Path) -> usize {
    if !dir.is_dir() {
        return 0;
    }
    let mod_rs = dir.join("mod.rs");
    let mut count = 0usize;
    for entry in walk_rs_files(dir) {
        if entry.file_name().is_some_and(|n| n == "mod.rs") {
            continue;
        }
        let Some(module) = entry.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        if ensure_mod_declaration(&mod_rs, module).unwrap_or(false) {
            count += 1;
        }
    }
    count
}

fn check_layout(project: &TrembitaProject, report: &mut DoctorReport) {
    let required = [
        project.main_rs(),
        project.app_rs(),
        project.root.join("src/config.rs"),
        project.consumers_dir(),
        project.domain_dir(),
    ];
    for path in required {
        if path.exists() {
            report.ok(format!("found {}", path.display()));
        } else {
            report.error(format!("missing required path: {}", path.display()));
        }
    }
}

fn check_markers(app: &str, report: &mut DoctorReport) {
    for marker in [names::IMPORTS, names::JOBS, names::TOPICS, names::WORKERS] {
        if app.contains(&format!("// {marker}")) {
            report.ok(format!("marker `{marker}` present"));
        } else {
            report.warn(format!(
                "marker `{marker}` missing — `trembita add` may not patch app.rs; re-run `trembita new` or add markers manually"
            ));
        }
    }
    if app.contains(".gateway(") {
        if app.contains(&format!("// {}", names::SURFACES)) {
            report.ok(format!("marker `{}` present", names::SURFACES));
        } else {
            report.warn(format!(
                "marker `{}` missing — `trembita add http-surface` may not patch app.rs",
                names::SURFACES
            ));
        }
    }
}

fn check_main_rs(project: &TrembitaProject, report: &mut DoctorReport) {
    let Ok(content) = fs::read_to_string(project.main_rs()) else {
        return;
    };
    if content.contains("TrembitaApp::builder") {
        report.warn("main.rs contains TrembitaApp::builder — wiring should live in app.rs");
    }
    if content.contains("App::new") && content.contains(".run().await") {
        report.ok("main.rs delegates to App::run()");
    } else {
        report.warn("main.rs should call App::new(...).run().await");
    }
    let lines = content.lines().count();
    if lines > 45 {
        report.warn(format!("main.rs is {lines} lines — keep boot-only (<45)"));
    }
}

fn check_domain_boundary(project: &TrembitaProject, report: &mut DoctorReport) {
    let domain = project.domain_dir();
    if !domain.is_dir() {
        return;
    }
    for entry in walk_rs_files(&domain) {
        let Ok(content) = fs::read_to_string(&entry) else {
            continue;
        };
        if content.contains("use trembita") || content.contains("trembita::") {
            report.error(format!(
                "domain hexagon violation: {} imports trembita",
                entry.display()
            ));
        }
    }
    if !report
        .findings
        .iter()
        .any(|f| f.message.contains("hexagon violation"))
    {
        report.ok("domain/ has no trembita imports");
    }
}

fn check_consumers(project: &TrembitaProject, app: &str, report: &mut DoctorReport) {
    let consumers_dir = project.consumers_dir();
    if !consumers_dir.is_dir() {
        return;
    }

    let mod_rs = consumers_dir.join("mod.rs");
    let mod_content = fs::read_to_string(&mod_rs).unwrap_or_default();

    for entry in walk_rs_files(&consumers_dir) {
        if entry.file_name().is_some_and(|n| n == "mod.rs") {
            continue;
        }
        let Ok(content) = fs::read_to_string(&entry) else {
            continue;
        };
        let module = entry
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");

        if !mod_content.contains(&format!("pub mod {module};")) {
            report.error(format!(
                "consumers/{module}.rs not declared in consumers/mod.rs"
            ));
        }

        for stream in extract_consumer_streams(&content) {
            let consumer_type = super::markers::consumer_type_name(module);
            let registered = app.contains(&format!("JobOpts::new(\"{stream}\")"))
                || app.contains(&format!("consumer(&{consumer_type})"));
            if registered {
                report.ok(format!("consumer stream `{stream}` wired in app.rs"));
            } else {
                report.error(format!(
                    "consumer `{stream}` in {} not registered in app.rs (.jobs / JobOpts)",
                    entry.display()
                ));
            }
        }
    }
}

fn check_actors(project: &TrembitaProject, app: &str, report: &mut DoctorReport) {
    let actors_dir = project.actors_dir();
    if !actors_dir.is_dir() {
        return;
    }
    let mod_rs = actors_dir.join("mod.rs");
    let mod_content = fs::read_to_string(&mod_rs).unwrap_or_default();
    for entry in walk_rs_files(&actors_dir) {
        if entry.file_name().is_some_and(|n| n == "mod.rs") {
            continue;
        }
        let Ok(content) = fs::read_to_string(&entry) else {
            continue;
        };
        let module = entry
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");
        if !mod_content.contains(&format!("pub mod {module};")) {
            report.error(format!("actors/{module}.rs not declared in actors/mod.rs"));
        }
        for group in extract_worker_groups(&content) {
            if app.contains(&format!("::new(\"{group}\")")) {
                report.ok(format!("actor group `{group}` registered in app.rs"));
            } else {
                report.warn(format!(
                    "actor file {} defines group `{group}` but app.rs has no WorkerOpts::new(\"{group}\")",
                    entry.display()
                ));
            }
        }
    }
}

fn check_http(project: &TrembitaProject, app: &str, report: &mut DoctorReport) {
    let http_dir = project.http_dir();
    if !http_dir.is_dir() {
        return;
    }
    let mod_rs = http_dir.join("mod.rs");
    let mod_content = fs::read_to_string(&mod_rs).unwrap_or_default();
    let main = fs::read_to_string(project.main_rs()).unwrap_or_default();
    if !main.contains("mod http;") {
        report
            .warn("src/http/ exists but main.rs has no `mod http;` — run `trembita doctor --fix`");
    }
    for entry in walk_rs_files(&http_dir) {
        if entry.file_name().is_some_and(|n| n == "mod.rs") {
            continue;
        }
        let module = entry
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");
        if !mod_content.contains(&format!("pub mod {module};")) {
            report.error(format!("http/{module}.rs not declared in http/mod.rs"));
        }
        if app.contains(&format!("http::{module}::route_table()")) {
            report.ok(format!("http surface `{module}` wired in app.rs"));
        } else if content_has_route_table(&entry) {
            report.warn(format!(
                "http/{module}.rs defines route_table() but app.rs has no matching surface"
            ));
        }
    }
}

fn content_has_route_table(path: &Path) -> bool {
    fs::read_to_string(path).is_ok_and(|c| c.contains("pub fn route_table()"))
}

fn check_topics(app: &str, report: &mut DoctorReport) {
    if app.contains(".topics(") {
        if app.contains("TopicOpts::topic") {
            report.ok("topics registered via TopicOpts");
        } else {
            report.warn(".topics() present but no TopicOpts::topic entries found");
        }
    }
    if app.contains(".gateway(") {
        if app.contains(".routes(|") {
            report.error(
                "GatewayOpts::routes() removed in 0.4.0 — use .surfaces(|state| Gateway::new(...))",
            );
        } else if app.contains(".surfaces(") {
            report.ok("gateway uses GatewayOpts::surfaces()");
        }
        if app.contains("SessionGate") && !app.contains(".session(") {
            report.warn("SessionGate imported but no .session(...) on a surface");
        }
        if app.contains(".session(")
            && app.contains(".cors(")
            && !app.contains("CorsPolicy::credentials")
        {
            report.warn(
                "SessionGate with CORS — prefer CorsPolicy::credentials(...) so browsers send cookies",
            );
        }
        if app.contains("Gateway::new(true)") && app.contains("dev_fallback") {
            report.warn("dev_fallback on Gateway::new(true) — disable in production");
        }
        if app.contains(".surface(") && app.contains(".hosts(") {
            let surfaces = app.matches(".surface(").count();
            let routes = app.matches(".routes(").count()
                + app.matches("route_table()").count()
                + app.matches("StaticSite").count();
            if routes < surfaces {
                report.warn("gateway surface with hosts but no .routes(...) or route_table()");
            }
        }
        if app.contains("with_jobs_api(true)")
            || app.contains("with_actors_api(true)")
            || app.contains("with_workflows_api(true)")
            || app.contains("with_introspect_api(true)")
        {
            report.error(
                "GatewayOpts::with_*_api removed in 0.5.0 — merge explicit route tables in .surfaces()",
            );
        }
        if app.contains("protect_product_apis(true)") {
            report.error(
                "protect_product_apis removed in 0.5.0 — use RouteTable::with_auth_mode(AuthMode::Identity)",
            );
        }
        if app.contains("AuthMode::Identity") {
            if app.contains("GatewayBearerIdentity") || app.contains(".identity(") {
                report.ok("gateway has identity for protected routes");
            } else {
                report.warn("identity-protected routes without gateway identity");
            }
        }
        if app.contains("admin_addr") {
            report.warn("admin_addr removed — merge http::ops::route_table in gateway surfaces");
        }
    }
}

fn walk_rs_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
    out.sort();
    out
}

fn extract_consumer_streams(source: &str) -> Vec<String> {
    let mut streams = Vec::new();
    for line in source.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("#[consumer(\"")
            && let Some(stream) = rest.split('"').next()
        {
            streams.push(stream.to_string());
        }
    }
    streams
}

fn extract_worker_groups(source: &str) -> Vec<String> {
    let mut groups = Vec::new();
    for line in source.lines() {
        if line.contains("Stateful worker for group `")
            && let Some(start) = line.find('`')
        {
            let rest = &line[start + 1..];
            if let Some(end) = rest.find('`') {
                groups.push(rest[..end].to_string());
            }
        }
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scaffold::{AppFeature, NewProjectOpts, scaffold_project};
    use tempfile::tempdir;

    #[test]
    fn doctor_passes_fresh_scaffold() {
        let dir = tempdir().unwrap();
        let opts = NewProjectOpts {
            name: "doc-test".into(),
            output: dir.path().to_path_buf(),
            features: AppFeature::defaults(),
            trembita_version: "0.3.2".into(),
            trembita_path: None,
        };
        let root = scaffold_project(&opts).unwrap();
        let project = TrembitaProject { root };
        let report = run_doctor(&project);
        assert!(!report.has_errors());
    }

    #[test]
    fn doctor_catches_unregistered_consumer() {
        let dir = tempdir().unwrap();
        let opts = NewProjectOpts {
            name: "doc-bad".into(),
            output: dir.path().to_path_buf(),
            features: AppFeature::defaults(),
            trembita_version: "0.3.2".into(),
            trembita_path: None,
        };
        let root = scaffold_project(&opts).unwrap();
        let project = TrembitaProject { root };
        std::fs::write(
            project.consumers_dir().join("orphan.rs"),
            r#"#[consumer("orphan")]
async fn handle_orphan(_: &[u8]) -> Result<(), ()> { Ok(()) }
"#,
        )
        .unwrap();
        let report = run_doctor(&project);
        assert!(report.has_errors());
    }

    #[test]
    fn doctor_fix_adds_missing_consumer_mod() {
        let dir = tempdir().unwrap();
        let opts = NewProjectOpts {
            name: "fix-test".into(),
            output: dir.path().to_path_buf(),
            features: AppFeature::defaults(),
            trembita_version: "0.3.2".into(),
            trembita_path: None,
        };
        let root = scaffold_project(&opts).unwrap();
        let project = TrembitaProject { root };
        std::fs::write(
            project.consumers_dir().join("extra.rs"),
            r#"use trembita::consumer;

#[consumer("extra")]
async fn handle_extra(_: &[u8]) -> Result<(), ()> { Ok(()) }
"#,
        )
        .unwrap();
        let fix = doctor_fix(&project);
        assert!(fix.fixes_applied >= 1);
        let mod_rs = fs::read_to_string(project.consumers_dir().join("mod.rs")).unwrap();
        assert!(mod_rs.contains("pub mod extra;"));
        let report = run_doctor(&project);
        assert!(
            !report
                .findings
                .iter()
                .any(|f| f.message.contains("not declared in consumers/mod.rs"))
        );
    }
}
