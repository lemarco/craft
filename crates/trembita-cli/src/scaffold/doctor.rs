//! `trembita doctor` — layout and wiring consistency checks.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use super::markers::names;
use super::project::TrembitaProject;

/// Single finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    /// `error`, `warn`, or `ok`.
    pub level: Level,
    /// Human-readable message.
    pub message: String,
    /// Optional fix hint (lint v2, B-38).
    pub suggestion: Option<String>,
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

impl DoctorReport {
    fn error(&mut self, message: impl Into<String>) {
        self.error_with_suggestion(message, Option::<String>::None);
    }

    fn error_with_suggestion(
        &mut self,
        message: impl Into<String>,
        suggestion: Option<impl Into<String>>,
    ) {
        self.findings.push(Finding {
            level: Level::Error,
            message: message.into(),
            suggestion: suggestion.map(Into::into),
        });
    }

    fn warn(&mut self, message: impl Into<String>) {
        self.warn_with_suggestion(message, Option::<String>::None);
    }

    fn warn_with_suggestion(
        &mut self,
        message: impl Into<String>,
        suggestion: Option<impl Into<String>>,
    ) {
        self.findings.push(Finding {
            level: Level::Warn,
            message: message.into(),
            suggestion: suggestion.map(Into::into),
        });
    }

    fn ok(&mut self, message: impl Into<String>) {
        self.findings.push(Finding {
            level: Level::Ok,
            message: message.into(),
            suggestion: None,
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
            if let Some(s) = &f.suggestion {
                eprintln!("       → {s}");
            }
        }
        i32::from(self.has_errors())
    }
}

/// Founder scale narrative + B-31 footguns only ([`--explain-scale`](../../docs/decisions/capability-dx.md#founder-dx-v2-b-38)).
#[must_use]
pub fn run_explain_scale(project: &TrembitaProject) -> DoctorReport {
    let mut report = DoctorReport::default();
    let Ok(manifest) = fs::read_to_string(project.manifest_rs()) else {
        report.error(format!("missing {}", project.manifest_rs().display()));
        return report;
    };
    explain_scale_narrative(project, &manifest, &mut report);
    check_capability_scale_footguns(project, &manifest, &mut report);
    report
}

fn explain_scale_narrative(project: &TrembitaProject, manifest: &str, report: &mut DoctorReport) {
    let cap_dir = project.capabilities_dir();
    let keyed = capabilities_have_keyed_handlers(&cap_dir);
    let shared_ram = capabilities_likely_shared_ram(&cap_dir);
    let uses_queued = capability_queued_wiring(manifest, &cap_dir);
    let fixed_one =
        manifest.contains(".instances(1)") || capabilities_declare_instances_one(&cap_dir);
    let per_node = manifest.contains(".per_node()") || capabilities_declare_per_node(&cap_dir);
    let session_ops = capability_uses_session_route(manifest, &cap_dir);

    report.ok(
        "Founder scale (B-28/B-31): stateless inline caps default to PerNode — add VPS with the same binary to grow handler hosts",
    );
    if uses_queued {
        report.ok(
            "Queued capabilities: job stream depth scales with consumers; handler placement follows CapGroup scale (not the queue shard count alone)",
        );
    }
    if keyed {
        report.ok(
            "Keyed handlers: one logical owner per key — use for entity-scoped RAM or single-writer semantics",
        );
    }
    if session_ops {
        report.ok(
            "Session routes: sticky to one host for the session TTL — pair with `.per_node()` for a pool on every node (realtime)",
        );
    }
    if fixed_one && !shared_ram {
        report.warn_with_suggestion(
            "Manifest declares `.instances(1)` on a likely stateless group",
            Some("Remove `.instances(1)` to use automatic PerNode when adding nodes"),
        );
    }
    if !fixed_one && !per_node && !keyed && !uses_queued {
        report.ok(
            "No Fixed(1) foot-gun detected — marker-state groups should scale PerNode by default",
        );
    }
    if manifest.contains("TREMBITA_COORDINATION_PROFILE")
        || manifest.contains("with_coordination_growth_preset")
    {
        report.ok("Coordination growth preset referenced — see docs/getting-started.md § B-37");
    }
}

/// Run all doctor checks on `project`.
///
/// With `preflight`, deploy env / compose checks are stricter (pre-push / pre-deploy).
#[must_use]
pub fn run_doctor(project: &TrembitaProject, preflight: bool) -> DoctorReport {
    let mut report = DoctorReport::default();
    check_layout(project, &mut report);
    let Ok(app) = fs::read_to_string(project.app_rs()) else {
        report.error(format!("missing {}", project.app_rs().display()));
        return report;
    };
    let Ok(manifest) = fs::read_to_string(project.manifest_rs()) else {
        report.error(format!("missing {}", project.manifest_rs().display()));
        return report;
    };
    check_markers(&manifest, &mut report);
    check_app_wiring(project, &app, &mut report);
    check_main_rs(project, &mut report);
    check_domain_boundary(project, &mut report);
    check_domain_module_declared(project, &mut report);
    check_consumers(project, &manifest, &mut report);
    check_capabilities(project, &manifest, &mut report);
    check_capability_scale_footguns(project, &manifest, &mut report);
    check_capability_store(project, &manifest, &mut report);
    check_actors(project, &manifest, &mut report);
    check_http(project, &app, &mut report);
    check_gateway_wiring(&app, &mut report);
    check_topics(&manifest, &mut report);
    check_workflows(project, &manifest, &mut report);
    check_manifest_product_http(&manifest, &app, &mut report);
    check_manifest_duplicates(&manifest, &mut report);
    check_builtin_gateway_routes(&app, &manifest, &mut report);
    check_deprecated_api(project, &mut report);
    if preflight || project.root.join("deploy").is_dir() {
        check_deploy_preflight(project, preflight, &mut report);
    }
    report
}

/// Apply safe, mechanical fixes (run loop simplification). Returns human-readable actions taken.
///
/// # Errors
/// I/O or unsupported project layout.
pub fn run_doctor_fix(project: &TrembitaProject) -> Result<Vec<String>, String> {
    let mut actions = Vec::new();
    let app_path = project.app_rs();
    let mut app = fs::read_to_string(&app_path).map_err(|e| e.to_string())?;
    let mut changed = false;

    if app.contains("RunOpts::for_manifest") {
        app = app.replace(
            "        let cfg = self.config;\n        let manifest = manifest::build();\n        let run = RunOpts::for_manifest(&cfg, &manifest);\n",
            "        let cfg = self.config;\n        let manifest = manifest::build();\n",
        );
        app = app.replace(
            "        let run = run.with_wait_ready(ReadyOpts::default());\n",
            "",
        );
        app = app.replace(".run(run)\n", ".run()\n");
        if app.contains("use trembita::{RunOpts, TrembitaApp") {
            app = app.replace(
                "use trembita::{RunOpts, TrembitaApp, TrembitaConfigure};",
                "use trembita::TrembitaApp;",
            );
        } else if app.contains("RunOpts, TrembitaApp") {
            app = app.replace("RunOpts, ", "");
        }
        if app.contains("use trembita::ReadyOpts;") {
            app = app.replace("use trembita::ReadyOpts;\n", "");
        }
        changed = true;
        actions.push("simplified app.rs: .run() without RunOpts::for_manifest".into());
    }

    if app.contains("TrembitaConfigure {") {
        app = app.replace(
            "            .configure(TrembitaConfigure {\n                ..TrembitaConfigure::default()\n            })\n",
            "",
        );
        changed = true;
        actions.push("removed no-op TrembitaConfigure from app.rs".into());
    }

    if changed {
        fs::write(&app_path, app).map_err(|e| e.to_string())?;
    }
    Ok(actions)
}

fn check_layout(project: &TrembitaProject, report: &mut DoctorReport) {
    let required = [
        project.main_rs(),
        project.app_rs(),
        project.manifest_rs(),
        project.root.join("src/config.rs"),
        project.consumers_dir(),
        project.domain_dir(),
    ];
    for path in &required {
        if path.exists() {
            report.ok(format!("found {}", path.display()));
        } else {
            report.error(format!("missing required path: {}", path.display()));
        }
    }
}

fn check_markers(manifest: &str, report: &mut DoctorReport) {
    for marker in [names::IMPORTS, names::JOBS, names::TOPICS, names::WORKERS] {
        if manifest.contains(&format!("// {marker}")) {
            report.ok(format!("marker `{marker}` present (manifest.rs)"));
        } else {
            report.warn(format!(
                "marker `{marker}` missing in manifest.rs — add `// {marker}` / `// {marker}-end` or re-run `trembita new`"
            ));
        }
    }
    if manifest.contains(".workflows(") {
        if manifest.contains(&format!("// {}", names::WORKFLOWS)) {
            report.ok(format!(
                "marker `{}` present (manifest.rs)",
                names::WORKFLOWS
            ));
        } else {
            report.warn(format!(
                "marker `{}` missing in manifest.rs — register workflows inside the marker region",
                names::WORKFLOWS
            ));
        }
    }
}

fn check_app_wiring(project: &TrembitaProject, app: &str, report: &mut DoctorReport) {
    if app.contains(".manifest(") {
        report.ok("app.rs applies manifest::build()");
    } else {
        report.error(
            "app.rs must call .manifest(manifest::build()) — capabilities live in manifest.rs",
        );
    }
    if app.contains(".jobs(")
        || app.contains(".topics(")
        || app.contains(".workers(")
        || app.contains(".workflows(")
    {
        report.error(
            "app.rs must not register .jobs/.topics/.workers/.workflows — move registrations to manifest.rs",
        );
    }
    if app.contains(".gateway(") {
        if app.contains(&format!("// {}", names::SURFACES)) {
            report.ok(format!(
                "marker `{}` present (app.rs gateway)",
                names::SURFACES
            ));
        } else {
            report.warn(format!(
                "marker `{}` missing in app.rs — wire gateway surfaces in `// trembita:surfaces` region",
                names::SURFACES
            ));
        }
    }
    if project.http_dir().join("ws.rs").is_file()
        && !app.contains("http::ws::route_table")
        && !app.contains("mount_sticky_websocket")
        && !app.contains("websocket_routes")
    {
        report.warn(
            "src/http/ws.rs exists but app.rs does not merge WebSocket routes — wire `.merge_routes(http::ws::route_table(&state))` in gateway surfaces",
        );
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

fn check_domain_module_declared(project: &TrembitaProject, report: &mut DoctorReport) {
    if !project.domain_dir().is_dir() {
        return;
    }
    let Ok(main) = fs::read_to_string(project.main_rs()) else {
        return;
    };
    if main.contains("mod domain;") {
        report.ok("main.rs declares mod domain");
    } else {
        report.error("main.rs must declare `mod domain;` when src/domain/ exists");
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
            let registered = job_stream_registered_in_manifest(app, &stream, &consumer_type);
            if registered {
                report.ok(format!(
                    "consumer stream `{stream}` wired in manifest/app registry"
                ));
            } else {
                report.error(format!(
                    "consumer `{stream}` in {} not registered in manifest.rs (.jobs / JobOpts)",
                    entry.display()
                ));
            }
        }
    }
}

fn check_capabilities(project: &TrembitaProject, manifest: &str, report: &mut DoctorReport) {
    if !manifest.contains(".capabilities(") && !manifest.contains("CapManifest") {
        return;
    }
    let cap_dir = project.capabilities_dir();
    if !cap_dir.is_dir() {
        report.warn(
            "manifest registers capabilities but src/capabilities/ is missing (see docs/decisions/capability-dx.md layout)",
        );
        return;
    }
    report.ok(format!("found {}", cap_dir.display()));
    let uses_queued = manifest.contains("Route::Queued")
        || manifest.contains("Route::QueuedWait")
        || manifest.contains("Route::Scheduled")
        || manifest.contains(".enqueue(");
    let has_queue = manifest.contains("queue_stream(") || manifest.contains("default_queue_for");
    if uses_queued && !has_queue {
        report.warn(
            "capabilities use queued routes or .enqueue() but manifest has no .queue_stream(...) / .default_queue_for::<...>()",
        );
    }
    if manifest.contains(".capabilities(") && !manifest.contains("// trembita:capabilities") {
        report.warn(
            "manifest.rs registers capabilities but lacks // trembita:capabilities marker region (see framework-conventions)",
        );
    }
    let actors_dir = project.actors_dir();
    if actors_dir.is_dir() {
        let n = walk_rs_files(&actors_dir)
            .into_iter()
            .filter(|p| p.file_name().is_some_and(|n| n != "mod.rs"))
            .count();
        if n > 0 {
            report.warn(
                "actors/ and capabilities/ both present — register new product ops in capabilities/ (actors/ is Advanced)",
            );
        }
    }
}

/// B-31 — founder scale model: catch manifest combos that pin compute to one host while
/// queued or stateless ops should fan out with the cluster.
fn check_capability_scale_footguns(
    project: &TrembitaProject,
    manifest: &str,
    report: &mut DoctorReport,
) {
    if !manifest.contains(".capabilities(") && !manifest.contains("CapManifest") {
        return;
    }
    let cap_dir = project.capabilities_dir();
    if !cap_dir.is_dir() {
        return;
    }

    let keyed = capabilities_have_keyed_handlers(&cap_dir);
    let shared_ram = capabilities_likely_shared_ram(&cap_dir);
    let uses_queued = capability_queued_wiring(manifest, &cap_dir);
    let fixed_one =
        manifest.contains(".instances(1)") || capabilities_declare_instances_one(&cap_dir);
    let session_ops = capability_uses_session_route(manifest, &cap_dir);
    let per_node = manifest.contains(".per_node()") || capabilities_declare_per_node(&cap_dir);

    if fixed_one && uses_queued && !keyed {
        report.error_with_suggestion(
            "capability group uses `.instances(1)` with queued/default_queue wiring but no `#[cap_handler(key = …)]` — \
             queue consumers scale cluster-wide while Fixed(1) pins handlers to one host (B-31)",
            Some(
                "Remove `.instances(1)` for PerNode hosts, or add `key = \"…\"` on handlers for keyed ownership",
            ),
        );
    } else if fixed_one && !shared_ram && !keyed {
        report.warn_with_suggestion(
            "`.instances(1)` on a stateless capability group — omit it to use automatic PerNode scale when you add VPS nodes (B-28/B-31)",
            Some("Delete `.instances(1)` from the CapGroup chain unless you intentionally want one global host"),
        );
    }

    if session_ops && !per_node {
        report.warn_with_suggestion(
            "capabilities use `Route::Session` but manifest has no `.per_node()` — default host scale is Fixed(1)",
            Some("Add `.per_node()` on the session CapGroup (see `trembita new --profile realtime`)"),
        );
    }

    if (!fixed_one || keyed || !uses_queued)
        && uses_queued
        && (has_queue_wiring(manifest) || capability_queued_wiring("", &cap_dir))
    {
        report.ok(
            "capability queued wiring: handler placement follows group scale; job consumers scale separately (B-31)",
        );
    }
}

fn has_queue_wiring(manifest: &str) -> bool {
    manifest.contains("queue_stream(") || manifest.contains("default_queue_for")
}

fn capability_queued_wiring(manifest: &str, cap_dir: &Path) -> bool {
    if manifest.contains("Route::Queued")
        || manifest.contains("Route::QueuedWait")
        || manifest.contains("Route::Scheduled")
        || manifest.contains(".enqueue(")
        || manifest.contains("default_queue_for")
        || manifest.contains("queue_stream(")
    {
        return true;
    }
    for entry in walk_rs_files(cap_dir) {
        let Ok(content) = fs::read_to_string(&entry) else {
            continue;
        };
        if content.contains("Route::Queued")
            || content.contains("Route::QueuedWait")
            || content.contains(".enqueue(")
            || content.contains("cap_enqueue")
            || content.contains("default_queue_for")
            || content.contains("queue_stream(")
        {
            return true;
        }
    }
    false
}

fn capability_uses_session_route(manifest: &str, cap_dir: &Path) -> bool {
    if manifest.contains("Route::Session") {
        return true;
    }
    for entry in walk_rs_files(cap_dir) {
        let Ok(content) = fs::read_to_string(&entry) else {
            continue;
        };
        if content.contains("Route::Session") {
            return true;
        }
    }
    false
}

fn capabilities_have_keyed_handlers(cap_dir: &Path) -> bool {
    for entry in walk_rs_files(cap_dir) {
        let Ok(content) = fs::read_to_string(&entry) else {
            continue;
        };
        for line in content.lines() {
            if line.contains("#[cap_handler") && (line.contains("key =") || line.contains("key=\""))
            {
                return true;
            }
        }
    }
    false
}

fn capabilities_declare_instances_one(cap_dir: &Path) -> bool {
    for entry in walk_rs_files(cap_dir) {
        let Ok(content) = fs::read_to_string(&entry) else {
            continue;
        };
        if content.contains(".instances(1)") {
            return true;
        }
    }
    false
}

fn capabilities_declare_per_node(cap_dir: &Path) -> bool {
    for entry in walk_rs_files(cap_dir) {
        let Ok(content) = fs::read_to_string(&entry) else {
            continue;
        };
        if content.contains(".per_node()") {
            return true;
        }
    }
    false
}

fn capabilities_likely_shared_ram(cap_dir: &Path) -> bool {
    for entry in walk_rs_files(cap_dir) {
        let Ok(content) = fs::read_to_string(&entry) else {
            continue;
        };
        if !content.contains("pub struct") {
            continue;
        }
        if content.contains("Mutex<")
            || content.contains("RwLock<")
            || content.contains("BTreeMap")
            || content.contains("HashMap<")
            || content.contains("Vec<")
        {
            return true;
        }
    }
    false
}

fn check_capability_store(project: &TrembitaProject, manifest: &str, report: &mut DoctorReport) {
    if !manifest.contains(".capabilities(") && !manifest.contains("CapManifest") {
        return;
    }
    let cap_dir = project.capabilities_dir();
    if !cap_dir.is_dir() {
        return;
    }
    for entry in walk_rs_files(&cap_dir) {
        if entry.file_name().is_some_and(|n| n == "mod.rs") {
            continue;
        }
        let Ok(content) = fs::read_to_string(&entry) else {
            continue;
        };
        if !content.contains("#[cap_handler") {
            continue;
        }
        let uses_store_api = content.contains("store_get")
            || content.contains("store_set")
            || content.contains("store_cas")
            || content.contains("CapStore");
        if uses_store_api && !content.contains("require_store") {
            report.error_with_suggestion(
                format!(
                    "{} uses cap store APIs but never calls OpCtx::require_store() (R4)",
                    entry.display()
                ),
                Some(
                    "Call `ctx.require_store()?` at handler start; set `TREMBITA_DATA_DIR` — see `capabilities/task.rs` in jobs scaffold",
                ),
            );
        }
        if content.contains("default_queue_for")
            && content.contains("require_store")
            && !content.contains("store_get")
        {
            report.warn_with_suggestion(
                format!(
                    "{}: queued capability with require_store but no idempotency marker read (R4)",
                    entry.file_name().and_then(|n| n.to_str()).unwrap_or("capability")
                ),
                Some("Use store_get/store_set marker before ack — copy from scaffold `task.rs` or examples/background-jobs"),
            );
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
                report.ok(format!("actor group `{group}` registered in manifest.rs"));
            } else {
                report.warn(format!(
                    "actor file {} defines group `{group}` but manifest.rs has no WorkerOpts::new(\"{group}\")",
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
        report.warn(
            "src/http/ exists but main.rs has no `mod http;` — add `mod http;` after `mod app;`",
        );
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

fn check_workflows(project: &TrembitaProject, manifest: &str, report: &mut DoctorReport) {
    let workflows_dir = project.workflows_dir();
    if !workflows_dir.is_dir() {
        return;
    }
    let mod_rs = workflows_dir.join("mod.rs");
    let mod_content = fs::read_to_string(&mod_rs).unwrap_or_default();
    let main = fs::read_to_string(project.main_rs()).unwrap_or_default();
    if !main.contains("mod workflows;") {
        report.warn(
            "src/workflows/ exists but main.rs has no `mod workflows;` — add `mod workflows;` after `mod app;`",
        );
    }
    for entry in walk_rs_files(&workflows_dir) {
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
                "workflows/{module}.rs not declared in workflows/mod.rs"
            ));
        }
        let Some(prefix) = extract_workflow_prefix(&content) else {
            report.warn(format!(
                "workflows/{module}.rs has no `pub const PREFIX` — doctor cannot verify manifest wiring"
            ));
            continue;
        };
        let wired = manifest.contains(&format!("WorkflowOpts::named(\"{prefix}\""))
            || manifest.contains(&format!("workflows::{module}::"));
        if wired {
            report.ok(format!("workflow prefix `{prefix}` wired in manifest.rs"));
        } else {
            report.error(format!(
                "workflow `{prefix}` in {} not registered in manifest.rs (.workflows / WorkflowOpts)",
                entry.display()
            ));
        }
    }
}

fn check_manifest_duplicates(manifest: &str, report: &mut DoctorReport) {
    report_duplicate_ids(report, "job stream", extract_job_stream_literals(manifest));
    report_duplicate_ids(
        report,
        "topic",
        extract_string_literals_after(manifest, "TopicOpts::topic(\""),
    );
    report_duplicate_ids(
        report,
        "worker group",
        extract_worker_group_literals(manifest),
    );
    report_duplicate_ids(
        report,
        "workflow prefix",
        extract_string_literals_after(manifest, "WorkflowOpts::named(\""),
    );
}

fn report_duplicate_ids(report: &mut DoctorReport, kind: &str, ids: Vec<String>) {
    let mut seen = std::collections::BTreeMap::<&str, usize>::new();
    for id in &ids {
        *seen.entry(id.as_str()).or_default() += 1;
    }
    let unique = seen.len();
    for (id, count) in &seen {
        if *count > 1 {
            report.error(format!(
                "manifest.rs registers duplicate {kind} `{id}` ({count} times)"
            ));
        }
    }
    if ids.is_empty() {
        return;
    }
    if unique == ids.len() {
        report.ok(format!("manifest.rs has no duplicate {kind} names"));
    }
}

fn check_gateway_wiring(app: &str, report: &mut DoctorReport) {
    if !app.contains(".gateway(") {
        return;
    }
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
    if app.contains("AuthMode::Identity") {
        if app.contains("GatewayBearerIdentity") || app.contains(".identity(") {
            report.ok("gateway has identity for protected routes");
        } else {
            report.warn("identity-protected routes without gateway identity");
        }
    }
}

fn check_topics(manifest: &str, report: &mut DoctorReport) {
    if manifest.contains(".topics(") {
        if manifest.contains("TopicOpts::topic") {
            report.ok("topics registered in manifest.rs");
        } else {
            report.warn(".topics() in manifest.rs but no TopicOpts::topic entries found");
        }
    }
}

fn check_manifest_product_http(manifest: &str, app: &str, report: &mut DoctorReport) {
    let needs_listener = manifest.contains(".workflows(")
        || (manifest.contains(".topics(") && !app.contains("without_topics_api()"));
    let has_listener = app.contains("TrembitaApp::from_env")
        || app.contains("TrembitaApp::from_config")
        || app.contains(".gateway(")
        || app.contains("GatewayOpts::");
    if needs_listener && !has_listener {
        report.warn(
            "manifest registers workflows/topics but app.rs has no TrembitaApp::from_env() or .gateway() — set TREMBITA_LISTEN or .gateway(...)",
        );
    }
    if manifest.contains(".workflows(") && app.contains("without_workflows_api()") {
        report.warn(
            "manifest .workflows() with .without_workflows_api() — HTTP /workflows/* disabled",
        );
    }
    if manifest.contains(".topics(") && app.contains("without_topics_api()") {
        report.warn("manifest .topics() with .without_topics_api() — HTTP /topics/* disabled");
    }
    if manifest.contains(".workflows(") && has_listener && ops_routes_zero_config(app) {
        report.ok(
            "workflows HTTP via default gateway (.workflows in manifest + from_env/gateway_routes)",
        );
    }
    if manifest.contains(".topics(")
        && !app.contains("without_topics_api()")
        && has_listener
        && ops_routes_zero_config(app)
    {
        report
            .ok("topics HTTP via default gateway (.topics in manifest + from_env/gateway_routes)");
    }
}

fn check_builtin_gateway_routes(app: &str, manifest: &str, report: &mut DoctorReport) {
    if app.contains("without_ops()") {
        report.ok("ops HTTP disabled (.without_ops())");
        return;
    }
    if ops_routes_zero_config(app) {
        report.ok(
            "ops on unified listener (/health, /ready, /metrics, /dashboard, /introspect/*) — default gateway",
        );
    } else if app.contains(".gateway(") || app.contains("GatewayOpts::") {
        if app.contains("http::ops::route_table") {
            report.ok("ops routes merged explicitly in custom gateway");
        } else {
            report.warn(
                "custom gateway without ops — prefer TrembitaApp::from_env() or merge http::ops::route_table",
            );
        }
    }
    let jobs_registered = manifest.contains(".jobs([")
        || manifest.contains("JobOpts::")
        || manifest.contains("http_enqueue(true)")
        || app.contains("http_enqueue(true)");
    if jobs_registered {
        if product_jobs_zero_config(app, manifest) || app.contains("http::jobs::route_table") {
            if product_jobs_zero_config(app, manifest) {
                report.ok("jobs HTTP API via default gateway (registration + default or explicit .http_enqueue)");
            }
        } else if app.contains(".gateway(") {
            report.warn(
                "jobs registered but /jobs/* not on gateway — use `.http_enqueue(true)` + `GatewayOpts::from_env()` or merge `http/jobs.rs` in app.rs",
            );
        }
    }
    if manifest.contains(".workflows(")
        && !app.contains("without_workflows_api()")
        && manual_product_api_merge(app, "workflows_api")
    {
        report.warn(
                "manual TrembitaApp::workflows_api route merge — remove; .workflows in manifest mounts /workflows/* on the default gateway",
            );
    }
    if manifest.contains(".topics(")
        && !app.contains("without_topics_api()")
        && manual_product_api_merge(app, "topics_api")
    {
        report.warn(
                "manual TrembitaApp::topics_api route merge — remove; .topics in manifest mounts /topics/* on the default gateway",
            );
    }
}

fn manual_product_api_merge(app: &str, api_fn: &str) -> bool {
    app.contains(api_fn) && (app.contains(".route_table()") || app.contains(".merge("))
}

fn ops_routes_zero_config(app: &str) -> bool {
    app.contains("TrembitaApp::from_env")
        || app.contains("TrembitaApp::from_config")
        || app.contains("default_surfaces")
        || app.contains("default_product_routes")
        || app.contains("gateway_routes(")
}

fn product_jobs_zero_config(app: &str, manifest: &str) -> bool {
    ops_routes_zero_config(app)
        && (app.contains("http_enqueue(true)")
            || manifest.contains("http_enqueue(true)")
            || manifest.contains("JobOpts::product(")
            || manifest.contains(".jobs(["))
}

fn check_deploy_preflight(project: &TrembitaProject, strict: bool, report: &mut DoctorReport) {
    let deploy = project.root.join("deploy");
    if !deploy.is_dir() {
        if strict {
            report.warn("preflight: no deploy/ — add deploy/.env.example before production");
        }
        return;
    }
    report.ok("deploy/ present — running deploy preflight checks");
    for rel in ["deploy/.env.example", "deploy/.env"] {
        let path = project.root.join(rel);
        if !path.is_file() {
            continue;
        }
        let Ok(content) = fs::read_to_string(&path) else {
            continue;
        };
        preflight_env_vars(&content, rel, strict, report);
    }
    let compose = deploy.join("docker-compose.yml");
    if compose.is_file() {
        let Ok(content) = fs::read_to_string(&compose) else {
            return;
        };
        preflight_compose(&content, "deploy/docker-compose.yml", strict, report);
    }
    check_deploy_cert_material(project, strict, report);
}

fn preflight_env_vars(content: &str, path: &str, strict: bool, report: &mut DoctorReport) {
    let has = |key: &str| {
        content.lines().any(|line| {
            let t = line.trim();
            !t.starts_with('#') && (t.starts_with(key) || t.starts_with(&format!("{key}=")))
        })
    };
    if has("TREMBITA_LISTEN") {
        for line in content.lines() {
            if let Some(addr) = env_assign_value(line, "TREMBITA_LISTEN") {
                if listen_addr_valid(&addr) {
                    report.ok(format!("{path}: TREMBITA_LISTEN={addr}"));
                } else if strict {
                    report.error(format!(
                        "{path}: TREMBITA_LISTEN={addr} — expected host:port (see docs/env.md)"
                    ));
                } else {
                    report.warn(format!("{path}: invalid TREMBITA_LISTEN={addr}"));
                }
                break;
            }
        }
    } else if strict {
        report.error(format!(
            "{path}: missing TREMBITA_LISTEN — one port for QUIC + HTTP (see docs/env.md)"
        ));
    } else {
        report.warn(format!("{path}: add TREMBITA_LISTEN"));
    }
    if has("TREMBITA_DATA_DIR") {
        report.ok(format!("{path}: TREMBITA_DATA_DIR set"));
    } else if strict {
        report.error(format!("{path}: missing TREMBITA_DATA_DIR"));
    }
    if has("TREMBITA_CERT_DIR") {
        report.ok(format!("{path}: TREMBITA_CERT_DIR set"));
    } else if strict {
        report.warn(format!(
            "{path}: missing TREMBITA_CERT_DIR — required in prod (or enable dev-certs locally)"
        ));
    }
    if has("TREMBITA_NODE_ID") {
        let level = if strict { Level::Error } else { Level::Warn };
        let msg = format!(
            "{path}: TREMBITA_NODE_ID — omit; id comes from join + TREMBITA_DATA_DIR/node-id"
        );
        match level {
            Level::Error => report.error(msg),
            Level::Warn => report.warn(msg),
            Level::Ok => report.ok(msg),
        }
    }
    if has("TREMBITA_PEERS") {
        report.warn(format!(
            "{path}: TREMBITA_PEERS — static bootstrap only; product deploys use TREMBITA_JOIN_SEEDS"
        ));
    }
    let join_seeds = content.lines().any(|line| {
        let t = line.trim();
        !t.starts_with('#') && t.starts_with("TREMBITA_JOIN_SEEDS=")
    });
    let session_secret = has("TREMBITA_GATEWAY_SESSION_SECRET") || has("GATEWAY_SESSION_SECRET");
    if join_seeds && !session_secret {
        let msg = format!(
            "{path}: TREMBITA_JOIN_SEEDS without TREMBITA_GATEWAY_SESSION_SECRET — multi-node gateway sessions need a shared secret (docs/env.md)"
        );
        if strict {
            report.error(msg);
        } else {
            report.warn(msg);
        }
    } else if join_seeds && session_secret {
        report.ok(format!(
            "{path}: join seeds + gateway session secret documented for multi-node"
        ));
    }
    if strict && content.contains("dev-change-me") {
        report.warn(format!(
            "{path}: replace placeholder GATEWAY_TOKEN / secrets before production"
        ));
    }
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("TREMBITA_HTTP=")
            && !trimmed.contains("=-")
            && !trimmed.starts_with('#')
        {
            report.warn(format!(
                "{path}: `{trimmed}` — omit; use TREMBITA_LISTEN only (docs/env.md)"
            ));
        }
    }
}

fn preflight_compose(content: &str, path: &str, strict: bool, report: &mut DoctorReport) {
    let _ = strict;
    for line in content.lines() {
        let t = line.trim();
        if t.starts_with('#') {
            continue;
        }
        if t.contains("TREMBITA_NODE_ID") {
            report.error(format!(
                "{path}: remove TREMBITA_NODE_ID — use dynamic join + persisted node-id"
            ));
            break;
        }
    }
    for line in content.lines() {
        let t = line.trim();
        if t.starts_with('#') {
            continue;
        }
        if t.contains("TREMBITA_PEERS") {
            report.warn(format!(
                "{path}: prefer TREMBITA_JOIN_SEEDS over static TREMBITA_PEERS"
            ));
            break;
        }
    }
    if content.contains("TREMBITA_LISTEN") {
        report.ok(format!("{path}: TREMBITA_LISTEN in compose"));
    } else {
        report.warn(format!("{path}: set TREMBITA_LISTEN per service"));
    }
    let joiners = content
        .lines()
        .filter(|l| {
            let t = l.trim();
            !t.starts_with('#') && t.contains("TREMBITA_JOIN_SEEDS")
        })
        .count();
    if joiners > 0 {
        report.ok(format!(
            "{path}: {joiners} service(s) declare TREMBITA_JOIN_SEEDS (elastic join)"
        ));
    }
}

fn env_assign_value(line: &str, key: &str) -> Option<String> {
    let t = line.trim();
    if t.starts_with('#') {
        return None;
    }
    let prefix = format!("{key}=");
    if !t.starts_with(&prefix) {
        return None;
    }
    let raw = t[prefix.len()..].trim();
    let unquoted = raw.trim_matches('"').trim_matches('\'');
    Some(unquoted.to_string())
}

fn listen_addr_valid(value: &str) -> bool {
    let v = value.trim();
    if v.is_empty() {
        return false;
    }
    v.parse::<SocketAddr>().is_ok()
}

fn check_deploy_cert_material(project: &TrembitaProject, strict: bool, report: &mut DoctorReport) {
    let deploy_certs = project.root.join("deploy/certs");
    if deploy_certs.is_dir() {
        let ca = deploy_certs.join("ca.pem");
        if ca.is_file() {
            report.ok("deploy/certs/ca.pem present");
        } else {
            let msg =
                "deploy/certs/ missing ca.pem — mint with dev/certs/generate.sh or copy prod PEMs";
            if strict {
                report.error(msg);
            } else {
                report.warn(msg);
            }
        }
    }

    for rel in ["deploy/.env.example", "deploy/.env"] {
        let path = project.root.join(rel);
        if !path.is_file() {
            continue;
        }
        let Ok(content) = fs::read_to_string(&path) else {
            continue;
        };
        for line in content.lines() {
            let Some(dir) = env_assign_value(line, "TREMBITA_CERT_DIR") else {
                continue;
            };
            if dir == "/certs" || dir.starts_with("/var/") {
                report.ok(format!(
                    "{rel}: TREMBITA_CERT_DIR={dir} — mount ca.pem + node-{{id}}.pem at runtime"
                ));
                continue;
            }
            let cert_root = cert_dir_on_disk(project, &dir);
            if cert_root.join("ca.pem").is_file() {
                report.ok(format!("{rel}: ca.pem under {dir}"));
            } else if cert_root.is_dir() {
                report.warn(format!(
                    "{rel}: TREMBITA_CERT_DIR={dir} — directory exists but ca.pem missing"
                ));
            } else if strict && !deploy_certs.is_dir() {
                report.warn(format!(
                    "{rel}: TREMBITA_CERT_DIR={dir} — path not found locally (ok if only mounted in compose)"
                ));
            }
        }
    }
}

fn cert_dir_on_disk(project: &TrembitaProject, dir: &str) -> PathBuf {
    let trimmed = dir.trim_start_matches("./");
    if Path::new(trimmed).is_absolute() {
        PathBuf::from(trimmed)
    } else {
        project.root.join("deploy").join(trimmed)
    }
}

fn check_deprecated_api(project: &TrembitaProject, report: &mut DoctorReport) {
    let src = project.root.join("src");
    for path in walk_tree_rs(&src) {
        let Ok(content) = fs::read_to_string(&path) else {
            continue;
        };
        let rel = path
            .strip_prefix(&project.root)
            .unwrap_or(&path)
            .display()
            .to_string();
        scan_deprecated_source(&content, &rel, report);
    }
    for rel in ["deploy/.env.example", ".env.example"] {
        let path = project.root.join(rel);
        if path.is_file() {
            let Ok(content) = fs::read_to_string(&path) else {
                continue;
            };
            scan_deprecated_env(&content, rel, report);
        }
    }
}

fn scan_deprecated_source(content: &str, path: &str, report: &mut DoctorReport) {
    let flags: &[(&str, &str, Level)] = &[
        (
            "with_jobs_api(",
            "with_jobs_api removed in 0.5.0 — merge http::jobs::route_table in GatewayOpts::surfaces()",
            Level::Error,
        ),
        (
            "with_actors_api(",
            "with_actors_api removed in 0.5.0 — merge explicit actors route table in .surfaces()",
            Level::Error,
        ),
        (
            "with_workflows_api(",
            "with_workflows_api removed in 0.5.0 — register .workflows([…]) in manifest.rs (default gateway mounts /workflows/*)",
            Level::Error,
        ),
        (
            "TrembitaApp::workflows_api(",
            "manual workflows_api merge — .workflows in manifest + from_env mounts /workflows/* automatically",
            Level::Warn,
        ),
        (
            "TrembitaApp::topics_api(",
            "manual topics_api merge — .topics in manifest + from_env mounts /topics/* automatically",
            Level::Warn,
        ),
        (
            "with_introspect_api(",
            "with_introspect_api removed in 0.5.0 — merge http::ops or introspect route table in .surfaces()",
            Level::Error,
        ),
        (
            "protect_product_apis(",
            "protect_product_apis removed in 0.5.0 — use RouteTable::with_auth_mode(AuthMode::Identity)",
            Level::Error,
        ),
        (
            "collect_builtin_routes",
            "collect_builtin_routes removed — merge OpsApi/JobsApi route tables explicitly",
            Level::Error,
        ),
        (
            "admin_addr(",
            "admin_addr removed — merge http::ops::route_table on the unified HTTP listener",
            Level::Warn,
        ),
        (
            ".admin_addr",
            "admin_addr removed — merge http::ops::route_table on the unified HTTP listener",
            Level::Warn,
        ),
        (
            "TrembitaClusterBuilder::admin",
            "cluster admin listener removed — use spawn_cluster_ops_http / OpsApi route table",
            Level::Warn,
        ),
        (
            "TrembitaCluster::builder",
            "TrembitaCluster::builder removed — use TrembitaApp::from_env / from_config + AppManifest (see docs/env.md)",
            Level::Error,
        ),
        (
            "TrembitaClusterBuilder::new",
            "low-level cluster builder is not public — product: TrembitaApp; custom SM: trembita-showcase / integration tests only",
            Level::Warn,
        ),
    ];
    for (needle, message, level) in flags {
        if !content.contains(needle) {
            continue;
        }
        let msg = format!("{path}: {message}");
        match level {
            Level::Error => report.error(msg),
            Level::Warn => report.warn(msg),
            Level::Ok => report.ok(msg),
        }
    }
}

fn scan_deprecated_env(content: &str, path: &str, report: &mut DoctorReport) {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("TREMBITA_NODE_ID") {
            report.warn(format!(
                "{path}: `{trimmed}` — node ids come from join assignment + TREMBITA_DATA_DIR/node-id; omit in product deploy"
            ));
        } else if trimmed.starts_with("TREMBITA_PEERS") {
            report.warn(format!(
                "{path}: `{trimmed}` — static voter bootstrap; product elastic clusters use TREMBITA_JOIN_SEEDS only (see docs/env.md)"
            ));
        } else if trimmed.starts_with("TREMBITA_NODE_CERT")
            || trimmed.starts_with("TREMBITA_NODE_KEY")
            || trimmed.starts_with("TREMBITA_CA_CERT")
        {
            report.warn(format!(
                "{path}: `{trimmed}` — prefer TREMBITA_CERT_DIR with node-{{id}}.pem (docs/env.md)"
            ));
        } else if trimmed.starts_with("TREMBITA_GATEWAY=") && !trimmed.contains("=-") {
            report.warn(format!(
                "{path}: `{trimmed}` — deprecated alias for TREMBITA_HTTP; omit and use TREMBITA_LISTEN only"
            ));
        } else if trimmed.starts_with("TREMBITA_ADMIN")
            || trimmed.starts_with("TREMBITA_ADMIN_TLS")
            || trimmed.starts_with("TREMBITA_GATEWAY_JOBS")
            || trimmed.starts_with("TREMBITA_GATEWAY_")
        {
            report.error(format!(
                "{path}: `{trimmed}` — use TREMBITA_LISTEN (one port for wire + HTTP) and explicit route tables"
            ));
        } else if trimmed.starts_with("TREMBITA_HTTP=")
            && !trimmed.contains("=-")
            && !trimmed.starts_with("TREMBITA_HTTP=-")
        {
            report.warn(format!(
                "{path}: `{trimmed}` — prefer TREMBITA_LISTEN only; HTTP uses the same port (TREMBITA_HTTP=- to disable TCP)"
            ));
        }
    }
}

fn walk_tree_rs(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    walk_tree_rs_inner(dir, &mut out);
    out.sort();
    out
}

fn walk_tree_rs_inner(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_tree_rs_inner(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
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

fn extract_workflow_prefix(source: &str) -> Option<String> {
    for line in source.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("pub const PREFIX: &str = \"") {
            return rest.split('"').next().map(str::to_string);
        }
    }
    None
}

fn job_stream_registered_in_manifest(manifest: &str, stream: &str, consumer_type: &str) -> bool {
    manifest.contains(&format!("JobOpts::new(\"{stream}\")"))
        || manifest.contains(&format!("JobOpts::product(\"{stream}\")"))
        || manifest.contains(&format!("consumer(&{consumer_type})"))
        || (manifest.contains(consumer_type)
            && (manifest.contains(&format!("\"{stream}\""))
                || (stream == "jobs" && manifest.contains("SAMPLE_STREAM"))))
}

fn push_job_stream_from_rest(rest: &str, streams: &mut Vec<String>) {
    let rest = rest.trim_start();
    if let Some(lit) = rest.strip_prefix('"')
        && let Some(end) = lit.find('"')
    {
        streams.push(lit[..end].to_string());
        return;
    }
    let ident: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if !ident.is_empty() {
        streams.push(ident);
    }
}

fn extract_job_stream_literals(manifest: &str) -> Vec<String> {
    let mut streams = Vec::new();
    for prefix in [
        "JobOpts::new(",
        "JobOpts::product(",
        "JobsPreset::idempotent_stream(",
        "JobsPreset::stream(",
    ] {
        let mut search = manifest;
        while let Some(idx) = search.find(prefix) {
            push_job_stream_from_rest(&search[idx + prefix.len()..], &mut streams);
            search = &search[idx + prefix.len()..];
        }
    }
    streams
}

fn extract_string_literals_after(source: &str, needle: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut search_from = 0usize;
    while let Some(rel) = source[search_from..].find(needle) {
        let start = search_from + rel + needle.len();
        let rest = source[start..].trim_start();
        if let Some(end) = rest.find('"') {
            values.push(rest[..end].to_string());
        }
        search_from = start.saturating_add(1);
    }
    values
}

fn extract_worker_group_literals(manifest: &str) -> Vec<String> {
    let mut groups = Vec::new();
    for line in manifest.lines() {
        if !line.contains("WorkerOpts") {
            continue;
        }
        if let Some(idx) = line.find("::new(\"") {
            let rest = &line[idx + "::new(\"".len()..];
            if let Some(end) = rest.find('"') {
                groups.push(rest[..end].to_string());
            }
        }
    }
    groups
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
    fn preflight_requires_listen_in_env_example() {
        let dir = tempdir().unwrap();
        let opts = NewProjectOpts {
            name: "preflight".into(),
            output: dir.path().to_path_buf(),
            features: AppFeature::defaults(),
            trembita_version: "0.3.2".into(),
            trembita_path: None,
            template: None,
        };
        let root = scaffold_project(&opts).unwrap();
        let project = TrembitaProject { root };
        let env = project.root.join("deploy/.env.example");
        let content = fs::read_to_string(&env)
            .unwrap()
            .replace("TREMBITA_LISTEN=", "X=");
        fs::write(&env, content).unwrap();
        let report = run_doctor(&project, true);
        assert!(
            report
                .findings
                .iter()
                .any(|f| f.message.contains("TREMBITA_LISTEN") && f.level == Level::Error)
        );
    }

    #[test]
    fn doctor_passes_fresh_scaffold() {
        let dir = tempdir().unwrap();
        let opts = NewProjectOpts {
            name: "doc-test".into(),
            output: dir.path().to_path_buf(),
            features: AppFeature::defaults(),
            trembita_version: "0.3.2".into(),
            trembita_path: None,
            template: None,
        };
        let root = scaffold_project(&opts).unwrap();
        let project = TrembitaProject { root };
        let report = run_doctor(&project, false);
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
            template: None,
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
        let report = run_doctor(&project, false);
        assert!(report.has_errors());
    }

    #[test]
    fn doctor_flags_removed_gateway_flags_in_app_rs() {
        let dir = tempdir().unwrap();
        let opts = NewProjectOpts {
            name: "legacy-gw".into(),
            output: dir.path().to_path_buf(),
            features: AppFeature::defaults(),
            trembita_version: "0.3.2".into(),
            trembita_path: None,
            template: None,
        };
        let root = scaffold_project(&opts).unwrap();
        let project = TrembitaProject { root };
        let app_path = project.app_rs();
        let mut app = fs::read_to_string(&app_path).unwrap();
        app.push_str("\n// legacy\n.with_jobs_api(true).protect_product_apis(true)\n");
        fs::write(&app_path, app).unwrap();
        let report = run_doctor(&project, false);
        assert!(report.has_errors());
        assert!(
            report
                .findings
                .iter()
                .any(|f| f.message.contains("with_jobs_api"))
        );
        assert!(
            report
                .findings
                .iter()
                .any(|f| f.message.contains("protect_product_apis"))
        );
    }

    #[test]
    fn doctor_errors_on_missing_consumer_mod_declaration() {
        let dir = tempdir().unwrap();
        let opts = NewProjectOpts {
            name: "mod-test".into(),
            output: dir.path().to_path_buf(),
            features: AppFeature::defaults(),
            trembita_version: "0.3.2".into(),
            trembita_path: None,
            template: None,
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
        let report = run_doctor(&project, false);
        assert!(
            report.findings.iter().any(|f| {
                f.level == Level::Error && f.message.contains("not declared in consumers/mod.rs")
            }),
            "doctor errors: {:?}",
            report.findings
        );
    }

    fn b31_minimal_cap_project(
        cap_files: &[(&str, &str)],
    ) -> (tempfile::TempDir, TrembitaProject, String) {
        let dir = tempdir().unwrap();
        let src = dir.path().join("src");
        let cap = src.join("capabilities");
        fs::create_dir_all(&cap).unwrap();
        fs::write(src.join("app.rs"), "// trembita app").unwrap();
        let manifest = "// CapManifest wiring\npub fn caps() -> trembita::CapManifest {\n    trembita::CapManifest::new()\n}\n".to_string();
        fs::write(src.join("manifest.rs"), &manifest).unwrap();
        for (name, body) in cap_files {
            fs::write(cap.join(name), body).unwrap();
        }
        let project = TrembitaProject {
            root: dir.path().to_path_buf(),
        };
        (dir, project, manifest)
    }

    fn scale_findings(report: &DoctorReport) -> Vec<&Finding> {
        report
            .findings
            .iter()
            .filter(|f| {
                f.message.contains("B-31")
                    || f.message.contains("instances(1)")
                    || f.message.contains("Route::Session")
                    || f.message.contains("PerNode")
            })
            .collect()
    }

    /// B-31 table — `check_capability_scale_footguns` outcomes on synthetic capability trees.
    #[test]
    fn founder_scale_b31_doctor_scenarios_table() {
        const QUEUED_FOOTGUN: &str = r#"
use trembita::{cap_handler, CapGroup};

#[derive(Default)]
struct S;

#[derive(serde::Serialize, serde::Deserialize)]
struct Work { n: u32 }

#[cap_handler(group = "g")]
async fn work(_: Work, _: &mut S) -> Result<(), trembita::CapError> { Ok(()) }

fn _w() {
    let _ = CapGroup::<S>::for_cap::<Work>()
        .instances(1)
        .default_queue_for::<Work>();
}
"#;
        const KEYED_QUEUED_OK: &str = r#"
use std::sync::Mutex;
use trembita::{cap_handler, CapGroup};

#[derive(Default)]
pub struct S { store: Mutex<()> }

#[derive(serde::Serialize, serde::Deserialize)]
struct Work { id: String }

#[cap_handler(group = "g", key = "id")]
async fn work(_: Work, _: &mut S) -> Result<(), trembita::CapError> { Ok(()) }

fn _w() {
    let _ = CapGroup::<S>::for_cap::<Work>()
        .instances(1)
        .default_queue_for::<Work>();
}
"#;
        const STATELESS_FIXED_WARN: &str = r#"
use trembita::CapGroup;

#[derive(Default)]
struct S;

fn _w() {
    let _ = CapGroup::<S>::with_state("g").instances(1);
}
"#;
        const SESSION_WARN: &str = r#"
fn _call() {
    let _ = trembita::Route::Session;
}
"#;
        const SESSION_PER_NODE_OK: &str = r#"
use trembita::CapGroup;

#[derive(Default)]
struct S;

fn _w() {
    let _ = CapGroup::<S>::with_state("rt")
        .per_node();
    let _ = trembita::Route::Session;
}
"#;
        const QUEUE_STREAM_ERROR: &str = r#"
use trembita::CapGroup;

#[derive(Default)]
struct S;

#[derive(serde::Serialize, serde::Deserialize)]
struct Work { n: u32 }

fn _w() {
    let _ = CapGroup::<S>::for_cap::<Work>()
        .instances(1)
        .queue_stream("g.work");
}
"#;

        struct Row {
            name: &'static str,
            files: &'static [(&'static str, &'static str)],
            want_error: bool,
            want_warn: bool,
            want_ok_b31: bool,
        }

        let rows = [
            Row {
                name: "queued+Fixed(1)+no key",
                files: &[("bad.rs", QUEUED_FOOTGUN)],
                want_error: true,
                want_warn: false,
                want_ok_b31: false,
            },
            Row {
                name: "queued+Fixed(1)+key+shared ram",
                files: &[("ok.rs", KEYED_QUEUED_OK)],
                want_error: false,
                want_warn: false,
                want_ok_b31: true,
            },
            Row {
                name: "stateless Fixed(1) inline",
                files: &[("pin.rs", STATELESS_FIXED_WARN)],
                want_error: false,
                want_warn: true,
                want_ok_b31: false,
            },
            Row {
                name: "session without per_node",
                files: &[("sess.rs", SESSION_WARN)],
                want_error: false,
                want_warn: true,
                want_ok_b31: false,
            },
            Row {
                name: "session with per_node",
                files: &[("rt.rs", SESSION_PER_NODE_OK)],
                want_error: false,
                want_warn: false,
                want_ok_b31: false,
            },
            Row {
                name: "queue_stream+Fixed(1)+no key",
                files: &[("qs.rs", QUEUE_STREAM_ERROR)],
                want_error: true,
                want_warn: false,
                want_ok_b31: false,
            },
            Row {
                name: "QueuedWait+Fixed(1)+no key",
                files: &[(
                    "qw.rs",
                    r#"
use trembita::CapGroup;
#[derive(Default)] struct S;
#[derive(serde::Serialize, serde::Deserialize)] struct Work { n: u32 }
fn _w() {
    let _ = CapGroup::<S>::for_cap::<Work>().instances(1);
    let _ = trembita::Route::QueuedWait;
}
"#,
                )],
                want_error: true,
                want_warn: false,
                want_ok_b31: false,
            },
            Row {
                name: "cap_enqueue mention+Fixed(1)",
                files: &[(
                    "ce.rs",
                    r#"
use trembita::CapGroup;
#[derive(Default)] struct S;
fn _w() {
    let _ = CapGroup::<S>::with_state("g").instances(1);
    let _ = "cap_enqueue";
}
"#,
                )],
                want_error: true,
                want_warn: false,
                want_ok_b31: false,
            },
            Row {
                name: "marker queued PerNode no doctor error",
                files: &[(
                    "pn.rs",
                    r#"
use trembita::CapGroup;
#[derive(Default)] struct S;
fn _w() {
    let _ = CapGroup::<S>::with_state("g").default_queue_for::<Work>();
}
#[derive(serde::Serialize, serde::Deserialize)] struct Work { n: u32 }
"#,
                )],
                want_error: false,
                want_warn: false,
                want_ok_b31: true,
            },
        ];

        for row in rows {
            let (_dir, project, manifest) = b31_minimal_cap_project(row.files);
            let mut report = DoctorReport::default();
            check_capability_scale_footguns(&project, &manifest, &mut report);
            let scale = scale_findings(&report);
            let has_error = report.findings.iter().any(|f| f.level == Level::Error);
            let has_warn = report.findings.iter().any(|f| f.level == Level::Warn);
            let has_ok_b31 = report
                .findings
                .iter()
                .any(|f| f.level == Level::Ok && f.message.contains("B-31"));
            assert_eq!(
                has_error, row.want_error,
                "{}: errors={has_error} scale={scale:?}",
                row.name
            );
            assert_eq!(
                has_warn, row.want_warn,
                "{}: warns={has_warn} scale={scale:?}",
                row.name
            );
            assert_eq!(
                has_ok_b31, row.want_ok_b31,
                "{}: ok_b31={has_ok_b31} scale={scale:?}",
                row.name
            );
        }
    }

    /// B-38 — lint v2 attaches `→` suggestions on scale foot-guns.
    #[test]
    fn b38_scale_footgun_suggestions_scenarios_table() {
        const QUEUED_FOOTGUN: &str = r#"
use trembita::{cap_handler, CapGroup};

#[derive(Default)]
struct S;

#[derive(serde::Serialize, serde::Deserialize)]
struct Work { n: u32 }

#[cap_handler(group = "g")]
async fn work(_: Work, _: &mut S) -> Result<(), trembita::CapError> { Ok(()) }

fn _w() {
    let _ = CapGroup::<S>::for_cap::<Work>()
        .instances(1)
        .default_queue_for::<Work>();
}
"#;
        const STATELESS_FIXED: &str = r#"
use trembita::CapGroup;

#[derive(Default)]
struct S;

fn _w() {
    let _ = CapGroup::<S>::with_state("g").instances(1);
}
"#;
        const SESSION_NO_PER_NODE: &str = r#"
fn _call() {
    let _ = trembita::Route::Session;
}
"#;

        struct Row {
            name: &'static str,
            files: &'static [(&'static str, &'static str)],
            want_snippet: &'static str,
        }
        let rows = [
            Row {
                name: "queued Fixed(1) error",
                files: &[("bad.rs", QUEUED_FOOTGUN)],
                want_snippet: "key =",
            },
            Row {
                name: "stateless Fixed(1) warn",
                files: &[("pin.rs", STATELESS_FIXED)],
                want_snippet: "Delete `.instances(1)`",
            },
            Row {
                name: "session without per_node",
                files: &[("sess.rs", SESSION_NO_PER_NODE)],
                want_snippet: "realtime",
            },
        ];
        for row in rows {
            let (_dir, project, manifest) = b31_minimal_cap_project(row.files);
            let mut report = DoctorReport::default();
            check_capability_scale_footguns(&project, &manifest, &mut report);
            let hit = report.findings.iter().find(|f| {
                f.suggestion
                    .as_ref()
                    .is_some_and(|s| s.contains(row.want_snippet))
            });
            assert!(hit.is_some(), "{}: {:?}", row.name, report.findings);
        }
    }

    /// B-38 — R4 store guard suggestions from `check_capability_store`.
    #[test]
    fn b38_store_guard_suggestions_scenarios_table() {
        const STORE_WITHOUT_REQUIRE: &str = r#"
use trembita::{cap_handler, CapError, OpCtx};

#[cap_handler(group = "g")]
async fn read_marker(ctx: &mut OpCtx) -> Result<(), CapError> {
    let _ = ctx.store_get("marker");
    Ok(())
}
"#;
        let (_dir, project, manifest) =
            b31_minimal_cap_project(&[("store.rs", STORE_WITHOUT_REQUIRE)]);
        let mut report = DoctorReport::default();
        check_capability_store(&project, &manifest, &mut report);
        let err = report
            .findings
            .iter()
            .find(|f| f.level == Level::Error && f.message.contains("require_store"));
        assert!(err.is_some(), "{:?}", report.findings);
        assert!(
            err.unwrap()
                .suggestion
                .as_ref()
                .is_some_and(|s| s.contains("task.rs")),
            "{:?}",
            report.findings
        );
    }

    #[test]
    fn b38_explain_scale_skips_layout_lint() {
        let (_dir, project, _manifest) = b31_minimal_cap_project(&[]);
        let full = run_doctor(&project, false);
        assert!(
            full.findings.iter().any(|f| {
                f.level == Level::Error && f.message.contains("missing required path")
            }),
            "full doctor should lint layout: {:?}",
            full.findings
        );
        let explain = run_explain_scale(&project);
        assert!(
            !explain
                .findings
                .iter()
                .any(|f| f.message.contains("missing required path")),
            "explain-scale should not run layout checks: {:?}",
            explain.findings
        );
        assert!(
            explain
                .findings
                .iter()
                .any(|f| f.message.contains("Founder scale")),
            "{:?}",
            explain.findings
        );
    }

    #[test]
    fn founder_scale_b31_helper_key_and_queue_detection() {
        let (_dir, project, manifest) = b31_minimal_cap_project(&[(
            "detect.rs",
            r#"
#[cap_handler(group = "g", key = "id")]
async fn h() {}

fn q() {
    let _ = trembita::Route::Queued;
}
"#,
        )]);
        let cap = project.capabilities_dir();
        assert!(capabilities_have_keyed_handlers(&cap));
        assert!(capability_queued_wiring(&manifest, &cap));
        assert!(!capabilities_likely_shared_ram(&cap));
    }

    #[test]
    fn doctor_errors_queued_fixed_one_without_keyed_handlers() {
        let dir = tempdir().unwrap();
        let opts = NewProjectOpts {
            name: "scale-footgun".into(),
            output: dir.path().to_path_buf(),
            features: AppFeature::defaults(),
            trembita_version: "0.3.2".into(),
            trembita_path: None,
            template: None,
        };
        let root = scaffold_project(&opts).unwrap();
        let project = TrembitaProject { root };
        fs::write(
            project.capabilities_dir().join("bad_queue.rs"),
            r#"
use trembita::{cap_handler, cap_register_chain, CapError, CapGroup, CapManifest};

#[derive(Default)]
pub struct BadState;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Work { pub n: u32 }

#[cap_handler(group = "bad")]
async fn work(_: Work, _: &mut BadState) -> Result<(), CapError> { Ok(()) }

#[must_use]
pub fn manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<BadState>::for_cap::<Work>()
            .instances(1)
            .default_queue_for::<Work>(),
        work_register,
    ))
}
"#,
        )
        .unwrap();
        let report = run_doctor(&project, false);
        assert!(
            report.findings.iter().any(|f| {
                f.level == Level::Error
                    && f.message.contains("instances(1)")
                    && f.message.contains("queued")
            }),
            "findings: {:?}",
            report.findings
        );
    }

    #[test]
    fn doctor_accepts_onboarding_keyed_shared_fixed_one() {
        let dir = tempdir().unwrap();
        let opts = NewProjectOpts {
            name: "scale-ok".into(),
            output: dir.path().to_path_buf(),
            features: AppFeature::defaults(),
            trembita_version: "0.3.2".into(),
            trembita_path: None,
            template: None,
        };
        let root = scaffold_project(&opts).unwrap();
        let project = TrembitaProject { root };
        let report = run_doctor(&project, false);
        assert!(
            !report.findings.iter().any(|f| {
                f.level == Level::Error
                    && f.message.contains("instances(1)")
                    && f.message.contains("queued")
            }),
            "onboarding template: {:?}",
            report
                .findings
                .iter()
                .filter(|f| f.level == Level::Error)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn doctor_flags_duplicate_job_stream_in_manifest() {
        let dir = tempdir().unwrap();
        let opts = NewProjectOpts {
            name: "dup-jobs".into(),
            output: dir.path().to_path_buf(),
            features: AppFeature::defaults(),
            trembita_version: "0.3.2".into(),
            trembita_path: None,
            template: None,
        };
        let root = scaffold_project(&opts).unwrap();
        let project = TrembitaProject { root };
        let manifest_path = project.manifest_rs();
        let mut manifest = fs::read_to_string(&manifest_path).unwrap();
        manifest = manifest.replace(
            "// trembita:jobs-end",
            "JobOpts::new(SAMPLE_STREAM).lease(Duration::from_secs(1)),\n            // trembita:jobs-end",
        );
        fs::write(&manifest_path, manifest).unwrap();
        let report = run_doctor(&project, false);
        assert!(
            report
                .findings
                .iter()
                .any(|f| { f.level == Level::Error && f.message.contains("duplicate job stream") })
        );
    }

    /// B-33 — `preflight_env_vars` join seeds vs gateway session secret.
    #[test]
    fn b33_preflight_join_seeds_scenarios_table() {
        const BASE: &str = "TREMBITA_LISTEN=0.0.0.0:443\nTREMBITA_DATA_DIR=/var/lib/app\nTREMBITA_CERT_DIR=/var/lib/certs\n";

        struct Row {
            name: &'static str,
            env_suffix: &'static str,
            strict: bool,
            want_error: bool,
            want_warn: bool,
            want_ok_pair: bool,
        }

        let rows = [
            Row {
                name: "active join without secret (preflight)",
                env_suffix: "TREMBITA_JOIN_SEEDS=1@seed:443\n",
                strict: true,
                want_error: true,
                want_warn: false,
                want_ok_pair: false,
            },
            Row {
                name: "active join without secret (doctor only)",
                env_suffix: "TREMBITA_JOIN_SEEDS=1@seed:443\n",
                strict: false,
                want_error: false,
                want_warn: true,
                want_ok_pair: false,
            },
            Row {
                name: "join + session secret documented",
                env_suffix: "TREMBITA_JOIN_SEEDS=1@seed:443\nTREMBITA_GATEWAY_SESSION_SECRET=sixteen-bytes-min!!\n",
                strict: true,
                want_error: false,
                want_warn: false,
                want_ok_pair: true,
            },
            Row {
                name: "commented join seeds only",
                env_suffix: "# TREMBITA_JOIN_SEEDS=1@seed:443\n",
                strict: true,
                want_error: false,
                want_warn: false,
                want_ok_pair: false,
            },
        ];

        for row in rows {
            let dir = tempdir().unwrap();
            let opts = NewProjectOpts {
                name: "b33-preflight".into(),
                output: dir.path().to_path_buf(),
                features: AppFeature::defaults(),
                trembita_version: "0.3.2".into(),
                trembita_path: None,
                template: None,
            };
            let root = scaffold_project(&opts).unwrap();
            let body = format!("{BASE}{}", row.env_suffix);
            fs::write(root.join("deploy/.env.example"), body).unwrap();
            let project = TrembitaProject { root };
            let report = run_doctor(&project, row.strict);

            let join_error = report.findings.iter().any(|f| {
                f.level == Level::Error
                    && f.message.contains("TREMBITA_JOIN_SEEDS")
                    && f.message.contains("GATEWAY_SESSION_SECRET")
            });
            let join_warn = report.findings.iter().any(|f| {
                f.level == Level::Warn
                    && f.message.contains("TREMBITA_JOIN_SEEDS")
                    && f.message.contains("GATEWAY_SESSION_SECRET")
            });
            let join_ok = report.findings.iter().any(|f| {
                f.level == Level::Ok && f.message.contains("join seeds + gateway session secret")
            });

            assert_eq!(
                join_error, row.want_error,
                "{}: join_error={join_error} findings={:?}",
                row.name, report.findings
            );
            assert_eq!(
                join_warn, row.want_warn,
                "{}: join_warn={join_warn} findings={:?}",
                row.name, report.findings
            );
            assert_eq!(
                join_ok, row.want_ok_pair,
                "{}: join_ok={join_ok} findings={:?}",
                row.name, report.findings
            );
        }
    }
}
