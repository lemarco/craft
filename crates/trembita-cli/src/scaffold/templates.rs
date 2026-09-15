//! Static scaffold snippets (same sources as `trembita new` templates).

macro_rules! app_tpl {
    ($rel:literal) => {
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/templates/trembita-app/",
            $rel
        ))
    };
}

/// `src/http/ops.rs` from the product app template.
#[must_use]
pub fn http_ops_rs() -> &'static str {
    app_tpl!("src/http/ops.rs.tpl")
}

/// `src/http/jobs.rs` from the product app template.
#[must_use]
pub fn http_jobs_rs() -> &'static str {
    app_tpl!("src/http/jobs.rs.tpl")
}
