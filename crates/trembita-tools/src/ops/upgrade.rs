//! B-54 — external one-shot cluster upgrade operator (HTTP only, no SSH loop).

use std::time::{Duration, Instant};

use reqwest::StatusCode;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::Deserialize;
use thiserror::Error;
use trembita_core::UpgradeView;

/// Upgrade operator failures (CI-friendly messages).
#[derive(Debug, Error)]
pub enum UpgradeOpsError {
    /// Preflight check failed (`/ready`, `/cluster/upgrade`, optional ops-summary).
    #[error("preflight failed: {0}")]
    Preflight(String),
    /// HTTP transport or parse error.
    #[error("{0}")]
    Http(String),
    /// Rolling upgrade aborted or timed out.
    #[error("upgrade failed: {0}")]
    Upgrade(String),
}

/// Manifest for `POST /cluster/upgrade/desired`.
#[derive(Debug, Clone)]
pub struct UpgradeManifest {
    /// Target `TREMBITA_APP_VERSION` after roll.
    pub app_version: String,
    /// Artifact URL (`https://` or `file://` on nodes).
    pub url: String,
    /// Lowercase hex SHA-256 of artifact bytes.
    pub sha256_hex: String,
}

/// Options for [`run_upgrade`].
#[derive(Debug, Clone)]
pub struct RunUpgradeOpts {
    /// Gateway base URL (e.g. `https://seed:443` or `http://127.0.0.1:8190`).
    pub gateway: String,
    /// Rolling target manifest.
    pub manifest: UpgradeManifest,
    /// Optional `Authorization: Bearer …` (else env `GATEWAY_TOKEN` / `TREMBITA_GATEWAY_TOKEN`).
    pub bearer_token: Option<String>,
    /// Total time to wait for `fleet_ready`.
    pub timeout: Duration,
    /// Poll interval for `GET /cluster/upgrade`.
    pub poll_interval: Duration,
    /// When false, skip `/ready` and ops-summary checks.
    pub preflight: bool,
    /// When true, skip final `/ready` smoke after success.
    pub skip_post_smoke: bool,
}

impl RunUpgradeOpts {
    /// Defaults suitable for CI (15m timeout, 5s poll).
    #[must_use]
    pub fn with_gateway_and_manifest(
        gateway: impl Into<String>,
        manifest: UpgradeManifest,
    ) -> Self {
        Self {
            gateway: gateway.into(),
            manifest,
            bearer_token: None,
            timeout: Duration::from_secs(900),
            poll_interval: Duration::from_secs(5),
            preflight: true,
            skip_post_smoke: false,
        }
    }
}

#[derive(Debug, Deserialize)]
struct OpsSummaryJoin {
    #[serde(default)]
    phase: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpsSummaryLite {
    #[serde(default)]
    join: Option<OpsSummaryJoin>,
}

fn normalize_base(gateway: &str) -> String {
    gateway.trim_end_matches('/').to_string()
}

fn bearer_from_env() -> Option<String> {
    std::env::var("GATEWAY_TOKEN")
        .ok()
        .or_else(|| std::env::var("TREMBITA_GATEWAY_TOKEN").ok())
        .filter(|s| !s.is_empty())
}

fn client(timeout: Duration) -> Result<reqwest::Client, UpgradeOpsError> {
    reqwest::Client::builder()
        .timeout(timeout.min(Duration::from_secs(120)))
        .build()
        .map_err(|e| UpgradeOpsError::Http(e.to_string()))
}

fn apply_auth(req: reqwest::RequestBuilder, bearer: Option<&str>) -> reqwest::RequestBuilder {
    if let Some(token) = bearer.filter(|t| !t.is_empty()) {
        req.header(AUTHORIZATION, format!("Bearer {token}"))
    } else {
        req
    }
}

/// Evaluate preflight HTTP status codes (unit-testable).
pub fn preflight_upgrade_api_status(status: u16) -> Result<(), String> {
    match status {
        200 => Ok(()),
        404 => Err(
            "GET /cluster/upgrade returned 404 — wire UpgradeApi + coordinator on the app gateway (see upgrade-coordinator ADR)".into(),
        ),
        401 | 403 => Err(format!(
            "GET /cluster/upgrade returned {status} — set --bearer-token or GATEWAY_TOKEN"
        )),
        other => Err(format!(
            "GET /cluster/upgrade returned unexpected status {other}"
        )),
    }
}

pub fn preflight_ready_status(status: u16) -> Result<(), String> {
    if status == 200 {
        Ok(())
    } else {
        Err(format!(
            "GET /ready returned {status} (expected 200 before upgrade)"
        ))
    }
}

async fn get_status(
    client: &reqwest::Client,
    url: &str,
    bearer: Option<&str>,
) -> Result<StatusCode, UpgradeOpsError> {
    let req = apply_auth(client.get(url), bearer);
    let resp = req
        .send()
        .await
        .map_err(|e| UpgradeOpsError::Http(format!("GET {url}: {e}")))?;
    Ok(resp.status())
}

async fn preflight(opts: &RunUpgradeOpts, bearer: Option<&str>) -> Result<(), UpgradeOpsError> {
    if !opts.preflight {
        return Ok(());
    }
    let base = normalize_base(&opts.gateway);
    let client = client(Duration::from_secs(30))?;

    let ready = get_status(&client, &format!("{base}/ready"), bearer).await?;
    preflight_ready_status(ready.as_u16()).map_err(UpgradeOpsError::Preflight)?;

    let upgrade_status = get_status(&client, &format!("{base}/cluster/upgrade"), bearer).await?;
    preflight_upgrade_api_status(upgrade_status.as_u16()).map_err(UpgradeOpsError::Preflight)?;

    let summary_url = format!("{base}/introspect/ops-summary");
    if let Ok(status) = get_status(&client, &summary_url, bearer).await
        && status == StatusCode::OK
    {
        let req = apply_auth(client.get(&summary_url), bearer);
        if let Ok(resp) = req.send().await
            && let Ok(summary) = resp.json::<OpsSummaryLite>().await
            && let Some(phase) = summary.join.and_then(|j| j.phase)
            && phase != "pool_ready"
        {
            eprintln!(
                "warning: introspect join.phase={phase:?} (expected pool_ready when possible)"
            );
        }
    }
    Ok(())
}

async fn post_desired(opts: &RunUpgradeOpts, bearer: Option<&str>) -> Result<(), UpgradeOpsError> {
    let base = normalize_base(&opts.gateway);
    let client = client(Duration::from_secs(60))?;
    let url = format!("{base}/cluster/upgrade/desired");
    let body = serde_json::json!({
        "app_version": opts.manifest.app_version,
        "url": opts.manifest.url,
        "sha256_hex": opts.manifest.sha256_hex,
    });
    let req = apply_auth(client.post(&url), bearer).header(CONTENT_TYPE, "application/json");
    let resp = req
        .json(&body)
        .send()
        .await
        .map_err(|e| UpgradeOpsError::Http(format!("POST {url}: {e}")))?;
    let status = resp.status();
    if status == StatusCode::ACCEPTED || status == StatusCode::OK {
        Ok(())
    } else {
        let text = resp.text().await.unwrap_or_default();
        Err(UpgradeOpsError::Upgrade(format!(
            "POST /cluster/upgrade/desired returned {}: {}",
            status.as_u16(),
            text.trim()
        )))
    }
}

async fn fetch_upgrade_view(
    client: &reqwest::Client,
    base: &str,
    bearer: Option<&str>,
) -> Result<UpgradeView, UpgradeOpsError> {
    let url = format!("{base}/cluster/upgrade");
    let req = apply_auth(client.get(&url), bearer);
    let resp = req
        .send()
        .await
        .map_err(|e| UpgradeOpsError::Http(format!("GET {url}: {e}")))?;
    if !resp.status().is_success() {
        return Err(UpgradeOpsError::Http(format!(
            "GET /cluster/upgrade returned {}",
            resp.status()
        )));
    }
    resp.json::<UpgradeView>()
        .await
        .map_err(|e| UpgradeOpsError::Http(format!("decode UpgradeView: {e}")))
}

async fn poll_until_ready(
    opts: &RunUpgradeOpts,
    bearer: Option<&str>,
) -> Result<(), UpgradeOpsError> {
    let base = normalize_base(&opts.gateway);
    let client = client(Duration::from_secs(60))?;
    let deadline = Instant::now() + opts.timeout;
    loop {
        let view = fetch_upgrade_view(&client, &base, bearer).await?;
        if let Some(reason) = &view.aborted {
            return Err(UpgradeOpsError::Upgrade(format!(
                "cluster aborted: {reason}"
            )));
        }
        if view.fleet_ready {
            eprintln!(
                "upgrade: fleet_ready (completed={}, pending={})",
                view.completed.len(),
                view.pending.len()
            );
            return Ok(());
        }
        eprintln!(
            "upgrade: waiting… granted={:?} completed={} pending={}",
            view.granted,
            view.completed.len(),
            view.pending.len()
        );
        if Instant::now() >= deadline {
            return Err(UpgradeOpsError::Upgrade(format!(
                "timed out after {:?} waiting for fleet_ready",
                opts.timeout
            )));
        }
        tokio::time::sleep(opts.poll_interval).await;
    }
}

async fn post_smoke(opts: &RunUpgradeOpts, bearer: Option<&str>) -> Result<(), UpgradeOpsError> {
    if opts.skip_post_smoke {
        return Ok(());
    }
    let base = normalize_base(&opts.gateway);
    let client = client(Duration::from_secs(30))?;
    let ready = get_status(&client, &format!("{base}/ready"), bearer).await?;
    preflight_ready_status(ready.as_u16()).map_err(UpgradeOpsError::Upgrade)
}

/// Run external upgrade: preflight → set desired → poll → post smoke.
///
/// # Errors
/// [`UpgradeOpsError`] on any failed step.
pub async fn run_upgrade(opts: RunUpgradeOpts) -> Result<(), UpgradeOpsError> {
    let bearer = opts.bearer_token.clone().or_else(bearer_from_env);
    eprintln!("upgrade: gateway={}", normalize_base(&opts.gateway));
    preflight(&opts, bearer.as_deref()).await?;
    eprintln!("upgrade: preflight ok");
    post_desired(&opts, bearer.as_deref()).await?;
    eprintln!("upgrade: desired accepted");
    poll_until_ready(&opts, bearer.as_deref()).await?;
    post_smoke(&opts, bearer.as_deref()).await?;
    eprintln!("upgrade: success");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b54_preflight_upgrade_api_fails_fast_on_404() {
        let err = preflight_upgrade_api_status(404).expect_err("404");
        assert!(err.contains("404"));
        assert!(err.contains("UpgradeApi"));
    }

    #[test]
    fn b54_preflight_ready_requires_200_table() {
        assert!(preflight_ready_status(200).is_ok());
        assert!(preflight_ready_status(503).is_err());
    }

    #[test]
    fn b54_preflight_upgrade_auth_errors_table() {
        assert!(
            preflight_upgrade_api_status(401)
                .expect_err("401")
                .contains("GATEWAY_TOKEN")
        );
        assert!(
            preflight_upgrade_api_status(403)
                .expect_err("403")
                .contains("bearer-token")
        );
        assert!(preflight_upgrade_api_status(200).is_ok());
    }
}
