//! One-shot delayed enqueue via HTTP `run_at_ms` (not recurring cron).

#![allow(clippy::large_futures)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http::{Method, StatusCode, Uri, header};
use trembita::{QueueOpts, TrembitaApp, TrembitaConfigure};
use trembita_http::{ResponseBody, RouteTable};
use trembita_jobs::{JobLifecycle, WorkerId};
use trembita_test_support::{advance, boot_local_app, wait_for_trembita_app_leader};

async fn dispatch_body(
    table: &RouteTable,
    method: Method,
    uri: &str,
    body: Bytes,
    content_type: Option<&str>,
) -> (StatusCode, Bytes) {
    let parsed: Uri = uri.parse().expect("uri");
    let path = parsed.path().to_string();
    let query = parsed.query().map(parse_query).unwrap_or_default();
    let mut headers = http::HeaderMap::new();
    if let Some(ct) = content_type {
        headers.insert(header::CONTENT_TYPE, ct.parse().expect("content-type"));
    }
    let resp = table
        .dispatch_open(&method, &path, query, headers, body)
        .await
        .expect("dispatch");
    let bytes = match resp.body() {
        ResponseBody::Bytes(b) => b.clone(),
        ResponseBody::Json(v) => Bytes::from(serde_json::to_vec(v).expect("json")),
        ResponseBody::Empty => Bytes::new(),
    };
    (resp.status_code(), bytes)
}

fn parse_query(raw: &str) -> HashMap<String, String> {
    raw.split('&')
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            Some((k.to_string(), v.to_string()))
        })
        .collect()
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

#[tokio::test(start_paused = true)]
async fn http_run_at_ms_delays_until_visible() {
    let base = std::env::temp_dir().join(format!(
        "trembita-http-delayed-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .data_dir(&base)
                .queue([QueueOpts::new("jobs", Duration::from_secs(60))])
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    ..TrembitaConfigure::default()
                })
        },
        None,
    )
    .await;

    wait_for_trembita_app_leader(&app).await;
    advance(Duration::from_millis(200)).await;

    let table = TrembitaApp::jobs_api(Arc::clone(&app)).route_table();

    // Wall-clock `not_before_ms` (not Tokio virtual time) — use a far-future instant.
    let run_at = now_ms().saturating_add(3_600_000);
    let uri = format!("/jobs/jobs?run_at_ms={run_at}");
    let (status, body) = dispatch_body(
        &table,
        Method::POST,
        &uri,
        Bytes::from_static(b"later"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let accepted: serde_json::Value = serde_json::from_slice(&body).expect("json");
    let job_id = accepted["job_id"].as_u64().expect("job_id");

    let status = app
        .job_status("jobs", trembita_jobs::JobId(job_id))
        .await
        .expect("status")
        .expect("found");
    assert_eq!(status.lifecycle, JobLifecycle::Delayed);

    let queue = app.job_queue("jobs").expect("queue");
    let worker = WorkerId {
        node: app.node_id(),
        instance: 0,
    };
    assert!(queue.lease(worker, 1).await.expect("lease").is_empty());
}

#[tokio::test(start_paused = true)]
async fn enqueue_at_in_the_past_is_immediately_leasable() {
    let base = std::env::temp_dir().join(format!(
        "trembita-enqueue-at-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .data_dir(&base)
                .queue([QueueOpts::new("jobs", Duration::from_secs(60))])
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    ..TrembitaConfigure::default()
                })
        },
        None,
    )
    .await;

    wait_for_trembita_app_leader(&app).await;
    advance(Duration::from_millis(200)).await;

    let job_id = app
        .enqueue_at("jobs", b"now", now_ms().saturating_sub(1))
        .await
        .expect("enqueue");

    let queue = app.job_queue("jobs").expect("queue");
    let worker = WorkerId {
        node: app.node_id(),
        instance: 0,
    };
    let leased = queue.lease(worker, 1).await.expect("lease");
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].job_id, job_id);
}
