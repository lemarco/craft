//! HTTP enqueue → worker integration (B-03).

#![allow(clippy::large_futures)] // boot_local_app future grows with product builder surface

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, StatusCode, Uri, header};
use trembita::cluster::EnqueueOptions;
use trembita::{QueueOpts, TrembitaApp, TrembitaConfigure};
use trembita_http::RouteTable;
use trembita_jobs::JobLifecycle;
use trembita_jobs::WorkerId;
use trembita_test_support::{advance, boot_local_app, wait_for_trembita_app_leader};

async fn dispatch(
    table: &RouteTable,
    method: Method,
    uri: &str,
    body: Bytes,
    content_type: Option<&str>,
) -> StatusCode {
    let parsed: Uri = uri.parse().expect("uri");
    let path = parsed.path().to_string();
    let query = parsed.query().map(parse_query).unwrap_or_default();
    let mut headers = http::HeaderMap::new();
    if let Some(ct) = content_type {
        headers.insert(header::CONTENT_TYPE, ct.parse().expect("content-type"));
    }
    table
        .dispatch_open(&method, &path, query, headers, body)
        .await
        .expect("dispatch")
        .status_code()
}

fn parse_query(raw: &str) -> HashMap<String, String> {
    raw.split('&')
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            Some((k.to_string(), v.to_string()))
        })
        .collect()
}

#[tokio::test(start_paused = true)]
#[allow(clippy::too_many_lines)]
async fn http_post_job_returns_202_and_enqueues() {
    let base = std::env::temp_dir().join(format!(
        "trembita-http-jobs-{}",
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
                .configure(
                    TrembitaConfigure::default()
                        .with_local_gateway_apis()
                        .with_data_dir(&base),
                )
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

    let api = TrembitaApp::jobs_api(Arc::clone(&app));
    let table = api.route_table();

    assert_eq!(
        dispatch(
            &table,
            Method::POST,
            "/jobs/jobs?dedup=invoice-1",
            Bytes::from(r#"{"payload":"send-email"}"#),
            Some("application/json"),
        )
        .await,
        StatusCode::ACCEPTED
    );

    assert_eq!(
        dispatch(
            &table,
            Method::POST,
            "/jobs/jobs/batch",
            Bytes::from(r#"{"jobs":[{"payload":"batch-a"},{"payload":"batch-b"}]}"#),
            Some("application/json"),
        )
        .await,
        StatusCode::ACCEPTED
    );

    let job_id = app.enqueue("jobs", b"send-email").await.expect("enqueue");
    assert_eq!(
        dispatch(
            &table,
            Method::GET,
            &format!("/jobs/jobs/{}", job_id.0),
            Bytes::new(),
            None,
        )
        .await,
        StatusCode::OK
    );

    let poison_id = app
        .enqueue_opts(
            "jobs",
            b"poison",
            EnqueueOptions::max_attempts(trembita::proto::MaxAttempts(1)),
        )
        .await
        .expect("enqueue poison");
    let queue = app.job_queue("jobs").expect("queue");
    let worker = WorkerId {
        node: app.node_id(),
        instance: 0,
    };
    let leased = loop {
        let jobs = queue.lease(worker, 1).await.expect("lease");
        if jobs.is_empty() {
            advance(Duration::from_millis(50)).await;
            continue;
        }
        if jobs[0].job_id == poison_id {
            break jobs[0].clone();
        }
        queue
            .nack(worker, jobs[0].lease_id)
            .await
            .expect("nack unrelated job");
    };
    queue
        .nack(worker, leased.lease_id)
        .await
        .expect("nack poison");
    advance(Duration::from_secs(2)).await;

    let dl_status = app
        .job_status("jobs", poison_id)
        .await
        .expect("status")
        .expect("row");
    assert_eq!(dl_status.lifecycle, JobLifecycle::DeadLetter);

    assert_eq!(
        dispatch(
            &table,
            Method::POST,
            &format!("/jobs/jobs/{}/requeue", poison_id.0),
            Bytes::new(),
            None,
        )
        .await,
        StatusCode::OK
    );

    let pending = app
        .job_status("jobs", poison_id)
        .await
        .expect("status")
        .expect("row");
    assert_eq!(pending.lifecycle, JobLifecycle::Pending);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
