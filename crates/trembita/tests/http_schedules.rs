//! HTTP schedule admin round-trip (B-20).

#![allow(clippy::large_futures)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, StatusCode};
use trembita::cluster::RecurringJob;
use trembita::{CronOpts, QueueOpts, TrembitaApp, TrembitaConfigure};
use trembita_http::RouteTable;
use trembita_test_support::{advance, boot_local_app, wait_for_trembita_app_leader};

async fn dispatch(
    table: &RouteTable,
    method: Method,
    uri: &str,
    body: Bytes,
    content_type: Option<&str>,
) -> StatusCode {
    let mut headers = http::HeaderMap::new();
    if let Some(ct) = content_type {
        headers.insert(
            http::header::CONTENT_TYPE,
            ct.parse().expect("content-type"),
        );
    }
    table
        .dispatch_open(&method, uri, Default::default(), headers, body)
        .await
        .expect("dispatch")
        .status_code()
}

#[tokio::test(start_paused = true)]
async fn http_schedules_upsert_list_remove() {
    let base = std::env::temp_dir().join(format!(
        "trembita-http-schedules-{}",
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
                .cron([CronOpts::new(
                    "jobs",
                    RecurringJob::new("seed", "0 9 * * *", b"seed"),
                )])
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

    let mut table = TrembitaApp::jobs_api(Arc::clone(&app)).route_table();
    table = table.merge(TrembitaApp::schedules_api(Arc::clone(&app)).route_table());

    assert_eq!(
        dispatch(
            &table,
            Method::GET,
            "/jobs/jobs/schedules",
            Bytes::new(),
            None,
        )
        .await,
        StatusCode::OK
    );

    let put_body = br#"{"name":"nightly","cron":"0 10 * * *","payload":"tick","enabled":true}"#;
    assert_eq!(
        dispatch(
            &table,
            Method::PUT,
            "/jobs/jobs/schedules/nightly",
            Bytes::from_static(put_body),
            Some("application/json"),
        )
        .await,
        StatusCode::OK
    );

    assert_eq!(
        dispatch(
            &table,
            Method::DELETE,
            "/jobs/jobs/schedules/nightly",
            Bytes::new(),
            None,
        )
        .await,
        StatusCode::OK
    );
}
