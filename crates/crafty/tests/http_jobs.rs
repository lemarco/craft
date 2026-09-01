//! HTTP enqueue → worker integration (B-03).

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use crafty::actor::WorkerId;
use crafty::cluster::EnqueueOptions;
use crafty::{CraftyApp, CraftyConfigure, QueueOpts};
use crafty_actor::JobLifecycle;
use crafty_test_support::{advance, boot_local_app, wait_for_crafty_app_leader};
use tower::ServiceExt;

#[tokio::test(start_paused = true)]
async fn http_post_job_returns_202_and_enqueues() {
    let base = std::env::temp_dir().join(format!(
        "crafty-http-jobs-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        CraftyApp::builder()
            .data_dir(&base)
            .queue([QueueOpts::new("jobs", Duration::from_secs(60))])
            .configure(CraftyConfigure {
                tick_period: Duration::from_millis(5),
                ..CraftyConfigure::default()
            }),
        None,
    )
    .await;

    wait_for_crafty_app_leader(&app).await;
    advance(Duration::from_millis(200)).await;

    let api = CraftyApp::jobs_api(Arc::clone(&app));
    let router = api.router().with_state(Arc::new(api.into_state()));

    let req = Request::builder()
        .method("POST")
        .uri("/jobs/jobs?dedup=invoice-1")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"payload":"send-email"}"#))
        .unwrap();

    let resp = router.clone().oneshot(req).await.expect("route");
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let batch_req = Request::builder()
        .method("POST")
        .uri("/jobs/jobs/batch")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"jobs":[{"payload":"batch-a"},{"payload":"batch-b"}]}"#,
        ))
        .unwrap();
    let batch_resp = router.clone().oneshot(batch_req).await.expect("batch");
    assert_eq!(batch_resp.status(), StatusCode::ACCEPTED);

    let job_id = app.enqueue("jobs", b"send-email").await.expect("enqueue");
    let get_req = Request::builder()
        .method("GET")
        .uri(format!("/jobs/jobs/{}", job_id.0))
        .body(Body::empty())
        .unwrap();
    let get_resp = router.clone().oneshot(get_req).await.expect("get");
    assert_eq!(get_resp.status(), StatusCode::OK);

    let poison_id = app
        .enqueue_opts("jobs", b"poison", EnqueueOptions::max_attempts(1))
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

    let requeue_req = Request::builder()
        .method("POST")
        .uri(format!("/jobs/jobs/{}/requeue", poison_id.0))
        .body(Body::empty())
        .unwrap();
    let requeue_resp = router.oneshot(requeue_req).await.expect("requeue");
    assert_eq!(requeue_resp.status(), StatusCode::OK);

    let pending = app
        .job_status("jobs", poison_id)
        .await
        .expect("status")
        .expect("row");
    assert_eq!(pending.lifecycle, JobLifecycle::Pending);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
