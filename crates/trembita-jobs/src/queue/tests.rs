use std::time::{Duration, SystemTime, UNIX_EPOCH};

use trembita_proto::{NodeId, WorkerId};

use super::{
    EnqueueOptions, InMemoryJobQueue, JobId, JobLifecycle, JobListFilter, JobQueue, QueueError,
    run_queue_consumer,
};

fn worker(instance: u32) -> WorkerId {
    WorkerId {
        node: NodeId(1),
        instance,
    }
}

#[tokio::test]
async fn enqueue_lease_ack_round_trip() {
    let q = InMemoryJobQueue::new(Duration::from_secs(30));
    let id = q.enqueue(b"job").await.unwrap();
    assert_eq!(id, JobId(1));

    let leased = q.lease(worker(0), 8).await.unwrap();
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].payload, b"job");

    q.ack(worker(0), leased[0].lease_id).await.unwrap();
    let m = q.metrics().await.unwrap();
    assert_eq!(m.pending, 0);
    assert_eq!(m.leased, 0);
}

#[tokio::test]
async fn two_workers_get_distinct_jobs() {
    let q = InMemoryJobQueue::new(Duration::from_secs(30));
    q.enqueue(b"a").await.unwrap();
    q.enqueue(b"b").await.unwrap();

    let a = q.lease(worker(0), 1).await.unwrap();
    let b = q.lease(worker(1), 1).await.unwrap();
    assert_eq!(a[0].payload, b"a");
    assert_eq!(b[0].payload, b"b");
}

#[tokio::test]
async fn nack_requeues() {
    let q = InMemoryJobQueue::new(Duration::from_secs(30));
    q.enqueue(b"x").await.unwrap();
    let leased = q.lease(worker(0), 1).await.unwrap();
    q.nack(worker(0), leased[0].lease_id).await.unwrap();

    // First failure schedules ~1s backoff before the job is leasable again.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let again = q.lease(worker(1), 1).await.unwrap();
    assert_eq!(again[0].payload, b"x");
}

#[tokio::test]
async fn expired_lease_returns_to_pending() {
    let q = InMemoryJobQueue::new(Duration::from_millis(20));
    q.enqueue(b"z").await.unwrap();
    let leased = q.lease(worker(0), 1).await.unwrap();
    assert_eq!(leased.len(), 1);

    tokio::time::sleep(Duration::from_millis(40)).await;
    let m = q.metrics().await.unwrap();
    assert_eq!(m.leased, 0);
    // Reclaimed jobs wait out the retry backoff before counting as pending.
    assert_eq!(m.pending, 0);

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let m = q.metrics().await.unwrap();
    assert_eq!(m.pending, 1);

    let again = q.lease(worker(1), 1).await.unwrap();
    assert_eq!(again[0].payload, b"z");
}

#[tokio::test]
async fn extend_lease_prevents_reclaim() {
    let q = InMemoryJobQueue::new(Duration::from_millis(50));
    q.enqueue(b"long").await.unwrap();
    let leased = q.lease(worker(0), 1).await.unwrap();
    let lease_id = leased[0].lease_id;

    tokio::time::sleep(Duration::from_millis(40)).await;
    q.extend_lease(worker(0), lease_id).await.unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let other = q.lease(worker(1), 1).await.unwrap();
    assert!(other.is_empty());
    let m = q.metrics().await.unwrap();
    assert_eq!(m.leased, 1);

    q.ack(worker(0), lease_id).await.unwrap();
}

#[tokio::test]
async fn dedup_key_returns_existing_job_id() {
    let q = InMemoryJobQueue::new(Duration::from_secs(30));
    let id1 = q
        .enqueue_opts(b"first", EnqueueOptions::dedup_key("order-1"))
        .await
        .unwrap();
    let id2 = q
        .enqueue_opts(b"retry", EnqueueOptions::dedup_key("order-1"))
        .await
        .unwrap();
    assert_eq!(id1, id2);
    assert_eq!(q.metrics().await.unwrap().pending, 1);
}

#[tokio::test]
async fn priority_jobs_leased_first() {
    let q = InMemoryJobQueue::new(Duration::from_secs(30));
    q.enqueue_opts(b"low", EnqueueOptions::default())
        .await
        .unwrap();
    q.enqueue_opts(b"high", EnqueueOptions::priority(10))
        .await
        .unwrap();

    let leased = q.lease(worker(0), 1).await.unwrap();
    assert_eq!(leased[0].payload, b"high");
}

#[tokio::test]
async fn delayed_job_not_leased_before_not_before() {
    let q = InMemoryJobQueue::new(Duration::from_secs(30));
    let far_future = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
        + 3_600_000;
    q.enqueue_opts(
        b"later",
        EnqueueOptions {
            not_before_ms: Some(far_future),
            ..EnqueueOptions::default()
        },
    )
    .await
    .unwrap();

    let empty = q.lease(worker(0), 1).await.unwrap();
    assert!(empty.is_empty());
    assert_eq!(q.metrics().await.unwrap().pending, 0);
}

#[tokio::test]
async fn ack_rejects_wrong_worker() {
    let q = InMemoryJobQueue::new(Duration::from_secs(30));
    q.enqueue(b"j").await.unwrap();
    let leased = q.lease(worker(0), 1).await.unwrap();
    assert!(matches!(
        q.ack(worker(1), leased[0].lease_id).await,
        Err(QueueError::InvalidLease)
    ));
}

#[tokio::test]
async fn job_status_reports_lifecycle() {
    let q = InMemoryJobQueue::new(Duration::from_secs(30));
    let id = q.enqueue(b"job").await.unwrap();
    let pending = q.job_status(id).await.unwrap().expect("pending");
    assert_eq!(pending.lifecycle, JobLifecycle::Pending);

    let leased = q.lease(worker(0), 1).await.unwrap();
    let status = q.job_status(id).await.unwrap().expect("leased");
    assert_eq!(status.lifecycle, JobLifecycle::Leased);
    q.ack(worker(0), leased[0].lease_id).await.unwrap();
    assert!(q.job_status(id).await.unwrap().is_none());
}

#[tokio::test]
async fn max_attempts_moves_job_to_dead_letter() {
    let q = InMemoryJobQueue::new(Duration::from_secs(30));
    let id = q
        .enqueue_opts(b"poison", EnqueueOptions::max_attempts(2))
        .await
        .unwrap();
    for _ in 0..2 {
        let leased = q.lease(worker(0), 1).await.unwrap();
        q.nack(worker(0), leased[0].lease_id).await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    let status = q.job_status(id).await.unwrap().expect("dead letter");
    assert_eq!(status.lifecycle, JobLifecycle::DeadLetter);
    assert_eq!(q.metrics().await.unwrap().dead_letter, 1);
    assert!(q.lease(worker(1), 1).await.unwrap().is_empty());

    q.requeue_dead_letter(id).await.unwrap();
    let pending = q.job_status(id).await.unwrap().expect("pending again");
    assert_eq!(pending.lifecycle, JobLifecycle::Pending);
    assert_eq!(pending.attempts, 0);
}

#[tokio::test]
async fn list_jobs_filters_dead_letter() {
    let q = InMemoryJobQueue::new(Duration::from_secs(30)).default_max_attempts(1);
    let id = q.enqueue(b"x").await.unwrap();
    let leased = q.lease(worker(0), 1).await.unwrap();
    q.nack(worker(0), leased[0].lease_id).await.unwrap();

    let dl = q
        .list_jobs(JobListFilter {
            lifecycle: Some(JobLifecycle::DeadLetter),
            ..JobListFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(dl.jobs.len(), 1);
    assert_eq!(dl.jobs[0].job_id, id);
    assert_eq!(dl.jobs[0].lifecycle, JobLifecycle::DeadLetter);
}

#[tokio::test]
async fn requeue_dead_letter_batch_partial_success() {
    let q = InMemoryJobQueue::new(Duration::from_secs(30)).default_max_attempts(1);
    let id = q.enqueue(b"poison").await.unwrap();
    let leased = q.lease(worker(0), 1).await.unwrap();
    q.nack(worker(0), leased[0].lease_id).await.unwrap();

    let result = q
        .requeue_dead_letter_batch(&[id, JobId(999)])
        .await
        .unwrap();
    assert_eq!(result.requeued, vec![id]);
    assert_eq!(result.failures.len(), 1);
    assert_eq!(result.failures[0].0, JobId(999));
}

#[tokio::test]
async fn stream_default_max_attempts_applies_when_job_leaves_it_unset() {
    let q = InMemoryJobQueue::new(Duration::from_secs(30)).default_max_attempts(2);
    // Plain enqueue: no per-job options at all.
    let id = q.enqueue(b"inherits").await.unwrap();
    assert_eq!(
        q.job_status(id)
            .await
            .unwrap()
            .expect("status")
            .max_attempts,
        2
    );

    for _ in 0..2 {
        let leased = q.lease(worker(0), 1).await.unwrap();
        q.nack(worker(0), leased[0].lease_id).await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    assert_eq!(
        q.job_status(id).await.unwrap().expect("status").lifecycle,
        JobLifecycle::DeadLetter
    );
}

#[tokio::test]
async fn explicit_max_attempts_overrides_stream_default() {
    let q = InMemoryJobQueue::new(Duration::from_secs(30)).default_max_attempts(2);

    // Explicit `0` is a request for unlimited retries, not "unset".
    let unlimited = q
        .enqueue_opts(b"unlimited", EnqueueOptions::max_attempts(0))
        .await
        .unwrap();
    assert_eq!(
        q.job_status(unlimited)
            .await
            .unwrap()
            .expect("status")
            .max_attempts,
        0
    );

    // A non-zero explicit ceiling wins over the stream default too.
    let capped = q
        .enqueue_opts(b"capped", EnqueueOptions::max_attempts(5))
        .await
        .unwrap();
    assert_eq!(
        q.job_status(capped)
            .await
            .unwrap()
            .expect("status")
            .max_attempts,
        5
    );
}

#[tokio::test]
async fn zero_stream_default_keeps_retries_unlimited() {
    let q = InMemoryJobQueue::new(Duration::from_secs(30));
    let id = q.enqueue(b"forever").await.unwrap();
    assert_eq!(
        q.job_status(id)
            .await
            .unwrap()
            .expect("status")
            .max_attempts,
        0
    );
    for _ in 0..3 {
        let leased = q.lease(worker(0), 1).await.unwrap();
        q.nack(worker(0), leased[0].lease_id).await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    // Still retrying (pending, or delayed by retry backoff) — never dead-lettered.
    let status = q.job_status(id).await.unwrap().expect("status");
    assert_ne!(status.lifecycle, JobLifecycle::DeadLetter);
    assert_eq!(status.attempts, 3);
    assert_eq!(q.metrics().await.unwrap().dead_letter, 0);
}

#[tokio::test]
async fn redelivered_gauge_counts_jobs_that_failed_an_attempt() {
    let q = InMemoryJobQueue::new(Duration::from_secs(30));
    let id = q.enqueue(b"flaky").await.unwrap();
    assert_eq!(q.metrics().await.unwrap().redelivered, 0, "first delivery");

    let leased = q.lease(worker(0), 1).await.unwrap();
    assert_eq!(leased[0].attempts, 1, "attempts start at 1");
    q.nack(worker(0), leased[0].lease_id).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert_eq!(
        q.metrics().await.unwrap().redelivered,
        1,
        "job awaiting redelivery is an idempotency smell"
    );

    let again = q.lease(worker(0), 1).await.unwrap();
    assert_eq!(again[0].attempts, 2, "redelivery reports attempt 2");
    q.ack(worker(0), again[0].lease_id).await.unwrap();
    assert_eq!(
        q.metrics().await.unwrap().redelivered,
        0,
        "acked job leaves the queue"
    );
    let _ = id;
}

#[tokio::test]
async fn enqueue_batch_and_ack_batch_round_trip() {
    let q = InMemoryJobQueue::new(Duration::from_secs(30));
    let ids = q
        .enqueue_batch(&[b"a".as_slice(), b"b", b"c"])
        .await
        .unwrap();
    assert_eq!(ids.len(), 3);

    let leased = q.lease(worker(0), 8).await.unwrap();
    assert_eq!(leased.len(), 3);
    let lease_ids: Vec<_> = leased.iter().map(|j| j.lease_id).collect();
    q.ack_batch(worker(0), &lease_ids).await.unwrap();
    assert_eq!(q.metrics().await.unwrap().pending, 0);
}

#[tokio::test]
async fn run_queue_consumer_acks_leased_batch() {
    use std::sync::Arc;

    let q = Arc::new(InMemoryJobQueue::new(Duration::from_secs(30)));
    q.enqueue_batch(&[b"1".as_slice(), b"2"]).await.unwrap();
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let worker = worker(0);
    let queue = Arc::clone(&q);
    let consumer = tokio::spawn(async move {
        run_queue_consumer(
            queue,
            worker,
            8,
            Duration::from_millis(10),
            stop_rx,
            |job| {
                let len = job.payload.len();
                let attempts = job.attempts;
                async move {
                    assert!(len > 0);
                    assert_eq!(attempts, 1, "first delivery");
                    Ok::<(), ()>(())
                }
            },
            None,
            1,
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    stop_tx.send(true).unwrap();
    consumer.await.unwrap();
    assert_eq!(q.metrics().await.unwrap().pending, 0);
}

#[tokio::test]
async fn run_queue_consumer_finishes_in_flight_batch_on_stop() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let q = Arc::new(InMemoryJobQueue::new(Duration::from_secs(30)));
    q.enqueue_batch(&[b"a".as_slice(), b"b"]).await.unwrap();
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let processed = Arc::new(AtomicUsize::new(0));
    let worker = worker(0);
    let queue = Arc::clone(&q);
    let processed_in_task = Arc::clone(&processed);
    let consumer = tokio::spawn(async move {
        run_queue_consumer(
            queue,
            worker,
            8,
            Duration::from_millis(10),
            stop_rx,
            move |_job| {
                let processed = Arc::clone(&processed_in_task);
                async move {
                    processed.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), ()>(())
                }
            },
            None,
            1,
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    stop_tx.send(true).unwrap();
    consumer.await.unwrap();
    assert_eq!(
        processed.load(Ordering::SeqCst),
        2,
        "both leased jobs must finish"
    );
    assert_eq!(q.metrics().await.unwrap().pending, 0);
}
