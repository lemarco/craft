//! Durable job queue — enqueue, lease/ack worker loop ([job-queue](../../docs/decisions/job-queue.md)).
//!
//! Run: `cargo run -p craft --example job_queue_worker`

use std::sync::Arc;
use std::time::Duration;

use craft::proto::NodeId;
use craft::{JobQueue, RedbJobQueue, WorkerId, run_queue_consumer};

#[tokio::main]
async fn main() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("queue-default.redb");
    let queue: Arc<dyn JobQueue> =
        Arc::new(RedbJobQueue::open(&path, Duration::from_secs(60)).expect("open queue"));

    for i in 0..5u64 {
        let payload = format!("job-{i}");
        let id = queue.enqueue(payload.as_bytes()).await.expect("enqueue");
        println!("enqueued {id:?}");
    }

    let worker_id = WorkerId {
        node: NodeId(1),
        instance: 0,
    };
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let q = Arc::clone(&queue);
    let consumer = tokio::spawn(async move {
        run_queue_consumer(
            q,
            worker_id,
            2,
            Duration::from_millis(50),
            stop_rx,
            |payload| {
                let bytes = payload.to_vec();
                async move {
                    let text = String::from_utf8_lossy(&bytes);
                    println!("handled {text}");
                    Ok::<(), ()>(())
                }
            },
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;
    stop_tx.send(true).expect("stop");
    consumer.await.expect("consumer task");

    let metrics = queue.metrics().await.expect("metrics");
    println!(
        "done: pending={} leased={}",
        metrics.pending, metrics.leased
    );
}
