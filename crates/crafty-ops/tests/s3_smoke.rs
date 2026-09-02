//! Optional S3-compatible object-store smoke (production ops follow-up).
//!
//! ```bash
//! AWS_ENDPOINT_URL=http://127.0.0.1:9000 \
//! AWS_ACCESS_KEY_ID=minio AWS_SECRET_ACCESS_KEY=minio123 \
//! CRAFTY_OPS_S3_BUCKET=crafty-test \
//! cargo test -p crafty-ops --test s3_smoke -- --ignored
//! ```

use std::env;

use crafty_ops::backup::{export_local, import_local, pull_object, push_object};
use tempfile::tempdir;

#[tokio::test]
#[ignore = "heavy: requires S3-compatible endpoint (AWS_ENDPOINT_URL)"]
async fn s3_push_pull_roundtrip_when_configured() {
    if env::var("AWS_ENDPOINT_URL").is_err() && env::var("AWS_ACCESS_KEY_ID").is_err() {
        eprintln!("SKIP: no S3 credentials / endpoint configured");
        return;
    }
    let bucket = env::var("CRAFTY_OPS_S3_BUCKET").unwrap_or_else(|_| "crafty-test".into());
    let key = "e2e-backup.tar.gz";
    let dest = format!("s3://{bucket}/{key}");

    let data = tempdir().unwrap();
    std::fs::write(data.path().join("group-0.redb"), b"snapshot").unwrap();
    let archive = tempdir().unwrap();
    let tarball = archive.path().join("backup.tar.gz");
    export_local(data.path(), &tarball).unwrap();

    push_object(&tarball, &dest).await.expect("push to s3");
    let pulled = archive.path().join("out.tar.gz");
    pull_object(&dest, &pulled).await.expect("pull from s3");

    let restore = tempdir().unwrap();
    import_local(restore.path(), &pulled).unwrap();
    assert_eq!(
        std::fs::read(restore.path().join("group-0.redb")).unwrap(),
        b"snapshot"
    );
}
