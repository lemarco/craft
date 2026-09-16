//! Test harness for booting with background consumers.

use std::sync::Arc;

use crate::TrembitaApp;

/// Result of [`crate::TrembitaAppBuilder::boot_for_test_with_consumers`].
pub struct TestBoot {
    /// Running app.
    pub app: Arc<TrembitaApp>,
    pub(crate) consumers: Option<(
        tokio::sync::watch::Sender<bool>,
        Vec<tokio::task::JoinHandle<()>>,
    )>,
}

impl TestBoot {
    /// Stop consumers (if any) and shut down the app.
    pub async fn shutdown(self) {
        if let Some((stop, handles)) = self.consumers {
            let _ = stop.send(true);
            for handle in handles {
                let _ = handle.await;
            }
        }
        self.app.shutdown();
    }
}
