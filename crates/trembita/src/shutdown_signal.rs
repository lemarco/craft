//! Process shutdown signals for product binaries (`SIGINT` + `SIGTERM` on Unix).

/// Block until the process receives a shutdown signal.
///
/// On Unix this waits for **SIGINT** (Ctrl-C) or **SIGTERM** (`systemctl stop`, `docker stop`,
/// `kill`, Kubernetes pod termination). On other platforms only SIGINT is available.
pub async fn wait_for_int_or_term() {
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                tracing::warn!(%error, "failed to listen for SIGINT");
            }
        }
        () = wait_for_sigterm() => {}
    }
}

#[cfg(unix)]
async fn wait_for_sigterm() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut term = signal(SignalKind::terminate()).expect("register SIGTERM handler");
    term.recv().await;
}

#[cfg(not(unix))]
async fn wait_for_sigterm() {
    std::future::pending::<()>().await;
}
