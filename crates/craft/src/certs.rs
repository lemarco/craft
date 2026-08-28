//! Hot reload of on-disk mTLS material ([certificates](certificates.md)).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use craft_actor::ClusterState;
use craft_net::{
    CertFingerprint, CertPaths, PemMaterial, QuicServer, QuicTransport, TlsError, client_config,
    load_pem_material, server_config,
};
use craft_proto::NodeId;
use tokio::task::JoinHandle;

use crate::cluster::ClusterFacts;
use crate::security::Security;

/// PEM-backed security: identity loaded from disk plus the paths to re-read on rotation.
pub struct PemSecurity {
    /// The initial TLS material.
    pub security: Security,
    /// On-disk locations written by cert-manager, step-ca renewals, or `generate.sh`.
    pub paths: CertPaths,
}

impl PemSecurity {
    /// Load node cert/key and CA bundle from `paths`.
    ///
    /// # Errors
    /// Returns [`TlsError`] if any file is missing or invalid PEM.
    pub fn load(node_id: NodeId, paths: CertPaths) -> Result<Self, TlsError> {
        let material = load_pem_material(node_id, &paths)?;
        Ok(Self {
            security: Security::from_material(material),
            paths,
        })
    }
}

/// Options controlling a manual reload.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReloadOpts {
    /// When `false` (default), reload fails on the Raft leader so operators
    /// roll followers first ([certificates](https://gitlab.com/lemarco/craft/-/blob/main/docs/decisions/certificates.md)).
    pub allow_leader: bool,
}

/// An error applying a cert reload.
#[derive(Debug, thiserror::Error)]
pub enum CertReloadError {
    /// PEM files could not be read, parsed, or applied to QUIC.
    #[error("cert reload: {0}")]
    Failed(#[from] TlsError),

    /// The node is the Raft leader; reload followers first.
    #[error(
        "node is the Raft leader — reload followers first, or pass ReloadOpts {{ allow_leader: true }}"
    )]
    ReloadLeaderLast,
}

/// Handle for reloading mTLS configs on a live QUIC node ([certificates](certificates.md)).
pub struct CertReloadHandle {
    node_id: NodeId,
    paths: CertPaths,
    server: Arc<QuicServer>,
    transport: Arc<QuicTransport>,
    facts: Arc<ClusterFacts>,
}

impl CertReloadHandle {
    pub(crate) fn new(
        node_id: NodeId,
        paths: CertPaths,
        server: Arc<QuicServer>,
        transport: Arc<QuicTransport>,
        facts: Arc<ClusterFacts>,
    ) -> Self {
        Self {
            node_id,
            paths,
            server,
            transport,
            facts,
        }
    }

    /// The PEM paths this handle watches and reloads.
    #[must_use]
    pub fn paths(&self) -> &CertPaths {
        &self.paths
    }

    /// Re-read PEM files from disk and apply fresh TLS configs.
    ///
    /// # Errors
    /// Returns [`CertReloadError::ReloadLeaderLast`] when this node is leader
    /// and `opts.allow_leader` is false.
    pub async fn reload_now(&self, opts: ReloadOpts) -> Result<(), CertReloadError> {
        if !opts.allow_leader && ClusterState::is_leader(&self.facts) {
            return Err(CertReloadError::ReloadLeaderLast);
        }
        self.apply(load_pem_material(self.node_id, &self.paths)?)
            .await?;
        Ok(())
    }

    async fn apply(&self, material: PemMaterial) -> Result<(), CertReloadError> {
        let server_cfg = server_config(&material.identity, material.roots.clone())?;
        let client_cfg = client_config(&material.identity, material.roots)?;
        self.server.reload(server_cfg);
        self.transport.reload(client_cfg).await;
        Ok(())
    }

    /// Poll `paths` every `period` and reload when the on-disk fingerprint changes.
    #[must_use]
    pub fn spawn_watcher(self: Arc<Self>, period: Duration) -> JoinHandle<()> {
        tokio::spawn(async move {
            let Ok(mut last) = CertFingerprint::read(&self.paths) else {
                return;
            };
            let mut interval = tokio::time::interval(period);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let Ok(now) = CertFingerprint::read(&self.paths) else {
                    continue;
                };
                if now == last {
                    continue;
                }
                match self.reload_now(ReloadOpts::default()).await {
                    Ok(()) => last = now,
                    Err(CertReloadError::ReloadLeaderLast) => {
                        // Followers first: retry on the next tick once leadership moves.
                    }
                    Err(e) => eprintln!("craft cert reload failed: {e}"),
                }
            }
        })
    }

    /// Listen for `SIGHUP` and trigger [`reload_now`](Self::reload_now) with
    /// [`ReloadOpts::default`]. No-op on platforms without `SIGHUP`.
    #[must_use]
    pub fn spawn_sighup(self: Arc<Self>) -> Option<JoinHandle<()>> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Some(tokio::spawn(async move {
                let Ok(mut stream) = signal(SignalKind::hangup()) else {
                    return;
                };
                while stream.recv().await.is_some() {
                    if let Err(e) = self.reload_now(ReloadOpts::default()).await {
                        eprintln!("craft cert reload (SIGHUP): {e}");
                    }
                }
            }))
        }
        #[cfg(not(unix))]
        {
            let _ = self;
            None
        }
    }
}

/// Convenience: build [`CertPaths`] from env var locations.
#[must_use]
pub fn cert_paths_from_env(
    node_cert: impl Into<PathBuf>,
    node_key: impl Into<PathBuf>,
    ca_cert: impl Into<PathBuf>,
) -> CertPaths {
    CertPaths::new(node_cert, node_key, ca_cert)
}
