//! Start the assembled node over local or QUIC transport.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use trembita_core::StateMachine;
use trembita_net::{
    LocalNetwork, LocalTransport, PeerDirectory, QuicServer, QuicTransport, Transport,
    client_config, server_config,
};
use trembita_proto::NodeId;

use super::TrembitaClusterBuilder;
use crate::builder::error::StartError;
use crate::builder::join::{join_cluster, join_cluster_auto};
use crate::certs::PemSecurity;
use crate::cluster_handle::TrembitaCluster;
use crate::handler::{NoPeers, PeerSource};
use crate::security::Security;
use trembita_net::BackoffPolicy;

impl<M: trembita_core::StateMachine + Default + 'static> TrembitaClusterBuilder<M> {
    pub async fn start_local(self, net: &LocalNetwork) -> TrembitaCluster<M> {
        let node_id = self.node_id;
        let transport: Arc<dyn Transport> = Arc::new(LocalTransport::new(net.clone(), node_id));
        let peers: Arc<dyn PeerSource> = Arc::new(NoPeers);
        let (cluster, router) = self.assemble(transport, peers, None).await;
        net.attach(node_id, router);
        cluster
    }

    /// Start the node over the live HTTP/3-over-QUIC transport with mTLS (security,
    /// wire-transport) — the production path. Binds a QUIC listener on `listen`, dials
    /// peers found in `peers` (a [`NodeId`] → address book), and authenticates
    /// every connection with `security`.
    ///
    /// For a **static** cluster the `peers` directory should contain the address
    /// of every member (this node's own entry is ignored); give each node the
    /// same [`members`](Self::members) set and `peers` map. For **elastic**
    /// growth, pair [`join`](Self::join) with a `peers` map holding just the seed
    /// — addresses of the rest are discovered over `/cluster/peers` (discovery).
    ///
    /// Must run inside a Tokio runtime.
    ///
    /// # Errors
    /// Returns [`StartError`] if the mTLS configuration cannot be built, the
    /// QUIC listener cannot bind `listen`, or a requested dynamic
    /// [`join`](Self::join) could not be completed.
    pub async fn start_quic(
        self,
        security: Security,
        listen: SocketAddr,
        peers: PeerDirectory,
    ) -> Result<TrembitaCluster<M>, StartError> {
        self.start_quic_inner(security, listen, peers, None, None)
            .await
    }

    /// Like [`start_quic`](Self::start_quic) with PEM reload paths and optional cert directory.
    ///
    /// # Errors
    /// Same as [`start_quic`](Self::start_quic).
    pub async fn start_quic_cluster(
        self,
        security: Security,
        listen: SocketAddr,
        peers: PeerDirectory,
        pem_paths: Option<trembita_net::CertPaths>,
        cert_dir: Option<PathBuf>,
    ) -> Result<TrembitaCluster<M>, StartError> {
        self.start_quic_inner(security, listen, peers, pem_paths, cert_dir)
            .await
    }

    /// Like [`start_quic`](Self::start_quic) but loads from [`PemSecurity`] and,
    /// when [`cert_watch`](Self::cert_watch) is set (or by default every **60s**),
    /// hot-reloads TLS when the PEM files change. Also reloads on `SIGHUP` (Unix).
    ///
    /// # Errors
    /// Same as [`start_quic`](Self::start_quic).
    pub async fn start_quic_pem(
        self,
        pem: PemSecurity,
        listen: SocketAddr,
        peers: PeerDirectory,
    ) -> Result<TrembitaCluster<M>, StartError> {
        let paths = pem.paths.clone();
        self.start_quic_inner(pem.security, listen, peers, Some(paths), None)
            .await
    }

    #[allow(clippy::too_many_lines)]
    async fn start_quic_inner(
        mut self,
        mut security: Security,
        listen: SocketAddr,
        mut peers: PeerDirectory,
        mut pem_paths: Option<trembita_net::CertPaths>,
        cert_dir: Option<PathBuf>,
    ) -> Result<TrembitaCluster<M>, StartError> {
        let dynamic_join = !self.join_seeds.is_empty();
        if let Some(ref data_dir) = self.data_dir {
            if let Some(persisted) = node_id::read_persisted(data_dir) {
                self.node_id = persisted;
                if !dynamic_join {
                    if !self.members.contains(&persisted) {
                        self.members.push(persisted);
                        self.members.sort();
                    }
                    if !peers.contains(persisted) {
                        peers.insert(persisted, listen);
                    }
                }
            } else if !dynamic_join {
                if self.node_id == NodeId(0) {
                    self.node_id = NodeId(1);
                }
                node_id::persist(data_dir, self.node_id)
                    .map_err(|e| StartError::Config(format!("persist node id: {e}")))?;
                if !self.members.contains(&self.node_id) {
                    self.members.push(self.node_id);
                    self.members.sort();
                }
                if !peers.contains(self.node_id) {
                    peers.insert(self.node_id, listen);
                }
            }
        }

        let mut server_cfg = server_config(&security.identity, security.roots.clone())?;
        let mut client_cfg = client_config(&security.identity, security.roots.clone())?;

        let server = Arc::new(
            QuicServer::bind(listen, server_cfg.clone()).map_err(|source| StartError::Bind {
                addr: listen,
                source,
            })?,
        );
        // Share the bound endpoint so outbound dials reuse the listener socket.
        let endpoint = server.endpoint().clone();
        let quic = Arc::new(QuicTransport::with_policy(
            endpoint,
            client_cfg.clone(),
            peers.clone(),
            BackoffPolicy::default(),
            self.traffic_policy.clone(),
        ));
        let seeds = crate::discovery::dedupe_seeds(self.join_seeds.iter().copied(), self.node_id);
        for seed in &seeds {
            quic.learn_peer(seed.node_id, seed.addr);
        }

        let mut pre_joined = false;
        if dynamic_join
            && self
                .data_dir
                .as_ref()
                .is_none_or(|dir| node_id::read_persisted(dir).is_none())
        {
            let (assigned, membership) =
                join_cluster_auto(&quic, &seeds, listen, self.join_role).await?;
            self.node_id = assigned;
            self.members = membership.voters.clone();
            pre_joined = true;
            if let Some(ref data_dir) = self.data_dir {
                node_id::persist(data_dir, assigned)
                    .map_err(|e| StartError::Config(format!("persist assigned node id: {e}")))?;
            }
            quic.learn_peer(assigned, listen);
            if !peers.contains(assigned) {
                peers.insert(assigned, listen);
            }
            if let Some(ref dir) = cert_dir {
                pem_paths = Some(cert_paths_for_node(dir, assigned));
                let loaded = PemSecurity::load(assigned, pem_paths.clone().unwrap())?;
                security = loaded.security;
                server_cfg = server_config(&security.identity, security.roots.clone())?;
                client_cfg = client_config(&security.identity, security.roots.clone())?;
                server.reload(server_cfg);
                quic.reload(client_cfg).await;
            }
        }

        let node_id = self.node_id;
        let join_role = self.join_role;
        quic.learn_peer(node_id, listen);
        let transport: Arc<dyn Transport> = quic.clone();
        let peer_source: Arc<dyn PeerSource> = Arc::new(QuicPeers(Arc::clone(&quic)));
        let sync_period = self.publish_period;
        let cert_watch = self.cert_watch;
        let (mut cluster, router) = self
            .assemble(transport, peer_source, Some(sync_period))
            .await;

        let accept = tokio::spawn({
            let server = Arc::clone(&server);
            async move { server.run_arc(router).await }
        });
        cluster.tasks.lock().unwrap().push(accept);

        // Dynamically join an existing cluster: learn peer addresses from a
        // reachable seed, then ask to join (the seed forwards to the leader).
        // Blocks until the membership change commits or a deadline elapses
        // (discovery, join-rpc); tries every seed for resilience.
        if !seeds.is_empty() && !pre_joined {
            join_cluster(&quic, node_id, &seeds, listen, join_role).await?;
        } else if pre_joined {
            // Confirm membership (Duplicate) after auto-assigned pre-join.
            let _ = join_cluster(&quic, node_id, &seeds, listen, join_role).await;
        }

        if let Some(paths) = pem_paths {
            let reload = Arc::new(CertReloadHandle::new(
                node_id,
                paths,
                server,
                quic,
                Arc::clone(&cluster.facts),
            ));
            let period = cert_watch.unwrap_or(Duration::from_secs(60));
            cluster
                .tasks
                .lock()
                .unwrap()
                .push(reload.clone().spawn_watcher(period));
            if let Some(sighup) = reload.clone().spawn_sighup() {
                cluster.tasks.lock().unwrap().push(sighup);
            }
            cluster.cert_reload = Some(reload);
        }

        Ok(cluster)
    }
}
