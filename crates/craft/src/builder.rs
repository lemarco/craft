//! [`CraftClusterBuilder`] — the single ergonomic entry point (ADR 004, ADR
//! 028). Describe a node (its id, membership, state machine, actor types, and
//! managed groups), then `start_*` it over a transport; the builder assembles
//! the consensus runtime, the actor control/messaging/directory planes, the
//! leader-only supervisor, telemetry, and the admin server, and wires the
//! background loops that keep them current.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use craft_core::{Config, RaftNode, StateMachine};
use craft_dashboard::{AdminServer, EventBus, Metrics, Observer};
use craft_net::transport::RequestHandler;
use craft_net::{LocalNetwork, Transport};
use craft_proto::NodeId;
use tokio::net::TcpListener;

use craft_actor::{
    ActorDirectory, ActorRegistry, ClusterControl, ClusterMessaging, ClusterSupervisor,
    DirectorySync, NodeService, RaftDriver, RuntimeConfig, UserActor, spawn_node,
};

use crate::cluster::{ClusterFacts, CraftCluster};
use crate::handler::NodeRouter;
use crate::observer::CraftObserver;

/// Type-erased "register this actor type on the control plane" step.
type RegisterFn = Box<dyn FnOnce(&ClusterControl) + Send>;
/// Type-erased "declare this managed group on the supervisor" step.
type ManageFn = Box<dyn FnOnce(&ClusterSupervisor<Arc<ClusterFacts>>) + Send>;

/// A fluent builder for a single craft node (ADR 004). Create it with
/// [`CraftCluster::builder`](crate::CraftCluster::builder).
pub struct CraftClusterBuilder<M: StateMachine> {
    node_id: NodeId,
    machine: M,
    members: Vec<NodeId>,
    raft: Config,
    runtime: RuntimeConfig,
    dev_multi_workers: bool,
    forward_timeout: Duration,
    reconcile_period: Duration,
    publish_period: Duration,
    refresh_period: Duration,
    event_capacity: usize,
    admin_addr: Option<SocketAddr>,
    registrations: Vec<RegisterFn>,
    managed: Vec<ManageFn>,
}

impl<M: StateMachine> CraftClusterBuilder<M> {
    /// Start a builder for node `node_id` running `machine`. Defaults to a
    /// single-node cluster (`members = [node_id]`); call [`members`](Self::members)
    /// for a multi-node bootstrap.
    #[must_use]
    pub fn new(node_id: NodeId, machine: M) -> Self {
        Self {
            node_id,
            machine,
            members: vec![node_id],
            raft: Config::default(),
            runtime: RuntimeConfig::default(),
            dev_multi_workers: false,
            forward_timeout: Duration::from_secs(5),
            reconcile_period: Duration::from_millis(250),
            publish_period: Duration::from_millis(250),
            refresh_period: Duration::from_millis(50),
            event_capacity: 1024,
            admin_addr: None,
            registrations: Vec::new(),
            managed: Vec::new(),
        }
    }

    /// Set the initial cluster membership (voting nodes) to bootstrap with.
    #[must_use]
    pub fn members(mut self, members: impl IntoIterator<Item = NodeId>) -> Self {
        self.members = members.into_iter().collect();
        if self.members.is_empty() {
            self.members.push(self.node_id);
        }
        self
    }

    /// Override the core Raft timing configuration (election/heartbeat ticks).
    #[must_use]
    pub fn raft_config(mut self, config: Config) -> Self {
        self.raft = config;
        self
    }

    /// Wall-clock duration of one logical Raft tick.
    #[must_use]
    pub fn tick_period(mut self, period: Duration) -> Self {
        self.runtime.tick_period = period;
        self
    }

    /// Accept cluster joins on this node (`--allow-join`, ADR 017).
    #[must_use]
    pub fn allow_join(mut self, allow: bool) -> Self {
        self.runtime.allow_join = allow;
        self
    }

    /// Permit multiple local instances per actor name (`--dev-multi-workers`,
    /// ADR 014). Off by default: production keeps one worker per node per name.
    #[must_use]
    pub fn dev_multi_workers(mut self, dev: bool) -> Self {
        self.dev_multi_workers = dev;
        self
    }

    /// Deadline for proxying a client request to the leader (ADR 003).
    #[must_use]
    pub fn forward_timeout(mut self, timeout: Duration) -> Self {
        self.forward_timeout = timeout;
        self
    }

    /// How often the leader reconciles managed/auto-worker groups (ADR 018).
    #[must_use]
    pub fn reconcile_period(mut self, period: Duration) -> Self {
        self.reconcile_period = period;
        self
    }

    /// How often this node republishes its local actor set to peers (E7
    /// anti-entropy).
    #[must_use]
    pub fn directory_publish_period(mut self, period: Duration) -> Self {
        self.publish_period = period;
        self
    }

    /// Capacity of the telemetry [`EventBus`] ring buffer per subscriber.
    #[must_use]
    pub fn event_capacity(mut self, capacity: usize) -> Self {
        self.event_capacity = capacity.max(1);
        self
    }

    /// Serve the admin HTTP/1.1 endpoints (health, readiness, metrics,
    /// introspection, dashboard) on `addr` (default `0.0.0.0:8080`, ADR 025).
    #[must_use]
    pub fn admin_addr(mut self, addr: SocketAddr) -> Self {
        self.admin_addr = Some(addr);
        self
    }

    /// Register actor type `A` so this node can host it (locally, on remote
    /// spawn, or as a migration target). Managed groups register their type
    /// automatically; use this for types you spawn imperatively.
    #[must_use]
    pub fn register_actor<A: UserActor>(mut self) -> Self {
        self.registrations
            .push(Box::new(|control: &ClusterControl| {
                control.register_type::<A>();
            }));
        self
    }

    /// Keep exactly `total` instances of actor `A` (named `name`) placed across
    /// the cluster, reconciled by the leader (ADR 014).
    #[must_use]
    pub fn manage<A>(mut self, name: &str, total: usize, config: A::Config) -> Self
    where
        A: UserActor,
        A::Config: Clone + Send + Sync + 'static,
    {
        let name = name.to_string();
        self.managed.push(Box::new(
            move |sup: &ClusterSupervisor<Arc<ClusterFacts>>| {
                sup.manage::<A>(&name, total, config);
            },
        ));
        self
    }

    /// Declare an auto-worker group: one instance of `A` on every live node,
    /// tracking membership so new nodes get a worker automatically (ADR 015).
    #[must_use]
    pub fn manage_auto<A>(mut self, name: &str, config: A::Config) -> Self
    where
        A: UserActor,
        A::Config: Clone + Send + Sync + 'static,
    {
        let name = name.to_string();
        self.managed.push(Box::new(
            move |sup: &ClusterSupervisor<Arc<ClusterFacts>>| {
                sup.manage_auto::<A>(&name, config);
            },
        ));
        self
    }

    /// Start the node over an in-memory [`LocalNetwork`] (tests, the simulator,
    /// and single-process multi-node dev clusters). Attaches this node's router
    /// to `net` under its id.
    ///
    /// Must run inside a Tokio runtime.
    pub async fn start_local(self, net: &LocalNetwork) -> CraftCluster<M> {
        let node_id = self.node_id;
        let transport: Arc<dyn Transport> = Arc::new(net.clone());
        let (cluster, router) = self.assemble(transport).await;
        net.attach(node_id, router);
        cluster
    }

    /// Assemble every runtime component over `transport`, spawn the background
    /// loops, and return the cluster handle plus the router to attach.
    async fn assemble(
        self,
        transport: Arc<dyn Transport>,
    ) -> (CraftCluster<M>, Arc<dyn RequestHandler>) {
        let node_id = self.node_id;

        // --- Consensus runtime -------------------------------------------
        let node = RaftNode::new(node_id, self.members.clone(), self.raft.clone());
        let driver = RaftDriver::new(node, self.machine);
        let handle = spawn_node(driver, Arc::clone(&transport), self.runtime.clone());

        // --- Actor planes -------------------------------------------------
        let registry = if self.dev_multi_workers {
            ActorRegistry::new_dev()
        } else {
            ActorRegistry::new()
        };
        let directory = ActorDirectory::new();
        let control = Arc::new(ClusterControl::new(
            node_id,
            registry.clone(),
            Arc::clone(&directory),
            Arc::clone(&transport),
        ));
        let messaging = Arc::new(ClusterMessaging::new(
            node_id,
            Arc::clone(&directory),
            registry.clone(),
            Arc::clone(&transport),
        ));
        let directory_sync = Arc::new(DirectorySync::new(
            node_id,
            Arc::clone(&directory),
            Arc::clone(&transport),
        ));

        // Register imperative actor types.
        for register in self.registrations {
            register(&control);
        }

        // --- Supervisor + facts ------------------------------------------
        let facts = Arc::new(ClusterFacts::default());
        let supervisor = Arc::new(ClusterSupervisor::new(
            Arc::clone(&control),
            Arc::clone(&facts),
        ));
        for manage in self.managed {
            manage(&supervisor);
        }

        // --- Observability ------------------------------------------------
        let events = EventBus::new(self.event_capacity);
        let metrics = Metrics::new();

        // --- Router -------------------------------------------------------
        let service = NodeService::new(handle.clone(), Arc::clone(&transport))
            .with_forward_timeout(self.forward_timeout);
        let router: Arc<dyn RequestHandler> = Arc::new(NodeRouter::new(
            service,
            Arc::clone(&control),
            Arc::clone(&messaging),
            Arc::clone(&directory_sync),
        ));

        // --- Background loops --------------------------------------------
        let mut tasks = Vec::new();

        // Facts refresher: mirror consensus status into the supervisor's view.
        {
            let handle = handle.clone();
            let facts = Arc::clone(&facts);
            let period = self.refresh_period;
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(period);
                loop {
                    interval.tick().await;
                    match handle.status().await {
                        Some(status) => facts.update(&status),
                        None => break,
                    }
                }
            }));
        }

        // Directory anti-entropy: republish local registrations to peers.
        {
            let directory_sync = Arc::clone(&directory_sync);
            let registry = registry.clone();
            let members = self.members.clone();
            let period = self.publish_period;
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(period);
                loop {
                    interval.tick().await;
                    let regs = registry.local_registrations(node_id);
                    let _ = directory_sync.publish(&members, regs).await;
                }
            }));
        }

        // Supervisor reconcile: leader-only placement convergence (ADR 018).
        {
            let supervisor = Arc::clone(&supervisor);
            let period = self.reconcile_period;
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(period);
                loop {
                    interval.tick().await;
                    let _ = supervisor.reconcile().await;
                }
            }));
        }

        // Admin/observability HTTP server.
        if let Some(addr) = self.admin_addr {
            let observer: Arc<dyn Observer> = Arc::new(CraftObserver::new(
                node_id,
                handle.clone(),
                Arc::clone(&directory),
                registry.clone(),
            ));
            let admin = AdminServer::new(observer, metrics.clone(), events.clone());
            match TcpListener::bind(addr).await {
                Ok(listener) => {
                    tasks.push(tokio::spawn(async move {
                        let _ = admin.serve(listener).await;
                    }));
                }
                Err(e) => {
                    // A bad admin bind must not take the node down; surface it
                    // and carry on serving the craft wire.
                    eprintln!("craft: admin server bind to {addr} failed: {e}");
                }
            }
        }

        let cluster = CraftCluster {
            node_id,
            handle,
            registry,
            control,
            messaging,
            directory,
            directory_sync,
            supervisor,
            events,
            metrics,
            members: self.members,
            tasks: Mutex::new(tasks),
        };
        (cluster, router)
    }
}
