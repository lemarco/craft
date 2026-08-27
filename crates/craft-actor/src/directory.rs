//! Cross-node actor directory (backlog E7,
//! [ADR 013](../../../docs/decisions/013-cross-node-actors.md)).
//!
//! Every node keeps a **merged view** of which actors live where. Each node is
//! authoritative for its own instances and publishes its *complete* local set
//! as a [`DirectoryUpdate`] over `/actor/register`; receivers replace
//! everything they hold for that node, keyed by a monotonic per-node `epoch`
//! (a simple state-based, reorder-safe last-writer-wins). Publishing an empty
//! set revokes a node's entries (leave / drain).
//!
//! [`ActorDirectory`] is the pure, in-memory view (resolve / lookup / route
//! selection). [`DirectorySync`] bridges it to the network: it publishes the
//! local snapshot to peers and applies inbound updates, and serves the
//! `/actor/register` route as a [`RequestHandler`].
//!
//! Actual cross-node message *delivery* (`/actor/deliver`, RR + keyed routing
//! over the wire) builds on this directory in E8; here `cluster(name)` resolves
//! **where** a message should go (a target [`ActorRegistration`]).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use craft_net::transport::{Body, BoxFuture};
use craft_net::{
    RequestHandler, Route, Transport, TransportError, decode_body, encode_body,
    send_directory_update,
};
use craft_proto::{ActorId, ActorRegistration, DirectoryUpdate, NodeId, RegisterAck};

/// A node-local, merged view of every actor known in the cluster (E7).
///
/// Wrap it in an `Arc` and share it between the [`DirectorySync`] that keeps it
/// current and the callers that query it.
#[derive(Debug, Default)]
pub struct ActorDirectory {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// Per-node authoritative snapshot, keyed by owning node.
    by_node: HashMap<NodeId, NodeEntry>,
    /// Round-robin cursor per group name (for `pick_rr`).
    rr: HashMap<String, usize>,
}

#[derive(Debug)]
struct NodeEntry {
    epoch: u64,
    registrations: Vec<ActorRegistration>,
}

impl ActorDirectory {
    /// An empty directory.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Apply a directory update. Returns `true` if it was newer than what we
    /// held for `update.node` (and therefore applied), `false` if stale.
    pub fn apply(&self, update: &DirectoryUpdate) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if let Some(existing) = inner.by_node.get(&update.node)
            && existing.epoch >= update.epoch
        {
            return false;
        }
        inner.by_node.insert(
            update.node,
            NodeEntry {
                epoch: update.epoch,
                registrations: update.registrations.clone(),
            },
        );
        true
    }

    /// Drop all entries owned by `node` (e.g. after it leaves the cluster).
    /// Returns whether the node had any entry.
    pub fn remove_node(&self, node: NodeId) -> bool {
        self.inner.lock().unwrap().by_node.remove(&node).is_some()
    }

    /// Resolve a specific instance by its full id.
    #[must_use]
    pub fn resolve(&self, id: &ActorId) -> Option<ActorRegistration> {
        let inner = self.inner.lock().unwrap();
        inner
            .by_node
            .get(&id.node)?
            .registrations
            .iter()
            .find(|r| &r.id == id)
            .cloned()
    }

    /// Every registration for group `name`, cluster-wide, in a stable order.
    #[must_use]
    pub fn lookup(&self, name: &str) -> Vec<ActorRegistration> {
        let inner = self.inner.lock().unwrap();
        Self::members_locked(&inner, name)
    }

    /// Distinct group names currently known.
    #[must_use]
    pub fn groups(&self) -> Vec<String> {
        let inner = self.inner.lock().unwrap();
        let mut names: Vec<String> = inner
            .by_node
            .values()
            .flat_map(|e| e.registrations.iter())
            .map(|r| r.id.name.clone())
            .collect();
        names.sort();
        names.dedup();
        names
    }

    /// Total number of known registrations across all nodes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap()
            .by_node
            .values()
            .map(|e| e.registrations.len())
            .sum()
    }

    /// Whether the directory holds no registrations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Pick the next instance of `name` round-robin across the whole cluster.
    #[must_use]
    pub fn pick_rr(&self, name: &str) -> Option<ActorRegistration> {
        let mut inner = self.inner.lock().unwrap();
        let members = Self::members_locked(&inner, name);
        if members.is_empty() {
            return None;
        }
        let counter = inner.rr.entry(name.to_string()).or_insert(0);
        let index = *counter % members.len();
        *counter = counter.wrapping_add(1);
        members.into_iter().nth(index)
    }

    /// Pick the instance of `name` that `key` hashes to, so a given key always
    /// resolves to the same instance while membership is stable.
    #[must_use]
    pub fn pick_keyed<K: Hash>(&self, name: &str, key: &K) -> Option<ActorRegistration> {
        let inner = self.inner.lock().unwrap();
        let members = Self::members_locked(&inner, name);
        if members.is_empty() {
            return None;
        }
        let index = (hash_key(key) % members.len() as u64) as usize;
        members.into_iter().nth(index)
    }

    /// A routing handle to the cluster-wide pool `name`.
    #[must_use]
    pub fn cluster(self: &Arc<Self>, name: &str) -> ClusterRef {
        ClusterRef {
            directory: Arc::clone(self),
            name: name.to_string(),
        }
    }

    fn members_locked(inner: &Inner, name: &str) -> Vec<ActorRegistration> {
        let mut out: Vec<ActorRegistration> = inner
            .by_node
            .values()
            .flat_map(|e| e.registrations.iter())
            .filter(|r| r.id.name == name)
            .cloned()
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }
}

fn hash_key<K: Hash>(key: &K) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// A handle to a cluster-wide actor pool that resolves a target instance for a
/// message (E7). Cross-node delivery to the resolved target is E8.
#[derive(Clone)]
pub struct ClusterRef {
    directory: Arc<ActorDirectory>,
    name: String,
}

impl ClusterRef {
    /// The pool's group name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// All instances in the pool, cluster-wide.
    #[must_use]
    pub fn members(&self) -> Vec<ActorRegistration> {
        self.directory.lookup(&self.name)
    }

    /// Number of instances in the pool.
    #[must_use]
    pub fn len(&self) -> usize {
        self.members().len()
    }

    /// Whether the pool has no instances anywhere.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.members().is_empty()
    }

    /// Distinct nodes currently hosting an instance of this pool.
    #[must_use]
    pub fn nodes(&self) -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> = self.members().into_iter().map(|r| r.id.node).collect();
        nodes.sort();
        nodes.dedup();
        nodes
    }

    /// Resolve the next target instance round-robin.
    #[must_use]
    pub fn pick(&self) -> Option<ActorRegistration> {
        self.directory.pick_rr(&self.name)
    }

    /// Resolve the target instance for `key` (consistent while membership is
    /// stable).
    #[must_use]
    pub fn pick_keyed<K: Hash>(&self, key: &K) -> Option<ActorRegistration> {
        self.directory.pick_keyed(&self.name, key)
    }
}

/// Bridges an [`ActorDirectory`] to the network (E7): publishes this node's
/// local registration snapshot to peers, applies inbound updates, and serves
/// the `/actor/register` route as a [`RequestHandler`].
pub struct DirectorySync {
    node_id: NodeId,
    directory: Arc<ActorDirectory>,
    transport: Arc<dyn Transport>,
    epoch: AtomicU64,
}

impl DirectorySync {
    /// Create a sync bridge for `node_id` over `transport`, keeping `directory`
    /// current.
    #[must_use]
    pub fn new(
        node_id: NodeId,
        directory: Arc<ActorDirectory>,
        transport: Arc<dyn Transport>,
    ) -> Self {
        Self {
            node_id,
            directory,
            transport,
            epoch: AtomicU64::new(0),
        }
    }

    /// This node's id.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// The shared directory this bridge maintains.
    #[must_use]
    pub fn directory(&self) -> &Arc<ActorDirectory> {
        &self.directory
    }

    /// Publish this node's complete local registration set to `peers` (skipping
    /// self) and apply it locally at a fresh epoch. Returns the number of peers
    /// that acknowledged applying it. Best-effort: unreachable peers are
    /// skipped (they converge on the next publish or via anti-entropy later).
    pub async fn publish(&self, peers: &[NodeId], registrations: Vec<ActorRegistration>) -> usize {
        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        let update = DirectoryUpdate {
            node: self.node_id,
            epoch,
            registrations,
        };
        self.directory.apply(&update);
        let mut acks = 0;
        for &peer in peers {
            if peer == self.node_id {
                continue;
            }
            if let Ok(ack) = send_directory_update(self.transport.as_ref(), peer, &update).await
                && ack.applied
            {
                acks += 1;
            }
        }
        acks
    }

    /// Apply an inbound update to the directory and produce its acknowledgement.
    #[must_use]
    pub fn apply_inbound(&self, update: &DirectoryUpdate) -> RegisterAck {
        RegisterAck {
            applied: self.directory.apply(update),
        }
    }
}

impl RequestHandler for DirectorySync {
    fn handle(&self, route: Route, body: Body) -> BoxFuture<'static, Result<Body, TransportError>> {
        let result = match route {
            Route::ActorRegister => decode_body::<DirectoryUpdate>(&body)
                .map_err(TransportError::from)
                .and_then(|update| Ok(encode_body(&self.apply_inbound(&update))?)),
            other => Err(TransportError::Io(format!(
                "directory handler received unexpected route {other:?}"
            ))),
        };
        Box::pin(async move { result })
    }
}
