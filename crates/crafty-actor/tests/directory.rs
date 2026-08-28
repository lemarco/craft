//! Tests for the cross-node actor directory (backlog E7): the pure
//! [`ActorDirectory`] merge/resolve/routing semantics, and end-to-end
//! convergence of three nodes' directories via [`DirectorySync`] over a
//! `LocalNetwork`.

#![allow(clippy::unused_async_trait_impl)] // test mock actors have sync handle bodies

use std::sync::Arc;

use crafty_actor::crafty_net::{LocalNetwork, Transport};
use crafty_actor::crafty_proto::{
    ActorId, ActorRegistration, ActorTypeId, DirectoryUpdate, NodeId,
};
use crafty_actor::{ActorDirectory, ActorRegistry, DirectorySync, UserActor};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn reg(node: u64, name: &str, instance: u32) -> ActorRegistration {
    ActorRegistration {
        id: ActorId {
            node: NodeId(node),
            name: name.to_string(),
            instance,
            generation: 0,
        },
        actor_type: ActorTypeId("Worker".to_string()),
        migratable: false,
    }
}

fn update(node: u64, epoch: u64, regs: Vec<ActorRegistration>) -> DirectoryUpdate {
    DirectoryUpdate {
        node: NodeId(node),
        epoch,
        registrations: regs,
    }
}

// ---------------------------------------------------------------------------
// Pure ActorDirectory
// ---------------------------------------------------------------------------

#[test]
fn apply_is_epoch_ordered_and_idempotent() {
    let dir = ActorDirectory::new();
    assert!(dir.apply(&update(1, 1, vec![reg(1, "w", 0)])));
    // Equal or lower epoch is stale and ignored.
    assert!(!dir.apply(&update(1, 1, vec![reg(1, "w", 0), reg(1, "w", 1)])));
    assert!(!dir.apply(&update(1, 0, vec![])));
    assert_eq!(dir.len(), 1);

    // A newer epoch supersedes — including a revoking empty snapshot.
    assert!(dir.apply(&update(1, 2, vec![])));
    assert!(dir.is_empty(), "empty snapshot at a higher epoch revokes");
}

#[test]
fn resolve_and_lookup_merge_across_nodes() {
    let dir = ActorDirectory::new();
    dir.apply(&update(2, 1, vec![reg(2, "w", 0)]));
    dir.apply(&update(1, 1, vec![reg(1, "w", 0), reg(1, "w", 1)]));
    dir.apply(&update(1, 1, vec![])); // stale (equal epoch) → ignored

    let members = dir.lookup("w");
    assert_eq!(members.len(), 3);
    // Sorted by (node, name, instance): n1/0, n1/1, n2/0.
    assert_eq!(members[0].id, reg(1, "w", 0).id);
    assert_eq!(members[1].id, reg(1, "w", 1).id);
    assert_eq!(members[2].id, reg(2, "w", 0).id);

    assert_eq!(dir.resolve(&reg(2, "w", 0).id), Some(reg(2, "w", 0)));
    assert!(dir.resolve(&reg(3, "w", 0).id).is_none());
    assert_eq!(dir.groups(), vec!["w".to_string()]);
}

#[test]
fn round_robin_cycles_through_all_instances() {
    let dir = ActorDirectory::new();
    dir.apply(&update(1, 1, vec![reg(1, "w", 0), reg(1, "w", 1)]));
    dir.apply(&update(2, 1, vec![reg(2, "w", 0)]));

    let picks: Vec<_> = (0..6).filter_map(|_| dir.pick_rr("w")).collect();
    let ids: Vec<_> = picks.iter().map(|r| r.id.clone()).collect();
    // Two full cycles over the three sorted members.
    assert_eq!(ids[0], reg(1, "w", 0).id);
    assert_eq!(ids[1], reg(1, "w", 1).id);
    assert_eq!(ids[2], reg(2, "w", 0).id);
    assert_eq!(ids[3], reg(1, "w", 0).id, "RR wraps around");
    assert!(dir.pick_rr("missing").is_none());
}

#[test]
fn keyed_routing_is_stable_for_a_key() {
    let dir = ActorDirectory::new();
    dir.apply(&update(1, 1, vec![reg(1, "w", 0), reg(1, "w", 1)]));
    dir.apply(&update(2, 1, vec![reg(2, "w", 0), reg(2, "w", 1)]));

    let first = dir.pick_keyed("w", &"tenant-7").unwrap();
    for _ in 0..20 {
        assert_eq!(dir.pick_keyed("w", &"tenant-7").unwrap().id, first.id);
    }
}

#[test]
fn remove_node_drops_its_entries() {
    let dir = ActorDirectory::new();
    dir.apply(&update(1, 1, vec![reg(1, "w", 0)]));
    dir.apply(&update(2, 1, vec![reg(2, "w", 0)]));
    assert!(dir.remove_node(NodeId(2)));
    assert!(!dir.remove_node(NodeId(2)), "already gone");

    let members = dir.lookup("w");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].id.node, NodeId(1));
}

#[test]
fn cluster_ref_reports_nodes_and_members() {
    let dir = ActorDirectory::new();
    dir.apply(&update(1, 1, vec![reg(1, "w", 0)]));
    dir.apply(&update(3, 1, vec![reg(3, "w", 0), reg(3, "w", 1)]));

    let cluster = dir.cluster("w");
    assert_eq!(cluster.len(), 3);
    assert_eq!(cluster.nodes(), vec![NodeId(1), NodeId(3)]);
    assert!(!cluster.is_empty());
    assert!(dir.cluster("nope").is_empty());
}

// ---------------------------------------------------------------------------
// End-to-end convergence over LocalNetwork
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
#[error("worker error")]
struct WorkerError;

struct Worker;

impl UserActor for Worker {
    type Config = ();
    type Message = ();
    type Error = WorkerError;

    fn start(_config: Self::Config) -> Result<Self, Self::Error> {
        Ok(Worker)
    }

    async fn handle(&mut self, _msg: Self::Message) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Wire a `DirectorySync` for `id` onto the shared network and return both the
/// sync bridge (to publish through) and its directory (to query).
fn node(net: &LocalNetwork, id: u64) -> (Arc<DirectorySync>, Arc<ActorDirectory>) {
    let directory = ActorDirectory::new();
    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let sync = Arc::new(DirectorySync::new(
        NodeId(id),
        Arc::clone(&directory),
        transport,
    ));
    net.attach(NodeId(id), sync.clone());
    (sync, directory)
}

#[tokio::test]
async fn three_nodes_converge_on_a_merged_directory() {
    let net = LocalNetwork::new();
    let (sync1, dir1) = node(&net, 1);
    let (sync2, dir2) = node(&net, 2);
    let (_sync3, dir3) = node(&net, 3);
    let peers = [NodeId(1), NodeId(2), NodeId(3)];

    // Nodes 1 and 2 each host a two-instance "workers" pool (dev registries).
    let reg1 = ActorRegistry::new_dev();
    reg1.spawn_pool::<Worker>("workers", 2, ()).unwrap();
    let reg2 = ActorRegistry::new_dev();
    reg2.spawn_pool::<Worker>("workers", 2, ()).unwrap();

    let acks1 = sync1
        .publish(&peers, reg1.local_registrations(NodeId(1)))
        .await;
    let acks2 = sync2
        .publish(&peers, reg2.local_registrations(NodeId(2)))
        .await;
    assert_eq!(acks1, 2, "peers 2 and 3 acknowledge node 1's publish");
    assert_eq!(acks2, 2, "peers 1 and 3 acknowledge node 2's publish");

    // Every node now sees all four instances across nodes 1 and 2.
    for dir in [&dir1, &dir2, &dir3] {
        let cluster = dir.cluster("workers");
        assert_eq!(cluster.len(), 4, "all four workers visible");
        assert_eq!(cluster.nodes(), vec![NodeId(1), NodeId(2)]);
    }

    // Node 3 (hosting nothing locally) can still route to remote instances.
    let target = dir3.cluster("workers").pick().unwrap();
    assert_eq!(target.id.name, "workers");
    assert!([NodeId(1), NodeId(2)].contains(&target.id.node));
}

#[tokio::test]
async fn republishing_a_smaller_set_revokes_missing_instances() {
    let net = LocalNetwork::new();
    let (sync1, _dir1) = node(&net, 1);
    let (_sync2, dir2) = node(&net, 2);
    let peers = [NodeId(1), NodeId(2)];

    let reg = ActorRegistry::new_dev();
    reg.spawn_pool::<Worker>("workers", 3, ()).unwrap();
    sync1
        .publish(&peers, reg.local_registrations(NodeId(1)))
        .await;
    assert_eq!(dir2.cluster("workers").len(), 3);

    // Scale in to one instance and republish: the directory converges down.
    reg.scale_local::<Worker>("workers", 1, ()).await.unwrap();
    sync1
        .publish(&peers, reg.local_registrations(NodeId(1)))
        .await;
    assert_eq!(
        dir2.cluster("workers").len(),
        1,
        "node 2 sees the scaled-in count after republish"
    );
}
