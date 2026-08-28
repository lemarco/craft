//! I4 scenarios: cluster-wide actor placement (`scale_cluster`) and stateful
//! actor migration across nodes, driven end-to-end over a `LocalNetwork`.
//!
//! These complement the single-operation planner/placement unit tests in
//! `crafty-actor` with multi-step narratives: scaling out then relocating off a
//! dead node, and migrating a stateful actor across *two* hops with its state
//! intact.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crafty_actor::crafty_net::{LocalNetwork, Transport};
use crafty_actor::crafty_proto::{
    self, ActorId, ActorRegistration, ActorTypeId, DirectoryUpdate, NodeId,
};
use crafty_actor::{
    ActorDirectory, ActorRegistry, ClusterControl, ConfigCodecError, MigrationError, RpcReplyPort,
    UserActor,
};

#[derive(Debug)]
struct ActorErr;

impl std::fmt::Display for ActorErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("actor error")
    }
}

impl std::error::Error for ActorErr {}

// --- A stateless worker (scale scenario) ----------------------------------

struct Worker;

impl UserActor for Worker {
    type Config = u32;
    type Message = ();
    type Error = ActorErr;

    fn start(_seed: Self::Config) -> Result<Self, Self::Error> {
        Ok(Worker)
    }

    fn handle(&mut self, _msg: Self::Message) -> impl Future<Output = Result<(), Self::Error>> {
        std::future::ready(Ok(()))
    }

    fn encode_config(config: &Self::Config) -> Result<Vec<u8>, ConfigCodecError> {
        crafty_proto::encode(config).map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }

    fn decode_config(bytes: &[u8]) -> Result<Self::Config, ConfigCodecError> {
        crafty_proto::decode(bytes).map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }
}

// --- A stateful counter (migration scenario) ------------------------------

enum CounterMsg {
    Inc,
    Get(RpcReplyPort<u64>),
}

struct Counter {
    count: u64,
}

impl UserActor for Counter {
    type Config = u64;
    type Message = CounterMsg;
    type Error = ActorErr;

    const MIGRATABLE: bool = true;

    fn start(initial: Self::Config) -> Result<Self, Self::Error> {
        Ok(Self { count: initial })
    }

    fn handle(&mut self, msg: Self::Message) -> impl Future<Output = Result<(), Self::Error>> {
        match msg {
            CounterMsg::Inc => self.count += 1,
            CounterMsg::Get(port) => {
                let _ = port.reply(self.count);
            }
        }
        std::future::ready(Ok(()))
    }

    fn encode_config(config: &Self::Config) -> Result<Vec<u8>, ConfigCodecError> {
        crafty_proto::encode(config).map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }

    fn decode_config(bytes: &[u8]) -> Result<Self::Config, ConfigCodecError> {
        crafty_proto::decode(bytes).map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }

    fn migration_snapshot(&self) -> Result<Vec<u8>, MigrationError> {
        crafty_proto::encode(&self.count).map_err(MigrationError::new)
    }

    fn restore_migration(&mut self, snapshot: &[u8]) -> Result<(), MigrationError> {
        self.count = crafty_proto::decode(snapshot).map_err(MigrationError::new)?;
        Ok(())
    }
}

// --- Multi-node harness ---------------------------------------------------

struct Node {
    control: Arc<ClusterControl>,
    registry: ActorRegistry,
    directory: Arc<ActorDirectory>,
}

fn node(net: &LocalNetwork, id: u64) -> Node {
    let registry = ActorRegistry::new();
    let directory = ActorDirectory::new();
    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let control = Arc::new(ClusterControl::new(
        NodeId(id),
        registry.clone(),
        Arc::clone(&directory),
        transport,
    ));
    control.register_type::<Worker>();
    control.register_type::<Counter>();
    net.attach(NodeId(id), control.clone());
    Node {
        control,
        registry,
        directory,
    }
}

fn nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

fn worker_reg(node: u64, name: &str) -> ActorRegistration {
    ActorRegistration {
        id: ActorId {
            node: NodeId(node),
            name: name.to_string(),
            instance: 0,
            generation: 0,
        },
        actor_type: ActorTypeId(std::any::type_name::<Worker>().to_string()),
        migratable: false,
    }
}

// --- Scenarios ------------------------------------------------------------

#[tokio::test]
async fn scale_out_then_relocate_off_a_dead_node() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1);
    let n2 = node(&net, 2);
    let n3 = node(&net, 3);

    // Scale a fresh group to one worker per node across the 3-node cluster.
    let plan = n1
        .control
        .scale_cluster::<Worker>("w", 3, 1, &nodes(&[1, 2, 3]))
        .await
        .unwrap();
    assert_eq!(plan.spawns, nodes(&[1, 2, 3]));
    assert!(plan.removes.is_empty());
    assert!(n1.registry.contains("w") && n2.registry.contains("w") && n3.registry.contains("w"));

    // Node 3 crashes. The scheduler's view (directory) still lists all three;
    // re-scaling to 2 against the *live* set [1, 2] must keep 1 and 2 and plan
    // to remove node 3's now-unreachable instance — no new spawns needed.
    n1.directory.apply(&DirectoryUpdate {
        node: NodeId(1),
        epoch: 1,
        registrations: vec![worker_reg(1, "w"), worker_reg(2, "w"), worker_reg(3, "w")],
    });

    let plan = n1
        .control
        .scale_cluster::<Worker>("w", 2, 1, &nodes(&[1, 2]))
        .await
        .unwrap();
    assert!(
        plan.spawns.is_empty(),
        "surviving nodes already host the group"
    );
    assert_eq!(
        plan.removes,
        vec![worker_reg(3, "w").id],
        "dead node's instance is reaped"
    );
    assert!(
        n1.registry.contains("w") && n2.registry.contains("w"),
        "survivors keep running"
    );
}

#[tokio::test]
async fn stateful_actor_migrates_across_two_hops_preserving_state() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1);
    let n2 = node(&net, 2);
    let n3 = node(&net, 3);

    // Start on node 1 and advance the counter to 3.
    n1.control
        .spawn_remote::<Counter>(NodeId(1), "c", 0)
        .await
        .unwrap();
    let src = n1.registry.get::<Counter>("c").unwrap();
    src.send(CounterMsg::Inc).unwrap();
    src.send(CounterMsg::Inc).unwrap();
    src.send(CounterMsg::Inc).unwrap();

    // Hop 1: node 1 → node 2.
    let hop1 = n1
        .control
        .migrate::<Counter>(
            ActorId {
                node: NodeId(1),
                name: "c".into(),
                instance: 0,
                generation: 0,
            },
            NodeId(2),
            0,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(hop1.node, NodeId(2));
    assert_eq!(hop1.generation, 1, "generation bumped each hop");
    assert!(!n1.registry.contains("c"), "source drained after hop 1");
    assert_eq!(
        n2.registry
            .get::<Counter>("c")
            .unwrap()
            .ask(CounterMsg::Get)
            .await
            .unwrap(),
        3,
        "state carried to node 2"
    );

    // Hop 2: node 2 → node 3, from the migrated instance.
    let hop2 = n2
        .control
        .migrate::<Counter>(hop1, NodeId(3), 0, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(hop2.node, NodeId(3));
    assert_eq!(hop2.generation, 2, "generation bumped again");
    assert!(
        !n2.registry.contains("c"),
        "intermediate node drained after hop 2"
    );

    let final_count = n3
        .registry
        .get::<Counter>("c")
        .unwrap()
        .ask(CounterMsg::Get)
        .await
        .unwrap();
    assert_eq!(final_count, 3, "state survives both migration hops");
}

#[tokio::test]
async fn spawn_remote_places_actor_on_target_node() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1);
    let n2 = node(&net, 2);

    n1.control
        .spawn_remote::<Counter>(NodeId(2), "c", 7)
        .await
        .expect("remote spawn");

    assert!(
        !n1.registry.contains("c"),
        "source node does not host the remote spawn"
    );
    assert!(n2.registry.contains("c"), "target node hosts the actor");

    let count = n2
        .registry
        .get::<Counter>("c")
        .expect("counter on node 2")
        .ask(CounterMsg::Get)
        .await
        .expect("ask");
    assert_eq!(count, 7);
}
