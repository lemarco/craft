//! Durable mailbox outbox/inbox integration over [`LocalNetwork`].

#![allow(clippy::unused_async_trait_impl)] // test mock actors have sync handle bodies

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crafty_actor::{
    ActorDirectory, ActorRegistry, ClusterMessaging, InMemoryMailboxSpool, MailboxSpool,
    MessageDecodeError, UserActor,
};
use crafty_net::{LocalNetwork, Transport};
use crafty_proto::{ActorId, ActorRegistration, ActorTypeId, DirectoryUpdate, NodeId};

#[derive(Clone)]
struct Worker {
    counter: Arc<AtomicU64>,
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
enum Work {
    Add(u64),
}

impl UserActor for Worker {
    type Config = Arc<AtomicU64>;
    type Message = Work;
    type Error = std::convert::Infallible;

    fn start(counter: Self::Config) -> Result<Self, Self::Error> {
        Ok(Worker { counter })
    }

    async fn handle(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        let Work::Add(n) = msg;
        self.counter.fetch_add(n, Ordering::SeqCst);
        Ok(())
    }

    fn decode_message(payload: &[u8]) -> Result<Self::Message, MessageDecodeError> {
        crafty_proto::decode(payload).map_err(|e| MessageDecodeError::Decode(e.to_string()))
    }
}

fn reg(node: u64, name: &str, instance: u32) -> ActorRegistration {
    ActorRegistration::new(
        ActorId {
            node: NodeId(node),
            name: name.to_string(),
            instance,
            generation: 0,
        },
        ActorTypeId("Worker".to_string()),
        false,
    )
}

fn update(node: u64, regs: Vec<ActorRegistration>) -> DirectoryUpdate {
    DirectoryUpdate {
        node: NodeId(node),
        epoch: 1,
        registrations: regs,
    }
}

fn add(n: u64) -> Vec<u8> {
    crafty_proto::encode(&Work::Add(n)).unwrap()
}

struct RemoteNode {
    messaging: Arc<ClusterMessaging>,
    directory: Arc<ActorDirectory>,
    counter: Arc<AtomicU64>,
}

fn remote_node(net: &LocalNetwork, id: u64) -> RemoteNode {
    let counter = Arc::new(AtomicU64::new(0));
    let registry = ActorRegistry::new();
    registry
        .spawn::<Worker>("worker", Arc::clone(&counter))
        .expect("spawn worker");
    let directory = ActorDirectory::new();
    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let messaging = Arc::new(ClusterMessaging::new(
        NodeId(id),
        Arc::clone(&directory),
        registry,
        transport,
    ));
    net.attach(NodeId(id), messaging.clone());
    RemoteNode {
        messaging,
        directory,
        counter,
    }
}

async fn eventually(mut cond: impl FnMut() -> bool) {
    for _ in 0..200 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("condition not met within the deadline");
}

#[tokio::test]
async fn outbox_replays_after_unreachable_peer_returns() {
    let net = LocalNetwork::new();
    let spool = Arc::new(InMemoryMailboxSpool::new());

    let remote = remote_node(&net, 2);
    remote
        .directory
        .apply(&update(2, vec![reg(2, "worker", 0)]));

    let sender_registry = ActorRegistry::new();
    let sender_directory = ActorDirectory::new();
    sender_directory.apply(&update(2, vec![reg(2, "worker", 0)]));
    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let sender = Arc::new(
        ClusterMessaging::new(
            NodeId(1),
            Arc::clone(&sender_directory),
            sender_registry,
            transport,
        )
        .with_mailbox_spool(Arc::clone(&spool) as Arc<dyn MailboxSpool>),
    );
    net.attach(NodeId(1), sender.clone());

    let _ = net.detach(NodeId(2));
    assert!(sender.cast("worker", add(7)).await.is_err());
    assert_eq!(spool.list_outbox(10).expect("list").len(), 1);

    net.attach(NodeId(2), remote.messaging.clone());
    sender.drain_mailbox_spool_once().await;

    eventually(|| remote.counter.load(Ordering::SeqCst) == 7).await;
    assert!(spool.list_outbox(10).expect("list").is_empty());
}
