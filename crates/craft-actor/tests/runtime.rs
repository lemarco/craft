//! End-to-end tests for [`craft_actor::spawn_node`] — the async node runtime.
//!
//! Three real runtime tasks are wired together over the in-memory
//! [`LocalNetwork`] transport (the same `Transport` port the live QUIC stack
//! implements). They elect a leader through timer-driven ticks and serve client
//! proposals/queries both directly via [`NodeHandle`] and over the
//! `/client/wire` path through the [`NodeService`] request handler.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use craft_actor::craft_core::{RaftNode, Role};
use craft_actor::craft_net::{
    LocalNetwork, RequestHandler, Transport, send_client_request, send_join_request,
    send_leave_request,
};
use craft_actor::craft_proto::{
    AppendEntries, ClientRequest, ClientResponse, JoinRejection, JoinRequest, JoinResponse,
    LeaveRejection, LeaveRequest, LeaveResponse, LogEntry, LogId, LogIndex, NodeId,
    PROTOCOL_VERSION, RaftRpc, Round, Term,
};
use craft_actor::craft_storage::{
    HardState, HardStateStore, LogStore, MemoryStorage, Snapshot, SnapshotStore, StorageError,
};
use craft_actor::{ClientError, NodeHandle, NodeService, RaftDriver, RuntimeConfig, spawn_node};
use craft_test_support::{
    Kv, KvCommand, KvQuery, KvResponse, TICK_PERIOD, advance, await_node_leader,
    fast_raft_config_with_seed,
};

/// A running cluster of async node runtimes wired over one `LocalNetwork`.
struct Cluster {
    handles: HashMap<NodeId, NodeHandle<Kv>>,
    ids: Vec<NodeId>,
    net: LocalNetwork,
}

impl Cluster {
    fn start(ids: &[NodeId]) -> Self {
        let net = LocalNetwork::new();
        let mut cluster = Self {
            handles: HashMap::new(),
            ids: Vec::new(),
            net,
        };
        for &id in ids {
            cluster.spawn_one(id, ids.iter().copied());
            cluster.ids.push(id);
        }
        cluster
    }

    /// Spawn a node runtime for `id` (initial membership `members`) and attach
    /// its request handler to the shared network. Does not add it to `ids`.
    fn spawn_one(&mut self, id: NodeId, members: impl IntoIterator<Item = NodeId>) {
        let node = RaftNode::new(id, members, fast_raft_config_with_seed(7));
        let driver = RaftDriver::new(node, Kv::default());
        let transport: Arc<dyn Transport> = Arc::new(self.net.clone());
        let handle = spawn_node(
            driver,
            Arc::clone(&transport),
            RuntimeConfig {
                tick_period: TICK_PERIOD,
                allow_join: true,
                allow_leave: true,
                ..RuntimeConfig::default()
            },
        );
        let service: Arc<dyn RequestHandler> =
            Arc::new(NodeService::new(handle.clone(), Arc::clone(&transport)));
        self.net.attach(id, service);
        self.handles.insert(id, handle);
    }

    /// Poll node statuses until exactly one leader exists, or panic on timeout.
    async fn wait_for_leader(&self) -> NodeId {
        let handles: Vec<_> = self
            .ids
            .iter()
            .map(|&id| (id, self.handles[&id].clone()))
            .collect();
        await_node_leader(&handles).await
    }

    fn shutdown(&self) {
        for handle in self.handles.values() {
            handle.shutdown();
        }
    }
}

#[tokio::test(start_paused = true)]
async fn three_nodes_elect_a_leader() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cluster = Cluster::start(&ids);
    let leader = cluster.wait_for_leader().await;
    assert!(ids.contains(&leader));
    cluster.shutdown();
}

#[tokio::test(start_paused = true)]
async fn leader_serves_propose_and_linearizable_query() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cluster = Cluster::start(&ids);
    let leader = cluster.wait_for_leader().await;

    let set = cluster.handles[&leader]
        .propose(KvCommand::Set {
            key: "color".into(),
            value: "amber".into(),
        })
        .await
        .expect("propose should succeed on the leader");
    assert_eq!(set, KvResponse::Set { previous: None });

    let got = cluster.handles[&leader]
        .query(KvQuery::Get {
            key: "color".into(),
        })
        .await
        .expect("query should succeed on the leader");
    assert_eq!(got, KvResponse::Value(Some("amber".into())));

    cluster.shutdown();
}

#[tokio::test(start_paused = true)]
async fn propose_on_a_follower_reports_not_leader() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cluster = Cluster::start(&ids);
    let leader = cluster.wait_for_leader().await;
    let follower = ids.into_iter().find(|id| *id != leader).unwrap();

    let err = cluster.handles[&follower]
        .propose(KvCommand::Set {
            key: "k".into(),
            value: "v".into(),
        })
        .await
        .expect_err("a follower must reject a direct proposal");
    match err {
        ClientError::NotLeader { leader: hint } => {
            // The follower usually knows who the leader is.
            if let Some(hint) = hint {
                assert_eq!(hint, leader);
            }
        }
        other => panic!("expected NotLeader, got {other:?}"),
    }

    cluster.shutdown();
}

#[tokio::test(start_paused = true)]
async fn follower_forwards_writes_and_serves_reads_locally() {
    // client-routing: writes hit any node and forward to the leader.
    // read-consistency: queries on a follower confirm ReadIndex with the leader,
    // wait for the apply barrier, then serve from local state.
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cluster = Cluster::start(&ids);
    let leader = cluster.wait_for_leader().await;
    let follower = ids.into_iter().find(|id| *id != leader).unwrap();

    let propose = ClientRequest::Propose(
        craft_actor::craft_proto::encode(&KvCommand::Set {
            key: "via".into(),
            value: "follower".into(),
        })
        .unwrap(),
    );
    let response = send_client_request(&cluster.net, follower, &propose)
        .await
        .expect("forwarded propose");
    match response {
        ClientResponse::Ok(bytes) => {
            let decoded: KvResponse = craft_actor::craft_proto::decode(&bytes).unwrap();
            assert_eq!(decoded, KvResponse::Set { previous: None });
        }
        other => panic!("expected forwarded Ok, got {other:?}"),
    }

    // Linearizable read through the follower: etcd-style follower read (read-consistency)
    // — confirm ReadIndex with the leader, wait for apply barrier, serve locally.
    let query = ClientRequest::Query(
        craft_actor::craft_proto::encode(&KvQuery::Get { key: "via".into() }).unwrap(),
    );
    let response = send_client_request(&cluster.net, follower, &query)
        .await
        .expect("follower read");
    match response {
        ClientResponse::Ok(bytes) => {
            let decoded: KvResponse = craft_actor::craft_proto::decode(&bytes).unwrap();
            assert_eq!(decoded, KvResponse::Value(Some("follower".into())));
        }
        other => panic!("expected follower read Ok, got {other:?}"),
    }

    cluster.shutdown();
}

#[tokio::test(start_paused = true)]
async fn leader_confirms_read_index_for_follower_reads() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cluster = Cluster::start(&ids);
    let leader = cluster.wait_for_leader().await;

    let set = ClientRequest::Propose(
        craft_actor::craft_proto::encode(&KvCommand::Set {
            key: "x".into(),
            value: "1".into(),
        })
        .unwrap(),
    );
    send_client_request(&cluster.net, leader, &set)
        .await
        .expect("seed write");

    let confirm = send_client_request(
        &cluster.net,
        leader,
        &ClientRequest::ReadIndexConfirm { route_key: None },
    )
    .await
    .expect("read index confirm");
    match confirm {
        ClientResponse::ReadIndexConfirmed { index, term } => {
            let status = cluster.handles[&leader].status().await.unwrap();
            assert!(index.0 >= 1);
            assert_eq!(term, status.term);
            assert!(status.last_applied >= index);
        }
        other => panic!("expected ReadIndexConfirmed, got {other:?}"),
    }

    cluster.shutdown();
}

#[tokio::test(start_paused = true)]
async fn pending_proposal_fails_when_leadership_is_lost() {
    // Regression: a proposal that cannot reach quorum must not hang forever.
    // Isolate the leader (detach its followers), start a proposal that can
    // never commit, then force a step-down with a higher-term RPC and assert
    // the proposal resolves with NotLeader rather than blocking.
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cluster = Cluster::start(&ids);
    let leader = cluster.wait_for_leader().await;

    let term_before = cluster.handles[&leader].status().await.unwrap().term;

    // Cut the leader off from both followers so the write cannot commit.
    for &id in &ids {
        if id != leader {
            cluster.net.detach(id);
        }
    }

    let leader_handle = cluster.handles[&leader].clone();
    let pending = tokio::spawn(async move {
        leader_handle
            .propose(KvCommand::Set {
                key: "orphan".into(),
                value: "x".into(),
            })
            .await
    });

    // Give the proposal a moment to be appended and left pending.
    advance(Duration::from_millis(30)).await;

    // A higher-term heartbeat from a "new leader" forces the old leader to
    // step down to a follower.
    let usurper = NodeId(99);
    let higher_term = RaftRpc::AppendEntries(AppendEntries {
        term: Term(term_before.0 + 5),
        leader_id: usurper,
        prev_log: LogId::ZERO,
        entries: Vec::new(),
        leader_commit: LogIndex::ZERO,
        round: Round::ZERO,
    });
    let _ = cluster.handles[&leader]
        .deliver_rpc(usurper, higher_term)
        .await;

    let result = tokio::time::timeout(Duration::from_secs(3), pending)
        .await
        .expect("proposal must resolve, not hang, after leadership loss")
        .expect("proposal task panicked");
    assert!(
        matches!(result, Err(ClientError::NotLeader { .. })),
        "expected NotLeader after step-down, got {result:?}"
    );

    cluster.shutdown();
}

#[tokio::test(start_paused = true)]
async fn client_wire_propose_and_query_round_trip() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cluster = Cluster::start(&ids);
    let leader = cluster.wait_for_leader().await;

    // Talk to the leader the way a remote client would: encoded ClientRequests
    // over the `/client/wire` route via the transport.
    let propose = ClientRequest::Propose(
        craft_actor::craft_proto::encode(&KvCommand::Set {
            key: "lang".into(),
            value: "rust".into(),
        })
        .unwrap(),
    );
    let response = send_client_request(&cluster.net, leader, &propose)
        .await
        .expect("client wire propose");
    match response {
        ClientResponse::Ok(bytes) => {
            let decoded: KvResponse = craft_actor::craft_proto::decode(&bytes).unwrap();
            assert_eq!(decoded, KvResponse::Set { previous: None });
        }
        other => panic!("expected Ok, got {other:?}"),
    }

    let query = ClientRequest::Query(
        craft_actor::craft_proto::encode(&KvQuery::Get { key: "lang".into() }).unwrap(),
    );
    let response = send_client_request(&cluster.net, leader, &query)
        .await
        .expect("client wire query");
    match response {
        ClientResponse::Ok(bytes) => {
            let decoded: KvResponse = craft_actor::craft_proto::decode(&bytes).unwrap();
            assert_eq!(decoded, KvResponse::Value(Some("rust".into())));
        }
        other => panic!("expected Ok, got {other:?}"),
    }

    cluster.shutdown();
}

#[tokio::test(start_paused = true)]
async fn a_new_node_joins_a_running_cluster() {
    // E5 (membership-early, join-rpc): a fourth node joins a live 3-node cluster via
    // /cluster/join, the leader runs a joint-consensus membership change, and
    // the joiner catches up as a follower.
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let mut cluster = Cluster::start(&ids);
    let leader = cluster.wait_for_leader().await;

    // Seed some state the joiner must replicate.
    cluster.handles[&leader]
        .propose(KvCommand::Set {
            key: "seed".into(),
            value: "state".into(),
        })
        .await
        .expect("seed propose");

    // Bring up node 4. It knows the post-join member set but starts with an
    // empty log; pre-vote keeps it from disrupting the existing leader.
    let joiner = NodeId(4);
    let full = [NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
    cluster.spawn_one(joiner, full.iter().copied());
    cluster.ids.push(joiner);

    // Ask to join by contacting a *follower* — it should transparently forward
    // to the leader, which commits the membership change.
    let entry = ids.into_iter().find(|id| *id != leader).unwrap();
    let request = JoinRequest {
        protocol_version: PROTOCOL_VERSION,
        node_id: joiner,
        advertise_addr: "node4.local:7443".to_string(),
    };
    let response = send_join_request(&cluster.net, entry, &request)
        .await
        .expect("join request");

    match response {
        JoinResponse::Accepted { membership, .. } => {
            assert!(
                membership.voters.contains(&joiner),
                "new node must be a voter in the committed membership: {membership:?}"
            );
        }
        other => panic!("expected Accepted, got {other:?}"),
    }

    // The joiner should converge to a follower that has caught up to the
    // cluster's committed state (including the seed command).
    let converged = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let leader_commit = cluster.handles[&leader]
                .status()
                .await
                .unwrap()
                .commit_index;
            if let Some(s) = cluster.handles[&joiner].status().await
                && matches!(s.role, Role::Follower)
                && s.commit_index >= leader_commit
                && leader_commit.0 > 0
            {
                break;
            }
            advance(TICK_PERIOD).await;
        }
    })
    .await;
    assert!(converged.is_ok(), "joiner failed to catch up in time");

    // A duplicate join is rejected.
    let dup = send_join_request(&cluster.net, leader, &request)
        .await
        .expect("duplicate join request");
    assert!(
        matches!(
            dup,
            JoinResponse::Rejected {
                reason: JoinRejection::Duplicate
            }
        ),
        "expected Duplicate, got {dup:?}"
    );

    // A version-skewed join is hard-rejected (join-version-skew).
    let skew = JoinRequest {
        protocol_version: PROTOCOL_VERSION + 1,
        node_id: NodeId(5),
        advertise_addr: "node5.local:7443".to_string(),
    };
    let skew_resp = send_join_request(&cluster.net, leader, &skew)
        .await
        .expect("skewed join request");
    assert!(
        matches!(
            skew_resp,
            JoinResponse::Rejected {
                reason: JoinRejection::VersionSkew { .. }
            }
        ),
        "expected VersionSkew, got {skew_resp:?}"
    );

    cluster.shutdown();
}

#[tokio::test(start_paused = true)]
async fn a_node_leaves_a_running_cluster() {
    let ids = [NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
    let mut cluster = Cluster::start(&ids[..3]);
    let leader = cluster.wait_for_leader().await;

    cluster.spawn_one(NodeId(4), ids.iter().copied());
    cluster.ids.push(NodeId(4));
    let joiner = NodeId(4);
    let join_request = JoinRequest {
        protocol_version: PROTOCOL_VERSION,
        node_id: joiner,
        advertise_addr: "node4.local:7443".to_string(),
    };
    let join_resp = send_join_request(&cluster.net, leader, &join_request)
        .await
        .expect("join");
    assert!(matches!(join_resp, JoinResponse::Accepted { .. }));

    let leave_request = LeaveRequest {
        protocol_version: PROTOCOL_VERSION,
        node_id: joiner,
    };
    let leave_resp = send_leave_request(&cluster.net, leader, &leave_request)
        .await
        .expect("leave");
    match leave_resp {
        LeaveResponse::Accepted { membership, .. } => {
            assert!(!membership.voters.contains(&joiner));
        }
        other => panic!("expected Accepted, got {other:?}"),
    }

    let dup = send_leave_request(&cluster.net, leader, &leave_request)
        .await
        .expect("duplicate leave");
    assert!(
        matches!(
            dup,
            LeaveResponse::Rejected {
                reason: LeaveRejection::NotMember
            }
        ),
        "expected NotMember, got {dup:?}"
    );

    cluster.shutdown();
}

/// Fail every append after `allow` successful appends.
struct FailAppendAfter {
    inner: MemoryStorage,
    allow: usize,
    seen: Mutex<AtomicUsize>,
}

impl FailAppendAfter {
    fn new(allow: usize) -> Self {
        Self {
            inner: MemoryStorage::default(),
            allow,
            seen: Mutex::new(AtomicUsize::new(0)),
        }
    }
}

impl HardStateStore for FailAppendAfter {
    fn load_hard_state(&self) -> Result<HardState, StorageError> {
        self.inner.load_hard_state()
    }

    fn save_hard_state(&mut self, state: &HardState) -> Result<(), StorageError> {
        self.inner.save_hard_state(state)
    }
}

impl LogStore for FailAppendAfter {
    fn first_index(&self) -> Result<LogIndex, StorageError> {
        self.inner.first_index()
    }
    fn last_index(&self) -> Result<LogIndex, StorageError> {
        self.inner.last_index()
    }
    fn read(&self, index: LogIndex) -> Result<Option<LogEntry>, StorageError> {
        self.inner.read(index)
    }
    fn read_from(&self, from: LogIndex) -> Result<Vec<LogEntry>, StorageError> {
        self.inner.read_from(from)
    }
    fn append(&mut self, entries: &[LogEntry]) -> Result<(), StorageError> {
        let seen = self.seen.lock().unwrap();
        let n = seen.fetch_add(1, Ordering::SeqCst) + 1;
        drop(seen);
        if n > self.allow {
            Err(StorageError::Backend("injected append failure".into()))
        } else {
            self.inner.append(entries)
        }
    }
    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), StorageError> {
        self.inner.truncate_suffix(from)
    }
    fn purge_prefix(&mut self, through: LogIndex) -> Result<(), StorageError> {
        self.inner.purge_prefix(through)
    }
}

impl SnapshotStore for FailAppendAfter {
    fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), StorageError> {
        self.inner.save_snapshot(snapshot)
    }
    fn load_snapshot(&self) -> Result<Option<Snapshot>, StorageError> {
        self.inner.load_snapshot()
    }
}

#[tokio::test(start_paused = true)]
async fn fatal_storage_error_stops_the_runtime() {
    let net = LocalNetwork::new();
    let node = RaftNode::new(NodeId(1), [NodeId(1)], fast_raft_config_with_seed(7));
    let driver = RaftDriver::with_storage(node, Kv::default(), Box::new(FailAppendAfter::new(0)));
    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let handle = spawn_node(
        driver,
        Arc::clone(&transport),
        RuntimeConfig {
            tick_period: TICK_PERIOD,
            allow_join: false,
            allow_leave: false,
            ..RuntimeConfig::default()
        },
    );
    let service: Arc<dyn RequestHandler> =
        Arc::new(NodeService::new(handle.clone(), Arc::clone(&transport)));
    net.attach(NodeId(1), service);

    handle.campaign();
    for _ in 0..500 {
        if handle.status().await.is_none() {
            break;
        }
        advance(TICK_PERIOD).await;
    }

    assert!(
        handle.status().await.is_none(),
        "runtime must stop after a fatal storage error"
    );
    let err = handle
        .propose(KvCommand::Set {
            key: "k".into(),
            value: "v".into(),
        })
        .await
        .expect_err("propose after fatal error");
    assert!(
        matches!(err, ClientError::Stopped),
        "expected Stopped, got {err:?}"
    );
}
