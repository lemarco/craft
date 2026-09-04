mod merge_app_config_tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use trembita_net::{LocalNetwork, PeerDirectory};
    use trembita_runtime::DEFAULT_DRAIN_TIMEOUT;

    use trembita_proto::NodeId;

    use super::TrembitaClusterBuilder;
    use crate::app::EmptyStateMachine;
    use crate::env_config::{AppConfig, EnvOverrides};
    use crate::security::Security;

    fn test_app_config(node_id: NodeId) -> AppConfig {
        let ca = trembita_net::tls::ClusterCa::generate().expect("ca");
        AppConfig {
            node_id,
            listen: "127.0.0.1:7443".parse::<SocketAddr>().expect("addr"),
            admin: None,
            admin_tls: None,
            peers: PeerDirectory::new(),
            members: Vec::new(),
            join_seeds: Vec::new(),
            allow_join: true,
            allow_voter_join: false,
            join_role: JoinRole::Learner,
            allow_leave: true,
            graceful_leave: true,
            voter_replacement: true,
            voter_replacement_grace_ticks: None,
            security: Security::dev(&ca, node_id).expect("security"),
            pem_paths: None,
            cert_dir: None,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
            cert_watch: Duration::from_secs(60),
            data_dir: None,
            job_queue_stream: None,
            job_queue_lease: Duration::from_secs(60),
            gateway: None,
            gateway_jobs_api: false,
            gateway_actors_api: false,
            gateway_workflows_api: false,
            gateway_introspect_api: false,
            gateway_drain_timeout: crate::gateway::DEFAULT_GATEWAY_DRAIN_TIMEOUT,
            gateway_tls: None,
            env: EnvOverrides::default(),
        }
    }

    #[tokio::test]
    async fn merge_app_config_applies_node_id_from_env_cfg() {
        let net = LocalNetwork::new();
        let cluster = TrembitaClusterBuilder::new(NodeId(1), EmptyStateMachine)
            .merge_app_config(&test_app_config(NodeId(7)))
            .tick_period(Duration::from_millis(5))
            .start_local(&net)
            .await;
        assert_eq!(cluster.node_id(), NodeId(7));

        let joiner = TrembitaClusterBuilder::new(NodeId(1), EmptyStateMachine)
            .merge_app_config(&test_app_config(NodeId(0)))
            .tick_period(Duration::from_millis(5))
            .start_local(&LocalNetwork::new())
            .await;
        assert_eq!(joiner.node_id(), NodeId(0));
    }

    #[test]
    fn merge_app_config_applies_join_role_and_allow_voter_join_from_env() {
        let mut cfg = test_app_config(NodeId(2));
        cfg.join_role = JoinRole::Voter;
        cfg.allow_voter_join = true;
        let builder =
            TrembitaClusterBuilder::new(NodeId(1), EmptyStateMachine).merge_app_config(&cfg);
        assert_eq!(builder.join_role, JoinRole::Voter);
        assert!(builder.runtime.allow_voter_join);
    }

    #[test]
    fn merge_app_config_code_join_role_wins_over_env() {
        let mut cfg = test_app_config(NodeId(2));
        cfg.join_role = JoinRole::Voter;
        let builder = TrembitaClusterBuilder::new(NodeId(1), EmptyStateMachine)
            .join_as(JoinRole::Learner)
            .merge_app_config(&cfg);
        assert_eq!(builder.join_role, JoinRole::Learner);
    }

    #[test]
    fn merge_app_config_explicit_node_id_wins_over_env() {
        let builder = TrembitaClusterBuilder::new(NodeId(1), EmptyStateMachine)
            .with_explicit_node_id()
            .merge_app_config(&test_app_config(NodeId(7)));
        assert_eq!(builder.node_id, NodeId(1));
    }

    #[test]
    fn merge_app_config_members_from_env_only_when_peers_set() {
        let mut cfg = test_app_config(NodeId(3));
        cfg.members = vec![NodeId(1), NodeId(2), NodeId(3)];
        cfg.env.peers = false;
        let builder = TrembitaClusterBuilder::new(NodeId(5), EmptyStateMachine)
            .members([NodeId(5)])
            .merge_app_config(&cfg);
        assert_eq!(builder.members, vec![NodeId(5)]);

        cfg.env.peers = true;
        let builder =
            TrembitaClusterBuilder::new(NodeId(5), EmptyStateMachine).merge_app_config(&cfg);
        assert_eq!(builder.members, vec![NodeId(1), NodeId(2), NodeId(3)]);
    }
}
