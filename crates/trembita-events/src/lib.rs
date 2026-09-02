//! `trembita-events` — durable pub/sub topics, redb adapter, and leader [`TopicService`].

mod redb_topic;
mod topic;
mod topic_service;

pub use redb_topic::RedbEventTopic;
pub use topic::{
    EventId, EventTopic, InMemoryEventTopic, LeasedEvent, SubscriptionStart, TopicContext,
    TopicError, TopicLeaseId, TopicMetrics, TopicReplicationOps, TopicRetentionOpts,
    TopicSubscriptionDef, TopicSubscriptionMetrics, run_topic_subscriber,
};
pub use topic_service::{ClusterEventTopic, TopicService};
