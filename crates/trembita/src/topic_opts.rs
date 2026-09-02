//! Durable event topic registration options for [`TrembitaAppBuilder`](super::app::TrembitaAppBuilder).

use std::time::Duration;

use trembita_events::{SubscriptionStart, TopicRetentionOpts, TopicSubscriptionDef};
use trembita_proto::{DEFAULT_TOPIC_MAX_EVENT_AGE_MS, DEFAULT_TOPIC_MAX_RETAINED_EVENTS};

/// One durable topic with named subscriptions ([event-topics](../../docs/decisions/event-topics.md)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicOpts {
    /// Topic name (`topic-{name}.redb` under `data_dir`).
    pub name: String,
    /// Lease timeout for subscribers holding events.
    pub lease: Duration,
    /// Boot-time subscriptions (stable set for v1).
    pub subscriptions: Vec<TopicSubscriptionDef>,
    /// Retention limits; lagging subscriptions are force-advanced when exceeded.
    pub retention: TopicRetentionOpts,
}

impl TopicOpts {
    /// Register a topic — alias for [`Self::new`] matching the product API shape.
    #[must_use]
    pub fn topic(name: impl Into<String>) -> Self {
        Self::new(name)
    }

    /// Register a topic with framework default retention.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            lease: Duration::from_secs(300),
            subscriptions: Vec::new(),
            retention: TopicRetentionOpts {
                max_event_age: DEFAULT_MAX_EVENT_AGE,
                max_retained_events: DEFAULT_MAX_RETAINED,
            },
        }
    }

    /// Lease timeout for subscribers holding events from this topic.
    #[must_use]
    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// Declare named subscriptions at build time.
    #[must_use]
    pub fn subscriptions(mut self, names: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        self.subscriptions = names
            .into_iter()
            .map(|n| TopicSubscriptionDef {
                name: n.as_ref().to_string(),
                start: SubscriptionStart::Earliest,
                max_attempts: 0,
            })
            .collect();
        self
    }

    /// Full subscription definitions (start position, retry ceiling).
    #[must_use]
    pub fn subscription_defs(
        mut self,
        defs: impl IntoIterator<Item = TopicSubscriptionDef>,
    ) -> Self {
        self.subscriptions = defs.into_iter().collect();
        self
    }

    /// Retention thresholds — see [`TopicRetentionOpts`].
    #[must_use]
    pub fn retention(mut self, retention: TopicRetentionOpts) -> Self {
        self.retention = retention;
        self
    }

    /// Maximum age of the oldest retained event before forced discard.
    #[must_use]
    pub fn max_event_age(mut self, age: Duration) -> Self {
        self.retention.max_event_age = age;
        self
    }

    /// Maximum retained events before forced discard on lagging subscriptions.
    #[must_use]
    pub fn max_retained_events(mut self, max: u64) -> Self {
        self.retention.max_retained_events = max;
        self
    }
}

/// Default max event age (7 days) — re-export for convenience.
pub const DEFAULT_MAX_EVENT_AGE: Duration = Duration::from_millis(DEFAULT_TOPIC_MAX_EVENT_AGE_MS);

/// Default max retained events (1M) — re-export for convenience.
pub const DEFAULT_MAX_RETAINED: u64 = DEFAULT_TOPIC_MAX_RETAINED_EVENTS;
