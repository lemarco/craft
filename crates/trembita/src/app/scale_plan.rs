//! Product scale snapshot at boot (B-33) — capability hosts, queue layout, coordination Raft.

use serde::Serialize;

use crate::capability::CapGroupScale;
use crate::queue_opts::QueueRegistrationScale;

/// Resolved host scale for one capability group.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CapabilityGroupScaleLine {
    /// Group name ([`CapGroup::name`](crate::CapGroup)).
    pub group: String,
    /// `PerNode` or `Fixed(n)` after [`resolved_scale`](crate::CapGroup::resolved_scale).
    pub hosts: String,
}

/// Job stream physical layout (B-32).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JobQueueScaleLine {
    /// Stream name (`queue-{name}.redb` under `data_dir`).
    pub stream: String,
    /// `standard`, `sharded(n)`, or `auto_shard`.
    pub mode: String,
}

/// Multi-Raft / shard routing from env or [`TrembitaConfigure`](crate::TrembitaConfigure).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CoordinationScaleLine {
    /// `TREMBITA_RAFT_GROUPS` — `1` = single coordination log.
    pub coordination_raft_groups: u32,
    /// `TREMBITA_RAFT_SHARD_COUNT` when groups > 1.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coordination_shard_count: Option<u32>,
}

impl Default for CoordinationScaleLine {
    fn default() -> Self {
        Self {
            coordination_raft_groups: 1,
            coordination_shard_count: None,
        }
    }
}

/// Snapshot merged from manifest + env at builder time; logged after [`TrembitaApp::wait_until_ready`].
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct ProductScalePlan {
    /// One row per [`CapManifest`](crate::CapManifest) group.
    pub capability_groups: Vec<CapabilityGroupScaleLine>,
    /// Registered [`QueueOpts`](crate::QueueOpts) / env job queue streams.
    pub job_queues: Vec<JobQueueScaleLine>,
    /// Coordination Raft layout.
    pub coordination: CoordinationScaleLine,
}

impl ProductScalePlan {
    pub(crate) fn record_capability_group(&mut self, group: &'static str, scale: CapGroupScale) {
        self.capability_groups.push(CapabilityGroupScaleLine {
            group: group.to_string(),
            hosts: scale_label(scale),
        });
    }

    pub(crate) fn record_job_queue(&mut self, stream: &str, scale: &QueueRegistrationScale) {
        if self.job_queues.iter().any(|q| q.stream == stream) {
            return;
        }
        self.job_queues.push(JobQueueScaleLine {
            stream: stream.to_string(),
            mode: queue_scale_label(scale),
        });
    }

    pub(crate) fn set_coordination(&mut self, groups: u32, shard_count: Option<u32>) {
        self.coordination = CoordinationScaleLine {
            coordination_raft_groups: groups.max(1),
            coordination_shard_count: shard_count,
        };
    }

    /// Structured boot log (`target = "trembita::product_scale"`).
    pub fn emit_boot_log(&self) {
        if self.capability_groups.is_empty()
            && self.job_queues.is_empty()
            && self.coordination.coordination_raft_groups <= 1
            && self.coordination.coordination_shard_count.is_none()
        {
            return;
        }
        let json = serde_json::to_string(self).unwrap_or_else(|_| "{}".into());
        tracing::info!(target: "trembita::product_scale", plan = %json, "product scale plan");
    }
}

#[must_use]
fn scale_label(scale: CapGroupScale) -> String {
    match scale {
        CapGroupScale::Fixed(n) => format!("Fixed({n})"),
        CapGroupScale::PerNode => "PerNode".into(),
    }
}

#[must_use]
fn queue_scale_label(scale: &QueueRegistrationScale) -> String {
    match scale {
        QueueRegistrationScale::Standard => "standard".into(),
        QueueRegistrationScale::Sharded(n) => format!("sharded({n})"),
        QueueRegistrationScale::AutoShard(_) => "auto_shard".into(),
    }
}

#[cfg(feature = "http-jobs")]
pub(crate) fn product_scale_route_table(plan: ProductScalePlan) -> trembita_http::RouteTable {
    use http::StatusCode;
    use trembita_http::{RequestCtx, Response, RouteTable};

    RouteTable::new().get("/introspect/product-scale", move |_ctx: RequestCtx| {
        let plan = plan.clone();
        async move {
            Ok(Response::json(
                StatusCode::OK,
                serde_json::to_value(plan).unwrap_or_default(),
            ))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_labels_match_founder_map() {
        assert_eq!(scale_label(CapGroupScale::PerNode), "PerNode");
        assert_eq!(scale_label(CapGroupScale::Fixed(1)), "Fixed(1)");
        assert_eq!(
            queue_scale_label(&QueueRegistrationScale::Sharded(4)),
            "sharded(4)"
        );
    }

    /// B-33 — plan fields match introspect JSON shape.
    #[test]
    fn b33_product_scale_plan_serde_roundtrip() {
        let mut plan = ProductScalePlan::default();
        plan.record_capability_group("orders", CapGroupScale::Fixed(2));
        plan.record_capability_group("ping", CapGroupScale::PerNode);
        plan.record_job_queue("app.jobs", &QueueRegistrationScale::Sharded(3));
        plan.set_coordination(2, Some(64));
        let json = serde_json::to_value(&plan).expect("serialize");
        assert_eq!(json["capability_groups"][0]["hosts"], "Fixed(2)");
        assert_eq!(json["capability_groups"][1]["hosts"], "PerNode");
        assert_eq!(json["job_queues"][0]["mode"], "sharded(3)");
        assert_eq!(json["coordination"]["coordination_raft_groups"], 2);
        assert_eq!(json["coordination"]["coordination_shard_count"], 64);
    }

    #[test]
    fn b33_record_job_queue_dedups_stream() {
        let mut plan = ProductScalePlan::default();
        plan.record_job_queue("jobs", &QueueRegistrationScale::Standard);
        plan.record_job_queue("jobs", &QueueRegistrationScale::Sharded(8));
        assert_eq!(plan.job_queues.len(), 1);
        assert_eq!(plan.job_queues[0].mode, "standard");
    }

    #[test]
    fn b33_emit_boot_log_no_panic_on_populated_plan() {
        let mut plan = ProductScalePlan::default();
        plan.record_capability_group("g", CapGroupScale::PerNode);
        plan.emit_boot_log();
    }
}
