//! Stateless **PerNode** capability — proves handler hosts scale with cluster size.

use trembita::{
    CapError, CapGroup, CapManifest, CapOp, CapRequest, OpCtx, Route, cap_register_chain,
};

#[derive(Default)]
pub struct Marker;

#[derive(Default, serde::Deserialize, serde::Serialize)]
pub struct WhoAmI;

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct NodeReply {
    /// Raft node id of the host that executed the op.
    pub node_id: u64,
}

impl CapRequest for WhoAmI {
    const GROUP: &'static str = "e2e_pool";
    const OP: &'static str = "whoami";
    const QUEUE_STREAM: &'static str = "e2e_pool.whoami";
    const EVENT_TOPIC: &'static str = "e2e_pool.whoami";
    const EVENT_SUBSCRIPTION: &'static str = "e2e_pool.whoami.cap";
    type Reply = NodeReply;
}

fn whoami_run(_msg: WhoAmI, ctx: OpCtx<'_>, _state: &mut Marker) -> Result<NodeReply, CapError> {
    let app = ctx
        .app()
        .ok_or_else(|| CapError::domain("missing app in handler context"))?;
    Ok(NodeReply {
        node_id: app.node_id().0,
    })
}

fn whoami_register(group: CapGroup<Marker>) -> CapGroup<Marker> {
    group.op(CapOp::new("whoami", whoami_run).routes([Route::Inline]))
}

#[must_use]
pub fn capabilities_manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<Marker>::with_state("e2e_pool"),
        whoami_register,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b34_capabilities_manifest_registers_e2e_pool() {
        let manifest = capabilities_manifest();
        assert!(
            !manifest.is_empty(),
            "elastic E2E binary must register the PerNode e2e_pool group"
        );
    }

    #[test]
    fn b34_whoami_reply_serializes_node_id() {
        let json = serde_json::to_value(NodeReply { node_id: 4 }).expect("json");
        assert_eq!(json["node_id"], 4);
    }

    /// B-42 — shared cap metadata for local cluster + E2E LB smoke.
    #[test]
    fn b42_whoami_cap_request_metadata_scenarios_table() {
        struct Row {
            field: &'static str,
            want: &'static str,
        }
        let rows = [
            Row {
                field: "GROUP",
                want: "e2e_pool",
            },
            Row {
                field: "OP",
                want: "whoami",
            },
            Row {
                field: "QUEUE_STREAM",
                want: "e2e_pool.whoami",
            },
        ];
        for row in rows {
            let got = match row.field {
                "GROUP" => WhoAmI::GROUP,
                "OP" => WhoAmI::OP,
                "QUEUE_STREAM" => WhoAmI::QUEUE_STREAM,
                _ => panic!("unknown field"),
            };
            assert_eq!(got, row.want, "{}", row.field);
        }
        assert!(
            !capabilities_manifest().is_empty(),
            "e2e_pool manifest required for B-42 cap-smoke"
        );
    }
}
