//! Reference rolling-upgrade state machine ([upgrade-coordinator](../../../docs/decisions/upgrade-coordinator.md)).
//!
//! Pure reconcile logic and a minimal [`StateMachine`] apps embed or use standalone.
//! Download, binary replace, and process exit live in the `crafty` facade (`upgrade` module).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crafty_proto::NodeId;
use serde::{Deserialize, Serialize};

use crate::proto::LogIndex;
use crate::state_machine::StateMachine;

/// Target artifact published by ops (manifest / `POST /cluster/upgrade/desired`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactManifest {
    /// Application semver after restart (`CRAFTY_APP_VERSION` / join skew).
    pub app_version: String,
    /// Download URL (`https://…` or `file://…` for local demos).
    pub url: String,
    /// SHA-256 of the artifact bytes (hex-encoded).
    pub sha256_hex: String,
    /// Optional lower bound for [`crafty_proto::PROTOCOL_VERSION`] compatibility.
    #[serde(default)]
    pub min_protocol: Option<u32>,
}

/// Replicated upgrade commands.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpgradeCommand {
    /// Begin or replace the desired rolling target.
    SetDesired(ArtifactManifest),
    /// Leader assigns the next node (one slot at a time).
    Grant {
        /// Node that may download, install, and restart.
        node_id: NodeId,
    },
    /// Executor lifecycle report from any member.
    Report {
        /// Reporting node.
        node_id: NodeId,
        /// Current phase on that node.
        phase: UpgradePhase,
    },
    /// Abort the rolling upgrade.
    Abort {
        /// Human-readable reason.
        reason: String,
    },
}

/// Executor / coordinator lifecycle phases.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpgradePhase {
    /// Download in progress.
    Downloading,
    /// Artifact verified and installed on disk; restart pending.
    Installed,
    /// Process is shutting down for restart.
    Restarting,
    /// Post-boot: running build matches desired.
    Ready,
    /// Unrecoverable error on this node.
    Failed {
        /// Error summary.
        message: String,
    },
}

impl UpgradePhase {
    /// Whether the phase is terminal for the current grant slot.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Ready | Self::Failed { .. })
    }
}

/// Read side of the upgrade machine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpgradeQuery {
    /// Fleet rolling snapshot for coordinator ticks and admin HTTP.
    View {
        /// Committed voter set used to compute `pending`.
        members: Vec<NodeId>,
    },
}

/// Apply/query responses.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpgradeResponse {
    /// Command applied (no payload).
    Ok,
    /// Result of [`UpgradeQuery::View`].
    View(UpgradeView),
}

/// Errors from the reference upgrade machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpgradeError {
    /// No rolling target configured.
    NoDesired,
    /// Grant rejected (unknown node, duplicate slot, …).
    InvalidGrant,
    /// Rolling aborted.
    Aborted,
    /// Snapshot encode/decode failed.
    Snapshot,
}

impl fmt::Display for UpgradeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoDesired => f.write_str("no desired upgrade"),
            Self::InvalidGrant => f.write_str("invalid grant"),
            Self::Aborted => f.write_str("upgrade aborted"),
            Self::Snapshot => f.write_str("upgrade snapshot error"),
        }
    }
}

impl std::error::Error for UpgradeError {}

/// Durable rolling-upgrade state replicated through Raft.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeState {
    desired: Option<ArtifactManifest>,
    granted: Option<NodeId>,
    completed: BTreeSet<NodeId>,
    last_report: BTreeMap<NodeId, UpgradePhase>,
    aborted: Option<String>,
}

/// Derived snapshot for coordinator ticks and admin APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeView {
    /// Active target, if any.
    pub desired: Option<ArtifactManifest>,
    /// Node currently holding the grant slot.
    pub granted: Option<NodeId>,
    /// Members that reported `Ready` for the current desired target.
    pub completed: BTreeSet<NodeId>,
    /// Committed members not yet `Ready`.
    pub pending: Vec<NodeId>,
    /// All committed members match desired (rolling complete).
    pub fleet_ready: bool,
    /// Abort reason when set.
    pub aborted: Option<String>,
}

/// Pick the next node to grant when the current slot is idle.
///
/// **Leader last:** the coordinator keeps planning until only the leader remains,
/// then grants the leader. Among eligible nodes, the lowest [`NodeId`] wins so
/// failovers compute the same next step from SM state alone.
#[must_use]
pub fn plan_next_grant(
    state: &UpgradeState,
    members: &[NodeId],
    leader_id: NodeId,
) -> Option<NodeId> {
    if state.aborted.is_some() || state.desired.is_none() {
        return None;
    }
    if state.granted.is_some() {
        return None;
    }
    let mut pending: Vec<NodeId> = members
        .iter()
        .copied()
        .filter(|id| !state.completed.contains(id))
        .collect();
    if pending.is_empty() {
        return None;
    }
    pending.sort_by_key(|id| id.0);
    let non_leader: Vec<_> = pending
        .iter()
        .copied()
        .filter(|&id| id != leader_id)
        .collect();
    if non_leader.is_empty() {
        pending.into_iter().min_by_key(|id| id.0)
    } else {
        non_leader.into_iter().min_by_key(|id| id.0)
    }
}

/// Build an [`UpgradeView`] from durable state and committed membership.
#[must_use]
pub fn upgrade_view(state: &UpgradeState, members: &[NodeId]) -> UpgradeView {
    let pending: Vec<NodeId> = members
        .iter()
        .copied()
        .filter(|id| !state.completed.contains(id))
        .collect();
    let fleet_ready = state.desired.is_some() && pending.is_empty() && state.aborted.is_none();
    UpgradeView {
        desired: state.desired.clone(),
        granted: state.granted,
        completed: state.completed.clone(),
        pending,
        fleet_ready,
        aborted: state.aborted.clone(),
    }
}

/// Build planning state from a query view (leader reconcile helper).
#[must_use]
pub fn upgrade_state_for_planning(view: &UpgradeView) -> UpgradeState {
    UpgradeState {
        desired: view.desired.clone(),
        granted: view.granted,
        completed: view.completed.clone(),
        last_report: BTreeMap::new(),
        aborted: view.aborted.clone(),
    }
}

fn apply_command(state: &mut UpgradeState, command: &UpgradeCommand) -> Result<(), UpgradeError> {
    match command {
        UpgradeCommand::SetDesired(manifest) => {
            state.desired = Some(manifest.clone());
            state.granted = None;
            state.completed.clear();
            state.last_report.clear();
            state.aborted = None;
        }
        UpgradeCommand::Grant { node_id } => {
            if state.aborted.is_some() || state.desired.is_none() || state.granted.is_some() {
                return Err(UpgradeError::InvalidGrant);
            }
            state.granted = Some(*node_id);
        }
        UpgradeCommand::Report { node_id, phase } => {
            state.last_report.insert(*node_id, phase.clone());
            if matches!(phase, UpgradePhase::Ready) {
                state.completed.insert(*node_id);
                if state.granted == Some(*node_id) {
                    state.granted = None;
                }
            }
            if matches!(phase, UpgradePhase::Failed { .. }) && state.granted == Some(*node_id) {
                state.granted = None;
            }
        }
        UpgradeCommand::Abort { reason } => {
            state.aborted = Some(reason.clone());
            state.granted = None;
        }
    }
    Ok(())
}

/// Reference upgrade [`StateMachine`] — use standalone or merge commands into your app SM.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeMachine {
    inner: UpgradeState,
}

impl UpgradeMachine {
    /// Borrow durable state.
    #[must_use]
    pub fn state(&self) -> &UpgradeState {
        &self.inner
    }
}

/// Shorter alias used in docs and tests.
pub type UpgradeStateMachine = UpgradeMachine;

impl StateMachine for UpgradeMachine {
    type Command = UpgradeCommand;
    type Query = UpgradeQuery;
    type Response = UpgradeResponse;
    type Error = UpgradeError;

    fn apply(
        &mut self,
        _index: LogIndex,
        command: &UpgradeCommand,
    ) -> Result<UpgradeResponse, UpgradeError> {
        apply_command(&mut self.inner, command)?;
        Ok(UpgradeResponse::Ok)
    }

    fn query(&self, query: &UpgradeQuery) -> Result<UpgradeResponse, UpgradeError> {
        Ok(match query {
            UpgradeQuery::View { members } => {
                UpgradeResponse::View(upgrade_view(&self.inner, members))
            }
        })
    }

    fn snapshot(&self) -> Result<Vec<u8>, UpgradeError> {
        crate::proto::encode(&self.inner).map_err(|_| UpgradeError::Snapshot)
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), UpgradeError> {
        self.inner = crate::proto::decode(snapshot).map_err(|_| UpgradeError::Snapshot)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn members() -> Vec<NodeId> {
        vec![NodeId(1), NodeId(2), NodeId(3)]
    }

    #[test]
    fn leader_last_grant_ordering() {
        let mut state = UpgradeState {
            desired: Some(ArtifactManifest {
                app_version: "1.0.0".into(),
                url: "file:///tmp/x".into(),
                sha256_hex: "00".repeat(64),
                min_protocol: None,
            }),
            ..Default::default()
        };
        let leader = NodeId(2);
        let m = members();

        assert_eq!(plan_next_grant(&state, &m, leader), Some(NodeId(1)));

        state.granted = Some(NodeId(1));
        state.last_report.insert(NodeId(1), UpgradePhase::Ready);
        state.completed.insert(NodeId(1));
        state.granted = None;

        assert_eq!(plan_next_grant(&state, &m, leader), Some(NodeId(3)));

        state.completed.insert(NodeId(3));
        assert_eq!(plan_next_grant(&state, &m, leader), Some(NodeId(2)));
    }

    #[test]
    fn report_ready_clears_grant_and_completes_node() {
        let mut sm = UpgradeMachine::default();
        let manifest = ArtifactManifest {
            app_version: "2.0.0".into(),
            url: "file:///bin".into(),
            sha256_hex: "ab".repeat(64),
            min_protocol: None,
        };
        sm.apply(LogIndex(1), &UpgradeCommand::SetDesired(manifest))
            .unwrap();
        sm.apply(LogIndex(2), &UpgradeCommand::Grant { node_id: NodeId(1) })
            .unwrap();
        sm.apply(
            LogIndex(3),
            &UpgradeCommand::Report {
                node_id: NodeId(1),
                phase: UpgradePhase::Ready,
            },
        )
        .unwrap();
        assert!(sm.state().completed.contains(&NodeId(1)));
        assert_eq!(sm.state().granted, None);
    }

    #[test]
    fn fleet_ready_when_all_members_complete() {
        let mut sm = UpgradeMachine::default();
        sm.apply(
            LogIndex(1),
            &UpgradeCommand::SetDesired(ArtifactManifest {
                app_version: "1.0.0".into(),
                url: "file:///x".into(),
                sha256_hex: "00".repeat(64),
                min_protocol: None,
            }),
        )
        .unwrap();
        for (idx, id) in members().into_iter().enumerate() {
            sm.apply(
                LogIndex(u64::try_from(idx + 2).unwrap()),
                &UpgradeCommand::Report {
                    node_id: id,
                    phase: UpgradePhase::Ready,
                },
            )
            .unwrap();
        }
        let UpgradeResponse::View(view) = sm
            .query(&UpgradeQuery::View { members: members() })
            .unwrap()
        else {
            panic!("expected view");
        };
        assert!(view.fleet_ready);
        assert!(view.pending.is_empty());
    }
}
