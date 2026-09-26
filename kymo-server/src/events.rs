//! In-process change-event bus: every code path that changes an effective version publishes a bump or requests a resync here; ws_proxy forwards them to dashboards as id-0 push frames. Values match what PollVersions serves. Slow version polling is only a backstop for an outcome-unknown commit, a lost event, or a clock-driven expiry.

/// One batch of version bumps; empty/None fields didn't change.
#[derive(Clone, Debug, Default)]
pub struct VersionEvent {
    /// (run_id, effective run version) — data landed or the run became terminal and unreadable.
    pub runs: Vec<(String, u64)>,
    /// (project_id, new projects.version) — the project's run LIST changed
    /// (run added, renamed, re-initialized, terminated, or liveness transitioned).
    pub projects: Vec<(String, u64)>,
    /// New global_seq version — global discovery or Trash visibility changed.
    pub global: Option<u64>,
    /// Runs whose metric registry gained a name or upgraded a type — the
    /// signal to re-list their metrics (run_versions alone can't
    /// distinguish "new data" from "new metric").
    pub metrics_changed_runs: Vec<String>,
    /// Conservatively require connected dashboards to poll and refetch. Used
    /// when a database commit acknowledgement is ambiguous and exact bumped
    /// versions may not be available to the request handler.
    pub resync: bool,
}

impl VersionEvent {
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
            && self.projects.is_empty()
            && self.global.is_none()
            && self.metrics_changed_runs.is_empty()
            && !self.resync
    }
}

pub type EventSender = tokio::sync::broadcast::Sender<VersionEvent>;
