use std::collections::HashMap;
use std::fmt;

use chrono::{DateTime, Utc};
use pyo3::prelude::*;

use crate::python::job::JobLogEvent;

/// The execution context for a run, capturing the parameters that were
/// used to launch it.
#[derive(Clone)]
#[pyclass(name = "RunExecutionContext", module = "bauplan", get_all)]
pub(crate) struct RunExecutionContext {
    pub snapshot_id: String,
    pub snapshot_uri: String,
    pub project_dir: String,
    pub r#ref: String,
    pub namespace: String,
    pub dry_run: bool,
    pub transaction: String,
    pub strict: String,
    pub cache: String,
    pub preview: String,
    pub debug: bool,
    pub detach: bool,
}

impl fmt::Debug for RunExecutionContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunExecutionContext")
            .field("ref", &self.r#ref)
            .field("namespace", &self.namespace)
            .field("dry_run", &self.dry_run)
            .field("transaction", &self.transaction)
            .field("strict", &self.strict)
            .field("cache", &self.cache)
            .field("preview", &self.preview)
            .finish()
    }
}

/// The state of a completed (or failed) run, including logs, timing, and
/// per-task lifecycle events.
#[derive(Debug, Clone)]
#[pyclass(name = "RunState", module = "bauplan", get_all)]
pub(crate) struct RunState {
    /// The job ID assigned by the server.
    pub job_id: Option<String>,
    /// The execution context for the run.
    pub ctx: RunExecutionContext,
    /// User log messages emitted during the run.
    pub user_logs: Vec<JobLogEvent>,
    /// Per-task start times, keyed by task ID.
    pub tasks_started: HashMap<String, DateTime<Utc>>,
    /// Per-task stop times, keyed by task ID.
    pub tasks_stopped: HashMap<String, DateTime<Utc>>,
    /// The final status string (e.g. "SUCCESS", "FAILURE").
    pub job_status: Option<String>,
    /// Epoch nanoseconds when the run started.
    pub started_at_ns: i64,
    /// Epoch nanoseconds when the run ended, if it has.
    pub ended_at_ns: Option<i64>,
    /// Error message, if the run failed.
    pub error: Option<String>,
}

#[pymethods]
impl RunState {
    /// Duration in seconds, or None if the run hasn't ended.
    #[getter]
    fn duration(&self) -> Option<f64> {
        self.ended_at_ns
            .map(|end| (end - self.started_at_ns) as f64 / 1_000_000_000.0)
    }

    /// Duration in nanoseconds, or None if the run hasn't ended.
    #[getter]
    fn duration_ns(&self) -> Option<i64> {
        self.ended_at_ns.map(|end| end - self.started_at_ns)
    }
}
