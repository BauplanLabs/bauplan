use std::collections::HashMap;
use std::fmt;

use chrono::{DateTime, Utc};
use pyo3::prelude::*;

use crate::python::job::JobLogEvent;

/// The execution context for a run, capturing the parameters that were
/// used to launch it.
#[derive(Clone)]
#[pyclass(
    name = "RunExecutionContext",
    module = "bauplan",
    skip_from_py_object,
    get_all
)]
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

#[pymethods]
impl RunExecutionContext {
    fn __repr__(&self) -> String {
        format!(
            "RunExecutionContext(ref={:?}, namespace={:?})",
            self.r#ref, self.namespace,
        )
    }
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
#[pyclass(name = "RunState", module = "bauplan", skip_from_py_object, get_all)]
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
    /// The final status string (e.g. "SUCCESS", "FAILED").
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
    fn __repr__(&self) -> String {
        format!(
            "RunState(job_id={:?}, status={:?}, error={:?})",
            self.job_id, self.job_status, self.error,
        )
    }

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

#[derive(Clone, Debug)]
#[pyclass(
    name = "TableCreatePlanContext",
    module = "bauplan",
    skip_from_py_object,
    get_all
)]
pub(crate) struct TableCreatePlanContext {
    pub branch_name: String,
    pub table_name: String,
    pub table_replace: bool,
    pub table_partitioned_by: Option<String>,
    pub namespace: String,
    pub search_string: String,
}

#[derive(Clone)]
#[pyclass(
    name = "TableCreatePlanState",
    module = "bauplan",
    from_py_object,
    get_all
)]
pub(crate) struct TableCreatePlanState {
    pub job_id: Option<String>,
    pub ctx: TableCreatePlanContext,
    pub job_status: Option<String>,
    pub error: Option<String>,
    pub plan: Option<String>,
    pub can_auto_apply: bool,
    pub files_to_be_imported: Vec<String>,
}

#[pymethods]
impl TableCreatePlanState {
    fn __repr__(&self) -> String {
        format!(
            "TableCreatePlanState(job_id={:?}, status={:?}, can_auto_apply={})",
            self.job_id, self.job_status, self.can_auto_apply,
        )
    }
}

impl fmt::Debug for TableCreatePlanState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableCreatePlanState")
            .field("job_id", &self.job_id)
            .field("job_status", &self.job_status)
            .field("can_auto_apply", &self.can_auto_apply)
            .field("files", &self.files_to_be_imported.len())
            .finish()
    }
}

#[derive(Clone)]
#[pyclass(
    name = "TableCreatePlanApplyState",
    module = "bauplan",
    from_py_object,
    get_all
)]
pub(crate) struct TableCreatePlanApplyState {
    pub job_id: Option<String>,
    pub job_status: Option<String>,
    pub error: Option<String>,
}

#[pymethods]
impl TableCreatePlanApplyState {
    fn __repr__(&self) -> String {
        format!(
            "TableCreatePlanApplyState(job_id={:?}, status={:?})",
            self.job_id, self.job_status,
        )
    }
}

impl fmt::Debug for TableCreatePlanApplyState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableCreatePlanApplyState")
            .field("job_id", &self.job_id)
            .field("job_status", &self.job_status)
            .finish()
    }
}

#[derive(Clone, Debug)]
#[pyclass(
    name = "TableDataImportContext",
    module = "bauplan",
    skip_from_py_object,
    get_all
)]
pub(crate) struct TableDataImportContext {
    pub branch_name: String,
    pub table_name: String,
    pub namespace: String,
    pub search_string: String,
    pub import_duplicate_files: bool,
    pub best_effort: bool,
    pub continue_on_error: bool,
    pub transformation_query: Option<String>,
    pub preview: String,
}

/// The state of a completed data import job.
#[derive(Clone)]
#[pyclass(
    name = "TableDataImportState",
    module = "bauplan",
    skip_from_py_object,
    get_all
)]
pub(crate) struct TableDataImportState {
    pub job_id: Option<String>,
    pub ctx: TableDataImportContext,
    pub job_status: Option<String>,
    pub error: Option<String>,
}

#[pymethods]
impl TableDataImportState {
    fn __repr__(&self) -> String {
        format!(
            "TableDataImportState(job_id={:?}, status={:?})",
            self.job_id, self.job_status,
        )
    }
}

impl fmt::Debug for TableDataImportState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableDataImportState")
            .field("job_id", &self.job_id)
            .field("job_status", &self.job_status)
            .finish()
    }
}

#[derive(Clone, Debug)]
#[pyclass(
    name = "ExternalTableCreateContext",
    module = "bauplan",
    skip_from_py_object,
    get_all
)]
pub(crate) struct ExternalTableCreateContext {
    pub branch_name: String,
    pub table_name: String,
    pub namespace: String,
}

#[derive(Clone)]
#[pyclass(
    name = "ExternalTableCreateState",
    module = "bauplan",
    skip_from_py_object,
    get_all
)]
pub(crate) struct ExternalTableCreateState {
    pub job_id: Option<String>,
    pub ctx: ExternalTableCreateContext,
    pub job_status: Option<String>,
    pub error: Option<String>,
}

#[pymethods]
impl ExternalTableCreateState {
    fn __repr__(&self) -> String {
        format!(
            "ExternalTableCreateState(job_id={:?}, status={:?})",
            self.job_id, self.job_status,
        )
    }
}

impl fmt::Debug for ExternalTableCreateState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalTableCreateState")
            .field("job_id", &self.job_id)
            .field("job_status", &self.job_status)
            .finish()
    }
}
