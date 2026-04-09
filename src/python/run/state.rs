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
    module = "bauplan.state",
    skip_from_py_object,
    get_all
)]
pub(crate) struct RunExecutionContext {
    /// Identifier of the immutable project snapshot that the server executed.
    pub snapshot_id: String,
    /// URI locating the project snapshot that the server executed.
    pub snapshot_uri: String,
    /// Local project directory that was packaged and submitted.
    pub project_dir: String,
    /// Ref (branch or tag) the run was executed against.
    pub r#ref: String,
    /// Namespace the run materialized models into.
    pub namespace: String,
    /// Whether the run was a dry run (no models materialized).
    pub dry_run: bool,
    /// Transaction mode (`"on"` / `"off"`). When on, all models are
    /// materialized on a temporary branch and merged atomically on success.
    pub transaction: String,
    /// Strict mode (`"on"` / `"off"`). When on, runtime warnings such as
    /// failing expectations or invalid column outputs fail the run.
    pub strict: String,
    /// Cache mode used for the run (`"on"` / `"off"`).
    pub cache: String,
    /// Preview mode used for the run (`"on"`, `"off"`, `"head"`, `"tail"`).
    pub preview: String,
    /// Whether debug logging was enabled for the run.
    pub debug: bool,
    /// Whether the run was submitted in detached (background) mode.
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
#[pyclass(name = "RunState", module = "bauplan.state", skip_from_py_object, get_all)]
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

/// The parameters that were passed to a `Client.plan_table_creation` call.
#[derive(Clone, Debug)]
#[pyclass(
    name = "TableCreatePlanContext",
    module = "bauplan.state",
    skip_from_py_object,
    get_all
)]
pub(crate) struct TableCreatePlanContext {
    /// Branch the table is being created on.
    pub branch_name: String,
    /// Name of the table to create.
    pub table_name: String,
    /// Whether an existing table with the same name should be replaced.
    pub table_replace: bool,
    /// Partitioning expression (e.g. a column name or transform) applied to
    /// the new table, or `None` if the table is not partitioned.
    pub table_partitioned_by: Option<String>,
    /// Namespace the table will be created in.
    pub namespace: String,
    /// URI pattern (e.g. `s3://bucket/path/*.parquet`) used to discover the
    /// source files to plan the table schema from.
    pub search_string: String,
}

/// The result of a `Client.plan_table_creation` call.
///
/// The `plan` field contains the schema plan as a YAML string. You can modify
/// it before applying, for example to add partitioning:
///
/// ```python
/// import bauplan
/// import yaml
///
/// client = bauplan.Client()
/// plan_state = client.plan_table_creation('my_table', 's3://bucket/path/*.parquet')
/// plan = yaml.safe_load(plan_state.plan)
/// plan['schema_info']['partitions'] = [
///     {
///         'from_column_name': 'datetime_column',
///         'transform': {'name': 'year'},
///     }
/// ]
/// modified_plan = yaml.dump(plan)
/// ```
#[derive(Clone)]
#[pyclass(
    name = "TableCreatePlanState",
    module = "bauplan.state",
    from_py_object,
    get_all
)]
pub(crate) struct TableCreatePlanState {
    /// The job ID assigned by the server.
    pub job_id: Option<String>,
    /// The parameters that were used to launch the planning job.
    pub ctx: TableCreatePlanContext,
    /// The final status string (e.g. `"SUCCESS"`, `"FAILED"`).
    pub job_status: Option<String>,
    /// Error message, if the planning job failed.
    pub error: Option<String>,
    /// The generated schema plan as a YAML string. You can edit this before
    /// calling `Client.apply_table_creation_plan` (for example to add partitioning).
    pub plan: Option<String>,
    /// Whether the plan has no schema conflicts and can be applied without
    /// manual intervention. If `False`, the caller must resolve conflicts in
    /// `plan` before applying.
    pub can_auto_apply: bool,
    /// The list of source files that the plan matched and will be imported
    /// when the plan is applied.
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

/// The state of a completed `Client.apply_table_creation_plan` job, which
/// materializes a previously produced `bauplan.state.TableCreatePlanState` plan.
#[derive(Clone)]
#[pyclass(
    name = "TableCreatePlanApplyState",
    module = "bauplan.state",
    from_py_object,
    get_all
)]
pub(crate) struct TableCreatePlanApplyState {
    /// The job ID assigned by the server.
    pub job_id: Option<String>,
    /// The final status string (e.g. `"SUCCESS"`, `"FAILED"`).
    pub job_status: Option<String>,
    /// Error message, if the apply job failed.
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

/// The parameters that were passed to a data import job.
#[derive(Clone, Debug)]
#[pyclass(
    name = "TableDataImportContext",
    module = "bauplan.state",
    skip_from_py_object,
    get_all
)]
pub(crate) struct TableDataImportContext {
    /// Branch the data is being imported into.
    pub branch_name: String,
    /// Name of the destination table.
    pub table_name: String,
    /// Namespace of the destination table.
    pub namespace: String,
    /// URI pattern (e.g. `s3://bucket/path/*.parquet`) used to locate the
    /// source files to import.
    pub search_string: String,
    /// If `True`, re-import files that have already been imported. This may
    /// result in duplicate rows.
    pub import_duplicate_files: bool,
    /// If `True`, ignore source columns that do not exist on the destination
    /// table instead of failing the import.
    pub best_effort: bool,
    /// If `True`, do not fail the job when individual files fail to import.
    pub continue_on_error: bool,
    /// Optional SQL transformation applied to each file during import.
    pub transformation_query: Option<String>,
    /// Preview mode used for the import (`"on"`, `"off"`, `"head"`, `"tail"`).
    pub preview: String,
}

/// The state of a completed data import job.
#[derive(Clone)]
#[pyclass(
    name = "TableDataImportState",
    module = "bauplan.state",
    skip_from_py_object,
    get_all
)]
pub(crate) struct TableDataImportState {
    /// The job ID assigned by the server.
    pub job_id: Option<String>,
    /// The parameters that were used to launch the import job.
    pub ctx: TableDataImportContext,
    /// The final status string (e.g. `"SUCCESS"`, `"FAILED"`).
    pub job_status: Option<String>,
    /// Error message, if the import job failed.
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

/// The parameters that were passed to an external table creation job.
///
/// External tables are read-only Iceberg tables registered against data that
/// already lives in object storage; no data is copied.
#[derive(Clone, Debug)]
#[pyclass(
    name = "ExternalTableCreateContext",
    module = "bauplan.state",
    skip_from_py_object,
    get_all
)]
pub(crate) struct ExternalTableCreateContext {
    /// Branch the external table is being created on.
    pub branch_name: String,
    /// Name of the external table to create.
    pub table_name: String,
    /// Namespace of the external table.
    pub namespace: String,
}

/// The state of a completed external table creation job.
#[derive(Clone)]
#[pyclass(
    name = "ExternalTableCreateState",
    module = "bauplan.state",
    skip_from_py_object,
    get_all
)]
pub(crate) struct ExternalTableCreateState {
    /// The job ID assigned by the server.
    pub job_id: Option<String>,
    /// The parameters that were used to launch the external table creation job.
    pub ctx: ExternalTableCreateContext,
    /// The final status string (e.g. `"SUCCESS"`, `"FAILED"`).
    pub job_status: Option<String>,
    /// Error message, if the job failed.
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
