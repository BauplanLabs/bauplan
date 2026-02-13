//! Jobs operations.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use pyo3::{Borrowed, exceptions::PyValueError, prelude::*};
use serde::Serialize;
use tonic::Request;

use crate::{
    PaginatedResponse,
    grpc::{
        generated as commanderpb,
        job::{Job, JobKind, JobState},
    },
    python::{exceptions::BauplanError, paginate::PyPaginator, rt},
};

use super::Client;

/// Accepts a job ID or Job object.
#[derive(Debug, Default)]
pub(crate) struct JobArg(pub String);

impl<'a, 'py> FromPyObject<'a, 'py> for JobArg {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(s) = ob.extract::<String>() {
            Ok(JobArg(s))
        } else if let Ok(job) = ob.extract::<PyRef<'_, Job>>() {
            Ok(JobArg(job.id.clone()))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "expected str or Job",
            ))
        }
    }
}

/// Accepts a single job ID, a list of job IDs, or list of Job objects.
#[derive(Debug, Default)]
pub(crate) struct JobListArg(pub Vec<String>);

impl<'a, 'py> FromPyObject<'a, 'py> for JobListArg {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(id) = ob.extract::<String>() {
            Ok(Self(vec![id]))
        } else if let Ok(ids) = ob.extract::<Vec<String>>() {
            Ok(Self(ids))
        } else if let Ok(jobs) = ob.extract::<Vec<Job>>() {
            Ok(Self(jobs.iter().map(|j| j.id.clone()).collect()))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "expected str, list[str], or list[Job]",
            ))
        }
    }
}

/// Accepts a single status string, JobState enum, or list of either.
#[derive(Debug, Default)]
pub(crate) struct JobStateListArg(pub Vec<JobState>);

impl From<JobStateListArg> for Vec<i32> {
    fn from(states: JobStateListArg) -> Self {
        states
            .0
            .into_iter()
            .map(|s| commanderpb::JobStateType::from(s) as i32)
            .collect()
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for JobStateListArg {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(state) = ob.extract::<JobState>() {
            Ok(Self(vec![state]))
        } else if let Ok(s) = ob.extract::<&str>() {
            let state: JobState = s.parse().map_err(|e: String| PyValueError::new_err(e))?;
            Ok(Self(vec![state]))
        } else if let Ok(states) = ob.extract::<Vec<JobState>>() {
            Ok(Self(states))
        } else if let Ok(strings) = ob.extract::<Vec<String>>() {
            let states: Result<Vec<_>, String> = strings.iter().map(|s| s.parse()).collect();
            Ok(Self(states.map_err(PyValueError::new_err)?))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "expected str, JobState, list[str], or list[JobState]",
            ))
        }
    }
}

/// Accepts a single kind string, JobKind enum, or list of either.
#[derive(Debug, Default)]
pub(crate) struct JobKindListArg(pub Vec<JobKind>);

impl From<JobKindListArg> for Vec<i32> {
    fn from(kinds: JobKindListArg) -> Self {
        kinds
            .0
            .into_iter()
            .map(|k| commanderpb::JobKind::from(k) as i32)
            .collect()
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for JobKindListArg {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(kind) = ob.extract::<JobKind>() {
            Ok(Self(vec![kind]))
        } else if let Ok(s) = ob.extract::<&str>() {
            let kind: JobKind = s.parse().map_err(|e: String| PyValueError::new_err(e))?;
            Ok(Self(vec![kind]))
        } else if let Ok(kinds) = ob.extract::<Vec<JobKind>>() {
            Ok(Self(kinds))
        } else if let Ok(strings) = ob.extract::<Vec<String>>() {
            let kinds: Result<Vec<_>, String> = strings.iter().map(|s| s.parse()).collect();
            Ok(Self(kinds.map_err(PyValueError::new_err)?))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "expected str, JobKind, list[str], or list[JobKind]",
            ))
        }
    }
}

/// The output stream for a log event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[pyclass(name = "JobLogStream", module = "bauplan", eq)]
pub(crate) enum JobLogStream {
    #[pyo3(name = "STDOUT")]
    Stdout,
    #[pyo3(name = "STDERR")]
    Stderr,
}

impl TryFrom<commanderpb::runtime_log_event::OutputStream> for JobLogStream {
    type Error = PyErr;

    fn try_from(value: commanderpb::runtime_log_event::OutputStream) -> Result<Self, Self::Error> {
        match value {
            commanderpb::runtime_log_event::OutputStream::Stdout => Ok(Self::Stdout),
            commanderpb::runtime_log_event::OutputStream::Stderr => Ok(Self::Stderr),
            commanderpb::runtime_log_event::OutputStream::Unspecified => {
                Err(PyValueError::new_err("invalid OutputStream: UNSPECIFIED"))
            }
        }
    }
}

impl TryFrom<i32> for JobLogStream {
    type Error = PyErr;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match commanderpb::runtime_log_event::OutputStream::try_from(value) {
            Ok(v) => v.try_into(),
            Err(_) => Err(PyValueError::new_err(format!(
                "invalid OutputStream: {value}"
            ))),
        }
    }
}

/// The log level for a log event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[pyclass(name = "JobLogLevel", module = "bauplan", eq)]
pub(crate) enum JobLogLevel {
    #[pyo3(name = "ERROR")]
    Error,
    #[pyo3(name = "WARN")]
    Warn,
    #[pyo3(name = "DEBUG")]
    Debug,
    #[pyo3(name = "INFO")]
    Info,
    #[pyo3(name = "TRACE")]
    Trace,
}

impl TryFrom<commanderpb::runtime_log_event::LogLevel> for JobLogLevel {
    type Error = PyErr;

    fn try_from(value: commanderpb::runtime_log_event::LogLevel) -> Result<Self, PyErr> {
        match value {
            commanderpb::runtime_log_event::LogLevel::Error => Ok(Self::Error),
            commanderpb::runtime_log_event::LogLevel::Warning => Ok(Self::Warn),
            commanderpb::runtime_log_event::LogLevel::Debug => Ok(Self::Debug),
            commanderpb::runtime_log_event::LogLevel::Info => Ok(Self::Info),
            commanderpb::runtime_log_event::LogLevel::Trace => Ok(Self::Trace),
            commanderpb::runtime_log_event::LogLevel::Unspecified => {
                Err(PyValueError::new_err("invalid LogLevel: UNSPECIFIED"))
            }
        }
    }
}

impl TryFrom<i32> for JobLogLevel {
    type Error = PyErr;

    fn try_from(value: i32) -> Result<Self, PyErr> {
        match commanderpb::runtime_log_event::LogLevel::try_from(value) {
            Ok(v) => Self::try_from(v),
            Err(_) => Err(PyValueError::new_err(format!("invalid LogLevel: {value}"))),
        }
    }
}

/// A log event from a job.
#[derive(Debug, Clone, Serialize)]
#[pyclass(name = "JobLogEvent", module = "bauplan", get_all)]
pub(crate) struct JobLogEvent {
    /// The output stream (STDOUT, STDERR).
    pub stream: JobLogStream,
    /// The log level (ERROR, WARN, DEBUG, INFO, TRACE).
    pub level: JobLogLevel,
    /// The log message.
    pub message: String,
}

#[pymethods]
impl JobLogEvent {
    fn __repr__(&self) -> String {
        format!(
            "JobLogEvent(level={:?}, message={:?})",
            self.level, self.message
        )
    }
}

impl TryFrom<commanderpb::RuntimeLogEvent> for JobLogEvent {
    type Error = PyErr;

    fn try_from(log: commanderpb::RuntimeLogEvent) -> Result<Self, Self::Error> {
        use commanderpb::runtime_log_event::*;

        if LogType::try_from(log.r#type) != Ok(LogType::User) {
            return Err(PyValueError::new_err("not a user log"));
        }

        Ok(JobLogEvent {
            stream: log.output_stream.try_into()?,
            level: log.level.try_into()?,
            message: log.msg,
        })
    }
}

/// A node in the job DAG (a model).
#[derive(Debug, Clone, Serialize)]
#[pyclass(module = "bauplan", get_all)]
pub(crate) struct DAGNode {
    id: String,
    name: String,
}

impl From<commanderpb::ModelNode> for DAGNode {
    fn from(value: commanderpb::ModelNode) -> Self {
        DAGNode {
            id: value.model_id,
            name: value.model_name,
        }
    }
}

/// An edge in the job DAG (a dependency).
#[derive(Debug, Clone, Serialize)]
#[pyclass(module = "bauplan", get_all)]
pub(crate) struct DAGEdge {
    source_model: Option<String>,
    destination_model: String,
}

impl From<commanderpb::ModelEdge> for DAGEdge {
    fn from(value: commanderpb::ModelEdge) -> Self {
        DAGEdge {
            source_model: value.source_id,
            destination_model: value.destination_id,
        }
    }
}

/// Context for a job, including logs, DAG, and code snapshot.
#[derive(Debug, Clone)]
#[pyclass(module = "bauplan", get_all)]
pub(crate) struct JobContext {
    pub id: String,
    pub project_id: Option<String>,
    pub project_name: Option<String>,
    pub r#ref: Option<String>,
    pub tx_ref: Option<String>,
    pub logs: Vec<JobLogEvent>,
    pub dag_nodes: Vec<DAGNode>,
    pub dag_edges: Vec<DAGEdge>,
    pub snapshot_dict: HashMap<String, String>,
}

impl TryFrom<commanderpb::JobContext> for JobContext {
    type Error = PyErr;

    fn try_from(ctx: commanderpb::JobContext) -> Result<Self, Self::Error> {
        let r#ref = ctx.r#ref.filter(|s| !s.is_empty());

        let tx_ref = ctx.transaction_branch.map(|b| b.name);

        let logs: Vec<JobLogEvent> = ctx
            .job_events
            .into_iter()
            .flat_map(|ev| {
                if let commanderpb::runner_event::Event::RuntimeUserLog(inner) = ev.event? {
                    inner.try_into().ok() // Skips non-user logs.
                } else {
                    None
                }
            })
            .collect();

        let dag_nodes: Vec<DAGNode> = ctx.models.into_iter().map(|m| m.into()).collect();
        let dag_edges: Vec<DAGEdge> = ctx.model_deps.into_iter().map(|e| e.into()).collect();

        // Decompress code snapshot if present.
        let snapshot_dict = ctx
            .code_snapshot
            .filter(|s| !s.is_empty())
            .and_then(|data| decompress_snapshot(&data))
            .unwrap_or_default();

        Ok(Self {
            id: ctx.job_id,
            project_id: ctx.project_id,
            project_name: ctx.project_name,
            r#ref,
            tx_ref,
            logs,
            dag_nodes,
            dag_edges,
            snapshot_dict,
        })
    }
}

fn decompress_snapshot(data: &[u8]) -> Option<HashMap<String, String>> {
    let cursor = std::io::Cursor::new(data);
    let mut archive = zip::ZipArchive::new(cursor).ok()?;

    let mut snapshot = HashMap::new();
    for i in 0..archive.len() {
        let mut file = archive.by_index(i).ok()?;
        if file.is_dir() {
            continue;
        }

        let mut contents = String::new();
        std::io::Read::read_to_string(&mut file, &mut contents).ok()?;
        snapshot.insert(file.name().to_owned(), contents);
    }

    Some(snapshot)
}

#[pymethods]
impl Client {
    /// EXPERIMENTAL: Get a job by ID or ID prefix.
    ///
    /// Parameters:
    ///     job_id: A job ID
    #[pyo3(signature = (job_id, /) -> "Job")]
    fn get_job(&mut self, job_id: &str) -> PyResult<Job> {
        let mut req = Request::new(commanderpb::GetJobsRequest {
            job_ids: vec![job_id.to_string()],
            ..Default::default()
        });
        req.set_timeout(self.client_timeout);

        let response = rt()
            .block_on(self.grpc.get_jobs(req))
            .map_err(|e| BauplanError::new_err(e.to_string()))?;

        let jobs = response.into_inner().jobs;
        if jobs.is_empty() {
            return Err(BauplanError::new_err(format!("job not found: {}", job_id)));
        }

        Ok(jobs.into_iter().next().unwrap().into())
    }

    /// Get jobs with optional filtering.
    ///
    /// Parameters:
    ///     all_users: Optional[bool]: Whether to list jobs from all users or only the current user.
    ///     filter_by_ids: Optional[Union[str, List[str]]]: Optional, filter by job IDs.
    ///     filter_by_users: Optional[Union[str, List[str]]]: Optional, filter by job users.
    ///     filter_by_kinds: Optional[Union[str, JobKind, List[Union[str, JobKind]]]]: Optional, filter by job kinds.
    ///     filter_by_statuses: Optional[Union[str, JobState, List[Union[str, JobState]]]]: Optional, filter by job statuses.
    ///     filter_by_created_after: Optional[datetime]: Optional, filter jobs created after this datetime.
    ///     filter_by_created_before: Optional[datetime]: Optional, filter jobs created before this datetime.
    ///     limit: Optional[int]: Optional, max number of jobs to return.
    ///
    /// Returns:
    ///     An iterator over `Job` objects.
    #[pyo3(signature = (
        *,
        all_users=None,
        filter_by_ids=None,
        filter_by_users=None,
        filter_by_kinds=None,
        filter_by_statuses=None,
        filter_by_created_after=None,
        filter_by_created_before=None,
        limit=None,
    ) -> "typing.Iterator[Job]")]
    #[allow(clippy::too_many_arguments)]
    fn get_jobs(
        &mut self,
        all_users: Option<bool>,
        filter_by_ids: Option<JobListArg>,
        filter_by_users: Option<JobListArg>,
        filter_by_kinds: Option<JobKindListArg>,
        filter_by_statuses: Option<JobStateListArg>,
        filter_by_created_after: Option<DateTime<Utc>>,
        filter_by_created_before: Option<DateTime<Utc>>,
        limit: Option<usize>,
    ) -> PyResult<PyPaginator> {
        let filter_created_after = filter_by_created_after.map(|dt| prost_types::Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32,
        });
        let filter_created_before = filter_by_created_before.map(|dt| prost_types::Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32,
        });

        let job_ids = filter_by_ids.unwrap_or_default().0;
        let all_users = all_users.unwrap_or(false);
        let filter_users = filter_by_users.unwrap_or_default().0;
        let filter_kinds: Vec<i32> = filter_by_kinds.unwrap_or_default().into();
        let filter_statuses: Vec<i32> = filter_by_statuses.unwrap_or_default().into();

        let client_timeout = self.client_timeout;
        let mut grpc = self.grpc.clone();

        PyPaginator::new(limit, move |token, page_limit| {
            let mut req = Request::new(commanderpb::GetJobsRequest {
                job_ids: job_ids.clone(),
                all_users,
                filter_users: filter_users.clone(),
                filter_kinds: filter_kinds.clone(),
                filter_statuses: filter_statuses.clone(),
                filter_created_after,
                filter_created_before,
                max_records: page_limit.unwrap_or_default() as _,
                pagination_token: token.unwrap_or("").to_string(),
                ..Default::default()
            });
            req.set_timeout(client_timeout);

            let page = rt()
                .block_on(grpc.get_jobs(req))
                .map_err(|e| BauplanError::new_err(e.to_string()))?
                .into_inner();

            let pagination_token = if page.pagination_token.is_empty() {
                None
            } else {
                Some(page.pagination_token)
            };

            Ok(PaginatedResponse {
                page: page.jobs.into_iter().map(Job::from).collect(),
                pagination_token,
            })
        })
    }

    /// EXPERIMENTAL: Get logs for a job.
    ///
    /// Parameters:
    ///     job: Union[str, Job]: A job ID, prefix of a job ID, or a Job instance.
    #[pyo3(signature = (job) -> "list[JobLogEvent]")]
    fn get_job_logs(&mut self, job: JobArg) -> PyResult<Vec<JobLogEvent>> {
        let mut req = Request::new(commanderpb::GetLogsRequest {
            job_id: job.0,
            ..Default::default()
        });
        req.set_timeout(self.client_timeout);

        let response = rt()
            .block_on(self.grpc.get_logs(req))
            .map_err(|e| BauplanError::new_err(e.to_string()))?;

        let events: Vec<JobLogEvent> = response
            .into_inner()
            .events
            .into_iter()
            .filter_map(|ev| {
                if let commanderpb::runner_event::Event::RuntimeUserLog(log) = ev.event? {
                    log.try_into().ok()
                } else {
                    None
                }
            })
            .collect();

        Ok(events)
    }

    /// EXPERIMENTAL: Get context for a job by ID.
    ///
    /// Parameters:
    ///     job: Union[str, Job]: A job ID, prefix of a job ID, a Job instance.
    ///     include_logs: bool: Whether to include logs in the response.
    ///     include_snapshot: bool: Whether to include the code snapshot in the response.
    #[pyo3(signature = (job, *, include_logs=None, include_snapshot=None) -> "JobContext")]
    fn get_job_context(
        &mut self,
        job: JobArg,
        include_logs: Option<bool>,
        include_snapshot: Option<bool>,
    ) -> PyResult<JobContext> {
        let job_id = job.0;
        let mut req = Request::new(commanderpb::GetJobContextRequest {
            job_ids: vec![job_id.clone()],
            include_logs: include_logs.unwrap_or(false),
            include_snapshot: include_snapshot.unwrap_or(false),
            ..Default::default()
        });
        req.set_timeout(self.client_timeout);

        let response = rt()
            .block_on(self.grpc.get_job_context(req))
            .map_err(|e| BauplanError::new_err(e.to_string()))?;

        let inner = response.into_inner();
        if !inner.errors.is_empty() {
            let err = &inner.errors[0];
            return Err(BauplanError::new_err(format!(
                "job context error for {}: {}",
                err.job_id, err.error_msg
            )));
        }

        let Some(m) = inner.job_contexts.into_iter().next() else {
            return Err(BauplanError::new_err(format!(
                "job context not found: {}",
                job_id
            )));
        };

        JobContext::try_from(m)
    }

    /// EXPERIMENTAL: Get context for multiple jobs.
    ///
    /// Parameters:
    ///     jobs: list[Union[str, Job]]: A list of job IDs or Job instances.
    ///     include_logs: bool: Whether to include logs in the response.
    ///     include_snapshot: bool: Whether to include the code snapshot in the response.
    #[pyo3(signature = (jobs, *, include_logs=None, include_snapshot=None) -> "list[JobContext]")]
    fn get_job_contexts(
        &mut self,
        jobs: JobListArg,
        include_logs: Option<bool>,
        include_snapshot: Option<bool>,
    ) -> PyResult<Vec<JobContext>> {
        let mut req = Request::new(commanderpb::GetJobContextRequest {
            job_ids: jobs.0,
            include_logs: include_logs.unwrap_or(false),
            include_snapshot: include_snapshot.unwrap_or(false),
            ..Default::default()
        });
        req.set_timeout(self.client_timeout);

        let resp = rt()
            .block_on(self.grpc.get_job_context(req))
            .map_err(|e| BauplanError::new_err(e.to_string()))?
            .into_inner();

        // Report errors but still return successful contexts.
        for err in &resp.errors {
            eprintln!(
                "Warning: job context error for {}: {}",
                err.job_id, err.error_msg
            );
        }

        let ctxs = resp
            .job_contexts
            .into_iter()
            .map(JobContext::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ctxs)
    }

    /// EXPERIMENTAL: Cancel a job by ID.
    ///
    /// Parameters:
    ///     id: A job ID
    #[pyo3(signature = (id, /) -> "None")]
    fn cancel_job(&mut self, id: &str) -> PyResult<()> {
        let req = commanderpb::CancelJobRequest {
            job_id: Some(commanderpb::JobId {
                id: id.to_owned(),
                ..Default::default()
            }),
        };
        rt().block_on(self.grpc.cancel(req))
            .map_err(|e| BauplanError::new_err(e.to_string()))?;

        Ok(())
    }
}
