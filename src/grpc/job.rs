//! Job types returned by the gRPC API.

use chrono::{DateTime, TimeZone, Utc};
use serde::Serialize;

use crate::{grpc::generated as commanderpb, project};

/// The state of a job.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
#[allow(missing_docs)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "bauplan",
        rename_all = "SCREAMING_SNAKE_CASE",
        from_py_object,
        eq,
        str
    )
)]
pub enum JobState {
    #[default]
    Unspecified,
    NotStarted,
    Running,
    Complete,
    Abort,
    Fail,
    Other,
}

impl std::fmt::Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobState::Unspecified => write!(f, "Unspecified"),
            JobState::NotStarted => write!(f, "Not Started"),
            JobState::Running => write!(f, "Running"),
            JobState::Complete => write!(f, "Complete"),
            JobState::Abort => write!(f, "Abort"),
            JobState::Fail => write!(f, "Fail"),
            JobState::Other => write!(f, "Other"),
        }
    }
}

#[cfg(feature = "python")]
impl std::str::FromStr for JobState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Accept PascalCase, lowercase, kebab-case, and SCREAMING_SNAKE_CASE.
        match s.to_ascii_lowercase().replace('_', "-").as_str() {
            "unspecified" => Ok(Self::Unspecified),
            "not-started" | "not started" => Ok(Self::NotStarted),
            "running" => Ok(Self::Running),
            "complete" => Ok(Self::Complete),
            "abort" => Ok(Self::Abort),
            "fail" => Ok(Self::Fail),
            "other" => Ok(Self::Other),
            _ => Err(format!("invalid job state: {s}")),
        }
    }
}

impl From<commanderpb::JobStateType> for JobState {
    fn from(s: commanderpb::JobStateType) -> Self {
        match s {
            commanderpb::JobStateType::Unspecified => Self::Unspecified,
            commanderpb::JobStateType::NotStarted => Self::NotStarted,
            commanderpb::JobStateType::Running => Self::Running,
            commanderpb::JobStateType::Complete => Self::Complete,
            commanderpb::JobStateType::Abort => Self::Abort,
            commanderpb::JobStateType::Fail => Self::Fail,
            commanderpb::JobStateType::Other => Self::Other,
        }
    }
}

impl From<JobState> for commanderpb::JobStateType {
    fn from(s: JobState) -> Self {
        match s {
            JobState::Unspecified => Self::Unspecified,
            JobState::NotStarted => Self::NotStarted,
            JobState::Running => Self::Running,
            JobState::Complete => Self::Complete,
            JobState::Abort => Self::Abort,
            JobState::Fail => Self::Fail,
            JobState::Other => Self::Other,
        }
    }
}

/// The kind/type of a job.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
#[allow(missing_docs)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "bauplan",
        rename_all = "SCREAMING_SNAKE_CASE",
        from_py_object,
        eq,
        str
    )
)]
pub enum JobKind {
    #[default]
    Unspecified,
    Run,
    Query,
    ImportPlanCreate,
    ImportPlanApply,
    TablePlanCreate,
    TablePlanCreateApply,
    TableImport,
}

impl std::fmt::Display for JobKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobKind::Unspecified => write!(f, "Unknown"),
            JobKind::Run => write!(f, "Run"),
            JobKind::Query => write!(f, "Query"),
            JobKind::ImportPlanCreate => write!(f, "ImportPlanCreate"),
            JobKind::ImportPlanApply => write!(f, "ImportPlanApply"),
            JobKind::TablePlanCreate => write!(f, "TablePlanCreate"),
            JobKind::TablePlanCreateApply => write!(f, "TablePlanCreateApply"),
            JobKind::TableImport => write!(f, "TableImport"),
        }
    }
}

impl From<commanderpb::JobKind> for JobKind {
    fn from(k: commanderpb::JobKind) -> Self {
        match k {
            commanderpb::JobKind::Unspecified => Self::Unspecified,
            commanderpb::JobKind::CodeSnapshotRun => Self::Run,
            commanderpb::JobKind::QueryRun => Self::Query,
            commanderpb::JobKind::ImportPlanCreate => Self::ImportPlanCreate,
            commanderpb::JobKind::ImportPlanApply => Self::ImportPlanApply,
            commanderpb::JobKind::TablePlanCreate => Self::TablePlanCreate,
            commanderpb::JobKind::TablePlanCreateApply => Self::TablePlanCreateApply,
            commanderpb::JobKind::TableDataImport => Self::TableImport,
        }
    }
}

impl From<JobKind> for commanderpb::JobKind {
    fn from(k: JobKind) -> Self {
        match k {
            JobKind::Unspecified => Self::Unspecified,
            JobKind::Run => Self::CodeSnapshotRun,
            JobKind::Query => Self::QueryRun,
            JobKind::ImportPlanCreate => Self::ImportPlanCreate,
            JobKind::ImportPlanApply => Self::ImportPlanApply,
            JobKind::TablePlanCreate => Self::TablePlanCreate,
            JobKind::TablePlanCreateApply => Self::TablePlanCreateApply,
            JobKind::TableImport => Self::TableDataImport,
        }
    }
}

#[cfg(feature = "python")]
impl std::str::FromStr for JobKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Accept PascalCase, lowercase, kebab-case, and SCREAMING_SNAKE_CASE.
        match s.to_ascii_lowercase().replace('_', "-").as_str() {
            "unknown" | "unspecified" => Ok(Self::Unspecified),
            "run" | "codesnapshotrun" | "code-snapshot-run" => Ok(Self::Run),
            "query" => Ok(Self::Query),
            "import-plan-create" | "importplancreate" => Ok(Self::ImportPlanCreate),
            "import-plan-apply" | "importplanapply" => Ok(Self::ImportPlanApply),
            "table-plan-create" | "tableplancreate" => Ok(Self::TablePlanCreate),
            "table-plan-create-apply" | "tableplancreateapply" => Ok(Self::TablePlanCreateApply),
            "table-import" | "tableimport" | "table-data-import" => Ok(Self::TableImport),
            _ => Err(format!("invalid job kind: {s}")),
        }
    }
}

/// A bauplan job, representing a unit of work such as a query, run, or import.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "bauplan", from_py_object, get_all)
)]
pub struct Job {
    /// The unique identifier for this job.
    pub id: String,
    /// A human-readable status string (e.g. "running", "complete").
    pub status: String,
    /// A human-readable job kind string.
    pub kind: String,
    /// The job's current state as an enum.
    pub status_type: JobState,
    /// The type of job (query, run, import, etc.) as an enum.
    pub kind_type: JobKind,
    /// The user who submitted the job.
    pub user: String,
    /// When the job was created.
    pub created_at: Option<DateTime<Utc>>,
    /// When the job started executing.
    pub started_at: Option<DateTime<Utc>>,
    /// When the job finished (successfully or not).
    pub finished_at: Option<DateTime<Utc>>,
    /// The runner instance assigned to execute this job.
    pub runner: String,
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl Job {
    fn __repr__(&self) -> String {
        format!(
            "Job(id={:?}, kind={:?}, status={:?}, user={:?})",
            self.id, self.kind, self.status, self.user,
        )
    }
}

impl From<commanderpb::JobInfo> for Job {
    fn from(info: commanderpb::JobInfo) -> Self {
        Self {
            id: info.id,
            status: info.human_readable_status,
            kind: info.kind,
            status_type: commanderpb::JobStateType::try_from(info.status)
                .map(JobState::from)
                .unwrap_or_default(),
            kind_type: commanderpb::JobKind::try_from(info.kind_type)
                .map(JobKind::from)
                .unwrap_or_default(),
            user: info.user,
            created_at: info.created_at.and_then(pb_to_chrono),
            started_at: info.started_at.and_then(pb_to_chrono),
            finished_at: info.finished_at.and_then(pb_to_chrono),
            runner: info.runner,
        }
    }
}

impl From<project::ParameterValue> for commanderpb::parameter::Value {
    fn from(param: project::ParameterValue) -> Self {
        match param {
            project::ParameterValue::Str(value) => {
                Self::StrValue(commanderpb::StrParameterValue { value: Some(value) })
            }
            project::ParameterValue::Int(value) => {
                Self::IntValue(commanderpb::IntParameterValue { value: Some(value) })
            }
            project::ParameterValue::Float(value) => {
                Self::FloatValue(commanderpb::FloatParameterValue { value: Some(value) })
            }
            project::ParameterValue::Bool(value) => {
                Self::BoolValue(commanderpb::BoolParameterValue { value: Some(value) })
            }
            project::ParameterValue::Secret {
                key,
                encrypted_value: value,
            } => Self::SecretValue(commanderpb::SecretParameterValue {
                key: Some(key),
                value: Some(value),
            }),
            project::ParameterValue::Vault(value) => {
                Self::VaultValue(commanderpb::VaultParameterValue { value: Some(value) })
            }
        }
    }
}

fn pb_to_chrono(ts: prost_types::Timestamp) -> Option<DateTime<Utc>> {
    Utc.timestamp_opt(ts.seconds, ts.nanos as u32).single()
}
