//! Job types returned by the gRPC API.

use chrono::{DateTime, TimeZone, Utc};
use serde::Serialize;

use crate::grpc::generated as commanderpb;

/// The state of a job.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "bauplan", eq, str, rename_all = "SCREAMING_SNAKE_CASE")
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
        match s {
            "Unspecified" => Ok(Self::Unspecified),
            "Not Started" => Ok(Self::NotStarted),
            "Running" => Ok(Self::Running),
            "Complete" => Ok(Self::Complete),
            "Abort" => Ok(Self::Abort),
            "Fail" => Ok(Self::Fail),
            "Other" => Ok(Self::Other),
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
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "bauplan", eq, str, rename_all = "SCREAMING_SNAKE_CASE")
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
        match s {
            "Unknown" => Ok(Self::Unspecified),
            "Run" | "CodeSnapshotRun" => Ok(Self::Run),
            "Query" => Ok(Self::Query),
            "ImportPlanCreate" => Ok(Self::ImportPlanCreate),
            "ImportPlanApply" => Ok(Self::ImportPlanApply),
            "TablePlanCreate" => Ok(Self::TablePlanCreate),
            "TablePlanCreateApply" => Ok(Self::TablePlanCreateApply),
            "TableImport" => Ok(Self::TableImport),
            _ => Err(format!("invalid job kind: {s}")),
        }
    }
}

/// A bauplan job, representing a unit of work such as a query, run, or import.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "python", pyo3::pyclass(module = "bauplan", get_all))]
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

fn pb_to_chrono(ts: prost_types::Timestamp) -> Option<DateTime<Utc>> {
    Utc.timestamp_opt(ts.seconds, ts.nanos as u32).single()
}
