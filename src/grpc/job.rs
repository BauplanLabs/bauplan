//! Job types returned by the gRPC API.

use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bauplan_longbow::{BauplanPreset, iroh};
use chrono::{DateTime, TimeZone, Utc};
use futures::StreamExt;
use serde::Serialize;
use tokio_util::codec::{FramedRead, LinesCodec};
use tracing::{debug, error};

use crate::{
    grpc::generated::{
        self as commanderpb, RuntimeLogEvent, SubscribeLogsResponse, TaskMetadata, TaskStartEvent,
        runner_event::Event as RunnerEvent,
    },
    project,
};

/// The execution state of a job.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
#[allow(missing_docs)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "bauplan.schema",
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
        module = "bauplan.schema",
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

/// The record of running a pipeline, query, or an import (see `bauplan.schema.JobKind` for all job kinds).
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "bauplan.schema", from_py_object, get_all)
)]
pub struct Job {
    /// The unique identifier for this job.
    pub id: String,
    /// The job's current state.
    pub status: JobState,
    /// A human-readable status string (e.g. "running", "complete").
    pub human_readable_status: String,
    /// The type of job (query, run, import, etc.).
    pub kind: JobKind,
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
    /// Error message for failed jobs, when available.
    pub error_message: Option<String>,
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl Job {
    fn __repr__(&self) -> String {
        format!(
            "Job(id={:?}, kind={:?}, status={}, user={:?})",
            self.id, self.kind, self.status, self.user,
        )
    }
}

impl From<commanderpb::JobInfo> for Job {
    fn from(info: commanderpb::JobInfo) -> Self {
        Self {
            id: info.id,
            status: commanderpb::JobStateType::try_from(info.status)
                .map(JobState::from)
                .unwrap_or_default(),
            human_readable_status: info.human_readable_status,
            kind: commanderpb::JobKind::try_from(info.kind_type)
                .map(JobKind::from)
                .unwrap_or_default(),
            user: info.user,
            created_at: info.created_at.and_then(pb_to_chrono),
            started_at: info.started_at.and_then(pb_to_chrono),
            finished_at: info.finished_at.and_then(pb_to_chrono),
            runner: info.runner,
            error_message: info.error_message,
        }
    }
}

fn pb_to_chrono(ts: prost_types::Timestamp) -> Option<DateTime<Utc>> {
    Utc.timestamp_opt(ts.seconds, ts.nanos as u32).single()
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Stdio {
    Stdout,
    Stderr,
}

/// A wrapper around the event stream created by SubscribeLogs, with one
/// additional trick: in order to bridge functionality between the new python
/// runtime and the old python runtime, it supports "attaching" to those tasks
/// and synthesizing old-style RuntimeUserLog events to match.
///
/// The way that works is that it streams the events as normal, but, when it
/// encounters a TaskStartEvent with a nonempty `longbow_public_key`, it spawns
/// a task to to push log lines into the event stream in parallel.
pub(super) struct JobEventStream {
    inner: Option<tonic::Streaming<SubscribeLogsResponse>>,
    longbow_output_rx: tokio::sync::mpsc::UnboundedReceiver<((String, Stdio), String)>,
    longbow_output_tx: tokio::sync::mpsc::UnboundedSender<((String, Stdio), String)>,
    endpoint: Arc<tokio::sync::OnceCell<iroh::Endpoint>>,
    task_metadata: BTreeMap<String, TaskMetadata>,
    task_connections: tokio::task::JoinSet<()>,
}

impl JobEventStream {
    pub(super) fn new(
        inner: tonic::Streaming<SubscribeLogsResponse>,
        endpoint: Arc<tokio::sync::OnceCell<iroh::Endpoint>>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            inner: Some(inner),
            longbow_output_rx: rx,
            longbow_output_tx: tx,
            endpoint,
            task_metadata: BTreeMap::new(),
            task_connections: tokio::task::JoinSet::new(),
        }
    }
}

impl JobEventStream {
    /// Attach to the task using longbow, then push events into the
    /// longbow_output_tx channel.
    fn attach_task(&mut self, event: &TaskStartEvent) {
        let task_id = event.task_id.clone();
        if let Some(metadata) = event.task_metadata.clone() {
            self.task_metadata.insert(task_id.clone(), metadata);
        } else {
            error!(task_id, "task_metadata missing");
            return;
        }

        let Ok(public_key) = iroh::PublicKey::try_from(event.longbow_public_key.as_slice()) else {
            error!(task_id = event.task_id, "invalid longbow public key");
            return;
        };

        let tx = self.longbow_output_tx.clone();
        let endpoint = self.endpoint.clone();

        self.task_connections.spawn(async move {
            let preset = BauplanPreset::default();
            let addr = preset.add_relay_urls(iroh::EndpointAddr::new(public_key));

            let endpoint = match endpoint
                .get_or_try_init(|| iroh::Endpoint::bind(preset))
                .await
            {
                Ok(ep) => ep,
                Err(e) => {
                    error!(task_id, err = %e, "failed to attach to task");
                    return;
                }
            };

            let mut task = match bauplan_longbow::attach_task(endpoint, addr).await {
                Ok(task) => task,
                Err(e) => {
                    error!(task_id, err = %e, "failed to attach to task");
                    return;
                }
            };

            debug!(task_id, "attached to task");

            let codec = LinesCodec::new_with_max_length(1024 * 1024);
            let stdout =
                FramedRead::new(&mut task.stdout, codec.clone()).map(|line| (Stdio::Stdout, line));
            let stderr =
                FramedRead::new(&mut task.stderr, codec.clone()).map(|line| (Stdio::Stderr, line));
            let mut lines = futures::stream::select(stdout, stderr);

            while let Some((stdio, line)) = lines.next().await {
                match line {
                    Ok(line) => {
                        if tx.send(((task_id.clone(), stdio), line)).is_err() {
                            // Receiver dropped.
                            break;
                        }
                    }
                    Err(e) => {
                        error!(stream = ?stdio, err = %e, "error from task stream");
                        break;
                    }
                };
            }

            debug!(task_id, "detached from task");
        });
    }
}

impl futures::Stream for JobEventStream {
    type Item = Result<RunnerEvent, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            // Clear the JoinSet.
            if !this.task_connections.is_empty()
                && let Poll::Ready(Some(v)) = this.task_connections.poll_join_next(cx)
            {
                if let Err(e) = v {
                    error!(err = %e, "failed to read task output");
                }

                continue;
            }

            // The order here is intentional, to make sure we drain the output
            // before exiting.
            if let Poll::Ready(v) = this.longbow_output_rx.poll_recv(cx) {
                let Some(((task_id, stdio), line)) = v else {
                    continue;
                };

                let metadata = this.task_metadata.get(&task_id);
                let ev = synthetic_line_event(metadata, stdio, line);
                return Poll::Ready(Some(Ok(ev)));
            } else if let Some(inner) = this.inner.as_mut()
                && let Poll::Ready(v) = Pin::new(inner).poll_next(cx)
            {
                let Some(resp) = v else {
                    // The stream has ended. If there are still longbow
                    // connection tasks, wait for those. Otherwise, we could
                    // exit before printing all the output for those tasks.
                    this.inner.take();
                    if this.task_connections.is_empty() {
                        return Poll::Ready(None);
                    } else {
                        return Poll::Pending;
                    }
                };

                let resp = resp?;
                let Some(event) = resp.runner_event.and_then(|e| e.event) else {
                    // Empty event.
                    continue;
                };

                // If the task has output to stream via longbow, connect to it
                // and synthesize RuntimeLogEvents for the output lines.
                if let RunnerEvent::TaskStart(st) = &event
                    && !st.longbow_public_key.is_empty()
                {
                    this.attach_task(st);
                }

                return Poll::Ready(Some(Ok(event)));
            }

            if this.inner.is_none() && this.task_connections.is_empty() {
                return Poll::Ready(None);
            }

            return Poll::Pending;
        }
    }
}

/// Synthesizes a runner event from a longbow output line.
fn synthetic_line_event(metadata: Option<&TaskMetadata>, stdio: Stdio, msg: String) -> RunnerEvent {
    use commanderpb::runtime_log_event::*;

    let output_stream = match stdio {
        Stdio::Stdout => OutputStream::Stdout,
        Stdio::Stderr => OutputStream::Stderr,
    };

    RunnerEvent::RuntimeUserLog(RuntimeLogEvent {
        level: LogLevel::Debug.into(),
        output_stream: output_stream.into(),
        r#type: LogType::User.into(),
        emit_timestamp_ns: 0,
        msg,
        task_metadata: metadata.cloned(),
        job_id: String::new(),
    })
}
