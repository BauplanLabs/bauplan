//! Run operations.

#![allow(unused_imports)]

pub(crate) mod state;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time;

use anyhow::bail;
use chrono::{TimeZone, Utc};
use commanderpb::runner_event::Event as RunnerEvent;
use futures::TryStreamExt;
use tracing::{error, info, trace};

use super::Client;
use super::refs::RefArg;
use crate::grpc::{self, generated as commanderpb};
use crate::project::{ParameterType, ParameterValue, ProjectFile};
use crate::python::job::JobLogEvent;
use crate::python::namespace::NamespaceArg;
use crate::python::{job_err, optional_on_off, rt};
use gethostname::gethostname;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use rsa::RsaPublicKey;

use self::state::{RunExecutionContext, RunState};

pub(crate) fn job_status_strings(result: Result<(), grpc::JobError>) -> (String, Option<String>) {
    match result {
        Ok(()) => ("SUCCESS".to_owned(), None),
        Err(e) => (e.status_str().to_owned(), Some(e.to_string())),
    }
}

impl Client {
    pub(crate) fn job_timeout(&self, client_timeout: Option<u64>) -> time::Duration {
        if let Some(v) = client_timeout
            && v > 0
        {
            time::Duration::from_secs(v)
        } else {
            self.client_timeout
        }
    }

    pub(crate) fn job_request_common(
        &self,
        priority: Option<u32>,
        args: HashMap<String, String>,
    ) -> PyResult<commanderpb::JobRequestCommon> {
        if let Some(p) = priority
            && !(1..=10).contains(&p)
        {
            return Err(PyValueError::new_err("priority must be between 1 and 10"));
        }

        let hostname = gethostname().to_string_lossy().into_owned();
        Ok(commanderpb::JobRequestCommon {
            module_version: Default::default(),
            hostname,
            args,
            debug: 0,
            priority: priority.map(|p| p as _),
        })
    }

    pub(crate) async fn monitor_job(
        &mut self,
        job_id: &str,
        timeout: time::Duration,
        mut on_event: impl FnMut(RunnerEvent),
    ) -> PyResult<Result<(), grpc::JobError>> {
        info!(job_id, "running job");

        let mut client = self.grpc.clone();
        let stream = client.monitor_job(job_id.to_owned(), timeout);
        futures::pin_mut!(stream);

        loop {
            let event = match stream.try_next().await {
                Ok(Some(ev)) => ev,
                Ok(None) => {
                    return Ok(Err(grpc::JobError::Failed(
                        Default::default(),
                        "stream ended without completion".to_owned(),
                    )));
                }
                Err(e)
                    if e.code() == tonic::Code::Cancelled
                        || e.code() == tonic::Code::DeadlineExceeded =>
                {
                    error!(job_id, "timeout reached, cancelling job");
                    if let Err(e) = self.grpc.cancel(job_id).await {
                        return Err(job_err(format!("failed to cancel job: {e}")));
                    }
                    return Err(job_err("client timed out"));
                }
                Err(e) => return Err(job_err(e)),
            };

            trace!(job_id, ?event, "received runner event");

            if let RunnerEvent::JobCompletion(ev) = event {
                return Ok(grpc::interpret_outcome(ev.outcome).map(|_| ()));
            }

            on_event(event);
        }
    }

    pub(crate) async fn monitor_run(
        &mut self,
        timeout: time::Duration,
        state: &mut RunState,
    ) -> PyResult<()> {
        let job_id = state.job_id.clone().unwrap_or_default();

        let status = self
            .monitor_job(&job_id, timeout, |event| match event {
                RunnerEvent::TaskStart(ev) => {
                    if let Some(ts) = ev.timestamp
                        && let Some(dt) = Utc.timestamp_opt(ts.seconds, ts.nanos as u32).single()
                    {
                        state.tasks_started.insert(ev.task_id, dt);
                    }
                }
                RunnerEvent::TaskCompletion(ev) => {
                    if let Some(ts) = ev.timestamp
                        && let Some(dt) = Utc.timestamp_opt(ts.seconds, ts.nanos as u32).single()
                    {
                        state.tasks_stopped.insert(ev.task_id, dt);
                    }
                }
                RunnerEvent::RuntimeUserLog(ev)
                    if ev.r#type() == commanderpb::runtime_log_event::LogType::User =>
                {
                    if let Ok(log) = JobLogEvent::try_from(ev) {
                        state.user_logs.push(log);
                    }
                }
                _ => (),
            })
            .await?;

        state.ended_at_ns = Some(Utc::now().timestamp_nanos_opt().unwrap());
        let (job_status, error) = job_status_strings(status);
        state.job_status = Some(job_status);
        state.error = error;

        Ok(())
    }
}

#[derive(FromPyObject)]
enum RawParameterValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
}

impl From<RawParameterValue> for ParameterValue {
    fn from(value: RawParameterValue) -> Self {
        match value {
            RawParameterValue::Bool(b) => ParameterValue::Bool(b),
            RawParameterValue::Int(i) => ParameterValue::Int(i),
            RawParameterValue::Float(f) => ParameterValue::Float(f),
            RawParameterValue::Str(s) => ParameterValue::Str(s),
        }
    }
}

impl RawParameterValue {
    fn type_str(&self) -> &'static str {
        match self {
            RawParameterValue::Bool(_) => "bool",
            RawParameterValue::Int(_) => "int",
            RawParameterValue::Float(_) => "float",
            RawParameterValue::Str(_) => "str",
        }
    }
}

#[pymethods]
impl Client {
    /// Run a Bauplan project and return the state of the run. This is the equivalent of
    /// running through the CLI the `bauplan run` command. All parameters default to 'off'/false unless otherwise specified.
    ///
    /// ## Examples
    ///
    /// ```python notest
    /// # Run a daily sales pipeline on a dev branch, and if successful and data is good, merge to main
    /// run_state = client.run(
    ///     project_dir='./etl_pipelines/daily_sales',
    ///     ref="username.dev_branch",
    ///     namespace='sales_analytics',
    /// )
    ///
    /// if str(run_state.job_status).lower() != "success":
    ///     raise Exception(f"{run_state.job_id} failed: {run_state.job_status}")
    /// ```
    ///
    /// Parameters:
    ///     project_dir: The directory of the project (where the `bauplan_project.yml` or `bauplan_project.yaml` file is located).
    ///     ref: The ref, branch name or tag name from which to run the project.
    ///     namespace: The Namespace to run the job in. If not set, the job will be run in the default namespace.
    ///     parameters: Parameters for templating into SQL or Python models.
    ///     cache: Whether to enable or disable caching for the run. Defaults to 'on'.
    ///     transaction: Whether to enable or disable transaction mode for the run. Defaults to 'on'.
    ///     dry_run: Whether to enable or disable dry-run mode for the run; models are not materialized.
    ///     strict: Whether to enable or disable strict schema validation.
    ///     preview: Whether to enable or disable preview mode for the run.
    ///     debug: Whether to enable or disable debug mode for the run.
    ///     args: Additional arguments (optional).
    ///     priority: Optional job priority (1-10, where 10 is highest priority).
    ///     verbose: Whether to enable or disable verbose mode for the run.
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution.
    ///     detach: Whether to detach the run and return immediately instead of blocking on log streaming.
    /// Returns:
    ///     `bauplan.state.RunState`: The state of the run.
    #[pyo3(signature = (
        project_dir: "str",
        r#ref: "str | Ref | None" = None,
        namespace: "str | Namespace | None" = None,
        parameters: "Dict[str, Optional[Union[str, int, float, bool]]]] | None" = None,
        cache: "str | None" = None,
        transaction: "str | None" = None,
        dry_run: "bool | None" = None,
        strict: "str | None" = None,
        preview: "str | None" = None,
        args: "dict[str, str] | None" = None,
        priority: "int | None" = None,
        client_timeout: "int | None" = None,
        detach: "bool | None" = None,
    ) -> "RunState")]
    #[allow(clippy::too_many_arguments)]
    fn run(
        &mut self,
        project_dir: PathBuf,
        r#ref: Option<RefArg>,
        namespace: Option<NamespaceArg>,
        parameters: Option<HashMap<String, Option<RawParameterValue>>>,
        cache: Option<&str>,
        transaction: Option<&str>,
        dry_run: Option<bool>,
        strict: Option<&str>,
        preview: Option<&str>,
        args: Option<HashMap<String, String>>,
        priority: Option<u32>,
        client_timeout: Option<u64>,
        detach: Option<bool>,
    ) -> PyResult<RunState> {
        let timeout = self.job_timeout(client_timeout);
        let common = self.job_request_common(priority, args.unwrap_or_default())?;
        let cache = optional_on_off("cache", cache)?;
        let transaction = optional_on_off("transaction", transaction)?;
        let strict = optional_on_off("strict", strict)?;
        let detach = detach.unwrap_or(false);

        let dry_run = match dry_run {
            Some(true) => commanderpb::JobRequestOptionalBool::True,
            Some(false) => commanderpb::JobRequestOptionalBool::False,
            None => commanderpb::JobRequestOptionalBool::Unspecified,
        };

        let project_dir = Path::new(&project_dir);
        let project = ProjectFile::from_dir(project_dir).map_err(job_err)?;
        let zip_file = project.create_code_snapshot().map_err(job_err)?;

        let parameters = rt().block_on(resolve_job_parameters(
            &mut self.grpc,
            self.client_timeout,
            &project,
            parameters.unwrap_or_default(),
        ))?;

        let req = commanderpb::CodeSnapshotRunRequest {
            job_request_common: Some(common),
            zip_file,
            r#ref: r#ref.map(|a| a.0),
            namespace: namespace.map(|a| a.0),
            dry_run: dry_run.into(),
            transaction: transaction.unwrap_or_default().to_owned(),
            strict: strict.unwrap_or_default().to_owned(),
            cache: cache.unwrap_or_default().to_owned(),
            preview: preview.unwrap_or_default().to_owned(),
            project_id: project.project.id.as_hyphenated().to_string(),
            project_name: project.project.name.clone().unwrap_or_default(),
            parameters,
            ..Default::default()
        };

        let state = rt().block_on(async {
            let resp = self
                .grpc
                .code_snapshot_run(req)
                .await
                .map_err(job_err)?
                .into_inner();

            let Some(commanderpb::JobResponseCommon { job_id, .. }) = resp.job_response_common
            else {
                return Err(job_err("response missing job ID"));
            };

            let ctx = RunExecutionContext {
                snapshot_id: resp.snapshot_id,
                snapshot_uri: resp.snapshot_uri,
                project_dir: project_dir.display().to_string(),
                r#ref: resp.r#ref,
                namespace: resp.namespace,
                dry_run: resp.dry_run,
                transaction: resp.transaction,
                strict: resp.strict,
                cache: resp.cache,
                preview: resp.preview,
                debug: false,
                detach,
            };

            let mut state = RunState {
                job_id: Some(job_id),
                ctx,
                user_logs: Vec::new(),
                tasks_started: HashMap::new(),
                tasks_stopped: HashMap::new(),
                job_status: None,
                started_at_ns: Utc::now().timestamp_nanos_opt().unwrap_or(0),
                ended_at_ns: None,
                error: None,
            };

            if detach {
                return Ok(state);
            }

            // Run the job until we get a completion. A job error is not an
            // Err here.
            match self.monitor_run(timeout, &mut state).await {
                Ok(()) => Ok(state),
                Err(e) => Err(e),
            }
        })?;

        Ok(state)
    }
}

async fn resolve_job_parameters(
    grpc: &mut grpc::Client,
    timeout: time::Duration,
    project: &ProjectFile,
    mut parameters: HashMap<String, Option<RawParameterValue>>,
) -> PyResult<Vec<commanderpb::Parameter>> {
    for name in parameters.keys() {
        if !project.parameters.contains_key(name) {
            return Err(PyValueError::new_err(format!(
                "unknown parameter: {:?}",
                name
            )));
        }
    }

    // If any of the params are a secret, we need to fetch the org-wide public
    // key from commander. This is used to cache the result, in case multiple
    // parameters are secrets.
    let mut key_cache: Option<(String, RsaPublicKey)> = None;

    let mut resolved = Vec::with_capacity(project.parameters.len());
    for (name, param) in &project.parameters {
        if let Some(Some(raw_value)) = parameters.remove(name) {
            let parsed = if param.param_type == ParameterType::Secret {
                let RawParameterValue::Str(value) = raw_value else {
                    return Err(PyValueError::new_err(format!(
                        "Expected string value for parameter '{}', got {:?}",
                        name,
                        raw_value.type_str()
                    )));
                };

                let (key_name, key) = if let Some((key_name, key)) = &key_cache {
                    (key_name.clone(), key)
                } else {
                    let (key_name, key) = grpc
                        .org_default_public_key(timeout)
                        .await
                        .map_err(job_err)?;
                    let (_, key) = key_cache.insert((key_name.clone(), key));

                    (key_name, &*key)
                };

                ParameterValue::encrypt_secret(key_name, key, project.project.id, value)
                    .map_err(job_err)?
            } else {
                raw_value.into()
            };

            resolved.push(commanderpb::Parameter {
                name: name.clone(),
                value: Some(parsed.into()),
            });
        } else if let Some(default_value) = param.eval_default().map_err(job_err)? {
            resolved.push(commanderpb::Parameter {
                name: name.clone(),
                value: Some(default_value.into()),
            });
        } else if param.required {
            return Err(PyValueError::new_err(format!(
                "missing required parameter: {name:?}"
            )));
        }
    }

    Ok(resolved)
}
