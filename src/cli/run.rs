use std::{
    cell::RefCell,
    collections::BTreeMap,
    fmt::Display,
    io::{Write as _, stderr, stdout},
    path::PathBuf,
    time,
};

use anyhow::{anyhow, bail};
use bauplan::{
    grpc::{self, generated as commanderpb},
    project::{ParameterType, ParameterValue, ProjectFile},
};
use chrono::Utc;
use futures::TryStreamExt as _;
use gethostname::gethostname;
use indicatif::{ProgressBar, ProgressDrawTarget};
use rsa::RsaPublicKey;
use serde::Serialize;
use tabwriter::TabWriter;
use tracing::{debug, error, info};
use yansi::Paint;

use crate::cli::{
    Cli, KeyValue, OnOff, Priority,
    parameter::{parse_parameter, resolve_project_dir},
};
use commanderpb::runner_event::Event as RunnerEvent;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Preview {
    On,
    #[default]
    Off,
    Head,
    Tail,
}

impl Display for Preview {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Preview::On => write!(f, "on"),
            Preview::Off => write!(f, "off"),
            Preview::Head => write!(f, "head"),
            Preview::Tail => write!(f, "tail"),
        }
    }
}

#[derive(Debug, clap::Args)]
pub(crate) struct RunArgs {
    /// Path to the root Bauplan project directory.
    #[arg(short, long)]
    pub project_dir: Option<PathBuf>,
    /// Set the cache mode.
    #[arg(long)]
    pub cache: Option<OnOff>,
    /// Set the preview mode.
    #[arg(long)]
    pub preview: Option<Preview>,
    /// Exit upon encountering runtime warnings (e.g., invalid column output)
    #[arg(long)]
    pub strict: Option<OnOff>,
    /// Set a parameter for the job. Format: key=value. Can be used multiple times.
    #[arg(long, action = clap::ArgAction::Append)]
    pub param: Vec<KeyValue>,
    /// Namespace to run the job in. If not set, the job will be run in the default namespace for the project.
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// Ref or branch name from which to run the job.
    #[arg(short, long)]
    pub r#ref: Option<String>,
    /// Run the dag as a transaction. Will create a temporary branch where models are materialized. Once all models succeed, it will be merged to branch in which this run is happenning in
    #[arg(short, long)]
    pub transaction: Option<OnOff>,
    /// Dry run the job without materializing any models.
    #[arg(long)]
    pub dry_run: bool,
    /// Run the job in the background instead of streaming logs
    #[arg(short, long)]
    pub detach: bool,
    /// Set the job priority (1-10, where 10 is highest priority)
    #[arg(long)]
    pub priority: Option<Priority>,
    /// Arguments to pass to the job. Format: key=value
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub arg: Vec<KeyValue>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum SummaryOutcome {
    Success,
    Failed,
    Timeout,
    Cancelled,
    Skipped,
}

#[derive(Debug, Serialize)]
struct TaskSummary {
    task_id: String,
    description: String,
    name: String,
    file_name: Option<String>,
    line_number: Option<u32>,
    started: chrono::DateTime<Utc>,
    ended: chrono::DateTime<Utc>,
    outcome: SummaryOutcome,
}

#[derive(Debug, Serialize)]
struct Summary {
    job_id: String,
    outcome: SummaryOutcome,
    started: chrono::DateTime<Utc>,
    ended: chrono::DateTime<Utc>,
    tasks: Vec<TaskSummary>,
}

pub(crate) fn handle(cli: &Cli, args: RunArgs) -> anyhow::Result<()> {
    crate::cli::with_rt(handle_run(cli, args))
}

/// Common handler for running a job and managing spinners for it. This handles
/// the following common behavior:
///  - Cancelling a job on a cancel signal or a request timeout
///  - Monitoring job logs until a JobCompletion event is recieved.
///
/// `thing` influences the format of the spinner message ("Running {thing}...").
///
/// The provided closure is called on every event except the final JobCompletion.
pub(crate) async fn monitor_job_progress(
    client: &mut grpc::Client,
    job_id: String,
    thing: &'static str,
    progress: ProgressBar,
    mut cancel_signal: impl Future + Unpin,
    timeout: time::Duration,
    mut handler: impl FnMut(RunnerEvent),
) -> anyhow::Result<commanderpb::JobSuccess> {
    let mut client_clone = client.clone();
    let mut kill_job = async |reason: &str| -> anyhow::Result<commanderpb::JobSuccess> {
        error!(job_id, "{reason}, cancelling {thing}");

        progress.set_message(format!("Cancelling {thing}..."));
        progress.enable_steady_tick(time::Duration::from_millis(100));

        if let Err(e) = client_clone.cancel(&job_id).await {
            error!(job_id, error = %e, "failed to cancel {thing}");
            progress.finish_with_message(format!("Cancelling {thing}... {}", "failed".red()));
        } else {
            debug!(job_id, "job successfully cancelled");
            progress.finish_with_message(format!("Cancelling {thing}... {}", "done".green()));
        }

        Err(grpc::JobError::Cancelled.into())
    };

    // We have to manually tick the progress bar here, or we get ghosting.
    let mut ticker = tokio::time::interval(time::Duration::from_millis(100));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let stream = client.monitor_job(job_id.clone(), timeout);
    futures::pin_mut!(stream);

    loop {
        let res = tokio::select! {
            v = stream.try_next() => v,
            _ = ticker.tick() => {
                progress.tick();
                continue;
            }
            _ = &mut cancel_signal => return kill_job("interrupt received").await,
        };

        let event = match res {
            Ok(Some(v)) => v,
            Ok(None) => bail!("no JobCompletion event found"),
            Err(ref e)
                if e.code() == tonic::Code::Cancelled
                    || e.code() == tonic::Code::DeadlineExceeded =>
            {
                return kill_job("execution timed out").await;
            }
            Err(e) => return Err(e.into()),
        };

        match event {
            RunnerEvent::RuntimeUserLog(commanderpb::RuntimeLogEvent {
                level,
                output_stream,
                r#type,
                ref msg,
                ref job_id,
                ..
            }) => {
                debug!(
                    job_id,
                    ?level,
                    ?output_stream,
                    ?r#type,
                    msg,
                    "runtime log event"
                );

                handler(event);
            }
            RunnerEvent::JobCompletion(ev) => return Ok(grpc::interpret_outcome(ev.outcome)?),
            _ => handler(event),
        }
    }
}

async fn handle_run(cli: &Cli, args: RunArgs) -> anyhow::Result<()> {
    let RunArgs {
        arg,
        project_dir,
        cache,
        preview,
        strict,
        param,
        namespace,
        r#ref,
        dry_run,
        transaction,
        detach,
        priority,
    } = args;

    let start = Utc::now();
    let timeout = cli.timeout.unwrap_or(time::Duration::from_secs(1800));
    let mut client = grpc::Client::new_lazy(&cli.profile, timeout)?;

    let project_dir = resolve_project_dir(project_dir.as_deref())?;
    let project = ProjectFile::from_dir(&project_dir)?;

    let parameters = resolve_parameters(cli, &project, param).await?;
    let zip_file = project.create_code_snapshot()?;

    let hostname = gethostname().to_string_lossy().into_owned();
    let args = arg.into_iter().map(KeyValue::into_strings).collect();

    let dry_run = if dry_run {
        commanderpb::JobRequestOptionalBool::True as _
    } else {
        commanderpb::JobRequestOptionalBool::False as _
    };

    let req = commanderpb::CodeSnapshotRunRequest {
        job_request_common: Some(commanderpb::JobRequestCommon {
            module_version: Default::default(),
            hostname,
            args,
            debug: 0,
            priority: priority.map(|p| p.0 as _),
        }),
        zip_file,
        r#ref,
        namespace,
        dry_run,
        transaction: transaction.unwrap_or(OnOff::On).to_string(),
        strict: strict.unwrap_or(OnOff::Off).to_string(),
        cache: cache.unwrap_or(OnOff::On).to_string(),
        preview: preview.unwrap_or_default().to_string(),
        project_id: project.project.id.as_hyphenated().to_string(),
        project_name: project.project.name.clone().unwrap_or_default(),
        parameters,
        ..Default::default()
    };

    let progress = cli.new_spinner().with_message("Planning job...");

    let resp = match client.code_snapshot_run(req).await {
        Ok(resp) => resp.into_inner(),
        Err(e) => {
            progress.finish_and_clear();
            return Err(anyhow!("{}", e.message()));
        }
    };

    let job_id = resp.job_response_common.as_ref().map(|c| &c.job_id);
    let Some(job_id) = job_id.cloned() else {
        bail!("response missing job ID");
    };

    debug!(job_id, "successfully planned job");

    if !resp.dag_ascii.is_empty() {
        cli.multiprogress.suspend(|| print_dag(resp.dag_ascii))?
    }

    if detach {
        progress.finish_and_clear();
        eprintln!("\nJob {job_id} is now running in detached mode.\n");
        eprintln!("Tip: use \"bauplan job <command>\" to list and inspect running jobs.");
        return Ok(());
    }

    progress.set_message("Executing job...");

    let ctrl_c = tokio::signal::ctrl_c();
    futures::pin_mut!(ctrl_c);

    // One spinner for each task.
    let spinners: RefCell<BTreeMap<String, ProgressBar>> = RefCell::new(BTreeMap::new());

    info!("view this job in the app: https://app.bauplanlabs.com/jobs/{job_id}");
    let show_previews = resp.preview != "off";

    // All events, collated for json output.
    let mut summary = Summary {
        job_id: job_id.clone(),
        outcome: SummaryOutcome::Success,
        started: start,
        ended: start,
        tasks: Vec::new(),
    };

    let outcome = monitor_job_progress(
        &mut client,
        job_id,
        "job",
        progress.clone(),
        &mut ctrl_c,
        timeout,
        |event| match event {
            RunnerEvent::TaskStart(ev) => {
                let Some(metadata) = ev.task_metadata else {
                    return;
                };

                if metadata.level() != commanderpb::task_metadata::TaskLevel::Dag {
                    return;
                }

                let task_id = ev.task_id;
                let mut spinners = spinners.borrow_mut();
                let task_spinner = spinners
                    .entry(task_id.clone())
                    .or_insert_with(|| cli.new_spinner());
                task_spinner.enable_steady_tick(time::Duration::from_millis(100));

                // Indent the task name to present a hierarchy.
                // TODO: maybe we can replicate the DAG hierarchy here a bit?
                let name = if metadata.task_type == "USER_CODE_EXPECTATION" {
                    let name = metadata.function_name.unwrap_or(ev.task_name);
                    task_spinner.set_message(format!("  {name} [expectation]").cyan().to_string());
                    name
                } else {
                    let name = metadata.model_name.unwrap_or(ev.task_name);
                    task_spinner.set_message(format!("  {}", name.blue()));
                    name
                };

                summary.tasks.push(TaskSummary {
                    task_id,
                    description: metadata.human_readable_task_type,
                    name,
                    file_name: metadata.file_name,
                    line_number: metadata.line_number.map(|x| x as _),
                    started: Utc::now(),
                    ended: Utc::now(),
                    outcome: SummaryOutcome::Success,
                });
            }
            RunnerEvent::TaskCompletion(ev) => {
                use commanderpb::task_complete_event::Outcome;
                let Some(outcome) = ev.outcome else {
                    return;
                };

                // Finish the task spinner.
                if let Some(task_spinner) = spinners.borrow().get(ev.task_id.as_str()) {
                    let suffix = match &outcome {
                        Outcome::Success(_) => "done".green(),
                        Outcome::Failure(f) if !f.is_fatal => "failed".yellow(),
                        Outcome::Failure(_) => "failed".red(),
                        Outcome::Cancel(_) => "cancelled".red(),
                        Outcome::Timeout(_) => "timeout".red(),
                        Outcome::Skipped(_) => "skipped".yellow(),
                    };

                    let name = task_spinner.message();
                    task_spinner.finish_with_message(format!("{name} {suffix}"));
                }

                // Print a preview(s), if relevant.
                if show_previews
                    && let Outcome::Success(success) = &outcome
                    && !success.runtime_table_preview.is_empty()
                {
                    for preview in &success.runtime_table_preview {
                        cli.multiprogress
                            .suspend(|| print_preview(preview).unwrap());
                    }
                }

                // Update the JSON summary.
                if let Some(task_summary) =
                    summary.tasks.iter_mut().find(|ts| ts.task_id == ev.task_id)
                {
                    task_summary.outcome = match outcome {
                        Outcome::Success(_) => SummaryOutcome::Success,
                        Outcome::Failure(_) => SummaryOutcome::Failed,
                        Outcome::Skipped(_) => SummaryOutcome::Skipped,
                        Outcome::Cancel(_) => SummaryOutcome::Cancelled,
                        Outcome::Timeout(_) => SummaryOutcome::Timeout,
                    };
                    task_summary.ended = Utc::now();
                }
            }
            RunnerEvent::RuntimeUserLog(ev)
                if ev.r#type() == commanderpb::runtime_log_event::LogType::User =>
            {
                let stream = ev.output_stream();
                let Some(metadata) = ev.task_metadata else {
                    return;
                };

                cli.multiprogress
                    .suspend(|| print_user_log(&ev.msg, stream, metadata));
            }
            _ => (),
        },
    )
    .await;

    summary.ended = Utc::now();
    let res = match outcome {
        Ok(_) => {
            summary.outcome = SummaryOutcome::Success;
            progress.finish_with_message(format!("Executing job... {}", "done".green()));
            Ok(())
        }
        Err(e) => {
            if let Some(job_err) = e.downcast_ref::<grpc::JobError>() {
                let (outcome, suffix) = match job_err {
                    grpc::JobError::Cancelled => (SummaryOutcome::Cancelled, "cancelled".red()),
                    grpc::JobError::Rejected(_) => (SummaryOutcome::Skipped, "skipped".yellow()),
                    grpc::JobError::Timeout => (SummaryOutcome::Timeout, "timeout".red()),
                    _ => (SummaryOutcome::Failed, "failed".red()),
                };

                summary.outcome = outcome;
                progress.finish_with_message(format!("Executing job... {suffix}"));
                Err(e)
            } else {
                // Exit now.
                return Err(e);
            }
        }
    };

    for sp in spinners.borrow().values() {
        if !sp.is_finished() {
            sp.finish_with_message(format!("{} {}", sp.message(), "cancelled".red()));
        }
    }

    if cli.global.output == Some(crate::cli::Output::Json) {
        // Redirect any further writes to stderr, so that they don't get
        // interleaved with the json to stdout.
        cli.multiprogress
            .set_draw_target(ProgressDrawTarget::hidden());

        let mut out = stdout().lock();
        serde_json::to_writer(&mut out, &summary)?;
        writeln!(&mut out)?;
    }

    res
}

fn print_dag(dag_ascii: String) -> anyhow::Result<()> {
    let mut stderr = stderr().lock();

    writeln!(&mut stderr, "{}", "=> DAG".dim())?;
    let arrow = "=>".dim();
    for line in dag_ascii.lines() {
        writeln!(&mut stderr, "{arrow} {line}")?;
    }

    Ok(())
}

fn print_user_log(
    msg: &str,
    stream: commanderpb::runtime_log_event::OutputStream,
    metadata: commanderpb::TaskMetadata,
) {
    let model_name = metadata
        .model_name
        .or(metadata.function_name)
        .unwrap_or(metadata.human_readable_task_type);

    let color = match stream {
        commanderpb::runtime_log_event::OutputStream::Stderr => yansi::Color::Yellow,
        _ => yansi::Color::Blue,
    };

    if let Some(file_name) = metadata.file_name
        && let Some(line_number) = metadata.line_number
    {
        eprintln!(
            "{} | {}",
            format!("{model_name}: @ {file_name}:{line_number}").paint(color),
            msg
        );
    } else {
        eprintln!("{} | {}", format!("{model_name}:").paint(color), msg);
    }
}

fn print_preview(preview: &commanderpb::RuntimeTablePreview) -> anyhow::Result<()> {
    if preview.columns.is_empty() {
        return Ok(());
    }

    let arrow = "=>".dim();
    println!(
        "{arrow} {} {}",
        "PREVIEW".blue().bold(),
        preview.table_name.blue()
    );

    let mut tw = TabWriter::new(std::io::stderr().lock()).ansi(true);
    write!(tw, "{arrow} ")?;
    for col in &preview.columns {
        write!(tw, "{}\t", col.column_name.to_uppercase().dim())?;
    }
    writeln!(tw)?;

    let num_rows = preview.columns[0].values.len();
    for i in 0..num_rows {
        write!(tw, "{arrow} ")?;
        for col in &preview.columns {
            let val = col.values.get(i).map(String::as_str).unwrap_or_default();
            write!(tw, "{val}\t")?;
        }

        writeln!(tw)?;
    }

    tw.flush()?;
    Ok(())
}

async fn resolve_parameters(
    cli: &Cli,
    project: &ProjectFile,
    cli_params: Vec<KeyValue>,
) -> anyhow::Result<Vec<commanderpb::Parameter>> {
    // Are all the parameters correct?
    for kv in &cli_params {
        if !project.parameters.contains_key(&kv.0) {
            bail!("unknown parameter: {:?}", kv.0);
        }
    }

    // If any of the params are a secret, we need to fetch the org-wide public
    // key from commander. This is used to cache the result, in case multiple
    // parameters are secrets.
    let mut key_cache: Option<(String, RsaPublicKey)> = None;

    let timeout = cli.timeout.unwrap_or(time::Duration::from_secs(5));
    let mut client = grpc::Client::new_lazy(&cli.profile, timeout)?;

    let mut resolved = Vec::with_capacity(project.parameters.len());
    for (name, param) in &project.parameters {
        let kv = cli_params.iter().find(|kv| &kv.0 == name);
        if let Some(KeyValue(_, value)) = kv {
            let parsed = if param.param_type == ParameterType::Secret {
                let (key_name, key) = if let Some((key_name, key)) = &key_cache {
                    (key_name.clone(), key)
                } else {
                    let (key_name, key) = client.org_default_public_key(timeout).await?;
                    let (_, key) = key_cache.insert((key_name.clone(), key));

                    (key_name, &*key)
                };

                ParameterValue::encrypt_secret(key_name, key, project.project.id, value)?
            } else {
                parse_parameter(param.param_type, value)?
            };

            resolved.push(commanderpb::Parameter {
                name: name.clone(),
                value: Some(parsed.into()),
            });
        } else if let Some(default_value) = param.eval_default()? {
            resolved.push(commanderpb::Parameter {
                name: name.clone(),
                value: Some(default_value.into()),
            });
        } else if param.required {
            bail!("missing required parameter: {name:?}");
        }
    }

    Ok(resolved)
}
