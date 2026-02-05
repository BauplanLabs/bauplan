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
    grpc::{
        self,
        generated::{self as commanderpb},
    },
    project::{ParameterType, ParameterValue, ProjectFile},
};
use chrono::Utc;
use futures::TryStreamExt as _;
use gethostname::gethostname;
use indicatif::ProgressBar;
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
    /// Do not truncate summary output
    #[arg(long)]
    pub summary_no_trunc: bool,
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

async fn handle_run(cli: &Cli, args: RunArgs) -> anyhow::Result<()> {
    let start = Utc::now();

    let RunArgs {
        arg,
        project_dir,
        cache,
        summary_no_trunc: _, // TODO: implement summary truncation
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

    let mut client_clone = client.clone();
    let stream = client_clone.monitor_job(job_id.clone(), timeout);
    futures::pin_mut!(stream);

    // We have to manually tick the progress bar here, or we get ghosting.
    let mut ticker = tokio::time::interval(time::Duration::from_millis(100));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // One spinner for each task.
    let spinners: RefCell<BTreeMap<String, ProgressBar>> = RefCell::new(BTreeMap::new());

    let mut kill_job = async |reason: &str| -> ! {
        // Clear the task spinners.
        for spinner in spinners.borrow().values() {
            spinner.finish_and_clear();
        }

        error!(job_id, "{reason}, cancelling job");

        progress.set_message("Cancelling job...");
        progress.enable_steady_tick(time::Duration::from_millis(100));

        if let Err(e) = client.cancel(&job_id).await {
            error!(job_id, error = %e, "failed to cancel job");
            progress.finish_with_message(format!("Cancelling job... {}", "failed".red()));
        } else {
            debug!(job_id, "job successfully cancelled");
            progress.finish_with_message(format!("Cancelling job... {}", "done".green()));
        }

        std::process::exit(1)
    };

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

    loop {
        let res = tokio::select! {
            v = stream.try_next() => v,
            _ = ticker.tick() => {
                progress.tick();
                spinners.borrow().values().for_each(|sp| sp.tick());
                continue;
            }
            _ = &mut ctrl_c => kill_job("interrupt received").await,
        };

        let event = match res {
            Ok(Some(v)) => v,
            Ok(None) => break,
            Err(ref e)
                if e.code() == tonic::Code::Cancelled
                    || e.code() == tonic::Code::DeadlineExceeded =>
            {
                kill_job("execution timed out").await
            }
            Err(e) => return Err(e.into()),
        };

        match event {
            RunnerEvent::TaskStart(ev) => {
                let Some(metadata) = ev.task_metadata else {
                    continue;
                };

                if metadata.level() != commanderpb::task_metadata::TaskLevel::Dag {
                    continue;
                }

                let task_id = ev.task_id;
                let mut spinners = spinners.borrow_mut();
                let task_spinner = spinners
                    .entry(task_id.clone())
                    .or_insert_with(|| cli.new_spinner());

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
                    continue;
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
                        cli.multiprogress.suspend(|| print_preview(preview))?;
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
            RunnerEvent::JobCompletion(ev) => {
                use commanderpb::job_complete_event::Outcome;
                let outcome = match &ev.outcome {
                    Some(Outcome::Success(_)) => SummaryOutcome::Success,
                    Some(Outcome::Cancellation(_)) => SummaryOutcome::Cancelled,
                    Some(Outcome::Timeout(_)) => SummaryOutcome::Timeout,
                    _ => SummaryOutcome::Failed,
                };

                // Finish the spinner.
                let suffix = match outcome {
                    SummaryOutcome::Success => "done".green(),
                    SummaryOutcome::Cancelled => "cancelled".red(),
                    SummaryOutcome::Timeout => "timeout".red(),
                    SummaryOutcome::Failed => "failed".red(),
                    SummaryOutcome::Skipped => unreachable!(),
                };

                progress.finish_with_message(format!("Executing job... {suffix}"));
                if let Err(e) = grpc::interpret_outcome(ev.outcome) {
                    return Err(e.into());
                };

                // Update the JSON summary.
                summary.outcome = outcome;
                summary.ended = Utc::now();

                break;
            }
            RunnerEvent::RuntimeUserLog(ev)
                if ev.r#type() == commanderpb::runtime_log_event::LogType::User =>
            {
                let stream = ev.output_stream();
                let Some(metadata) = ev.task_metadata else {
                    continue;
                };

                cli.multiprogress
                    .suspend(|| print_user_log(&ev.msg, stream, metadata));
            }
            _ => (),
        }
    }

    if cli.global.output == Some(crate::cli::Output::Json) {
        serde_json::to_writer(stdout(), &summary)?;
    }

    Ok(())
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
