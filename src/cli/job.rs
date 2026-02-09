use std::io::{Write as _, stdout};
use std::time;

use anyhow::bail;
use bauplan::grpc::{
    self, generated as commanderpb,
    job::{Job, JobState},
};
use chrono::{DateTime, Local, Utc};
use clap::ValueEnum;
use yansi::Paint as _;

use commanderpb::runtime_log_event::{LogLevel, LogType};
use futures::{Stream, StreamExt as _, TryStreamExt, stream};
use tabwriter::TabWriter;
use tonic::Request;
use tracing::info;

use crate::cli::{Cli, Output};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum JobKindArg {
    Run,
    Query,
    ImportPlanCreate,
    ImportPlanApply,
    TablePlanCreate,
    TablePlanApply,
    TableImport,
}

impl From<JobKindArg> for commanderpb::JobKind {
    fn from(arg: JobKindArg) -> Self {
        match arg {
            JobKindArg::Run => Self::CodeSnapshotRun,
            JobKindArg::Query => Self::QueryRun,
            JobKindArg::ImportPlanCreate => Self::ImportPlanCreate,
            JobKindArg::ImportPlanApply => Self::ImportPlanApply,
            JobKindArg::TablePlanCreate => Self::TablePlanCreate,
            JobKindArg::TablePlanApply => Self::TablePlanCreateApply,
            JobKindArg::TableImport => Self::TableDataImport,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum JobStatusArg {
    NotStarted,
    Running,
    Complete,
    Abort,
    Fail,
}

impl From<JobStatusArg> for commanderpb::JobStateType {
    fn from(arg: JobStatusArg) -> Self {
        match arg {
            JobStatusArg::NotStarted => Self::NotStarted,
            JobStatusArg::Running => Self::Running,
            JobStatusArg::Complete => Self::Complete,
            JobStatusArg::Abort => Self::Abort,
            JobStatusArg::Fail => Self::Fail,
        }
    }
}

#[derive(Debug, clap::Args)]
pub(crate) struct JobArgs {
    #[command(subcommand)]
    pub command: JobCommand,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum JobCommand {
    /// List all available jobs
    #[clap(alias = "list")]
    Ls(JobLsArgs),
    /// Get information about a job
    Get(JobGetArgs),
    /// Get logs for a job
    Logs(JobLogsArgs),
    /// Stop a job
    Stop(JobStopArgs),
}

#[derive(Debug, clap::Args)]
pub(crate) struct JobLsArgs {
    /// Show jobs from all users, not just your own
    #[arg(long)]
    pub all_users: bool,
    /// Filter by job ID (can be specified multiple times)
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub id: Vec<String>,
    /// Filter by username (can be specified multiple times)
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub user: Vec<String>,
    /// Filter by job kind
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub kind: Vec<JobKindArg>,
    /// Filter by status
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub status: Vec<JobStatusArg>,
    /// Filter jobs created after this date (e.g., 2024-01-15 or 2024-01-15T10:30:00Z)
    #[arg(long)]
    pub created_after: Option<String>,
    /// Filter jobs created before this date (e.g., 2024-01-15 or 2024-01-15T23:59:59Z)
    #[arg(long)]
    pub created_before: Option<String>,
    /// Maximum number of jobs to return (max: 500)
    #[arg(short = 'n', long, visible_alias = "limit", default_value = "10")]
    pub max_count: i32,
    /// Use UTC for date parsing and display
    #[arg(short = 'z', long)]
    pub utc: bool,
}

#[derive(Debug, clap::Args)]
pub(crate) struct JobGetArgs {
    /// Job id
    pub job_id: String,
}

#[derive(Debug, clap::Args)]
pub(crate) struct JobLogsArgs {
    /// Job id
    pub job_id: String,
    /// Include system logs
    #[arg(long)]
    pub system: bool,
    /// Include all logs
    #[arg(long)]
    pub all: bool,
}

#[derive(Debug, clap::Args)]
pub(crate) struct JobStopArgs {
    /// Job id
    pub job_id: String,
}

pub(crate) async fn handle(cli: &Cli, args: JobArgs) -> anyhow::Result<()> {
    match args.command {
        JobCommand::Ls(args) => handle_ls(cli, args).await,
        JobCommand::Get(args) => handle_get(cli, args).await,
        JobCommand::Logs(args) => handle_logs(cli, args).await,
        JobCommand::Stop(args) => handle_stop(cli, args).await,
    }
}

fn parse_datetime(s: &str, utc: bool) -> anyhow::Result<DateTime<Utc>> {
    if utc {
        dateparser::parse_with_timezone(s, &Utc)
    } else {
        dateparser::parse_with_timezone(s, &Local)
    }
    .map_err(|e| anyhow::anyhow!("invalid date format: {}", e))
}

fn format_datetime(dt: Option<DateTime<Utc>>, utc: bool) -> String {
    match dt {
        Some(d) if utc => d.to_rfc3339(),
        Some(d) => d.with_timezone(&Local).to_rfc3339(),
        None => String::new(),
    }
}

async fn handle_ls(cli: &Cli, args: JobLsArgs) -> anyhow::Result<()> {
    let timeout = cli.timeout.unwrap_or(time::Duration::from_secs(30));
    let client = grpc::Client::new_lazy(&cli.profile, timeout)?;

    let filter_kinds: Vec<i32> = args
        .kind
        .iter()
        .map(|k| commanderpb::JobKind::from(*k) as i32)
        .collect();
    let filter_statuses: Vec<i32> = args
        .status
        .iter()
        .map(|s| commanderpb::JobStateType::from(*s) as i32)
        .collect();

    let filter_created_after = args
        .created_after
        .map(|s| parse_datetime(&s, args.utc))
        .transpose()?
        .map(to_proto_timestamp);

    let filter_created_before = args
        .created_before
        .map(|s| parse_datetime(&s, args.utc))
        .transpose()?
        .map(to_proto_timestamp);

    let base_request = commanderpb::GetJobsRequest {
        job_ids: args.id,
        all_users: args.all_users,
        filter_users: args.user,
        filter_kinds,
        filter_statuses,
        filter_created_after,
        filter_created_before,
        ..Default::default()
    };

    let seed = (
        None,                    // Pagination token
        args.max_count as usize, // How many more rows to fetch.
    );

    let stream = stream::try_unfold(seed, move |(token, remaining)| {
        let base_request = base_request.clone();
        let mut client = client.clone();
        async move {
            // The pagination token starts as None. Some("") means we're done.
            if remaining == 0 || token.as_deref().is_some_and(str::is_empty) {
                return Ok::<_, tonic::Status>(None);
            }

            let mut req = Request::new(commanderpb::GetJobsRequest {
                max_records: remaining as i32,
                pagination_token: token.unwrap_or_default(),
                ..base_request
            });

            req.set_timeout(timeout);
            let page = client.get_jobs(req).await?.into_inner();

            let remaining = remaining.saturating_sub(page.jobs.len());
            let token = Some(page.pagination_token);
            let jobs = stream::iter(page.jobs).map(|j| Ok(Job::from(j)));
            Ok(Some((jobs, (token, remaining))))
        }
    })
    .try_flatten()
    .map_ok(Job::from);

    match cli.global.output.unwrap_or_default() {
        Output::Json => {
            let jobs: Vec<Job> = stream.try_collect().await?;
            serde_json::to_writer(stdout(), &jobs)?;
            println!();
        }
        Output::Tty => print_jobs_stream(stream, args.utc).await?,
    }

    Ok(())
}

async fn print_jobs_stream<S>(stream: S, utc: bool) -> anyhow::Result<()>
where
    S: Stream<Item = Result<Job, tonic::Status>>,
{
    let mut tw = TabWriter::new(stdout()).ansi(true);
    let mut headers_printed = false;

    futures::pin_mut!(stream);
    while let Some(job) = stream.try_next().await? {
        if !headers_printed {
            headers_printed = true;
            writeln!(&mut tw, "ID\tKIND\tUSER\tSTATUS\tCREATED\tFINISHED")?;
        }

        let status_colored = match job.status_type {
            JobState::Complete => job.status.green(),
            JobState::Fail | JobState::Abort => job.status.red(),
            JobState::Running => job.status.yellow(),
            _ => job.status.primary(),
        };

        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}",
            job.id,
            job.kind,
            job.user,
            status_colored,
            format_datetime(job.created_at, utc),
            format_datetime(job.finished_at, utc),
        )?;
    }

    tw.flush()?;

    if !headers_printed {
        eprintln!("No jobs found!")
    }

    Ok(())
}

async fn handle_get(cli: &Cli, args: JobGetArgs) -> anyhow::Result<()> {
    let timeout = cli.timeout.unwrap_or(time::Duration::from_secs(30));

    let mut client = grpc::Client::new_lazy(&cli.profile, timeout)?;

    let mut request = Request::new(commanderpb::GetJobsRequest {
        job_ids: vec![args.job_id.clone()],
        all_users: true,
        ..Default::default()
    });
    request.set_timeout(timeout);

    let response = client.get_jobs(request).await?.into_inner();
    let Some(job) = response.jobs.into_iter().next().map(Job::from) else {
        bail!("job not found: {}", args.job_id);
    };

    match cli.global.output.unwrap_or_default() {
        Output::Json => {
            serde_json::to_writer(stdout(), &[job])?;
            println!();
        }
        Output::Tty => {
            let mut tw = TabWriter::new(stdout()).ansi(true);
            writeln!(&mut tw, "Job ID:\t{}", job.id)?;
            writeln!(&mut tw, "Status:\t{}", job.status)?;
            writeln!(&mut tw, "Kind:\t{}", job.kind)?;
            writeln!(&mut tw, "User:\t{}", job.user)?;
            writeln!(&mut tw, "Runner:\t{}", job.runner)?;
            writeln!(
                &mut tw,
                "Created:\t{}",
                format_datetime(job.created_at, false)
            )?;
            writeln!(
                &mut tw,
                "Finished:\t{}",
                format_datetime(job.finished_at, false)
            )?;
            tw.flush()?;
        }
    }

    Ok(())
}

#[derive(Debug, serde::Serialize)]
struct LogEntry {
    timestamp: DateTime<Utc>,
    #[serde(serialize_with = "serialize_log_level")]
    level: LogLevel,
    #[serde(serialize_with = "serialize_log_type")]
    log_type: LogType,
    message: String,
}

fn serialize_log_type<S>(log_type: &LogType, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(log_type.as_str_name())
}

fn serialize_log_level<S>(level: &LogLevel, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(level.as_str_name())
}

async fn handle_logs(cli: &Cli, args: JobLogsArgs) -> anyhow::Result<()> {
    let timeout = cli.timeout.unwrap_or(time::Duration::from_secs(30));
    let mut client = grpc::Client::new_lazy(&cli.profile, timeout)?;

    let mut request = Request::new(commanderpb::GetLogsRequest {
        job_id: args.job_id.clone(),
        ..Default::default()
    });
    request.set_timeout(timeout);

    let response = client.get_logs(request).await?.into_inner();
    let entries = response.events.into_iter().filter_map(|ev| {
        let commanderpb::runner_event::Event::RuntimeUserLog(log) = ev.event? else {
            return None;
        };

        let log_type = LogType::try_from(log.r#type).unwrap_or(LogType::Unspecified);
        if args.all
            || (args.system && log_type == LogType::System)
            || (!args.system && log_type == LogType::User)
        {
            let timestamp = DateTime::from_timestamp_nanos(log.emit_timestamp_ns);
            let level = LogLevel::try_from(log.level).unwrap_or(LogLevel::Unspecified);

            Some(LogEntry {
                timestamp,
                level,
                log_type,
                message: log.msg,
            })
        } else {
            None
        }
    });

    match cli.global.output.unwrap_or_default() {
        Output::Json => {
            serde_json::to_writer(stdout(), &entries.collect::<Vec<_>>())?;
            println!();
        }
        Output::Tty => {
            let mut entries = entries.peekable();
            if entries.peek().is_none() {
                eprintln!("No log entries matched filter.");
            }

            let mut tw = TabWriter::new(stdout()).ansi(true);
            writeln!(&mut tw, "TIMESTAMP\tLEVEL\tTYPE\tMESSAGE")?;

            for entry in entries {
                let level = match entry.level {
                    LogLevel::Error => "ERROR".red(),
                    LogLevel::Warning => "WARNING".yellow(),
                    LogLevel::Debug => "DEBUG".blue(),
                    LogLevel::Info => "INFO".green(),
                    LogLevel::Trace => "TRACE".cyan(),
                    LogLevel::Unspecified => "UNKNOWN".dim(),
                };

                let log_type = match entry.log_type {
                    LogType::System => "SYSTEM".dim(),
                    LogType::User => "USER".green(),
                    LogType::Unspecified => "UNKNOWN".dim(),
                };

                writeln!(
                    &mut tw,
                    "{}\t{}\t{}\t{}",
                    entry.timestamp.to_rfc3339().dim(),
                    level,
                    log_type,
                    entry.message.replace('\n', "\\n")
                )?;
            }

            tw.flush()?;
        }
    }

    Ok(())
}

async fn handle_stop(cli: &Cli, args: JobStopArgs) -> anyhow::Result<()> {
    let timeout = cli.timeout.unwrap_or(time::Duration::from_secs(30));
    let mut client = grpc::Client::new_lazy(&cli.profile, timeout)?;

    client.cancel(&args.job_id).await?;
    info!(job_id = args.job_id, "job cancelled");
    Ok(())
}

fn to_proto_timestamp(dt: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}
