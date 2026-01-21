use crate::cli::Cli;

#[derive(Debug, clap::Args)]
pub(crate) struct JobArgs {
    #[command(subcommand)]
    pub command: JobCommand,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum JobCommand {
    /// List all available jobs
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
    /// Filter by job kind: run, query, import-plan-create, import-plan-apply, table-plan-create, table-plan-apply, table-import
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub kind: Vec<String>,
    /// Filter by status: not-started, running, complete, abort, fail
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub status: Vec<String>,
    /// Filter jobs created after this date (e.g., 2024-01-15 or 2024-01-15T10:30:00Z)
    #[arg(long)]
    pub created_after: Option<String>,
    /// Filter jobs created before this date (e.g., 2024-01-15 or 2024-01-15T23:59:59Z)
    #[arg(long)]
    pub created_before: Option<String>,
    /// Maximum number of jobs to return (max: 500)
    #[arg(short = 'n', long)]
    pub max_count: Option<i64>,
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
    /// Include system logs
    #[arg(long)]
    pub system: bool,
    /// Include all logs
    #[arg(long)]
    pub all: bool,
    /// Job id
    pub job_id: String,
}

#[derive(Debug, clap::Args)]
pub(crate) struct JobStopArgs {
    /// Job id
    pub job_id: String,
}

pub(crate) fn handle(_cli: &Cli, _args: JobArgs) -> anyhow::Result<()> {
    match _args.command {
        JobCommand::Ls(_) => todo!(),
        JobCommand::Get(_) => todo!(),
        JobCommand::Logs(_) => todo!(),
        JobCommand::Stop(_) => todo!(),
    }
}
