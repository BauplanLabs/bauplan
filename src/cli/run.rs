use crate::cli::Cli;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Cache {
    On,
    Off,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Preview {
    On,
    Off,
    Head,
    Tail,
}

#[derive(Debug, clap::Args)]
pub(crate) struct RunArgs {
    /// Arguments to pass to the job. Format: key=value
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub arg: Vec<String>,
    /// Path to the root Bauplan project directory.
    #[arg(short, long)]
    pub project_dir: Option<String>,
    /// Set the cache mode.
    #[arg(long)]
    pub cache: Option<Cache>,
    /// Do not truncate summary output
    #[arg(long)]
    pub summary_no_trunc: bool,
    /// Set the preview mode.
    #[arg(long)]
    pub preview: Option<Preview>,
    /// Exit upon encountering runtime warnings (e.g., invalid column output)
    #[arg(long)]
    pub strict: Option<String>,
    /// Node to run the job on. If not set, the job will be run on the default node for the project.
    #[arg(long)]
    pub runner_node: Option<String>,
    /// Set a parameter for the job. Format: key=value. Can be used multiple times.
    #[arg(long, action = clap::ArgAction::Append)]
    pub param: Vec<String>,
    /// Namespace to run the job in. If not set, the job will be run in the default namespace for the project.
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// Ref or branch name from which to run the job.
    #[arg(short, long)]
    pub r#ref: Option<String>,
    /// Dry run the job without materializing any models.
    #[arg(long)]
    pub dry_run: bool,
    /// Run the dag as a transaction. Will create a temporary branch where models are materialized. Once all models succeed, it will be merged to branch in which this run is happenning in
    #[arg(short, long)]
    pub transaction: Option<String>,
    /// Run the job in the background instead of streaming logs
    #[arg(short, long)]
    pub detach: bool,
    /// Set the job priority (1-10, where 10 is highest priority)
    #[arg(long)]
    pub priority: Option<i64>,
}

pub(crate) fn handle(_cli: &Cli, _args: RunArgs) -> anyhow::Result<()> {
    todo!()
}
