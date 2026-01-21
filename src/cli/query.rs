use crate::cli::Cli;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Cache {
    On,
    Off,
}

#[derive(Debug, clap::Args)]
pub(crate) struct QueryArgs {
    /// Do not truncate output
    #[arg(long)]
    pub no_trunc: bool,
    /// Set the cache mode.
    #[arg(long)]
    pub cache: Option<Cache>,
    /// read query from file
    #[arg(short, long)]
    pub file: Option<String>,
    /// Arguments to pass to the job. Format: key=value
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub arg: Vec<String>,
    /// Ref or branch name to run query against.
    #[arg(short, long)]
    pub r#ref: Option<String>,
    /// Limit number of returned rows. (use --all-rows to disable this)
    #[arg(long)]
    pub max_rows: Option<i64>,
    /// Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// Do not limit returned rows. Supercedes --max-rows
    #[arg(long)]
    pub all_rows: bool,
    /// Set the job priority (1-10, where 10 is highest priority)
    #[arg(long)]
    pub priority: Option<i64>,
    /// Sql
    pub sql: Option<String>,
}

pub(crate) fn handle(_cli: &Cli, _args: QueryArgs) -> anyhow::Result<()> {
    todo!()
}
