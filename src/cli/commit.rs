use crate::cli::Cli;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Pretty {
    Oneline,
    Short,
    Medium,
    Full,
    Fuller,
}

#[derive(Debug, clap::Args)]
pub(crate) struct CommitArgs {
    /// Ref or branch name to get the commits from; it defaults to the active branch
    #[arg(short, long)]
    pub r#ref: Option<String>,
    /// Filter by message content (string or a regex like "^something.*$")
    #[arg(long)]
    pub message: Option<String>,
    /// Filter by author username (string or a regex like "^something.*$")
    #[arg(long)]
    pub author_username: Option<String>,
    /// Filter by author name (string or a regex like "^something.*$")
    #[arg(long)]
    pub author_name: Option<String>,
    /// Filter by author email (string or a regex like "^something.*$")
    #[arg(long)]
    pub author_email: Option<String>,
    /// Filter by a property. Format: key=value. Can be used multiple times.
    #[arg(long, action = clap::ArgAction::Append)]
    pub property: Vec<String>,
    /// Limit the number of commits to show
    #[arg(short = 'n', long)]
    pub max_count: Option<i64>,
    /// Pretty-print the contents of the commit log
    #[arg(long)]
    pub pretty: Option<Pretty>,
}

pub(crate) fn handle(_cli: &Cli, _args: CommitArgs) -> anyhow::Result<()> {
    todo!()
}
