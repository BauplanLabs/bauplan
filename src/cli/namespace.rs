use crate::cli::Cli;

#[derive(Debug, clap::Args)]
pub(crate) struct NamespaceArgs {
    #[command(subcommand)]
    pub command: NamespaceCommand,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum NamespaceCommand {
    /// List available namespaces
    #[clap(alias = "list")]
    Ls(NamespaceLsArgs),
    /// Create a new namespace
    Create(NamespaceCreateArgs),
    /// Drop a namespace from the data catalog
    #[clap(alias = "delete")]
    Rm(NamespaceRmArgs),
}

#[derive(Debug, clap::Args)]
pub(crate) struct NamespaceLsArgs {
    /// Ref or branch name to get the namespaces from; it defaults to the active branch
    #[arg(short, long)]
    pub r#ref: Option<String>,
    /// Limit the number of namespaces to show
    #[arg(long)]
    pub limit: Option<usize>,
    /// Namespace
    pub namespace: String,
}

#[derive(Debug, clap::Args)]
pub(crate) struct NamespaceCreateArgs {
    /// Branch to create the namespace in; it defaults to the active branch
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Optinal commit body to append to the commit message
    #[arg(long)]
    pub commit_body: Option<String>,
    /// Do not fail if the namespace already exists
    #[arg(long)]
    pub if_not_exists: bool,
    /// Namespace
    pub namespace: String,
}

#[derive(Debug, clap::Args)]
pub(crate) struct NamespaceRmArgs {
    /// Branch to delete the namespace from; it defaults to the active branch
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Optinal commit body to append to the commit message
    #[arg(long)]
    pub commit_body: Option<String>,
    /// Do not fail if the namespace does not exist
    #[arg(long)]
    pub if_exists: bool,
    /// Namespace
    pub namespace: String,
}

pub(crate) fn handle(_cli: &Cli, _args: NamespaceArgs) -> anyhow::Result<()> {
    match _args.command {
        NamespaceCommand::Ls(_) => todo!(),
        NamespaceCommand::Create(_) => todo!(),
        NamespaceCommand::Rm(_) => todo!(),
    }
}
