use crate::cli::Cli;

#[derive(Debug, clap::Args)]
pub(crate) struct BranchArgs {
    #[command(subcommand)]
    pub command: BranchCommand,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum BranchCommand {
    /// List all available branches (default action)
    Ls(BranchLsArgs),
    /// Create a new branch
    Create(BranchCreateArgs),
    /// Delete a branch
    Rm(BranchRmArgs),
    /// Get information about a branch
    Get(BranchGetArgs),
    /// Set the active branch
    Checkout(BranchCheckoutArgs),
    /// Show the diff between the active branch and another branch
    Diff(BranchDiffArgs),
    /// Merge a branch into the active branch
    Merge(BranchMergeArgs),
    /// Rename a branch
    Rename(BranchRenameArgs),
}

#[derive(Debug, clap::Args)]
pub(crate) struct BranchLsArgs {
    /// Show all branches, including those from other namespaces (users)
    #[arg(short, long)]
    pub all_zones: bool,
    /// Filter by name
    #[arg(short, long)]
    pub name: Option<String>,
    /// Filter by user
    #[arg(short, long)]
    pub user: Option<String>,
    /// Limit the number of branches to show
    #[arg(long)]
    pub limit: Option<i64>,
    /// Branch name
    pub branch_name: Option<String>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct BranchCreateArgs {
    /// Ref from which to create. If not specified, default is active branch
    #[arg(long)]
    pub from_ref: Option<String>,
    /// Do not fail if the branch already exists
    #[arg(long)]
    pub if_not_exists: bool,
    /// Branch name
    pub branch_name: String,
}

#[derive(Debug, clap::Args)]
pub(crate) struct BranchRmArgs {
    /// Do not fail if the branch does not exist
    #[arg(long)]
    pub if_exists: bool,
    /// Branch name
    pub branch_name: String,
}

#[derive(Debug, clap::Args)]
pub(crate) struct BranchGetArgs {
    /// Filter by namespace
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// Branch name
    pub branch_name: String,
}

#[derive(Debug, clap::Args)]
pub(crate) struct BranchCheckoutArgs {
    /// Branch name
    pub branch_name: String,
}

#[derive(Debug, clap::Args)]
pub(crate) struct BranchDiffArgs {
    /// Filter by namespace
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// Branch name a
    pub branch_name_a: String,
    /// Branch name b
    pub branch_name_b: Option<String>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct BranchMergeArgs {
    /// Optinal commit message
    #[arg(long)]
    pub commit_message: Option<String>,
    /// Branch name
    pub branch_name: String,
}

#[derive(Debug, clap::Args)]
pub(crate) struct BranchRenameArgs {
    /// Branch name
    pub branch_name: String,
    /// New branch name
    pub new_branch_name: String,
}

pub(crate) fn handle(_cli: &Cli, _args: BranchArgs) -> anyhow::Result<()> {
    match _args.command {
        BranchCommand::Ls(_) => todo!(),
        BranchCommand::Create(_) => todo!(),
        BranchCommand::Rm(_) => todo!(),
        BranchCommand::Get(_) => todo!(),
        BranchCommand::Checkout(_) => todo!(),
        BranchCommand::Diff(_) => todo!(),
        BranchCommand::Merge(_) => todo!(),
        BranchCommand::Rename(_) => todo!(),
    }
}
