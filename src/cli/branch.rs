use std::io::{Write as _, stdout};

use bauplan::{ApiErrorKind, branch::*, table::GetTables};
use tabwriter::TabWriter;
use tracing::info;

use crate::cli::{Cli, Output, is_api_err_kind};

#[derive(Debug, clap::Args)]
pub(crate) struct BranchArgs {
    #[command(subcommand)]
    pub command: BranchCommand,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum BranchCommand {
    /// List branches (shows only your branches by default)
    #[clap(alias = "list")]
    Ls(BranchLsArgs),
    /// Create a new branch
    Create(BranchCreateArgs),
    /// Delete a branch
    #[clap(alias = "delete")]
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
    /// Branch name
    pub branch_name: Option<String>,
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
    pub limit: Option<usize>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct BranchCreateArgs {
    /// Branch name
    pub branch_name: String,
    /// Ref from which to create. If not specified, default is active branch
    #[arg(long)]
    pub from_ref: Option<String>,
    /// Do not fail if the branch already exists
    #[arg(long)]
    pub if_not_exists: bool,
}

#[derive(Debug, clap::Args)]
pub(crate) struct BranchRmArgs {
    /// Branch name
    pub branch_name: String,
    /// Do not fail if the branch does not exist
    #[arg(long)]
    pub if_exists: bool,
}

#[derive(Debug, clap::Args)]
pub(crate) struct BranchGetArgs {
    /// Branch name
    pub branch_name: String,
    /// Filter by namespace
    #[arg(short, long)]
    pub namespace: Option<String>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct BranchCheckoutArgs {
    /// Branch name
    pub branch_name: String,
}

#[derive(Debug, clap::Args)]
pub(crate) struct BranchDiffArgs {
    /// Branch name a
    pub branch_name_a: String,
    /// Branch name b
    pub branch_name_b: Option<String>,
    /// Filter by namespace
    #[arg(short, long)]
    pub namespace: Option<String>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct BranchMergeArgs {
    /// Branch name
    pub branch_name: String,
    /// Optional commit message
    #[arg(long)]
    pub commit_message: Option<String>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct BranchRenameArgs {
    /// Branch name
    pub branch_name: String,
    /// New branch name
    pub new_branch_name: String,
}

pub(crate) fn handle(cli: &Cli, args: BranchArgs) -> anyhow::Result<()> {
    match args.command {
        BranchCommand::Ls(args) => list_branches(cli, args),
        BranchCommand::Create(args) => create_branch(cli, args),
        BranchCommand::Rm(args) => delete_branch(cli, args),
        BranchCommand::Get(args) => get_branch(cli, args),
        BranchCommand::Checkout(_) => todo!(),
        BranchCommand::Diff(_) => todo!(),
        BranchCommand::Merge(args) => merge_branch(cli, args),
        BranchCommand::Rename(args) => rename_branch(cli, args),
    }
}

fn list_branches(
    cli: &Cli,
    BranchLsArgs {
        branch_name,
        all_zones,
        name,
        user,
        limit,
    }: BranchLsArgs,
) -> anyhow::Result<()> {
    // The branch_name positional arg acts as a name filter.
    let filter_by_name = name.as_deref().or(branch_name.as_deref());

    // By default, only show the current user's branches. The --all-zones
    // flag disables this, and --user overrides it.
    let filter_by_user = if all_zones {
        None
    } else if let Some(ref user) = user {
        Some(user.as_str())
    } else {
        Some(CURRENT_USER) // This is a special string the server understands.
    };

    let req = GetBranches {
        filter_by_name,
        filter_by_user,
    };

    let branches = bauplan::paginate(req, limit, |r| cli.roundtrip(r))?;

    match cli.global.output.unwrap_or_default() {
        Output::Json => {
            let all_branches = branches.collect::<anyhow::Result<Vec<_>>>()?;
            serde_json::to_writer(stdout(), &all_branches)?;
            println!();
        }
        Output::Tty => {
            let mut tw = TabWriter::new(stdout());
            writeln!(&mut tw, "NAME\tZONE\tHASH")?;
            for branch in branches {
                let branch = branch?;
                let zone = branch.name.split('.').next().unwrap_or("");
                writeln!(&mut tw, "{}\t{}\t{}", branch.name, zone, branch.hash)?;
            }

            tw.flush()?;
        }
    }

    Ok(())
}

fn get_branch(
    cli: &Cli,
    BranchGetArgs {
        branch_name,
        namespace,
    }: BranchGetArgs,
) -> anyhow::Result<()> {
    let req = GetTables {
        at_ref: &branch_name,
        filter_by_name: None,
        filter_by_namespace: namespace.as_deref(),
    };

    let tables = bauplan::paginate(req, None, |r| cli.roundtrip(r))?;

    match cli.global.output.unwrap_or_default() {
        Output::Json => {
            let all_tables = tables.collect::<anyhow::Result<Vec<_>>>()?;
            serde_json::to_writer(stdout(), &all_tables)?;
            println!();
        }
        Output::Tty => {
            let mut tw = TabWriter::new(stdout());
            writeln!(&mut tw, "NAMESPACE\tNAME\tKIND")?;
            for table in tables {
                let table = table?;
                writeln!(
                    &mut tw,
                    "{}\t{}\t{}",
                    table.namespace, table.name, table.kind
                )?;
            }

            tw.flush()?;
        }
    }

    Ok(())
}

fn create_branch(
    cli: &Cli,
    BranchCreateArgs {
        branch_name,
        from_ref,
        if_not_exists,
    }: BranchCreateArgs,
) -> anyhow::Result<()> {
    let from_ref = from_ref
        .as_deref()
        .or(cli.profile.active_branch.as_deref())
        .unwrap_or("main");

    let req = CreateBranch {
        name: &branch_name,
        from_ref,
    };

    let result = cli.roundtrip(req);
    match result {
        Ok(branch) => {
            info!(branch = branch.name, "created branch");
            info!(
                branch = branch.name,
                "To make it the active branch, run: bauplan checkout <branch>"
            );
        }
        Err(e) if if_not_exists && is_api_err_kind(&e, ApiErrorKind::BranchExists) => {
            info!(branch = branch_name, "Branch already exists");
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

fn delete_branch(
    cli: &Cli,
    BranchRmArgs {
        branch_name,
        if_exists,
    }: BranchRmArgs,
) -> anyhow::Result<()> {
    let req = DeleteBranch { name: &branch_name };

    let result = cli.roundtrip(req);
    match result {
        Ok(branch) => {
            info!(branch = branch.name, "deleted branch");
        }
        Err(e) if if_exists && is_api_err_kind(&e, ApiErrorKind::BranchNotFound) => {
            info!(branch = branch_name, "Branch does not exist");
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

fn merge_branch(
    cli: &Cli,
    BranchMergeArgs {
        branch_name,
        commit_message,
    }: BranchMergeArgs,
) -> anyhow::Result<()> {
    let into_branch = cli.profile.active_branch.as_deref().unwrap_or("main");

    let req = MergeBranch {
        source_ref: &branch_name,
        into_branch,
        commit: MergeCommitOptions {
            commit_message: commit_message.as_deref(),
            ..Default::default()
        },
    };

    cli.roundtrip(req)?;
    // Original prints to stdout, not log.
    println!("Merged branch \"{branch_name}\" into \"{into_branch}\"");

    Ok(())
}

fn rename_branch(
    cli: &Cli,
    BranchRenameArgs {
        branch_name,
        new_branch_name,
    }: BranchRenameArgs,
) -> anyhow::Result<()> {
    let req = RenameBranch {
        name: &branch_name,
        new_name: &new_branch_name,
    };

    let branch = cli.roundtrip(req)?;
    info!(
        branch = branch_name,
        new_branch = branch.name,
        hash = branch.hash,
        "Renamed branch"
    );

    Ok(())
}
