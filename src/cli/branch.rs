use std::{
    collections::BTreeMap,
    io::{Write as _, stdout},
};

use anyhow::bail;
use bauplan::{
    ApiErrorKind,
    branch::*,
    table::{GetTables, Table},
};
use tabwriter::TabWriter;
use yansi::Paint;

use crate::cli::{Cli, Output, api_err_kind, checkout};

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
    /// Ref to branch from [default: active branch]
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
    /// Filter by namespace (exact match or regex)
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
    /// Filter by namespace (exact match or regex)
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

#[derive(serde::Serialize)]
struct JsonDiff<'a> {
    added: Vec<&'a Table>,
    removed: Vec<&'a Table>,
}

pub(crate) fn handle(cli: &Cli, args: BranchArgs) -> anyhow::Result<()> {
    match args.command {
        BranchCommand::Ls(args) => list_branches(cli, args),
        BranchCommand::Create(args) => create_branch(cli, args),
        BranchCommand::Rm(args) => delete_branch(cli, args),
        BranchCommand::Get(args) => get_branch(cli, args),
        BranchCommand::Checkout(args) => checkout_branch(cli, args),
        BranchCommand::Diff(args) => diff_branch(cli, args),
        BranchCommand::Merge(args) => merge_branch(cli, args),
        BranchCommand::Rename(args) => rename_branch(cli, args),
    }
}

fn list_branches(cli: &Cli, args: BranchLsArgs) -> anyhow::Result<()> {
    let BranchLsArgs {
        branch_name,
        all_zones,
        name,
        user,
        limit,
    } = args;

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

fn get_branch(cli: &Cli, args: BranchGetArgs) -> anyhow::Result<()> {
    let BranchGetArgs {
        branch_name,
        namespace,
    } = args;

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

fn create_branch(cli: &Cli, args: BranchCreateArgs) -> anyhow::Result<()> {
    let BranchCreateArgs {
        branch_name,
        from_ref,
        if_not_exists,
    } = args;

    let from_ref = from_ref
        .as_deref()
        .or(cli.profile.active_branch.as_deref())
        .unwrap_or("main");

    let req = CreateBranch {
        name: &branch_name,
        from_ref,
    };

    if let Err(e) = cli.roundtrip(req) {
        if if_not_exists && matches!(api_err_kind(&e), Some(ApiErrorKind::BranchExists { .. })) {
            eprintln!("Branch {branch_name:?} already exists");
            return Ok(());
        } else {
            return Err(e);
        }
    }

    eprintln!("Created branch \"{branch_name}\"");
    eprintln!(
        "{} To create and switch to a branch in one command, run:",
        "TIP:".green()
    );
    eprintln!("\tbauplan checkout -b {branch_name:?}");
    Ok(())
}

fn delete_branch(cli: &Cli, args: BranchRmArgs) -> anyhow::Result<()> {
    let BranchRmArgs {
        branch_name,
        if_exists,
    } = args;

    let req = DeleteBranch { name: &branch_name };

    if let Err(e) = cli.roundtrip(req) {
        if if_exists && matches!(api_err_kind(&e), Some(ApiErrorKind::BranchNotFound { .. })) {
            eprintln!("Branch \"{branch_name}\" does not exist");
            return Ok(());
        } else {
            return Err(e);
        }
    }

    eprintln!("Deleted branch \"{branch_name}\"");

    Ok(())
}

fn merge_branch(cli: &Cli, args: BranchMergeArgs) -> anyhow::Result<()> {
    let BranchMergeArgs {
        branch_name,
        commit_message,
    } = args;

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
    eprintln!("Merged branch \"{branch_name}\" into \"{into_branch}\"");

    Ok(())
}

fn rename_branch(cli: &Cli, args: BranchRenameArgs) -> anyhow::Result<()> {
    let BranchRenameArgs {
        branch_name,
        new_branch_name,
    } = args;

    let req = RenameBranch {
        name: &branch_name,
        new_name: &new_branch_name,
    };

    cli.roundtrip(req)?;
    eprintln!("Renamed branch \"{branch_name}\" to \"{new_branch_name}\"");

    Ok(())
}

fn checkout_branch(cli: &Cli, args: BranchCheckoutArgs) -> anyhow::Result<()> {
    let BranchCheckoutArgs { branch_name } = args;
    checkout::switch_branch(cli, &branch_name)
}

fn diff_branch(cli: &Cli, args: BranchDiffArgs) -> anyhow::Result<()> {
    let BranchDiffArgs {
        branch_name_a,
        branch_name_b,
        namespace,
    } = args;

    let branch_a = branch_name_a.as_str();
    let branch_b = branch_name_b
        .as_deref()
        .or(cli.profile.active_branch.as_deref())
        .unwrap_or("main");

    if branch_a == branch_b {
        bail!("can not compare branch {branch_a:?} with itself");
    }

    let tables_a = collect_tables(cli, branch_a, namespace.as_deref())?;
    let tables_b = collect_tables(cli, branch_b, namespace.as_deref())?;

    match cli.global.output.unwrap_or_default() {
        Output::Json => {
            let added: Vec<_> = tables_b
                .iter()
                .filter_map(|(fqn, table)| {
                    if !tables_a.contains_key(fqn.as_str()) {
                        Some(table)
                    } else {
                        None
                    }
                })
                .collect();
            let removed: Vec<_> = tables_a
                .iter()
                .filter_map(|(fqn, table)| {
                    if !tables_b.contains_key(fqn.as_str()) {
                        Some(table)
                    } else {
                        None
                    }
                })
                .collect();

            serde_json::to_writer(stdout(), &JsonDiff { added, removed })?;
            println!();
        }
        Output::Tty => {
            eprintln!(
                "{}",
                format!("diff --bauplan a/{branch_name_a} b/{branch_b}").bold()
            );

            for (k, t) in &tables_b {
                if !tables_a.contains_key(k.as_str()) {
                    eprintln!("{}", format!("+{} {}", t.kind, t.fqn()).green());
                }
            }

            for (k, t) in &tables_a {
                if !tables_b.contains_key(k.as_str()) {
                    eprintln!("{}", format!("-{} {}", t.kind, t.fqn()).red());
                }
            }
        }
    }

    Ok(())
}

fn collect_tables(
    cli: &Cli,
    at_ref: &str,
    filter_by_namespace: Option<&str>,
) -> anyhow::Result<BTreeMap<String, Table>> {
    let req = GetTables {
        at_ref,
        filter_by_namespace,
        filter_by_name: None,
    };

    let mut out = BTreeMap::new();
    for table in bauplan::paginate(req, None, |r| cli.roundtrip(r))? {
        let table = table?;
        out.insert(table.fqn(), table);
    }

    Ok(out)
}
