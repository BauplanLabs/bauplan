use std::{
    collections::BTreeMap,
    io::{Write as _, stdout},
};

use crate::cli::{Cli, Output, checkout, color::*, datetime::format_datetime, paginate::paginate};
use anyhow::bail;
use bauplan::table::{GetTables, Table};
use bauplan_api::branch::{
    BranchFilters, CreateBranchBody, CreateBranchError, DeleteBranchError, MergeBranchBody,
    MergeBranchError, MergeStrategy, RenameBranchBody, ResolveBranchError,
};
use bauplan_api::gropius::client::ClientError;
use bauplan_api::model::ExpectedHead;
use tabwriter::TabWriter;

#[derive(Debug, clap::Args)]
pub(crate) struct BranchArgs {
    #[command(subcommand)]
    pub command: BranchCommand,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum BranchCommand {
    /// List branches
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
#[command(after_long_help = CliExamples("
  # List the active branch, main and your branches
  bauplan branch ls

  # List the branches of everyone
  bauplan branch ls --all

  # List your branches whose name starts with username.feature
  bauplan branch ls --name '^username.feature'

  # Limit results
  bauplan branch ls --limit 5
"))]
pub(crate) struct BranchLsArgs {
    /// Show the branches of everyone, not only yours
    #[arg(short, long)]
    pub all: bool,
    /// Filter by name, `^` and `$` anchor it to the start or the end
    #[arg(short, long)]
    pub name: Option<String>,
    /// Limit the number of branches to show
    #[arg(long)]
    pub limit: Option<usize>,
    /// Use UTC for date display
    #[arg(short = 'z', long)]
    pub utc: bool,
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  # Create branch from active branch
  bauplan branch create username.dev_branch

  # Create branch from specific ref
  bauplan branch create username.new_feature --from-ref main

  # Create branch if it doesn't already exist
  bauplan branch create username.my_branch --if-not-exists
"))]
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
#[command(after_long_help = CliExamples("
  # Delete a branch
  bauplan branch rm username.old_branch

  # Conditionally delete
  bauplan branch rm username.maybe_branch --if-exists

  # Delete only if nobody moved the branch since you read its head
  bauplan branch rm username.old_branch --if-head <commit_hash>
"))]
pub(crate) struct BranchRmArgs {
    /// Branch name
    pub branch_name: String,
    /// Command succeeds even if the branch does not exist
    #[arg(long)]
    pub if_exists: bool,
    /// Delete only if the branch head is still at this full commit hash
    #[arg(long, value_name = "HASH")]
    pub if_head: Option<String>,
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  # Get branch information
  bauplan branch get username.dev_branch

  # Show the dates in UTC
  bauplan branch get username.dev_branch --utc
"))]
pub(crate) struct BranchGetArgs {
    /// Branch name
    pub branch_name: String,
    /// Use UTC for date display
    #[arg(short = 'z', long)]
    pub utc: bool,
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  bauplan branch checkout main
  bauplan branch checkout username.dev_branch
"))]
pub(crate) struct BranchCheckoutArgs {
    /// Branch name
    pub branch_name: String,
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  # Diff between active branch and another
  bauplan branch diff username.dev_branch

  # Diff between two specific branches
  bauplan branch diff main username.dev_branch

  # Diff with namespace filter
  bauplan branch diff username.branch1 username.branch2 --namespace raw_data
"))]
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
#[command(after_long_help = CliExamples("
  # Merge branch into active branch
  bauplan branch merge username.dev_branch

  # Merge with custom commit message
  bauplan branch merge username.feature --commit-message \"Merge feature updates\"

  # Merge only if nobody moved the active branch since you read its head
  bauplan branch merge username.feature --if-head <commit_hash>
"))]
pub(crate) struct BranchMergeArgs {
    /// Branch, tag or full commit hash to merge into the active branch
    #[arg(value_name = "REF")]
    pub branch_name: String,
    /// Optional commit message
    #[arg(long)]
    pub commit_message: Option<String>,
    /// Merge only if the head of the active branch is still at this full
    /// commit hash
    #[arg(long, value_name = "HASH")]
    pub if_head: Option<String>,
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  bauplan branch rename username.old_name username.new_name
"))]
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
        all,
        name,
        limit,
        utc,
    } = args;

    let client = cli.branches()?;
    let filters = BranchFilters { owned: !all, name };
    let active_branch = cli.profile.active_branch.as_deref().unwrap_or("main");

    // Without a name filter, we show the active branch and main first, and
    // they count toward the limit.
    let mut top_names = match filters.name {
        None => vec![active_branch, "main"],
        Some(_) => Vec::new(),
    };
    top_names.dedup();

    let mut top = Vec::new();
    let mut missing_active = None;
    for &name in &top_names {
        if limit == Some(top.len()) {
            break;
        }

        match client.resolve_branch((cli.global.catalog.clone(), name.to_owned())) {
            Ok(branch) => top.push(branch),
            Err(ClientError::Api(err @ ResolveBranchError::BranchNotFound { .. })) => {
                if name == active_branch {
                    missing_active = Some(err);
                }
            }
            Err(err) => return Err(err.into()),
        }
    }

    // We ask for the whole limit, because the list can still contain the
    // branches we show first.
    let rest = paginate(limit, filters, |query| {
        client.list_branches(cli.global.catalog.clone(), query)
    })?
    .filter(|branch| !matches!(branch, Ok(branch) if top_names.contains(&branch.name.as_str())));
    let branches = top
        .into_iter()
        .map(Ok)
        .chain(rest)
        .take(limit.unwrap_or(usize::MAX));

    match cli.global.output {
        Output::Json => {
            let all_branches = branches.collect::<anyhow::Result<Vec<_>>>()?;
            serde_json::to_writer(stdout(), &all_branches)?;
            println!();
        }
        Output::Tty => {
            let mut tw = TabWriter::new(anstream::stdout()).ansi(true);
            writeln!(&mut tw, "NAME\tHASH\tUPDATED")?;
            for branch in branches {
                let branch = branch?;
                let updated_at = format_datetime(Some(branch.updated_at), utc, true);

                if branch.name == active_branch {
                    writeln!(
                        &mut tw,
                        "{GREEN}{}{GREEN:#} {DIM}[active]{DIM:#}\t{}\t{updated_at}",
                        branch.name, branch.commit_hash
                    )?;
                } else {
                    writeln!(
                        &mut tw,
                        "{}\t{}\t{updated_at}",
                        branch.name, branch.commit_hash
                    )?;
                }
            }

            tw.flush()?;
        }
    }

    if let Some(err) = missing_active {
        let err = cli.with_active_branch_tip(active_branch, err);
        anstream::eprintln!("{YELLOW}WARNING:{YELLOW:#} {err}");
    }

    Ok(())
}

fn get_branch(cli: &Cli, args: BranchGetArgs) -> anyhow::Result<()> {
    let BranchGetArgs { branch_name, utc } = args;

    let path = (cli.global.catalog.clone(), branch_name);
    let branch = cli.branches()?.resolve_branch(path)?;

    match cli.global.output {
        Output::Json => {
            serde_json::to_writer(stdout(), &branch)?;
            println!();
        }
        Output::Tty => {
            let active_branch = cli.profile.active_branch.as_deref().unwrap_or("main");
            let active = if branch.name == active_branch {
                format!(" {DIM}[active]{DIM:#}")
            } else {
                String::new()
            };

            let mut tw = TabWriter::new(anstream::stdout()).ansi(true);
            writeln!(&mut tw, "{GREEN}Name{GREEN:#}\t{}{active}", branch.name)?;
            writeln!(&mut tw, "{GREEN}Commit{GREEN:#}\t{}", branch.commit_hash)?;
            writeln!(
                &mut tw,
                "{GREEN}Created at{GREEN:#}\t{}",
                format_datetime(Some(branch.created_at), utc, true)
            )?;
            writeln!(
                &mut tw,
                "{GREEN}Updated at{GREEN:#}\t{}",
                format_datetime(Some(branch.updated_at), utc, true)
            )?;
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

    let body = CreateBranchBody {
        name: branch_name,
        from_ref: from_ref.to_owned(),
        ttl_seconds: None,
    };

    let catalog = cli.global.catalog.clone();
    let branch = match cli.branches()?.create_branch(catalog, body) {
        Ok(branch) => branch,
        Err(ClientError::Api(err @ CreateBranchError::BranchNameConflict { .. }))
            if if_not_exists =>
        {
            eprintln!("{err}");
            return Ok(());
        }
        Err(ClientError::Api(err @ CreateBranchError::FromRefNotFound { .. })) => {
            return Err(cli.with_active_branch_tip(from_ref, err));
        }
        Err(err) => return Err(err.into()),
    };

    eprintln!("Created branch {:?}", branch.name);
    anstream::eprintln!("{GREEN}TIP:{GREEN:#} To switch to the new branch, run:");
    eprintln!("\tbauplan checkout {:?}", branch.name);
    Ok(())
}

fn delete_branch(cli: &Cli, args: BranchRmArgs) -> anyhow::Result<()> {
    let BranchRmArgs {
        branch_name,
        if_exists,
        if_head,
    } = args;

    if cli.profile.active_branch.as_deref().unwrap_or("main") == branch_name.as_str() {
        bail!("cannot delete the active branch {branch_name:?}");
    }

    let path = (cli.global.catalog.clone(), branch_name.clone());
    let expected_head = if_head.map(|hash| ExpectedHead {
        expected_commit_hash: Some(hash),
    });
    match cli.branches()?.delete_branch(path, expected_head) {
        Ok(_) => (),
        Err(ClientError::Api(err @ DeleteBranchError::BranchNotFound { .. })) if if_exists => {
            eprintln!("{err}");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    }

    eprintln!("Deleted branch {branch_name:?}");

    Ok(())
}

fn merge_branch(cli: &Cli, args: BranchMergeArgs) -> anyhow::Result<()> {
    let BranchMergeArgs {
        branch_name,
        commit_message,
        if_head,
    } = args;

    let into_branch = cli.profile.active_branch.as_deref().unwrap_or("main");

    let path = (cli.global.catalog.clone(), into_branch.to_owned());
    let body = MergeBranchBody {
        from_ref: branch_name.clone(),
        strategy: MergeStrategy::Squash {
            message: commit_message,
        },
        expected_head: ExpectedHead {
            expected_commit_hash: if_head,
        },
    };

    let merged = match cli.branches()?.merge_branch(path, body) {
        Ok(merged) => merged,
        Err(ClientError::Api(err @ MergeBranchError::BranchNotFound { .. })) => {
            return Err(cli.with_active_branch_tip(into_branch, err));
        }
        Err(err) => return Err(err.into()),
    };

    match merged {
        Some(commit) => eprintln!(
            "Merged \"{branch_name}\" into \"{into_branch}\" (commit {})",
            commit.hash
        ),
        None => eprintln!("Nothing to merge from \"{branch_name}\" into \"{into_branch}\""),
    }

    Ok(())
}

fn rename_branch(cli: &Cli, args: BranchRenameArgs) -> anyhow::Result<()> {
    let BranchRenameArgs {
        branch_name,
        new_branch_name,
    } = args;

    let path = (cli.global.catalog.clone(), branch_name.clone());
    let body = RenameBranchBody {
        name: new_branch_name,
    };
    let branch = cli.branches()?.rename_branch(path, body)?;
    eprintln!("Renamed branch {branch_name:?} to {:?}", branch.name);

    if cli.profile.active_branch.as_deref() == Some(branch_name.as_str()) {
        checkout::set_active_branch(cli, &branch.name)?;
    }

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

    match cli.global.output {
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
            anstream::eprintln!("{BOLD}diff --bauplan a/{branch_name_a} b/{branch_b}{BOLD:#}");

            for (k, t) in &tables_b {
                if !tables_a.contains_key(k.as_str()) {
                    anstream::eprintln!("{GREEN}+{} {}{GREEN:#}", t.kind, t.fqn());
                }
            }

            for (k, t) in &tables_a {
                if !tables_b.contains_key(k.as_str()) {
                    anstream::eprintln!("{RED}-{} {}{RED:#}", t.kind, t.fqn());
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
