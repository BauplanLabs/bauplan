use std::io::{Write as _, stdout};

use crate::cli::{
    Cli, Output, api_error, checkout, color::*, datetime::format_datetime, paginate::paginate,
};
use anyhow::bail;
use bauplan_api::branch::{
    BranchFilters, CreateBranchBody, CreateBranchError, DeleteBranchError, MergeBranchBody,
    MergeBranchError, MergeStrategy, RenameBranchBody, ResolveBranchError,
};
use bauplan_api::catalog_ref::{BetweenCommits, DiffChange, DiffEntry, DiffRefsError};
use bauplan_api::gropius::client::ClientError;
use bauplan_api::model::{Entity, ExpectedHead};
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
    /// Show the diff between two refs
    Diff(BranchDiffArgs),
    /// Merge a ref into a branch
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
    /// Filter by name. Use `^` and `$` to anchor it to the start or the end
    #[arg(short, long)]
    pub name: Option<String>,
    /// Limit the number of branches to show
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    pub limit: Option<u32>,
    /// Continue from the `pagination_token` of a previous `-O json` output
    #[arg(long)]
    pub pagination_token: Option<String>,
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
    /// Branch, tag or full commit hash to branch from [default: active branch]
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
  bauplan branch rm username.old_branch --expected-head <commit_hash>
"))]
pub(crate) struct BranchRmArgs {
    /// Branch name
    pub branch_name: String,
    /// Command succeeds even if the branch does not exist
    #[arg(long)]
    pub if_exists: bool,
    /// Delete only if the branch head is still at this full commit hash
    #[arg(long, value_name = "HASH")]
    pub expected_head: Option<String>,
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
  # What the active branch changes compared to main
  bauplan branch diff main

  # Diff between two specific refs
  bauplan branch diff main username.dev_branch
"))]
pub(crate) struct BranchDiffArgs {
    /// Branch, tag or full commit hash to compare from
    pub from_ref: String,
    /// Branch, tag or full commit hash to compare to [default: active branch]
    pub to_ref: Option<String>,
    /// Limit the number of entries to show
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    pub limit: Option<u32>,
    /// Continue from the `pagination_token` of a previous `-O json` output. Pass
    /// its `from_commit_hash` and `to_commit_hash` as the refs to read the same
    /// state
    #[arg(long)]
    pub pagination_token: Option<String>,
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  # Merge branch into active branch
  bauplan branch merge username.dev_branch

  # Merge with custom commit message
  bauplan branch merge username.feature --commit-message \"Merge feature updates\"

  # Merge into another branch, without checking it out
  bauplan branch merge username.feature --into-branch username.staging

  # Merge only if nobody moved the branch since you read its head
  bauplan branch merge username.feature --expected-head <commit_hash>
"))]
pub(crate) struct BranchMergeArgs {
    /// Branch, tag or full commit hash to merge
    #[arg(value_name = "REF")]
    pub from_ref: String,
    /// Branch to merge into [default: active branch]
    #[arg(short, long)]
    pub into_branch: Option<String>,
    /// Optional commit message
    #[arg(long)]
    pub commit_message: Option<String>,
    /// Merge only if the head of the branch is still at this full commit hash
    #[arg(long, value_name = "HASH")]
    pub expected_head: Option<String>,
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
        pagination_token,
        utc,
    } = args;

    let client = cli.branches();
    let filters = BranchFilters { owned: !all, name };

    // In JSON we print the list in the order of the server. The active branch
    // and main go first only in the terminal.
    if cli.global.output == Output::Json {
        let branches = paginate(limit, pagination_token, filters, |query| {
            client
                .list_branches(cli.profile.catalog.clone(), query)
                .map_err(api_error)
        })?;
        let page = branches.into_page()?;
        serde_json::to_writer(stdout(), &page)?;
        println!();
        return Ok(());
    }

    let active_branch = cli.active_branch();

    // On the first page without a name filter, we show the active branch and
    // main first, and they count toward the limit.
    let mut top_names = match (&filters.name, &pagination_token) {
        (None, None) => vec![active_branch, "main"],
        _ => Vec::new(),
    };
    top_names.dedup();

    let mut top = Vec::new();
    let mut missing_active = None;
    for &name in &top_names {
        if limit == Some(top.len() as u32) {
            break;
        }

        match client.resolve_branch((cli.profile.catalog.clone(), name.to_owned())) {
            Ok(branch) => top.push(branch),
            Err(ClientError::Api(err @ ResolveBranchError::BranchNotFound { .. })) => {
                if name == active_branch {
                    missing_active = Some(err);
                }
            }
            Err(err) => return Err(api_error(err)),
        }
    }

    // We ask for the whole limit, because the list can still contain the
    // branches we show first.
    let rest = paginate(limit, pagination_token, filters, |query| {
        client
            .list_branches(cli.profile.catalog.clone(), query)
            .map_err(api_error)
    })?
    .filter(|branch| !matches!(branch, Ok(branch) if top_names.contains(&branch.name.as_str())));
    let branches = top
        .into_iter()
        .map(Ok)
        .chain(rest)
        .take(limit.map_or(usize::MAX, |limit| limit as usize));

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

    if let Some(err) = missing_active {
        let err = cli.with_active_branch_tip(active_branch, err);
        anstream::eprintln!("{YELLOW}WARNING:{YELLOW:#} {err}");
    }

    Ok(())
}

fn get_branch(cli: &Cli, args: BranchGetArgs) -> anyhow::Result<()> {
    let BranchGetArgs { branch_name, utc } = args;

    let path = (cli.profile.catalog.clone(), branch_name);
    let branch = cli.branches().resolve_branch(path).map_err(api_error)?;

    match cli.global.output {
        Output::Json => {
            serde_json::to_writer(stdout(), &branch)?;
            println!();
        }
        Output::Tty => {
            let active_branch = cli.active_branch();
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

    let from_ref = from_ref.as_deref().unwrap_or(cli.active_branch());

    let body = CreateBranchBody {
        name: branch_name,
        from_ref: from_ref.to_owned(),
        ttl_seconds: None,
    };

    let catalog = cli.profile.catalog.clone();
    let branch = match cli.branches().create_branch(catalog, body) {
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
        Err(err) => return Err(api_error(err)),
    };

    eprintln!("Created branch {:?}", branch.name);
    anstream::eprintln!("{GREEN}TIP:{GREEN:#} To switch to the new branch, run:");
    eprintln!("\tbauplan checkout {:?}", branch.name);

    if cli.global.output == Output::Json {
        serde_json::to_writer(stdout(), &branch)?;
        println!();
    }

    Ok(())
}

fn delete_branch(cli: &Cli, args: BranchRmArgs) -> anyhow::Result<()> {
    let BranchRmArgs {
        branch_name,
        if_exists,
        expected_head,
    } = args;

    if cli.active_branch() == branch_name.as_str() {
        bail!("cannot delete the active branch {branch_name:?}");
    }

    let path = (cli.profile.catalog.clone(), branch_name.clone());
    let expected_head = expected_head.map(|hash| ExpectedHead {
        expected_head: Some(hash),
    });
    match cli.branches().delete_branch(path, expected_head) {
        Ok(_) => (),
        Err(ClientError::Api(err @ DeleteBranchError::BranchNotFound { .. })) if if_exists => {
            eprintln!("{err}");
            return Ok(());
        }
        Err(err) => return Err(api_error(err)),
    }

    eprintln!("Deleted branch {branch_name:?}");

    Ok(())
}

fn merge_branch(cli: &Cli, args: BranchMergeArgs) -> anyhow::Result<()> {
    let BranchMergeArgs {
        from_ref,
        into_branch,
        commit_message,
        expected_head,
    } = args;

    let into_branch = into_branch.as_deref().unwrap_or(cli.active_branch());

    let path = (cli.profile.catalog.clone(), into_branch.to_owned());
    let body = MergeBranchBody {
        from_ref: from_ref.clone(),
        strategy: MergeStrategy::Squash {
            message: commit_message,
        },
        expected_head,
    };

    let merged = match cli.branches().merge_branch(path, body) {
        Ok(merged) => merged,
        Err(ClientError::Api(err @ MergeBranchError::BranchNotFound { .. })) => {
            return Err(cli.with_active_branch_tip(into_branch, err));
        }
        Err(ClientError::Api(MergeBranchError::MergeConflict {
            commit_hash,
            conflicts,
        })) => {
            let mut message = format!("The merge on {commit_hash} has conflicts:\n");
            for entity in conflicts {
                let (kind, name) = entity_label(entity);
                message.push_str(&format!("\n  {kind:<9}  {name}"));
            }
            bail!(message);
        }
        Err(err) => return Err(api_error(err)),
    };

    match &merged {
        Some(commit) => eprintln!(
            "Merged {from_ref:?} into {into_branch:?} (commit {})",
            commit.hash
        ),
        None => eprintln!("Nothing to merge from {from_ref:?} into {into_branch:?}"),
    }

    if cli.global.output == Output::Json {
        serde_json::to_writer(stdout(), &merged)?;
        println!();
    }

    Ok(())
}

/// The kind and the name of an entity, as the diff and the merge show it.
fn entity_label(entity: Entity) -> (&'static str, String) {
    match entity {
        Entity::Table(name) => ("table", name),
        Entity::Namespace(name) => ("namespace", name),
        Entity::Unknown(value) => ("unknown", value.to_string()),
    }
}

fn rename_branch(cli: &Cli, args: BranchRenameArgs) -> anyhow::Result<()> {
    let BranchRenameArgs {
        branch_name,
        new_branch_name,
    } = args;

    let path = (cli.profile.catalog.clone(), branch_name.clone());
    let body = RenameBranchBody {
        name: new_branch_name,
    };
    let branch = cli
        .branches()
        .rename_branch(path, body)
        .map_err(api_error)?;
    eprintln!("Renamed branch {branch_name:?} to {:?}", branch.name);

    if cli.global.output == Output::Json {
        serde_json::to_writer(stdout(), &branch)?;
        println!();
    }

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
        from_ref,
        to_ref,
        limit,
        pagination_token,
    } = args;

    let to_ref = to_ref.unwrap_or_else(|| cli.active_branch().to_owned());

    if from_ref == to_ref {
        bail!("cannot compare {from_ref:?} with itself");
    }

    let client = cli.refs();
    let mut commits = None;
    let entries = paginate(limit, pagination_token, (), |query| {
        // We read the next pages at the commits of the first one, so the diff
        // stays the same if the refs move meanwhile.
        let (from_at, to_at) = commits
            .clone()
            .unwrap_or_else(|| (from_ref.clone(), to_ref.clone()));
        match client.diff_refs((cli.profile.catalog.clone(), from_at, to_at), query) {
            Ok(diff) => {
                commits.get_or_insert((diff.from_commit_hash, diff.to_commit_hash));
                Ok(diff.inner)
            }
            Err(ClientError::Api(DiffRefsError::RefNotFound { ref_name })) => {
                let missing = ref_name.clone();
                let err = DiffRefsError::RefNotFound { ref_name };
                Err(cli.with_active_branch_tip(&missing, err))
            }
            Err(err) => Err(api_error(err)),
        }
    })?;

    let page = entries.into_page()?;
    let (from_commit_hash, to_commit_hash) = commits.unwrap_or_default();

    if cli.global.output == Output::Json {
        let diff = BetweenCommits {
            inner: page,
            from_commit_hash,
            to_commit_hash,
        };
        serde_json::to_writer(stdout(), &diff)?;
        println!();
        return Ok(());
    }

    // Like a unified diff, `-` is only in the first ref and `+` only in the
    // second one.
    for (marker, ref_name, hash) in [
        ("---", from_ref, from_commit_hash),
        ("+++", to_ref, to_commit_hash),
    ] {
        if ref_name == hash {
            anstream::println!("{BOLD}{marker} {hash}{BOLD:#}");
        } else {
            anstream::println!("{BOLD}{marker} {hash}  {ref_name}{BOLD:#}");
        }
    }
    println!();

    let (mut added, mut changed, mut renamed, mut removed, mut unknown) = (0, 0, 0, 0, 0);
    let mut tw = TabWriter::new(anstream::stdout()).ansi(true);
    for DiffEntry { entity, change } in page.items {
        let (sign, style, count, from) = match change {
            DiffChange::Added => ("+", GREEN, &mut added, None),
            DiffChange::Changed => ("~", YELLOW, &mut changed, None),
            DiffChange::Renamed { from } => (">", BLUE, &mut renamed, Some(from)),
            DiffChange::Removed => ("-", RED, &mut removed, None),
            DiffChange::Unknown(_) => ("?", DIM, &mut unknown, None),
        };
        *count += 1;

        let (kind, name) = entity_label(entity);
        match from {
            Some(from) => writeln!(&mut tw, "{style}{sign} {kind}\t{from} -> {name}{style:#}")?,
            None => writeln!(&mut tw, "{style}{sign} {kind}\t{name}{style:#}")?,
        }
    }
    tw.flush()?;

    let summary = [
        (added, "added"),
        (changed, "changed"),
        (renamed, "renamed"),
        (removed, "removed"),
        (unknown, "unknown"),
    ]
    .iter()
    .filter(|(count, _)| *count > 0)
    .map(|(count, label)| format!("{count} {label}"))
    .collect::<Vec<_>>();
    if summary.is_empty() {
        println!("No differences");
    } else {
        println!();
        println!("{}", summary.join(", "));
    }

    Ok(())
}
