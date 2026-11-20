use std::io::{Write, stdout};

use bauplan_api::catalog_ref::ListHistoryError;
use bauplan_api::gropius::client::ClientError;
use bauplan_api::model::{AtCommit, Commit};

use crate::cli::{
    Cli, Output, api_error,
    color::{CliExamples, YELLOW},
    datetime::format_datetime,
    paginate::paginate,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Format {
    Oneline,
    Short,
    #[default]
    Medium,
    Full,
    Fuller,
}

impl std::fmt::Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Format::Oneline => write!(f, "oneline"),
            Format::Short => write!(f, "short"),
            Format::Medium => write!(f, "medium"),
            Format::Full => write!(f, "full"),
            Format::Fuller => write!(f, "fuller"),
        }
    }
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  # Show recent commits on active branch
  bauplan commit

  # Show commits from specific branch
  bauplan commit main

  # Show more commits
  bauplan commit --max-count 20

  # Show commits in oneline format
  bauplan commit --format oneline
"))]
pub(crate) struct CommitArgs {
    /// Branch, tag or full commit hash to get the commits from [default: active branch]
    pub ref_name: Option<String>,
    /// Limit the number of commits to show
    #[arg(
        short = 'n',
        long,
        visible_alias = "limit",
        default_value = "10",
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    pub max_count: u32,
    /// Continue from the `pagination_token` of a previous `-O json` output. Pass
    /// its `commit_hash` as the ref to read the same state
    #[arg(long)]
    pub pagination_token: Option<String>,
    /// How to format commits.
    #[arg(long, default_value_t = Format::default(), alias = "pretty")]
    pub format: Format,
    /// Use UTC for date display
    #[arg(short = 'z', long)]
    pub utc: bool,
}

pub(crate) fn handle(cli: &Cli, args: CommitArgs) -> anyhow::Result<()> {
    let CommitArgs {
        ref_name,
        max_count,
        pagination_token,
        format,
        utc,
    } = args;

    let at_ref = ref_name.as_deref().unwrap_or(cli.active_branch());

    let client = cli.refs();
    let mut commit_hash = None;
    let commits = paginate(Some(max_count), pagination_token, (), |query| {
        // We read the next pages at the commit of the first one, so the list
        // stays the same if the ref moves meanwhile.
        let at = commit_hash.as_deref().unwrap_or(at_ref);
        let path = (cli.profile.catalog.clone(), at.to_owned());
        match client.list_history(path, query) {
            Ok(page) => {
                commit_hash.get_or_insert(page.commit_hash);
                Ok(page.inner)
            }
            Err(ClientError::Api(err @ ListHistoryError::RefNotFound { .. })) => {
                Err(cli.with_active_branch_tip(at_ref, err))
            }
            Err(err) => Err(api_error(err)),
        }
    })?;

    match cli.global.output {
        Output::Json => {
            let page = AtCommit {
                inner: commits.into_page()?,
                commit_hash: commit_hash.unwrap_or_default(),
            };
            serde_json::to_writer(stdout(), &page)?;
            println!();
        }
        Output::Tty => {
            let mut out = anstream::stdout().lock();
            for commit in commits {
                print_commit(&mut out, &commit?, format, utc)?;
            }
        }
    }

    Ok(())
}

/// Prints a commit like `git log --pretty=<format>`, plus the properties in
/// `full` and `fuller`.
fn print_commit(
    out: &mut impl Write,
    commit: &Commit,
    format: Format,
    utc: bool,
) -> std::io::Result<()> {
    let author = format_actor(&commit.author_name, commit.author_email.as_deref());
    let committer = format_actor(&commit.committer_name, commit.committer_email.as_deref());
    let authored_at = format_datetime(Some(commit.authored_at), utc, false);
    let committed_at = format_datetime(Some(commit.committed_at), utc, false);

    let header = match format {
        Format::Oneline => return writeln!(out, "{} {}", commit.hash, commit.title),
        Format::Short => format!("Author: {author}\n"),
        Format::Medium => format!("Author: {author}\nDate:   {authored_at}\n"),
        Format::Full => format!("Author: {author}\nCommit: {committer}\n"),
        Format::Fuller => format!(
            "Author:     {author}\nAuthorDate: {authored_at}\n\
             Commit:     {committer}\nCommitDate: {committed_at}\n"
        ),
    };

    writeln!(out, "{YELLOW}commit {}{YELLOW:#}", commit.hash)?;
    if let (Some(parent_hash), Some(merge_parent_hash)) =
        (&commit.parent_hash, &commit.merge_parent_hash)
    {
        writeln!(out, "Merge: {parent_hash} {merge_parent_hash}")?;
    }

    write!(out, "{header}")?;

    if matches!(format, Format::Full | Format::Fuller) && !commit.properties.is_empty() {
        writeln!(out, "Properties:")?;
        for (key, value) in &commit.properties {
            writeln!(out, "    {key} = {value}")?;
        }
    }

    let message = match format {
        Format::Short => &commit.title,
        _ => &commit.message,
    };
    writeln!(out)?;
    for line in message.lines() {
        writeln!(out, "    {line}")?;
    }
    writeln!(out)
}

fn format_actor(name: &str, email: Option<&str>) -> String {
    match email {
        Some(email) => format!("{name} <{email}>"),
        None => name.to_owned(),
    }
}
