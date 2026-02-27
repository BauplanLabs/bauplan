use std::collections::BTreeMap;
use std::io::{Write, stdout};

use bauplan::commit::{Commit, GetCommits};

use crate::cli::{Cli, Output, color::*};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Format {
    Oneline,
    Short,
    #[default]
    Medium,
    Full,
    Fuller,
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  # Show recent commits on active branch
  bauplan commit

  # Show commits from specific branch
  bauplan commit main

  # Show more commits
  bauplan commit --max-count 20

  # Show commits by specific author
  bauplan commit --author-username john_doe

  # Show commits matching message pattern
  bauplan commit --message \"^fix.*\" --max-count 5

  # Show commits in oneline format
  bauplan commit --format oneline
"))]
pub(crate) struct CommitArgs {
    /// Ref or branch name to get commits from [default: active branch]
    pub ref_name: Option<String>,
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
    #[arg(short = 'n', long, visible_alias = "limit", default_value = "10")]
    pub max_count: usize,
    /// How to format commits.
    #[arg(long, alias = "pretty")]
    pub format: Option<Format>,
}

pub(crate) fn handle(cli: &Cli, args: CommitArgs) -> anyhow::Result<()> {
    // Positional ref_name takes precedence over --ref flag.
    let at_ref = args
        .ref_name
        .as_deref()
        .or(cli.profile.active_branch.as_deref())
        .unwrap_or("main");

    let properties: BTreeMap<String, String> = args
        .property
        .iter()
        .filter_map(|p| {
            let (k, v) = p.split_once('=')?;
            Some((k.to_string(), v.to_string()))
        })
        .collect();

    let filter_by_properties = if properties.is_empty() {
        None
    } else {
        Some(&properties)
    };

    let req = GetCommits {
        at_ref,
        filter_by_message: args.message.as_deref(),
        filter_by_author_username: args.author_username.as_deref(),
        filter_by_author_name: args.author_name.as_deref(),
        filter_by_author_email: args.author_email.as_deref(),
        filter_by_authored_date: None,
        filter_by_authored_date_start_at: None,
        filter_by_authored_date_end_at: None,
        filter_by_parent_hash: None,
        filter_by_properties,
        filter: None,
    };

    let commits = bauplan::paginate(req, Some(args.max_count), |r| cli.roundtrip(r))?;

    match cli.global.output.unwrap_or_default() {
        Output::Json => {
            let all_commits = commits.collect::<anyhow::Result<Vec<_>>>()?;
            serde_json::to_writer(stdout(), &all_commits)?;
            println!();
        }
        Output::Tty => {
            let mut out = anstream::stdout().lock();
            for commit in commits {
                let commit = commit?;
                print_commit(&mut out, &commit, args.format.unwrap_or_default())?;
            }
        }
    }

    Ok(())
}

fn print_commit(out: &mut impl Write, commit: &Commit, format: Format) -> std::io::Result<()> {
    match format {
        Format::Oneline => {
            let subject = commit.subject().unwrap_or("");
            writeln!(out, "{}\t{}", commit.hash(), subject)?;
        }
        Format::Short => {
            writeln!(out, "{YELLOW}commit {}{YELLOW:#}", commit.hash())?;
            if let Some(author) = commit.author() {
                writeln!(out, "Author: {}", format_actor(author))?;
            }
            writeln!(out)?;
            if let Some(subject) = commit.subject() {
                writeln!(out, "    {subject}")?;
            }
            writeln!(out)?;
        }
        Format::Medium => {
            writeln!(out, "{YELLOW}commit {}{YELLOW:#}", commit.hash())?;
            if let Some(author) = commit.author() {
                writeln!(out, "Author: {}", format_actor(author))?;
            }
            writeln!(out, "Date: {}", format_date(&commit.authored_date))?;
            writeln!(out)?;
            if let Some(subject) = commit.subject() {
                writeln!(out, "    {subject}")?;
            }
            if let Some(body) = commit.body() {
                writeln!(out)?;
                for line in body.lines() {
                    writeln!(out, "    {line}")?;
                }
            }
            writeln!(out)?;
        }
        Format::Full => {
            writeln!(out, "{YELLOW}commit {}{YELLOW:#}", commit.hash())?;
            if let Some(author) = commit.author() {
                writeln!(out, "Author: {}", format_actor(author))?;
            }
            print_properties(out, commit)?;
            writeln!(out)?;
            if let Some(subject) = commit.subject() {
                writeln!(out, "    {subject}")?;
            }
            if let Some(body) = commit.body() {
                writeln!(out)?;
                for line in body.lines() {
                    writeln!(out, "    {line}")?;
                }
            }
            writeln!(out)?;
        }
        Format::Fuller => {
            writeln!(out, "{YELLOW}commit {}{YELLOW:#}", commit.hash())?;
            if let Some(author) = commit.author() {
                writeln!(out, "Author: {}", format_actor(author))?;
            }
            writeln!(out, "Author Date: {}", format_date(&commit.authored_date))?;
            writeln!(out, "Commit Date: {}", format_date(&commit.committed_date))?;
            print_properties(out, commit)?;
            writeln!(out)?;
            if let Some(subject) = commit.subject() {
                writeln!(out, "    {subject}")?;
            }
            if let Some(body) = commit.body() {
                writeln!(out)?;
                for line in body.lines() {
                    writeln!(out, "    {line}")?;
                }
            }
            writeln!(out)?;
        }
    }

    Ok(())
}

fn print_properties(out: &mut impl Write, commit: &Commit) -> std::io::Result<()> {
    if !commit.properties.is_empty() {
        writeln!(out, "Properties:")?;
        for (k, v) in &commit.properties {
            writeln!(out, "    {k} = {v}")?;
        }
    }
    Ok(())
}

fn format_date(date: &chrono::DateTime<chrono::Utc>) -> String {
    date.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string()
}

fn format_actor(actor: &bauplan::commit::Actor) -> String {
    match &actor.email {
        Some(email) => format!("{} <{}>", actor.name, email),
        None => actor.name.clone(),
    }
}
