use std::io::{Write as _, stdout};

use bauplan::{ApiErrorKind, tag::*};
use tabwriter::TabWriter;
use tracing::info;

use crate::cli::{Cli, Output, is_api_err_kind};

#[derive(Debug, clap::Args)]
pub(crate) struct TagArgs {
    #[command(subcommand)]
    pub command: TagCommand,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum TagCommand {
    /// List all available tags (default action)
    #[clap(alias = "list")]
    Ls(TagLsArgs),
    /// Create a new tag
    Create(TagCreateArgs),
    /// Delete a tag
    #[clap(alias = "delete")]
    Rm(TagRmArgs),
    /// Rename a tag
    Rename(TagRenameArgs),
}

#[derive(Debug, clap::Args)]
pub(crate) struct TagLsArgs {
    /// Filter by name (can be a regex)
    #[arg(long)]
    pub name: Option<String>,
    /// Limit the number of tags to show
    #[arg(long)]
    pub limit: Option<usize>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TagCreateArgs {
    /// Ref from which to create. If not specified, default is active branch
    #[arg(long)]
    pub from_ref: Option<String>,
    /// Do not fail if the tag already exists
    #[arg(long)]
    pub if_not_exists: bool,
    /// Tag name
    pub tag_name: String,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TagRmArgs {
    /// Do not fail if the tag does not exist
    #[arg(long)]
    pub if_exists: bool,
    /// Tag name
    pub tag_name: String,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TagRenameArgs {
    /// Tag name
    pub tag_name: String,
    /// New tag name
    pub new_tag_name: String,
}

pub(crate) fn handle(cli: &Cli, args: TagArgs) -> anyhow::Result<()> {
    match args.command {
        TagCommand::Ls(args) => list_tags(cli, args),
        TagCommand::Create(args) => create_tag(cli, args),
        TagCommand::Rm(args) => delete_tag(cli, args),
        TagCommand::Rename(args) => rename_tag(cli, args),
    }
}

fn list_tags(cli: &Cli, TagLsArgs { name, limit }: TagLsArgs) -> anyhow::Result<()> {
    let req = GetTags {
        filter_by_name: name.as_deref(),
    };

    let tags = bauplan::paginate(req, limit, |r| super::roundtrip(cli, r))?;

    match cli.global.output.unwrap_or_default() {
        Output::Json => {
            let all_tags = tags.collect::<anyhow::Result<Vec<_>>>()?;
            serde_json::to_writer(stdout(), &all_tags)?;
        }
        Output::Tty => {
            let mut tw = TabWriter::new(stdout());
            writeln!(&mut tw, "NAME\tHASH")?;
            for tag in tags {
                let tag = tag?;
                writeln!(&mut tw, "{}\t{}", tag.name, tag.hash)?;
            }

            tw.flush()?;
        }
    }

    Ok(())
}

fn create_tag(
    cli: &Cli,
    TagCreateArgs {
        from_ref,
        if_not_exists,
        tag_name,
    }: TagCreateArgs,
) -> anyhow::Result<()> {
    let from_ref = from_ref
        .as_deref()
        .or(cli.profile.active_branch.as_deref())
        .unwrap_or("main");

    let req = CreateTag {
        name: &tag_name,
        from_ref,
    };

    let result = super::roundtrip(cli, req);
    match result {
        Ok(tag) => {
            info!(tag = tag.name, "Created tag");
        }
        Err(e) if if_not_exists && is_api_err_kind(&e, ApiErrorKind::TagExists) => {
            info!(tag = tag_name, "Tag already exists");
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

fn delete_tag(
    cli: &Cli,
    TagRmArgs {
        if_exists,
        tag_name,
    }: TagRmArgs,
) -> anyhow::Result<()> {
    let req = DeleteTag { name: &tag_name };

    let result = super::roundtrip(cli, req);
    match result {
        Ok(tag) => {
            info!(tag = tag.name, "Deleted tag");
        }
        Err(e) if if_exists && is_api_err_kind(&e, ApiErrorKind::TagNotFound) => {
            info!(tag = tag_name, "Tag does not exist");
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

fn rename_tag(
    cli: &Cli,
    TagRenameArgs {
        tag_name,
        new_tag_name,
    }: TagRenameArgs,
) -> anyhow::Result<()> {
    let req = RenameTag {
        name: &tag_name,
        new_name: &new_tag_name,
    };

    let tag = super::roundtrip(cli, req)?;
    info!(
        tag = tag_name,
        new_tag = tag.name,
        hash = tag.hash,
        "Renamed tag"
    );

    Ok(())
}
