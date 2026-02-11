use std::io::{Write as _, stdout};

use bauplan::{ApiErrorKind, tag::*};
use tabwriter::TabWriter;

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
    /// Tag name
    pub tag_name: String,
    /// Ref to create the tag from [default: active branch]
    #[arg(long)]
    pub from_ref: Option<String>,
    /// Do not fail if the tag already exists
    #[arg(long)]
    pub if_not_exists: bool,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TagRmArgs {
    /// Tag name
    pub tag_name: String,
    /// Do not fail if the tag does not exist
    #[arg(long)]
    pub if_exists: bool,
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

    let tags = bauplan::paginate(req, limit, |r| cli.roundtrip(r))?;

    match cli.global.output.unwrap_or_default() {
        Output::Json => {
            let all_tags = tags.collect::<anyhow::Result<Vec<_>>>()?;
            serde_json::to_writer(stdout(), &all_tags)?;
            println!();
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
        tag_name,
        from_ref,
        if_not_exists,
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

    let result = cli.roundtrip(req);
    match result {
        Ok(_) => {
            eprintln!("Created tag {tag_name:?}");
        }
        Err(e) if if_not_exists && is_api_err_kind(&e, ApiErrorKind::TagExists) => {
            eprintln!("Tag {tag_name:?} already exists");
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

fn delete_tag(
    cli: &Cli,
    TagRmArgs {
        tag_name,
        if_exists,
    }: TagRmArgs,
) -> anyhow::Result<()> {
    let req = DeleteTag { name: &tag_name };

    let result = cli.roundtrip(req);
    match result {
        Ok(_) => {
            eprintln!("Deleted tag {tag_name:?}");
        }
        Err(e) if if_exists && is_api_err_kind(&e, ApiErrorKind::TagNotFound) => {
            eprintln!("Tag {tag_name:?} does not exist");
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

    cli.roundtrip(req)?;
    eprintln!("Renamed tag {tag_name:?} to {new_tag_name:?}");

    Ok(())
}
