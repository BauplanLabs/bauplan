use std::io::{Write as _, stdout};

use bauplan_api::gropius::client::ClientError;
use bauplan_api::tag::{CreateTagBody, CreateTagError, DeleteTagError, RenameTagBody, TagFilters};
use tabwriter::TabWriter;

use crate::cli::{
    Cli, Output, api_error,
    color::{CliExamples, GREEN},
    datetime::format_datetime,
    paginate::paginate,
};

#[derive(Debug, clap::Args)]
pub(crate) struct TagArgs {
    #[command(subcommand)]
    pub command: TagCommand,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum TagCommand {
    /// List all available tags
    #[clap(alias = "list")]
    Ls(TagLsArgs),
    /// Get information about a tag
    Get(TagGetArgs),
    /// Create a new tag
    Create(TagCreateArgs),
    /// Delete a tag
    #[clap(alias = "delete")]
    Rm(TagRmArgs),
    /// Rename a tag
    Rename(TagRenameArgs),
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  # List all tags
  bauplan tag ls

  # List the tags whose name contains release
  bauplan tag ls --name release

  # Limit results
  bauplan tag ls --limit 10
"))]
pub(crate) struct TagLsArgs {
    /// Filter by name. Use `^` and `$` to anchor it to the start or the end
    #[arg(short, long)]
    pub name: Option<String>,
    /// Limit the number of tags to show
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
  # Get tag information
  bauplan tag get v1.0

  # Show the dates in UTC
  bauplan tag get v1.0 --utc
"))]
pub(crate) struct TagGetArgs {
    /// Tag name
    pub tag_name: String,
    /// Use UTC for date display
    #[arg(short = 'z', long)]
    pub utc: bool,
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  # Create tag from active branch
  bauplan tag create v1.0

  # Create tag from specific ref
  bauplan tag create v1.0 --from-ref main

  # Create tag if it doesn't already exist
  bauplan tag create v1.0 --if-not-exists
"))]
pub(crate) struct TagCreateArgs {
    /// Tag name
    pub tag_name: String,
    /// Branch, tag or full commit hash to create the tag from [default: active branch]
    #[arg(long)]
    pub from_ref: Option<String>,
    /// Do not fail if the tag already exists
    #[arg(long)]
    pub if_not_exists: bool,
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  # Delete a tag
  bauplan tag rm v1.0

  # Conditionally delete
  bauplan tag rm v1.0 --if-exists
"))]
pub(crate) struct TagRmArgs {
    /// Tag name
    pub tag_name: String,
    /// Do not fail if the tag does not exist
    #[arg(long)]
    pub if_exists: bool,
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  bauplan tag rename v1.0 v1.0-stable
"))]
pub(crate) struct TagRenameArgs {
    /// Tag name
    pub tag_name: String,
    /// New tag name
    pub new_tag_name: String,
}

pub(crate) fn handle(cli: &Cli, args: TagArgs) -> anyhow::Result<()> {
    match args.command {
        TagCommand::Ls(args) => list_tags(cli, args),
        TagCommand::Get(args) => get_tag(cli, args),
        TagCommand::Create(args) => create_tag(cli, args),
        TagCommand::Rm(args) => delete_tag(cli, args),
        TagCommand::Rename(args) => rename_tag(cli, args),
    }
}

fn get_tag(cli: &Cli, TagGetArgs { tag_name, utc }: TagGetArgs) -> anyhow::Result<()> {
    let path = (cli.profile.catalog.clone(), tag_name);
    let tag = cli.tags().resolve_tag(path).map_err(api_error)?;

    match cli.global.output {
        Output::Json => {
            serde_json::to_writer(stdout(), &tag)?;
            println!();
        }
        Output::Tty => {
            let mut tw = TabWriter::new(anstream::stdout()).ansi(true);
            writeln!(&mut tw, "{GREEN}Name{GREEN:#}\t{}", tag.name)?;
            writeln!(&mut tw, "{GREEN}Commit{GREEN:#}\t{}", tag.commit_hash)?;
            writeln!(
                &mut tw,
                "{GREEN}Created at{GREEN:#}\t{}",
                format_datetime(Some(tag.created_at), utc, true)
            )?;
            writeln!(
                &mut tw,
                "{GREEN}Updated at{GREEN:#}\t{}",
                format_datetime(Some(tag.updated_at), utc, true)
            )?;
            tw.flush()?;
        }
    }

    Ok(())
}

fn list_tags(
    cli: &Cli,
    TagLsArgs {
        name,
        limit,
        pagination_token,
        utc,
    }: TagLsArgs,
) -> anyhow::Result<()> {
    let client = cli.tags();
    let filters = TagFilters { name };
    let tags = paginate(limit, pagination_token, filters, |query| {
        client
            .list_tags(cli.profile.catalog.clone(), query)
            .map_err(api_error)
    })?;

    match cli.global.output {
        Output::Json => {
            let page = tags.into_page()?;
            serde_json::to_writer(stdout(), &page)?;
            println!();
        }
        Output::Tty => {
            let mut tw = TabWriter::new(anstream::stdout()).ansi(true);
            writeln!(&mut tw, "NAME\tHASH\tUPDATED")?;
            for tag in tags {
                let tag = tag?;
                let updated_at = format_datetime(Some(tag.updated_at), utc, true);
                writeln!(&mut tw, "{}\t{}\t{updated_at}", tag.name, tag.commit_hash)?;
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
    let from_ref = from_ref.as_deref().unwrap_or(cli.active_branch());

    let body = CreateTagBody {
        name: tag_name,
        from_ref: from_ref.to_owned(),
    };

    let tag = match cli.tags().create_tag(cli.profile.catalog.clone(), body) {
        Ok(tag) => tag,
        Err(ClientError::Api(err @ CreateTagError::TagNameConflict { .. })) if if_not_exists => {
            eprintln!("{err}");
            return Ok(());
        }
        Err(ClientError::Api(err @ CreateTagError::FromRefNotFound { .. })) => {
            return Err(cli.with_active_branch_tip(from_ref, err));
        }
        Err(err) => return Err(api_error(err)),
    };

    eprintln!("Created tag {:?}", tag.name);

    if cli.global.output == Output::Json {
        serde_json::to_writer(stdout(), &tag)?;
        println!();
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
    let path = (cli.profile.catalog.clone(), tag_name.clone());
    match cli.tags().delete_tag(path) {
        Ok(_) => (),
        Err(ClientError::Api(err @ DeleteTagError::TagNotFound { .. })) if if_exists => {
            eprintln!("{err}");
            return Ok(());
        }
        Err(err) => return Err(api_error(err)),
    }

    eprintln!("Deleted tag {tag_name:?}");
    Ok(())
}

fn rename_tag(
    cli: &Cli,
    TagRenameArgs {
        tag_name,
        new_tag_name,
    }: TagRenameArgs,
) -> anyhow::Result<()> {
    let path = (cli.profile.catalog.clone(), tag_name.clone());
    let body = RenameTagBody { name: new_tag_name };
    let tag = cli.tags().rename_tag(path, body).map_err(api_error)?;
    eprintln!("Renamed tag {tag_name:?} to {:?}", tag.name);

    if cli.global.output == Output::Json {
        serde_json::to_writer(stdout(), &tag)?;
        println!();
    }

    Ok(())
}
