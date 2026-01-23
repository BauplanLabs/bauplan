use crate::cli::Cli;

#[derive(Debug, clap::Args)]
pub(crate) struct TagArgs {
    #[command(subcommand)]
    pub command: TagCommand,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum TagCommand {
    /// List all available tags (default action)
    Ls(TagLsArgs),
    /// Create a new tag
    Create(TagCreateArgs),
    /// Delete a tag
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

pub(crate) fn handle(_cli: &Cli, _args: TagArgs) -> anyhow::Result<()> {
    match _args.command {
        TagCommand::Ls(_) => todo!(),
        TagCommand::Create(_) => todo!(),
        TagCommand::Rm(_) => todo!(),
        TagCommand::Rename(_) => todo!(),
    }
}
