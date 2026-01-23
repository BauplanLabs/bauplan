use crate::cli::Cli;

#[derive(Debug, clap::Args)]
pub(crate) struct ParameterArgs {
    #[command(subcommand)]
    pub command: ParameterCommand,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum ParameterCommand {
    /// List all parameters in a project
    #[clap(alias = "list")]
    Ls(ParameterLsArgs),
    /// Remove a parameter from a project
    #[clap(alias = "delete")]
    Rm(ParameterRmArgs),
    /// Set a parameter value in a project
    Set(ParameterSetArgs),
}

#[derive(Debug, clap::Args)]
pub(crate) struct ParameterLsArgs {
    /// Path to the root Bauplan project directory.
    #[arg(short, long)]
    pub project_dir: Option<String>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct ParameterRmArgs {
    /// Name of the parameter to remove (required)
    #[arg(long)]
    pub name: Option<String>,
    /// Path to the root Bauplan project directory.
    #[arg(short, long)]
    pub project_dir: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Type {
    Int,
    Float,
    Bool,
    Str,
    Secret,
}

#[derive(Debug, clap::Args)]
pub(crate) struct ParameterSetArgs {
    /// Name of the parameter to set (required)
    #[arg(long)]
    pub name: Option<String>,
    /// Type of the parameter to set.
    #[arg(long)]
    pub r#type: Option<Type>,
    /// Value of the parameter to set
    #[arg(long)]
    pub value: Option<String>,
    /// Description of the parameter to set
    #[arg(long)]
    pub description: Option<String>,
    /// Mark the parameter as required
    #[arg(long)]
    pub required: bool,
    /// Mark the parameter as not required
    #[arg(long)]
    pub optional: bool,
    /// Read value from file
    #[arg(short, long)]
    pub file: Option<String>,
    /// Path to the root Bauplan project directory.
    #[arg(short, long)]
    pub project_dir: Option<String>,
}

pub(crate) fn handle(_cli: &Cli, _args: ParameterArgs) -> anyhow::Result<()> {
    match _args.command {
        ParameterCommand::Ls(_) => todo!(),
        ParameterCommand::Rm(_) => todo!(),
        ParameterCommand::Set(_) => todo!(),
    }
}
