use crate::cli::Cli;

#[derive(Debug, clap::Args)]
pub(crate) struct ConfigArgs {
    #[command(subcommand)]
    pub command: ConfigCommand,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum ConfigCommand {
    /// Set a configuration value
    Set(ConfigSetArgs),
    /// Get the current configuration
    Get(ConfigGetArgs),
}

#[derive(Debug, clap::Args)]
pub(crate) struct ConfigSetArgs {
    /// Name
    pub name: String,
    /// Value
    pub value: String,
}

#[derive(Debug, clap::Args)]
pub(crate) struct ConfigGetArgs {
    /// Name
    pub name: String,
    /// Show all the available profiles
    #[arg(short, long)]
    pub all: bool,
}

pub(crate) fn handle(_cli: &Cli, _args: ConfigArgs) -> anyhow::Result<()> {
    match _args.command {
        ConfigCommand::Set(_) => todo!(),
        ConfigCommand::Get(_) => todo!(),
    }
}
