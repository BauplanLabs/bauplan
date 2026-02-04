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
#[command(after_long_help = r#"Examples:
  bauplan config set api_key your_key  Set configuration value
"#)]
pub(crate) struct ConfigSetArgs {
    /// Name
    pub name: String,
    /// Value
    pub value: String,
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = r#"Examples:
  bauplan config get api_key       Get specific configuration
  bauplan config get --all         Get all profiles
"#)]
pub(crate) struct ConfigGetArgs {
    /// Show all the available profiles
    #[arg(short, long)]
    pub all: bool,
    /// Name
    pub name: String,
}

pub(crate) fn handle(_cli: &Cli, _args: ConfigArgs) -> anyhow::Result<()> {
    match _args.command {
        ConfigCommand::Set(_) => todo!(),
        ConfigCommand::Get(_) => todo!(),
    }
}
