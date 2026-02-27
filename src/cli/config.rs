use std::io::Write;

use bauplan::Profile;
use tabwriter::TabWriter;

use crate::cli::{GlobalArgs, Output, color::*, yaml};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "snake_case")]
pub(crate) enum ConfigSetting {
    ApiKey,
    ActiveBranch,
}

impl std::fmt::Display for ConfigSetting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigSetting::ApiKey => f.write_str("api_key"),
            ConfigSetting::ActiveBranch => f.write_str("active_branch"),
        }
    }
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  # Set configuration value
  bauplan config set api_key your_key
"))]
pub(crate) struct ConfigSetArgs {
    /// Setting name
    pub name: ConfigSetting,
    /// Value to set
    pub value: String,
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  # Get specific configuration
  bauplan config get api_key

  # Get all profiles
  bauplan config get --all
"))]
pub(crate) struct ConfigGetArgs {
    /// Show all the available profiles
    #[arg(short, long)]
    pub all: bool,
}

pub(crate) fn handle(args: ConfigArgs, global: GlobalArgs) -> anyhow::Result<()> {
    match args.command {
        ConfigCommand::Set(args) => config_set(args, global),
        ConfigCommand::Get(args) => config_get(args, global),
    }
}

fn config_set(args: ConfigSetArgs, global: GlobalArgs) -> anyhow::Result<()> {
    let key = args.name.to_string();

    let profile = match global.profile {
        Some(name) => Profile::from_env(&name)?,
        None => Profile::from_default_env()?,
    };

    yaml::edit(&profile.config_path, |doc| {
        let mut profile = yaml::mapping_at_path(doc, &["profiles", &profile.name])?;
        yaml::upsert_str(&mut profile, &key, &args.value);

        // Setting a new API key resets the active branch to "main".
        if args.name == ConfigSetting::ApiKey {
            yaml::upsert_str(&mut profile, "active_branch", "main");
        }

        Ok(())
    })?;

    eprintln!("Set {key} for profile {:?}", profile.name);

    if args.name == ConfigSetting::ApiKey {
        eprintln!(
            "Active branch reset to \"main\" for profile {:?}",
            &profile.name
        );
    }

    Ok(())
}

fn config_get(args: ConfigGetArgs, global: GlobalArgs) -> anyhow::Result<()> {
    let mut out = anstream::stdout().lock();

    match (global.output.unwrap_or_default(), args.all) {
        (Output::Tty, false) => {
            let profile = match global.profile {
                Some(name) => Profile::from_env(&name)?,
                None => Profile::from_default_env()?,
            };

            let mut tw = TabWriter::new(&mut out).ansi(true);
            print_profile(&mut tw, &profile)?;
        }
        (Output::Tty, true) => {
            let mut tw = TabWriter::new(&mut out).ansi(true);
            for (i, profile) in Profile::load_all()?.enumerate() {
                if i > 0 {
                    writeln!(&mut tw)?;
                }
                print_profile(&mut tw, &profile)?;
            }
        }
        (Output::Json, false) => {
            let profile = match global.profile {
                Some(name) => Profile::from_env(&name)?,
                None => Profile::from_default_env()?,
            };

            serde_json::to_writer(&mut out, &profile)?;
            writeln!(&mut out)?;
        }
        (Output::Json, true) => {
            let profiles: Vec<_> = Profile::load_all()?.collect();
            serde_json::to_writer(&mut out, &profiles)?;
            writeln!(&mut out)?;
        }
    }

    Ok(())
}

fn print_profile(out: &mut impl Write, profile: &bauplan::Profile) -> anyhow::Result<()> {
    let active_branch = profile.active_branch.as_deref().unwrap_or("main");

    writeln!(out, "{HEADER}Profile {:?}{HEADER:#}", profile.name)?;
    writeln!(out, "{GREEN}API Key{GREEN:#}\t*********")?;
    writeln!(out, "{GREEN}Active Branch{GREEN:#}\t{active_branch}",)?;

    Ok(())
}
