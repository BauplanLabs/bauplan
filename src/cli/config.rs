use std::io::{Write, stdout};

use bauplan::Profile;
use tabwriter::TabWriter;
use yansi::Paint as _;

use crate::cli::{GlobalArgs, Output, yaml};

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

fn config_set_help() -> &'static str {
    static HELP: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
        format!(
            "{}\n\n  {}\n  {}\n",
            "Examples".bold().underline(),
            "# Set configuration value".dim(),
            "bauplan config set api_key your_key".bold(),
        )
    });
    HELP.as_str()
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = config_set_help())]
pub(crate) struct ConfigSetArgs {
    /// Setting name
    pub name: ConfigSetting,
    /// Value to set
    pub value: String,
}

fn config_get_help() -> &'static str {
    static HELP: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
        format!(
            "{}\n\n  {}\n  {}\n\n  {}\n  {}\n",
            "Examples".bold().underline(),
            "# Get specific configuration".dim(),
            "bauplan config get api_key".bold(),
            "# Get all profiles".dim(),
            "bauplan config get --all".bold(),
        )
    });
    HELP.as_str()
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = config_get_help())]
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
    let mut out = stdout().lock();

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
    writeln!(
        out,
        "{}",
        format!("Profile {:?}", profile.name).white().bold()
    )?;
    writeln!(out, "{}\t*********", "API Key".green())?;
    writeln!(
        out,
        "{}\t{}",
        "Active Branch".green(),
        profile.active_branch.as_deref().unwrap_or("main"),
    )?;

    Ok(())
}
