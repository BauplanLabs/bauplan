use std::io::stdout;

use crate::cli::{Cli, Output, api_error, color::CliExamples, yaml};
use anyhow::bail;
use bauplan_api::branch::{CreateBranchBody, CreateBranchError};
use bauplan_api::gropius::client::ClientError;

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  # Checkout existing branch
  bauplan checkout main

  # Checkout user branch
  bauplan checkout username.dev_branch

  # Create and checkout new branch from main
  bauplan checkout -b username.new_feature --from-ref main

  # Create and checkout from active branch
  bauplan checkout -b username.new_feature
"))]
pub(crate) struct CheckoutArgs {
    /// Branch name
    pub branch_name: String,
    /// Create the branch before switching (fails if it already exists)
    #[arg(short = 'b')]
    pub create: bool,
    /// Branch, tag or full commit hash to create the branch from with -b [default: active branch]
    #[arg(long)]
    pub from_ref: Option<String>,
}

pub(crate) fn handle(cli: &Cli, args: CheckoutArgs) -> anyhow::Result<()> {
    let CheckoutArgs {
        branch_name,
        create,
        from_ref,
    } = args;

    if !create {
        if from_ref.is_some() {
            bail!("--from-ref can only be used with -b");
        }
        return switch_branch(cli, &branch_name);
    }

    let from_ref = from_ref.as_deref().unwrap_or(cli.active_branch());

    let body = CreateBranchBody {
        name: branch_name,
        from_ref: from_ref.to_owned(),
        ttl_seconds: None,
    };

    let catalog = cli.profile.catalog.clone();
    let branch = match cli.branches().create_branch(catalog, body) {
        Ok(branch) => branch,
        Err(ClientError::Api(err @ CreateBranchError::FromRefNotFound { .. })) => {
            return Err(cli.with_active_branch_tip(from_ref, err));
        }
        Err(err) => return Err(api_error(err)),
    };

    eprintln!("Created branch {:?}", branch.name);

    if cli.global.output == Output::Json {
        serde_json::to_writer(stdout(), &branch)?;
        println!();
    }

    set_active_branch(cli, &branch.name)
}

pub(crate) fn switch_branch(cli: &Cli, branch_name: &str) -> anyhow::Result<()> {
    let path = (cli.profile.catalog.clone(), branch_name.to_owned());
    let branch = cli.branches().resolve_branch(path).map_err(api_error)?;

    set_active_branch(cli, &branch.name)
}

/// Saves the active branch in the profile.
pub(crate) fn set_active_branch(cli: &Cli, branch_name: &str) -> anyhow::Result<()> {
    yaml::edit(&cli.profile.config_path, |doc| {
        let mut profile = yaml::mapping_at_path(doc, &["profiles", &cli.profile.name])?;
        yaml::upsert_str(&mut profile, "active_branch", branch_name);
        Ok(())
    })?;

    eprintln!(
        "Switched to branch {branch_name:?} in profile {:?}",
        cli.profile.name,
    );

    Ok(())
}
