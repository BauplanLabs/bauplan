use anyhow::{Context as _, bail};
use bauplan::branch::{CreateBranch, GetBranch};

use crate::cli::{Cli, yaml};

#[derive(Debug, clap::Args)]
pub(crate) struct CheckoutArgs {
    /// Branch name
    pub branch_name: String,
    /// Create the branch first (equivalent to "branch create --if-not-exists")
    #[arg(short = 'b')]
    pub create: bool,
    /// Ref from which to create when using -b. If not specified,
    /// the default is the currently active branch.
    #[arg(long)]
    pub from_ref: Option<String>,
}

pub(crate) fn handle(cli: &Cli, args: CheckoutArgs) -> anyhow::Result<()> {
    let CheckoutArgs {
        branch_name,
        create,
        from_ref,
    } = args;

    if create {
        let from_ref = from_ref
            .as_deref()
            .or(cli.profile.active_branch.as_deref())
            .unwrap_or("main");

        let req = CreateBranch {
            name: &branch_name,
            from_ref,
        };

        cli.roundtrip(req).context("Failed to create branch")?;
        eprintln!("Created branch {branch_name:?}");
    } else if from_ref.is_some() {
        bail!("--from-ref can only be used with -b");
    }

    switch_branch(cli, &branch_name)
}

pub(crate) fn switch_branch(cli: &Cli, branch_name: &str) -> anyhow::Result<()> {
    if cli.roundtrip(GetBranch { name: branch_name }).is_err() {
        bail!("branch {branch_name:?} doesn't exist or is inaccessible");
    }

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
