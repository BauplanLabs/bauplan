use std::io::{Write as _, stdout};
use tracing::info;

use bauplan::{ApiErrorKind, commit::CommitOptions, namespace::*};
use tabwriter::TabWriter;

use crate::cli::{Cli, Output, is_api_err_kind};

#[derive(Debug, clap::Args)]
pub(crate) struct NamespaceArgs {
    #[command(subcommand)]
    pub command: NamespaceCommand,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum NamespaceCommand {
    /// List available namespaces
    #[clap(alias = "list")]
    Ls(NamespaceLsArgs),
    /// Create a new namespace
    Create(NamespaceCreateArgs),
    /// Drop a namespace from the data catalog
    #[clap(aliases = ["delete", "drop"])]
    Rm(NamespaceRmArgs),
}

#[derive(Debug, clap::Args)]
pub(crate) struct NamespaceLsArgs {
    /// Ref or branch name to get the namespaces from; it defaults to the active branch
    #[arg(short, long)]
    pub r#ref: Option<String>,
    /// Limit the number of namespaces to show
    #[arg(long)]
    pub limit: Option<usize>,
    /// Filter namespaces by name
    pub namespace: Option<String>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct NamespaceCreateArgs {
    /// Branch to create the namespace in; it defaults to the active branch
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Optional commit body to append to the commit message
    #[arg(long)]
    pub commit_body: Option<String>,
    /// Do not fail if the namespace already exists
    #[arg(long)]
    pub if_not_exists: bool,
    /// Namespace
    pub namespace: String,
}

#[derive(Debug, clap::Args)]
pub(crate) struct NamespaceRmArgs {
    /// Branch to delete the namespace from; it defaults to the active branch
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Optional commit body to append to the commit message
    #[arg(long)]
    pub commit_body: Option<String>,
    /// Do not fail if the namespace does not exist
    #[arg(long)]
    pub if_exists: bool,
    /// Namespace
    pub namespace: String,
}

pub(crate) fn handle(cli: &Cli, args: NamespaceArgs) -> anyhow::Result<()> {
    match args.command {
        NamespaceCommand::Ls(args) => list_namespaces(cli, args),
        NamespaceCommand::Create(args) => create_namespace(cli, args),
        NamespaceCommand::Rm(args) => delete_namespace(cli, args),
    }
}

fn list_namespaces(
    cli: &Cli,
    NamespaceLsArgs {
        r#ref,
        limit,
        namespace,
    }: NamespaceLsArgs,
) -> anyhow::Result<()> {
    let at_ref = r#ref
        .as_deref()
        .or(cli.profile.active_branch.as_deref())
        .unwrap_or("main");

    let req = GetNamespaces {
        at_ref,
        filter_by_name: namespace.as_deref(),
    };

    let namespaces = bauplan::paginate(req, limit, |r| super::roundtrip(cli, r))?;

    match cli.global.output.unwrap_or_default() {
        Output::Json => {
            let all_namespaces = namespaces.collect::<anyhow::Result<Vec<_>>>()?;
            serde_json::to_writer(stdout(), &all_namespaces)?;
            println!();
        }
        Output::Tty => {
            let mut tw = TabWriter::new(stdout());
            writeln!(&mut tw, "NAME\tKIND")?;
            for ns in namespaces {
                let ns = ns?;
                writeln!(&mut tw, "{}\tNAMESPACE", ns.name)?;
            }

            tw.flush()?;
        }
    }

    Ok(())
}

fn create_namespace(
    cli: &Cli,
    NamespaceCreateArgs {
        branch,
        commit_body,
        if_not_exists,
        namespace,
    }: NamespaceCreateArgs,
) -> anyhow::Result<()> {
    let branch = branch
        .as_deref()
        .or(cli.profile.active_branch.as_deref())
        .unwrap_or("main");

    let req = CreateNamespace {
        name: &namespace,
        branch,
        commit: CommitOptions {
            body: commit_body.as_deref(),
            properties: Default::default(),
        },
    };

    let result = super::roundtrip(cli, req);
    match result {
        Ok(_) => {
            info!(namespace, branch, "Namespace created");
        }
        Err(e) if if_not_exists && is_api_err_kind(&e, ApiErrorKind::NamespaceExists) => {
            info!(namespace, "Namespace already exists");
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

fn delete_namespace(
    cli: &Cli,
    NamespaceRmArgs {
        branch,
        commit_body,
        if_exists,
        namespace,
    }: NamespaceRmArgs,
) -> anyhow::Result<()> {
    let branch = branch
        .as_deref()
        .or(cli.profile.active_branch.as_deref())
        .unwrap_or("main");

    let req = DeleteNamespace {
        name: &namespace,
        branch,
        commit: CommitOptions {
            body: commit_body.as_deref(),
            properties: Default::default(),
        },
    };

    let result = super::roundtrip(cli, req);
    match result {
        Ok(_) => {
            info!(namespace, branch, "Namespace deleted");
        }
        Err(e) if if_exists && is_api_err_kind(&e, ApiErrorKind::NamespaceNotFound) => {
            info!(namespace, "Namespace does not exist");
        }
        Err(e) => return Err(e),
    }

    Ok(())
}
