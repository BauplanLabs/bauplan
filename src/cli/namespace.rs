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
    /// Filter namespaces by name
    pub namespace: Option<String>,
    /// Ref or branch name to list namespaces from [default: active branch]
    #[arg(short, long)]
    pub r#ref: Option<String>,
    /// Limit the number of namespaces to show
    #[arg(long)]
    pub limit: Option<usize>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct NamespaceCreateArgs {
    /// Namespace
    pub namespace: String,
    /// Branch to create the namespace in [default: active branch]
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Do not fail if the namespace already exists
    #[arg(long)]
    pub if_not_exists: bool,
    /// Optional commit body to append to the commit message
    #[arg(long)]
    pub commit_body: Option<String>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct NamespaceRmArgs {
    /// Namespace
    pub namespace: String,
    /// Branch to delete the namespace from [default: active branch]
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Do not fail if the namespace does not exist
    #[arg(long)]
    pub if_exists: bool,
    /// Optional commit body to append to the commit message
    #[arg(long)]
    pub commit_body: Option<String>,
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
        namespace,
        r#ref,
        limit,
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

    let namespaces = bauplan::paginate(req, limit, |r| cli.roundtrip(r))?;

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
        namespace,
        branch,
        if_not_exists,
        commit_body,
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

    let result = cli.roundtrip(req);
    match result {
        Ok(_) => {
            info!(namespace, branch, "namespace created");
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
        namespace,
        branch,
        if_exists,
        commit_body,
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

    let result = cli.roundtrip(req);
    match result {
        Ok(_) => {
            info!(namespace, branch, "namespace deleted");
        }
        Err(e) if if_exists && is_api_err_kind(&e, ApiErrorKind::NamespaceNotFound) => {
            info!(namespace, "Namespace does not exist");
        }
        Err(e) => return Err(e),
    }

    Ok(())
}
