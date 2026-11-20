use std::io::{Write as _, stdout};

use bauplan_api::gropius::client::ClientError;
use bauplan_api::model;
use bauplan_api::namespace::{
    CreateNamespaceBody, CreateNamespaceError, DeleteNamespaceBody, DeleteNamespaceError,
    ListNamespacesError, NamespaceFilters,
};
use tabwriter::TabWriter;

use crate::cli::{Cli, Output, color::CliExamples, paginate::paginate};

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
#[command(after_long_help = CliExamples("
  # List namespaces on active branch
  bauplan namespace ls

  # List namespaces on specific branch
  bauplan namespace ls --ref main

  # List the namespaces whose name starts with raw
  bauplan namespace ls --name '^raw'

  # Limit results
  bauplan namespace ls --limit 10
"))]
pub(crate) struct NamespaceLsArgs {
    /// Filter by name, `^` and `$` anchor it to the start or the end
    #[arg(short, long)]
    pub name: Option<String>,
    /// Ref or branch name to list namespaces from [default: active branch]
    #[arg(short, long)]
    pub r#ref: Option<String>,
    /// Limit the number of namespaces to show
    #[arg(long)]
    pub limit: Option<usize>,
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  # Create namespace on active branch
  bauplan namespace create raw_data

  # Create namespace on specific branch
  bauplan namespace create transformed_data --branch main

  # Create namespace if it doesn't already exist
  bauplan namespace create my_namespace --if-not-exists

  # Create only if nobody moved the branch since you read its head
  bauplan namespace create my_namespace --if-head <commit_hash>
"))]
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
    /// Create only if the branch head is still at this full commit hash
    #[arg(long, value_name = "HASH")]
    pub if_head: Option<String>,
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  # Delete namespace from active branch
  bauplan namespace rm old_namespace

  # Delete from specific branch
  bauplan namespace rm old_namespace --branch main

  # Conditionally delete
  bauplan namespace rm maybe_namespace --if-exists

  # Delete only if nobody moved the branch since you read its head
  bauplan namespace rm old_namespace --if-head <commit_hash>
"))]
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
    /// Delete only if the branch head is still at this full commit hash
    #[arg(long, value_name = "HASH")]
    pub if_head: Option<String>,
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
    NamespaceLsArgs { name, r#ref, limit }: NamespaceLsArgs,
) -> anyhow::Result<()> {
    let at_ref = r#ref
        .as_deref()
        .or(cli.profile.active_branch.as_deref())
        .unwrap_or("main");

    let client = cli.namespaces()?;
    let filters = NamespaceFilters { name };
    let namespaces = paginate(limit, filters, |query| {
        let path = (cli.global.catalog.clone(), at_ref.to_owned());
        match client.list_namespaces(path, query) {
            Ok(page) => Ok(page.inner),
            Err(ClientError::Api(err @ ListNamespacesError::RefNotFound { .. })) => {
                Err(cli.with_active_branch_tip(at_ref, err))
            }
            Err(err) => Err(err.into()),
        }
    })?;

    match cli.global.output {
        Output::Json => {
            let all_namespaces = namespaces.collect::<anyhow::Result<Vec<_>>>()?;
            serde_json::to_writer(stdout(), &all_namespaces)?;
            println!();
        }
        Output::Tty => {
            let mut tw = TabWriter::new(stdout());
            writeln!(&mut tw, "NAME")?;
            for ns in namespaces {
                let ns = ns?;
                writeln!(&mut tw, "{}", ns.name)?;
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
        if_head,
    }: NamespaceCreateArgs,
) -> anyhow::Result<()> {
    let branch = branch
        .as_deref()
        .or(cli.profile.active_branch.as_deref())
        .unwrap_or("main");

    let path = (cli.global.catalog.clone(), branch.to_owned());
    let body = CreateNamespaceBody {
        name: namespace,
        commit: model::CommitOptions {
            body: commit_body,
            expected_commit_hash: if_head,
        },
    };

    let created = match cli.namespaces()?.create_namespace(path, body) {
        Ok(created) => created,
        Err(ClientError::Api(err @ CreateNamespaceError::RefNotFound { .. })) => {
            return Err(cli.with_active_branch_tip(branch, err));
        }
        Err(ClientError::Api(err @ CreateNamespaceError::NameConflict { .. })) if if_not_exists => {
            eprintln!("{err}");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };

    eprintln!(
        "Created namespace {:?} (commit {})",
        created.inner.name, created.commit.hash
    );
    Ok(())
}

fn delete_namespace(
    cli: &Cli,
    NamespaceRmArgs {
        namespace,
        branch,
        if_exists,
        commit_body,
        if_head,
    }: NamespaceRmArgs,
) -> anyhow::Result<()> {
    let branch = branch
        .as_deref()
        .or(cli.profile.active_branch.as_deref())
        .unwrap_or("main");

    let path = (
        cli.global.catalog.clone(),
        branch.to_owned(),
        namespace.clone(),
    );
    let body = DeleteNamespaceBody {
        commit: model::CommitOptions {
            body: commit_body,
            expected_commit_hash: if_head,
        },
    };

    let commit = match cli.namespaces()?.delete_namespace(path, Some(body)) {
        Ok(commit) => commit,
        Err(ClientError::Api(err @ DeleteNamespaceError::RefNotFound { .. })) => {
            return Err(cli.with_active_branch_tip(branch, err));
        }
        Err(ClientError::Api(err @ DeleteNamespaceError::NamespaceNotFound { .. }))
            if if_exists =>
        {
            eprintln!("{err}");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };

    eprintln!("Deleted namespace {namespace:?} (commit {})", commit.hash);
    Ok(())
}
