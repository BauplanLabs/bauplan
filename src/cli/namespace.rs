use std::io::{Write as _, stdout};

use bauplan_api::gropius::client::ClientError;
use bauplan_api::model::{self, AtCommit};
use bauplan_api::namespace::{
    CreateNamespaceBody, CreateNamespaceError, DeleteNamespaceBody, DeleteNamespaceError,
    GetNamespaceError, ListNamespacesError, NamespaceFilters,
};
use tabwriter::TabWriter;

use crate::cli::{
    Cli, Output, api_error,
    color::{CliExamples, GREEN},
    paginate::paginate,
};

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
    /// Get information about a namespace
    Get(NamespaceGetArgs),
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
    /// Filter by name. Use `^` and `$` to anchor it to the start or the end
    #[arg(short, long)]
    pub name: Option<String>,
    /// Branch, tag or full commit hash to list the namespaces from [default: active branch]
    #[arg(short, long)]
    pub r#ref: Option<String>,
    /// Limit the number of namespaces to show
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    pub limit: Option<u32>,
    /// Continue from the `pagination_token` of a previous `-O json` output. Pass
    /// its `commit_hash` as `--ref` to read the same state
    #[arg(long)]
    pub pagination_token: Option<String>,
}

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  # Get a namespace on the active branch
  bauplan namespace get raw_data

  # Get a namespace at a specific ref
  bauplan namespace get raw_data --ref main
"))]
pub(crate) struct NamespaceGetArgs {
    /// Namespace
    pub namespace: String,
    /// Branch, tag or full commit hash to get the namespace from [default: active branch]
    #[arg(short, long)]
    pub r#ref: Option<String>,
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
  bauplan namespace create my_namespace --expected-head <commit_hash>
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
    /// Optional commit message
    #[arg(long)]
    pub commit_message: Option<String>,
    /// Create only if the branch head is still at this full commit hash
    #[arg(long, value_name = "HASH")]
    pub expected_head: Option<String>,
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
  bauplan namespace rm old_namespace --expected-head <commit_hash>
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
    /// Optional commit message
    #[arg(long)]
    pub commit_message: Option<String>,
    /// Delete only if the branch head is still at this full commit hash
    #[arg(long, value_name = "HASH")]
    pub expected_head: Option<String>,
}

pub(crate) fn handle(cli: &Cli, args: NamespaceArgs) -> anyhow::Result<()> {
    match args.command {
        NamespaceCommand::Ls(args) => list_namespaces(cli, args),
        NamespaceCommand::Get(args) => get_namespace(cli, args),
        NamespaceCommand::Create(args) => create_namespace(cli, args),
        NamespaceCommand::Rm(args) => delete_namespace(cli, args),
    }
}

fn list_namespaces(
    cli: &Cli,
    NamespaceLsArgs {
        name,
        r#ref,
        limit,
        pagination_token,
    }: NamespaceLsArgs,
) -> anyhow::Result<()> {
    let at_ref = r#ref.as_deref().unwrap_or(cli.active_branch());

    let client = cli.namespaces();
    let mut commit_hash = None;
    let filters = NamespaceFilters { name };
    let namespaces = paginate(limit, pagination_token, filters, |query| {
        // We read the next pages at the commit of the first one, so the list
        // stays the same if the ref moves meanwhile.
        let at = commit_hash.as_deref().unwrap_or(at_ref);
        let path = (cli.profile.catalog.clone(), at.to_owned());
        match client.list_namespaces(path, query) {
            Ok(page) => {
                commit_hash.get_or_insert(page.commit_hash);
                Ok(page.inner)
            }
            Err(ClientError::Api(err @ ListNamespacesError::RefNotFound { .. })) => {
                Err(cli.with_active_branch_tip(at_ref, err))
            }
            Err(err) => Err(api_error(err)),
        }
    })?;

    match cli.global.output {
        Output::Json => {
            let page = AtCommit {
                inner: namespaces.into_page()?,
                commit_hash: commit_hash.unwrap_or_default(),
            };
            serde_json::to_writer(stdout(), &page)?;
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

fn get_namespace(
    cli: &Cli,
    NamespaceGetArgs { namespace, r#ref }: NamespaceGetArgs,
) -> anyhow::Result<()> {
    let at_ref = r#ref.as_deref().unwrap_or(cli.active_branch());

    let path = (cli.profile.catalog.clone(), at_ref.to_owned(), namespace);
    let namespace = match cli.namespaces().get_namespace(path) {
        Ok(namespace) => namespace,
        Err(ClientError::Api(err @ GetNamespaceError::RefNotFound { .. })) => {
            return Err(cli.with_active_branch_tip(at_ref, err));
        }
        Err(err) => return Err(api_error(err)),
    };

    match cli.global.output {
        Output::Json => {
            serde_json::to_writer(stdout(), &namespace)?;
            println!();
        }
        Output::Tty => {
            let mut tw = TabWriter::new(anstream::stdout()).ansi(true);
            writeln!(&mut tw, "{GREEN}Name{GREEN:#}\t{}", namespace.inner.name)?;
            writeln!(&mut tw, "{GREEN}Commit{GREEN:#}\t{}", namespace.commit.hash)?;
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
        commit_message,
        expected_head,
    }: NamespaceCreateArgs,
) -> anyhow::Result<()> {
    let branch = branch.as_deref().unwrap_or(cli.active_branch());

    let path = (cli.profile.catalog.clone(), branch.to_owned());
    let body = CreateNamespaceBody {
        name: namespace,
        commit: model::CommitOptions {
            message: commit_message,
            expected_head,
        },
    };

    let created = match cli.namespaces().create_namespace(path, body) {
        Ok(created) => created,
        Err(ClientError::Api(err @ CreateNamespaceError::RefNotFound { .. })) => {
            return Err(cli.with_active_branch_tip(branch, err));
        }
        Err(ClientError::Api(err @ CreateNamespaceError::NameConflict { .. })) if if_not_exists => {
            eprintln!("{err}");
            return Ok(());
        }
        Err(err) => return Err(api_error(err)),
    };

    eprintln!(
        "Created namespace {:?} (commit {})",
        created.inner.name, created.commit.hash
    );

    if cli.global.output == Output::Json {
        serde_json::to_writer(stdout(), &created)?;
        println!();
    }

    Ok(())
}

fn delete_namespace(
    cli: &Cli,
    NamespaceRmArgs {
        namespace,
        branch,
        if_exists,
        commit_message,
        expected_head,
    }: NamespaceRmArgs,
) -> anyhow::Result<()> {
    let branch = branch.as_deref().unwrap_or(cli.active_branch());

    let path = (
        cli.profile.catalog.clone(),
        branch.to_owned(),
        namespace.clone(),
    );
    let body = DeleteNamespaceBody {
        commit: model::CommitOptions {
            message: commit_message,
            expected_head,
        },
    };

    let commit = match cli.namespaces().delete_namespace(path, Some(body)) {
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
        Err(err) => return Err(api_error(err)),
    };

    eprintln!("Deleted namespace {namespace:?} (commit {})", commit.hash);

    if cli.global.output == Output::Json {
        serde_json::to_writer(stdout(), &commit)?;
        println!();
    }

    Ok(())
}
