use std::{
    io::{Write as _, stdout},
    path::PathBuf,
};

use bauplan::{ApiErrorKind, commit::CommitOptions, table::*};
use tabwriter::TabWriter;
use tracing::info;

use crate::cli::{Cli, KeyValue, Output, Priority, is_api_err_kind};

#[derive(Debug, clap::Args)]
pub(crate) struct TableArgs {
    #[command(subcommand)]
    pub command: TableCommand,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum TableCommand {
    /// List all available tables
    #[clap(alias = "list")]
    Ls(TableLsArgs),
    /// Get information about a table
    Get(TableGetArgs),
    /// Drop a table from the data catalog (does not free up storage)
    #[clap(alias = "delete")]
    Rm(TableRmArgs),
    /// create a new table
    Create(TableCreateArgs),
    /// create a plan for a new table
    CreatePlan(TableCreatePlanArgs),
    /// apply a table create plan manually
    CreatePlanApply(TableCreatePlanApplyArgs),
    /// Create an external read-only Iceberg table from existing data with any copies
    CreateExternal(TableCreateExternalArgs),
    /// import data to an existing table. Use `bauplan table create` to create
    Import(TableImportArgs),
    /// Revert a table to a previous state from a source ref
    Revert(TableRevertArgs),
}

#[derive(Debug, clap::Args)]
pub(crate) struct TableLsArgs {
    /// Namespace to get the table from
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// Ref or branch name to get the tables from; it defaults to the active branch
    #[arg(short, long)]
    pub r#ref: Option<String>,
    /// Limit the number of tables to show
    #[arg(long)]
    pub limit: Option<usize>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TableGetArgs {
    /// Ref or branch name to get the table from; it defaults to the active branch
    #[arg(short, long)]
    pub r#ref: Option<String>,
    /// Table name
    pub table_name: String,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TableRmArgs {
    /// Branch to delete the table from; it defaults to the active branch
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Optinal commit body to append to the commit message
    #[arg(long)]
    pub commit_body: Option<String>,
    /// Do not fail if the table does not exist
    #[arg(long)]
    pub if_exists: bool,
    /// Table name
    pub table_name: String,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TableCreateArgs {
    /// Name of the table to create
    #[arg(long)]
    pub name: Option<String>,
    /// Uri search string to s3 bucket containing parquet files to import e.g s3://bucket/path/a/*
    #[arg(long)]
    pub search_uri: Option<String>,
    /// Branch in which to create the table in. defaults to active branch
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Namespace the table is in. If not set, the default namespace in your account will be used
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// Partition the table by the given columns
    #[arg(long)]
    pub partitioned_by: Option<String>,
    /// Replace the existing table, if it exists
    #[arg(short, long)]
    pub replace: bool,
    /// Arguments to pass to the job. Format: key=value
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub arg: Vec<String>,
    /// Set the job priority (1-10, where 10 is highest priority)
    #[arg(long)]
    pub priority: Priority,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TableCreatePlanArgs {
    /// Name of the table to create
    #[arg(long)]
    pub name: Option<String>,
    /// Uri search string to s3 bucket containing parquet files to import e.g s3://bucket/path/a/*
    #[arg(long)]
    pub search_uri: Option<String>,
    /// Branch in which to create the table in. defaults to active branch
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Namespace the table is in. If not set, the default namespace in your account will be used
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// Partition the table by the given columns
    #[arg(long)]
    pub partitioned_by: Option<String>,
    /// Replace the existing table, if it exists
    #[arg(short, long)]
    pub replace: bool,
    /// A filename to write the plan to
    #[arg(short = 'p', long)]
    pub save_plan: Option<PathBuf>,
    /// Arguments to pass to the job. Format: key=value
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub arg: Vec<String>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TableCreatePlanApplyArgs {
    /// apply this plan
    #[arg(long)]
    pub plan: Option<String>,
    /// Arguments to pass to the job. Format: key=value
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub arg: Vec<String>,
    /// Set the job priority (1-10, where 10 is highest priority)
    #[arg(long)]
    pub priority: Priority,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TableCreateExternalArgs {
    /// Name of the external table to create
    #[arg(long)]
    pub name: Option<String>,
    /// URI to Iceberg metadata.json file (e.g., s3://bucket/metadata.json)
    #[arg(long)]
    pub metadata_json_uri: Option<String>,
    /// Search pattern for parquet files (e.g., s3://bucket/2025/*.parquet). Can be specified multiple times.
    #[arg(long, action = clap::ArgAction::Append)]
    pub search_pattern: Vec<String>,
    /// Branch in which to create the table (defaults to active branch)
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Namespace for the table
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// Overwrite the table if it already exists
    #[arg(long)]
    pub overwrite: bool,
    /// Run the job in the background (only for parquet mode)
    #[arg(short, long)]
    pub detach: bool,
    /// Arguments to pass to the job (only for parquet mode). Format: key=value
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub arg: Vec<String>,
    /// Set the job priority (1-10, where 10 is highest priority) (only for parquet mode)
    #[arg(long)]
    pub priority: Priority,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TableImportArgs {
    /// Name of table where data will be imported into
    #[arg(long)]
    pub name: Option<String>,
    /// Overwrite ref if needed. it defaults to active branch
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Uri search string e.g s3://bucket/path/a/*
    #[arg(long)]
    pub search_uri: Option<String>,
    /// Don't fail the command even if 1/N files failes to import
    #[arg(long)]
    pub continue_on_error: bool,
    /// Force importing of files without checking what was already imported. likely result in duplicate rows being imported
    #[arg(long)]
    pub import_duplicate_files: bool,
    /// Set to ignore new columns. if an import file  has column aa, bb, and parquet has col aa, bb, cc, columns aa and bb will be imported
    #[arg(long)]
    pub best_effort: bool,
    /// Namespace the table is in. If not set, the default namespace in your acconnt will be used
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// Run the job in the background
    #[arg(short, long)]
    pub detach: bool,
    /// Arguments to pass to the job. Format: key=value
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub arg: Vec<String>,
    /// Set the job priority (1-10, where 10 is highest priority)
    #[arg(long)]
    pub priority: Priority,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TableRevertArgs {
    /// The ref (branch or tag) to revert the table from
    #[arg(short, long)]
    pub source_ref: String,
    /// The branch to revert the table into (defaults to the active branch)
    #[arg(short, long)]
    pub into_branch: String,
    /// Replace the destination table if it exists
    #[arg(long)]
    pub replace: bool,
    /// Optional commit body to append to the commit message
    #[arg(long)]
    pub commit_body: Option<String>,
    /// Commit properties as key=value pairs (can be used multiple times)
    #[arg(long, action = clap::ArgAction::Append)]
    pub commit_property: Vec<KeyValue>,
    /// Table name
    pub table_name: String,
}

pub(crate) fn handle(cli: &Cli, args: TableArgs) -> anyhow::Result<()> {
    match args.command {
        TableCommand::Ls(args) => list_tables(cli, args),
        TableCommand::Get(args) => get_table(cli, args),
        TableCommand::Rm(args) => delete_table(cli, args),
        TableCommand::Create(_) => todo!(),
        TableCommand::CreatePlan(_) => todo!(),
        TableCommand::CreatePlanApply(_) => todo!(),
        TableCommand::CreateExternal(_) => todo!(),
        TableCommand::Import(_) => todo!(),
        TableCommand::Revert(args) => revert_table(cli, args),
    }
}

fn list_tables(
    cli: &Cli,
    TableLsArgs {
        namespace,
        r#ref,
        limit,
    }: TableLsArgs,
) -> anyhow::Result<()> {
    let req = GetTables {
        at_ref: r#ref.as_deref().unwrap_or("main"),
        filter_by_name: None,
        filter_by_namespace: namespace.as_deref(),
    };

    let tables = bauplan::paginate(req, limit, |r| super::roundtrip(cli, r))?;

    match cli.global.output.unwrap_or_default() {
        Output::Json => {
            let all_tables = tables.collect::<anyhow::Result<Vec<_>>>()?;
            serde_json::to_writer(stdout(), &all_tables)?;
        }
        Output::Tty => {
            let mut tw = TabWriter::new(stdout());
            writeln!(&mut tw, "NAMESPACE\tNAME\tKIND")?;
            for table in tables {
                let table = table?;
                writeln!(
                    &mut tw,
                    "{}\t{}\t{}",
                    table.namespace, table.name, table.kind
                )?;
            }

            tw.flush()?;
        }
    }

    Ok(())
}

fn get_table(cli: &Cli, TableGetArgs { r#ref, table_name }: TableGetArgs) -> anyhow::Result<()> {
    let req = GetTable {
        name: &table_name,
        at_ref: r#ref.as_deref().unwrap_or("main"),
        namespace: None,
    };

    let resp = super::roundtrip(cli, req)?;
    match cli.global.output.unwrap_or_default() {
        Output::Json => {
            serde_json::to_writer(stdout(), &resp)?;
        }
        Output::Tty => {
            let mut tw = TabWriter::new(stdout());
            writeln!(&mut tw, "NAME\tREQUIRED\tTYPE")?;

            for TableField {
                name,
                required,
                r#type,
                ..
            } in resp.fields
            {
                writeln!(&mut tw, "{name}\t{required}\t{type}")?;
            }

            tw.flush()?;
        }
    }

    Ok(())
}

fn delete_table(
    cli: &Cli,
    TableRmArgs {
        branch,
        commit_body,
        if_exists,
        table_name,
    }: TableRmArgs,
) -> anyhow::Result<()> {
    let branch = branch
        .as_deref()
        .or(cli.profile.active_branch.as_deref())
        .unwrap_or("main");

    let req = DeleteTable {
        name: &table_name,
        branch,
        namespace: None,
        commit: CommitOptions {
            body: commit_body.as_deref(),
            properties: Default::default(),
        },
    };

    let result = super::roundtrip(cli, req);
    match result {
        Ok(_) => {
            info!(table = table_name, branch, "Table deleted");
        }
        Err(e) if if_exists && is_api_err_kind(&e, ApiErrorKind::TableNotFound) => {
            info!(table = table_name, "Table does not exist");
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

fn revert_table(
    cli: &Cli,
    TableRevertArgs {
        source_ref,
        into_branch,
        replace,
        commit_body,
        commit_property,
        table_name,
    }: TableRevertArgs,
) -> anyhow::Result<()> {
    let req = RevertTable {
        name: &table_name,
        source_ref: &source_ref,
        into_branch: &into_branch,
        namespace: None,
        replace,
        commit: CommitOptions {
            body: commit_body.as_deref(),
            properties: commit_property.iter().map(KeyValue::as_strs).collect(),
        },
    };

    let r#ref = super::roundtrip(cli, req)?;
    tracing::debug!(?r#ref, "Created ref");
    info!(source_ref, into_branch, "Table reverted");

    Ok(())
}
