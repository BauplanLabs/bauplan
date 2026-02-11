use std::{
    io::{IsTerminal as _, Read as _, Write as _, stdout},
    path::PathBuf,
    time,
};

use anyhow::{anyhow, bail};
use bauplan::{
    ApiErrorKind,
    commit::CommitOptions,
    grpc::{self, generated as commanderpb},
    table::*,
};
use commanderpb::runner_event::Event as RunnerEvent;
use indicatif::ProgressBar;
use tabwriter::TabWriter;
use tracing::info;
use yansi::Paint;

use crate::cli::{
    Cli, KeyValue, Output, Priority, api_err_kind, format_grpc_status,
    run::{job_request_common, monitor_job_progress},
    spinner::ProgressExt as _,
    with_rt,
};

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
    /// Filter tables by name (exact match or regex)
    #[arg(long)]
    pub name: Option<String>,
    /// Filter by namespace (exact match or regex)
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// Ref or branch name to list tables from [default: active branch]
    #[arg(short, long)]
    pub r#ref: Option<String>,
    /// Limit the number of tables to show
    #[arg(long)]
    pub limit: Option<usize>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TableGetArgs {
    /// Table name
    pub table_name: String,
    /// Ref or branch name to get the table from [default: active branch]
    #[arg(short, long)]
    pub r#ref: Option<String>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TableRmArgs {
    /// Table name
    pub table_name: String,
    /// Branch to delete the table from [default: active branch]
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Do not fail if the table does not exist
    #[arg(long)]
    pub if_exists: bool,
    /// Optinal commit body to append to the commit message
    #[arg(long)]
    pub commit_body: Option<String>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TableCreateArgs {
    /// Name of the table to create
    pub table_name: String,
    /// Branch to create the table in [default: active branch]
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Namespace for the table
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// S3 URI pattern for parquet files to import (e.g. s3://bucket/path/*)
    #[arg(long)]
    pub search_uri: url::Url,
    /// Partition the table by the given columns
    #[arg(long)]
    pub partitioned_by: Option<String>,
    /// Replace the existing table, if it exists
    #[arg(short, long)]
    pub replace: bool,
    /// Extra arguments as key=value pairs (repeatable)
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub arg: Vec<KeyValue>,
    /// Set the job priority (1-10, where 10 is highest priority)
    #[arg(long)]
    pub priority: Option<Priority>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TableCreatePlanArgs {
    /// Name of the table to create
    pub table_name: String,
    /// Branch to create the table in [default: active branch]
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Namespace for the table
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// S3 URI pattern for parquet files to import (e.g. s3://bucket/path/*)
    #[arg(long)]
    pub search_uri: url::Url,
    /// Partition the table by the given columns
    #[arg(long)]
    pub partitioned_by: Option<String>,
    /// Replace the existing table, if it exists
    #[arg(short, long)]
    pub replace: bool,
    /// A filename to write the plan to
    #[arg(short = 'p', long)]
    pub save_plan: Option<PathBuf>,
    /// Extra arguments as key=value pairs (repeatable)
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub arg: Vec<KeyValue>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TableCreatePlanApplyArgs {
    /// Path to a plan YAML file; reads from stdin if not provided
    #[arg(long)]
    pub plan: Option<String>,
    /// Extra arguments as key=value pairs (repeatable)
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub arg: Vec<KeyValue>,
    /// Set the job priority (1-10, where 10 is highest priority)
    #[arg(long)]
    pub priority: Option<Priority>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TableCreateExternalArgs {
    /// Name of the external table to create
    pub table_name: String,
    /// Branch to create the table in [default: active branch]
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Namespace for the table
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// URI to Iceberg metadata.json file (e.g., s3://bucket/metadata.json)
    #[arg(long)]
    pub metadata_json_uri: Option<String>,
    /// Search pattern for parquet files (e.g., s3://bucket/2025/*.parquet). Can be specified multiple times.
    #[arg(long, action = clap::ArgAction::Append, conflicts_with = "metadata_json_uri")]
    pub search_pattern: Vec<String>,
    /// Overwrite the table if it already exists
    #[arg(long)]
    pub overwrite: bool,
    /// Run the job in the background (only for parquet mode)
    #[arg(short, long)]
    pub detach: bool,
    /// Extra arguments as key=value pairs, repeatable (only for parquet mode)
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub arg: Vec<KeyValue>,
    /// Set the job priority (1-10, where 10 is highest priority) (only for parquet mode)
    #[arg(long)]
    pub priority: Option<Priority>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TableImportArgs {
    /// Name of table where data will be imported into
    pub table_name: String,
    /// Branch to import into [default: active branch]
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Namespace for the table
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// Uri search string e.g s3://bucket/path/a/*
    #[arg(long)]
    pub search_uri: url::Url,
    /// Don't fail the command even if 1/N files fails to import
    #[arg(long)]
    pub continue_on_error: bool,
    /// Force importing of files without checking what was already imported. likely result in duplicate rows being imported
    #[arg(long)]
    pub import_duplicate_files: bool,
    /// Set to ignore new columns. if an import file  has column aa, bb, and parquet has col aa, bb, cc, columns aa and bb will be imported
    #[arg(long)]
    pub best_effort: bool,
    /// Run the job in the background
    #[arg(short, long)]
    pub detach: bool,
    /// Extra arguments as key=value pairs (repeatable)
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub arg: Vec<KeyValue>,
    /// Set the job priority (1-10, where 10 is highest priority)
    #[arg(long)]
    pub priority: Option<Priority>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TableRevertArgs {
    /// Table name
    pub table_name: String,
    /// The ref (branch or tag) to revert the table from
    #[arg(short, long)]
    pub source_ref: String,
    /// Branch to revert the table into [default: active branch]
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
}

pub(crate) fn handle(cli: &Cli, args: TableArgs) -> anyhow::Result<()> {
    match args.command {
        TableCommand::Ls(args) => handle_list_tables(cli, args),
        TableCommand::Get(args) => handle_get_table(cli, args),
        TableCommand::Rm(args) => handle_delete_table(cli, args),
        TableCommand::Create(args) => with_rt(handle_create_table(cli, args)),
        TableCommand::CreatePlan(args) => with_rt(handle_create_plan(cli, args)),
        TableCommand::CreatePlanApply(args) => with_rt(handle_apply_plan(cli, args)),
        TableCommand::CreateExternal(args) => {
            if args.metadata_json_uri.is_some() {
                handle_create_external_from_metadata(cli, args)
            } else {
                with_rt(handle_create_external(cli, args))
            }
        }
        TableCommand::Import(args) => with_rt(handle_import_data(cli, args)),
        TableCommand::Revert(args) => handle_revert_table(cli, args),
    }
}

fn handle_list_tables(
    cli: &Cli,
    TableLsArgs {
        name,
        namespace,
        r#ref,
        limit,
    }: TableLsArgs,
) -> anyhow::Result<()> {
    let req = GetTables {
        at_ref: r#ref.as_deref().unwrap_or("main"),
        filter_by_name: name.as_deref(),
        filter_by_namespace: namespace.as_deref(),
    };

    let tables = bauplan::paginate(req, limit, |r| cli.roundtrip(r))?;

    match cli.global.output.unwrap_or_default() {
        Output::Json => {
            let all_tables = tables.collect::<anyhow::Result<Vec<_>>>()?;
            serde_json::to_writer(stdout(), &all_tables)?;
            println!();
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

fn handle_get_table(
    cli: &Cli,
    TableGetArgs { table_name, r#ref }: TableGetArgs,
) -> anyhow::Result<()> {
    let req = GetTable {
        name: &table_name,
        at_ref: r#ref.as_deref().unwrap_or("main"),
        namespace: None,
    };

    let resp = cli.roundtrip(req)?;
    match cli.global.output.unwrap_or_default() {
        Output::Json => {
            serde_json::to_writer(stdout(), &resp)?;
            println!();
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

fn handle_delete_table(
    cli: &Cli,
    TableRmArgs {
        table_name,
        branch,
        if_exists,
        commit_body,
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

    if let Err(e) = cli.roundtrip(req) {
        if if_exists && matches!(api_err_kind(&e), Some(ApiErrorKind::TableNotFound { .. })) {
            eprintln!("Table {table_name:?} does not exist");
            return Ok(());
        } else {
            return Err(e);
        }
    }

    eprintln!("Deleted table {table_name:?}");
    Ok(())
}

async fn create_plan(
    cli: &Cli,
    client: &mut grpc::Client,
    req: commanderpb::TableCreatePlanRequest,
    progress: ProgressBar,
) -> anyhow::Result<(String, bool)> {
    let resp = client
        .table_create_plan(cli.traced(req))
        .await?
        .into_inner();
    let Some(commanderpb::JobResponseCommon { job_id, .. }) = resp.job_response_common else {
        bail!("response missing job ID");
    };

    let ctrl_c = tokio::signal::ctrl_c();
    futures::pin_mut!(ctrl_c);

    let mut res = Err(anyhow!("job completed without producing a plan"));

    monitor_job_progress(
        cli,
        client,
        job_id,
        "import planning job",
        progress.clone(),
        ctrl_c,
        |event| {
            if let RunnerEvent::TableCreatePlanDoneEvent(ev) = event {
                if !ev.error_message.is_empty() {
                    res = Err(anyhow!("plan creation failed: {}", ev.error_message));
                } else {
                    res = Ok((ev.plan_as_yaml, ev.can_auto_apply));

                    info!(
                        can_auto_apply = ev.can_auto_apply,
                        files = ev.files_to_be_imported.len(),
                        "plan created"
                    );
                }
            }
        },
    )
    .await?;

    res
}

async fn apply_plan(
    cli: &Cli,
    client: &mut grpc::Client,
    req: commanderpb::TableCreatePlanApplyRequest,
    progress: &indicatif::ProgressBar,
) -> anyhow::Result<()> {
    let resp = client
        .table_create_plan_apply(cli.traced(req))
        .await
        .map_err(format_grpc_status)?;

    let Some(commanderpb::JobResponseCommon { job_id, .. }) = resp.into_inner().job_response_common
    else {
        bail!("response missing job ID");
    };

    let ctrl_c = tokio::signal::ctrl_c();
    futures::pin_mut!(ctrl_c);

    monitor_job_progress(
        cli,
        client,
        job_id,
        "import job",
        progress.clone(),
        ctrl_c,
        |_| {},
    )
    .await?;

    Ok(())
}

async fn handle_create_plan(cli: &Cli, args: TableCreatePlanArgs) -> anyhow::Result<()> {
    let TableCreatePlanArgs {
        table_name: name,
        branch,
        namespace,
        search_uri,
        partitioned_by,
        replace,
        save_plan,
        arg,
    } = args;

    let timeout = cli.timeout.unwrap_or(time::Duration::from_secs(1800));
    let mut client = grpc::Client::new_lazy(&cli.profile, timeout)?;

    let branch = branch.or_else(|| cli.profile.active_branch.clone());

    let req = commanderpb::TableCreatePlanRequest {
        job_request_common: Some(job_request_common(arg, None)),
        branch_name: branch,
        table_name: name,
        namespace,
        search_string: search_uri.to_string(),
        table_replace: replace,
        table_partitioned_by: partitioned_by,
    };

    let progress = cli.new_spinner().with_message("Creating plan...");

    let yaml = match create_plan(cli, &mut client, req, progress.clone()).await {
        Ok((yaml, _)) => yaml,
        Err(e) => {
            progress.finish_with_failed();
            return Err(e);
        }
    };

    progress.finish_with_done();
    if let Some(path) = save_plan {
        std::fs::write(&path, &yaml)?;
        info!(path = %path.display(), "plan saved");
    } else {
        print!("{}", yaml);
    }

    Ok(())
}

async fn handle_apply_plan(cli: &Cli, args: TableCreatePlanApplyArgs) -> anyhow::Result<()> {
    let TableCreatePlanApplyArgs {
        plan,
        arg,
        priority,
    } = args;

    let plan_yaml = match plan {
        Some(path) => std::fs::read_to_string(&path)?,
        None => {
            if std::io::stdin().is_terminal() {
                bail!("no plan provided; use --plan <file> or pipe YAML to stdin");
            }

            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf)?;
            buf
        }
    };

    let timeout = cli.timeout.unwrap_or(time::Duration::from_secs(1800));
    let mut client = grpc::Client::new_lazy(&cli.profile, timeout)?;

    let req = commanderpb::TableCreatePlanApplyRequest {
        job_request_common: Some(job_request_common(arg, priority)),
        plan_yaml,
    };

    let progress = cli.new_spinner().with_message("Applying plan...");

    if let Err(e) = apply_plan(cli, &mut client, req, &progress).await {
        progress.finish_with_failed();
        return Err(e);
    }

    progress.finish_with_done();
    Ok(())
}

async fn handle_create_table(cli: &Cli, args: TableCreateArgs) -> anyhow::Result<()> {
    let TableCreateArgs {
        table_name: name,
        branch,
        namespace,
        search_uri,
        partitioned_by,
        replace,
        arg,
        priority,
    } = args;

    let timeout = cli.timeout.unwrap_or(time::Duration::from_secs(1800));
    let mut client = grpc::Client::new_lazy(&cli.profile, timeout)?;

    let branch = branch.or_else(|| cli.profile.active_branch.clone());
    let common = job_request_common(arg, priority);

    // Step 1: create the plan.
    let plan_req = commanderpb::TableCreatePlanRequest {
        job_request_common: Some(common.clone()),
        branch_name: branch,
        table_name: name,
        namespace,
        search_string: search_uri.to_string(),
        table_replace: replace,
        table_partitioned_by: partitioned_by,
    };

    let progress = cli.new_spinner().with_message("Creating plan...");

    let (yaml, can_auto_apply) =
        match create_plan(cli, &mut client, plan_req, progress.clone()).await {
            Ok(v) => v,
            Err(e) => {
                progress.finish_with_failed();
                return Err(e);
            }
        };

    progress.finish_with_done();

    if !can_auto_apply {
        bail!(
            "plan has schema conflicts and cannot be auto-applied; \
             use `table create-plan` and `table create-plan-apply` instead"
        );
    }

    // Step 2: apply the plan.
    let progress = cli.new_spinner().with_message("Applying plan...");
    progress.enable_steady_tick(time::Duration::from_millis(100));

    let apply_req = commanderpb::TableCreatePlanApplyRequest {
        job_request_common: Some(common),
        plan_yaml: yaml,
    };

    if let Err(e) = apply_plan(cli, &mut client, apply_req, &progress).await {
        progress.finish_with_failed();
        return Err(e);
    }

    progress.finish_with_done();
    Ok(())
}

async fn handle_import_data(cli: &Cli, args: TableImportArgs) -> anyhow::Result<()> {
    let TableImportArgs {
        table_name: name,
        branch,
        namespace,
        search_uri,
        continue_on_error,
        import_duplicate_files,
        best_effort,
        detach,
        arg,
        priority,
    } = args;

    let timeout = cli.timeout.unwrap_or(time::Duration::from_secs(1800));
    let mut client = grpc::Client::new_lazy(&cli.profile, timeout)?;

    let branch = branch.or_else(|| cli.profile.active_branch.clone());

    let req = commanderpb::TableDataImportRequest {
        job_request_common: Some(job_request_common(arg, priority)),
        branch_name: branch,
        table_name: name,
        namespace,
        search_string: search_uri.to_string(),
        import_duplicate_files,
        best_effort,
        continue_on_error,
        transformation_query: None,
        preview: String::new(),
    };

    let progress = cli.new_spinner().with_message("Importing data...");

    let resp = match client.table_data_import(cli.traced(req)).await {
        Ok(v) => v.into_inner(),
        Err(e) => {
            progress.finish_with_failed();
            return Err(format_grpc_status(e));
        }
    };

    let Some(commanderpb::JobResponseCommon { job_id, .. }) = resp.job_response_common else {
        bail!("response missing job ID");
    };

    if detach {
        progress.finish_with_append("started".yellow());
        eprintln!("\nJob {job_id} is now running in detached mode.\n");
        eprintln!("Tip: use \"bauplan job <command>\" to list and inspect running jobs.");
        return Ok(());
    }

    let ctrl_c = tokio::signal::ctrl_c();
    futures::pin_mut!(ctrl_c);

    if let Err(e) = monitor_job_progress(
        cli,
        &mut client,
        job_id,
        "job",
        progress.clone(),
        ctrl_c,
        |_| {},
    )
    .await
    {
        progress.finish_with_failed();
        return Err(e);
    }

    progress.finish_with_done();
    info!("data imported successfully");
    Ok(())
}

async fn handle_create_external(cli: &Cli, args: TableCreateExternalArgs) -> anyhow::Result<()> {
    let TableCreateExternalArgs {
        table_name,
        branch,
        namespace,
        metadata_json_uri,
        search_pattern,
        overwrite,
        detach,
        arg,
        priority,
    } = args;

    if metadata_json_uri.is_some() {
        // We should be in `handle_create_external_from_metadata`.
        unreachable!()
    }

    let timeout = cli.timeout.unwrap_or(time::Duration::from_secs(1800));
    let mut client = grpc::Client::new_lazy(&cli.profile, timeout)?;

    let branch = branch.or_else(|| cli.profile.active_branch.clone());

    let req = commanderpb::ExternalTableCreateRequest {
        job_request_common: Some(job_request_common(arg, priority)),
        branch_name: branch,
        table_name,
        namespace,
        input_source: Some(
            commanderpb::external_table_create_request::InputSource::InputFiles(
                commanderpb::SearchUris {
                    uris: search_pattern,
                },
            ),
        ),
        overwrite,
    };

    let progress = cli.new_spinner().with_message("Creating external table...");

    let resp = match client.external_table_create(cli.traced(req)).await {
        Ok(resp) => resp.into_inner(),
        Err(e) => {
            progress.finish_and_clear();
            return Err(format_grpc_status(e));
        }
    };

    let job_id = resp
        .job_response_common
        .as_ref()
        .map(|c| c.job_id.clone())
        .ok_or_else(|| anyhow!("response missing job ID"))?;

    if detach {
        progress.finish_and_clear();
        eprintln!("\nJob {job_id} is now running in detached mode.\n");
        eprintln!("Tip: use \"bauplan job <command>\" to list and inspect running jobs.");
        return Ok(());
    }

    let ctrl_c = tokio::signal::ctrl_c();
    futures::pin_mut!(ctrl_c);

    monitor_job_progress(
        cli,
        &mut client,
        job_id,
        "job",
        progress.clone(),
        ctrl_c,
        |_| {},
    )
    .await?;

    Ok(())
}

fn handle_create_external_from_metadata(
    cli: &Cli,
    args: TableCreateExternalArgs,
) -> anyhow::Result<()> {
    let TableCreateExternalArgs {
        table_name,
        branch,
        namespace,
        metadata_json_uri,
        overwrite,
        ..
    } = args;

    // If this were None, we'd be in the other function.
    let metadata_uri = metadata_json_uri.unwrap();
    let Ok(url) = url::Url::parse(&metadata_uri) else {
        bail!("invalid metadata URI: {metadata_uri}");
    };

    if url.scheme() != "s3" {
        bail!("metadata JSON URI must use s3:// scheme");
    }

    // Namespace is required for metadata mode, because it forms part of the
    // iceberg endpoint.
    let namespace = namespace.ok_or_else(|| {
        anyhow!(
            "namespace must be specified when creating from metadata-json-uri. \
             This restriction will be lifted in future versions"
        )
    })?;

    let branch = branch
        .or_else(|| cli.profile.active_branch.clone())
        .unwrap_or_else(|| "main".to_string());

    let req = bauplan::iceberg::RegisterTable {
        name: &table_name,
        metadata_location: &metadata_uri,
        overwrite,
        branch: &branch,
        namespace: &namespace,
    };

    let resp = cli.roundtrip(req)?;

    let table_id = resp.metadata.uuid();
    info!(
        table_id = %table_id.as_hyphenated(),
        namespace = namespace,
        "registered external table"
    );

    Ok(())
}

fn handle_revert_table(
    cli: &Cli,
    TableRevertArgs {
        table_name,
        source_ref,
        into_branch,
        replace,
        commit_body,
        commit_property,
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

    let r#ref = cli.roundtrip(req)?;
    tracing::debug!(?r#ref, "Created ref");
    eprintln!("Reverted table {table_name:?} to {source_ref:?} in {into_branch:?}");

    Ok(())
}
