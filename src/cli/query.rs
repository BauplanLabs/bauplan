use std::{fmt::Write as _, io::Write, path::PathBuf, time};

use crate::cli::{
    Cli, KeyValue, OnOff, Output, Priority, format_grpc_status,
    run::{job_request_common, monitor_job_progress},
    spinner::ProgressExt,
};
use anyhow::{Context as _, bail};
use arrow::{
    array::RecordBatch,
    datatypes::Schema,
    error::ArrowError,
    util::display::{ArrayFormatter, FormatOptions},
};
use arrow_flight::error::{FlightError, Result as FlightResult};
use bauplan::flight::{fetch_flight_results, limit_rows};
use bauplan::grpc::{self, generated as commanderpb};
use bauplan_longbow::BauplanPreset;
use commanderpb::runner_event::Event as RunnerEvent;
use futures::{Stream, StreamExt, TryStreamExt, future::Either};
use tabwriter::TabWriter;

#[derive(Debug, clap::Args)]
#[command(after_long_help = crate::cli::CliExamples("
  # Run query inline
  bauplan query \"SELECT * FROM raw_data.customers LIMIT 10\"

  # Run query from file
  bauplan query --file query.sql

  # Run query with no row limit
  bauplan query --all-rows \"SELECT COUNT(*) FROM raw_data.orders\"

  # Run query on specific branch
  bauplan query --ref main \"SELECT * FROM my_table\"

  # Run query in specific namespace
  bauplan query --namespace raw_data \"SELECT * FROM customers LIMIT 5\"

  # Run query with full output (no truncation)
  bauplan query --no-trunc \"SELECT * FROM wide_table\"
"))]
pub(crate) struct QueryArgs {
    /// Sql
    pub sql: Option<String>,
    /// Ref or branch name to run query against [default: active branch]
    #[arg(short, long)]
    pub r#ref: Option<String>,
    /// Namespace to run the query in
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// Read query from file
    #[arg(short, long, conflicts_with = "sql")]
    pub file: Option<PathBuf>,
    /// Set the cache mode.
    #[arg(long, default_value_t = OnOff::On)]
    pub cache: OnOff,
    /// Limit number of returned rows. (use --all-rows to disable this)
    #[arg(long, default_value_t = 10)]
    pub max_rows: u64,
    /// Do not limit returned rows. Supersedes --max-rows
    #[arg(long)]
    pub all_rows: bool,
    /// Do not truncate output
    #[arg(long)]
    pub no_trunc: bool,
    /// Extra arguments as key=value pairs (repeatable)
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub arg: Vec<KeyValue>,
    /// Set the job priority (1-10, where 10 is highest priority)
    #[arg(long)]
    pub priority: Option<Priority>,
}

pub(crate) async fn handle(cli: &Cli, args: QueryArgs) -> anyhow::Result<()> {
    let QueryArgs {
        sql,
        r#ref,
        namespace,
        file,
        cache,
        max_rows,
        all_rows,
        no_trunc,
        arg,
        priority,
    } = args;

    let timeout = cli.timeout.unwrap_or(time::Duration::from_secs(1800));

    let mut client = grpc::Client::new_lazy(&cli.profile, timeout)?;

    let sql_query = match (sql, file) {
        (None, Some(path)) => std::fs::read_to_string(&path)?,
        (Some(s), None) => s,
        _ => bail!("exactly one of either '--file' or inline SQL must be specified"),
    };

    let row_limit = if max_rows > 0 && !all_rows {
        Some(max_rows)
    } else {
        None
    };

    let job_request_common = job_request_common(arg, priority);

    let progress = cli.new_spinner().with_message("Planning query...");
    progress.enable_steady_tick(time::Duration::from_millis(100));

    let r#ref = r#ref.or_else(|| cli.profile.active_branch.clone());

    let req = commanderpb::QueryRunRequest {
        job_request_common: Some(job_request_common),
        r#ref,
        sql_query,
        cache: cache.to_string(),
        namespace,
    };

    let resp = match client.query_run(cli.traced(req)).await {
        Ok(resp) => resp.into_inner(),
        Err(e) => {
            progress.finish_with_failed();
            return Err(format_grpc_status(e));
        }
    };

    let Some(commanderpb::JobResponseCommon { job_id, .. }) = resp.job_response_common else {
        bail!("response missing job ID");
    };

    progress.set_message("Executing query...");

    let ctrl_c = tokio::signal::ctrl_c();
    futures::pin_mut!(ctrl_c);

    let mut flight_event = None;
    monitor_job_progress(
        cli,
        &mut client,
        job_id.clone(),
        "query",
        progress.clone(),
        &mut ctrl_c,
        |event| {
            if let RunnerEvent::FlightServerStart(flight) = event {
                flight_event = Some(flight);
            }
        },
    )
    .await?;

    progress.set_message("Fetching results...");

    let tp = cli.traceparent();
    let longbow_key = if resp.longbow_public_key.is_empty() {
        None
    } else {
        Some(resp.longbow_public_key)
    };

    let (schema, batches) =
        fetch_results(longbow_key, flight_event, timeout, row_limit, tp).await?;

    futures::pin_mut!(batches);

    progress.finish_with_done();
    match cli.global.output {
        Output::Tty => print_tty(schema, batches, !no_trunc).await,
        Output::Json => print_json(batches, &job_id).await,
    }
}

async fn fetch_results(
    longbow_public_key: Option<Vec<u8>>,
    flight_event: Option<commanderpb::FlightServerStartEvent>,
    timeout: time::Duration,
    row_limit: Option<u64>,
    traceparent: String,
) -> anyhow::Result<(Schema, impl Stream<Item = FlightResult<RecordBatch>>)> {
    if let Some(public_key_bytes) = longbow_public_key {
        let public_key = bauplan_longbow::iroh::PublicKey::try_from(public_key_bytes.as_slice())
            .context("invalid longbow public key")?;
        let preset = BauplanPreset::default();
        let addr = bauplan_longbow::iroh::EndpointAddr::new(public_key);
        let addr = preset.add_relay_urls(addr);

        let (schema, stream) =
            tokio::time::timeout(timeout, bauplan_longbow::fetch_query_results(preset, addr))
                .await
                .context("failed to fetch query results")??;

        let schema: Schema = schema.as_ref().clone();
        let stream = stream
            .map(|r| r.map_err(|e| FlightError::Arrow(ArrowError::ExternalError(Box::new(e)))));
        let stream = limit_rows(stream, row_limit);
        return Ok((schema, Either::Left(stream)));
    }

    let Some(commanderpb::FlightServerStartEvent {
        endpoint,
        magic_token,
        ..
    }) = flight_event
    else {
        bail!("Query completed, but no results available");
    };

    let endpoint = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint
    } else {
        format!("https://{}", endpoint)
    };

    let Ok(endpoint) = endpoint.parse() else {
        bail!("Invalid endpoint: {}", endpoint);
    };

    let (schema, batches) = fetch_flight_results(
        endpoint,
        magic_token,
        timeout,
        row_limit,
        Some(&traceparent),
    )
    .await
    .context("Failed to fetch query results")?;

    Ok((schema, Either::Right(batches)))
}

async fn print_tty(
    schema: Schema,
    mut batches: impl Stream<Item = FlightResult<RecordBatch>> + Unpin,
    truncate: bool,
) -> anyhow::Result<()> {
    const TRUNCATE_TO_COLUMN_WIDTH: usize = 32;

    let mut stdout = std::io::stdout().lock();

    // Print the schema.
    {
        let mut tw = TabWriter::new(&mut stdout);

        writeln!(tw, "COLUMN\tTYPE\tNULLABLE")?;
        for field in schema.fields() {
            writeln!(
                tw,
                "{}\t{}\t{}",
                field.name(),
                field.data_type(),
                field.is_nullable()
            )?;
        }

        tw.flush()?;
        writeln!(stdout)?;
    }

    // Track if we truncated any values, so we can print a helpful note at the end.
    let mut truncation_occurred = false;
    let mut header_printed = false;
    let mut tw = TabWriter::new(&mut stdout);
    let mut buf = String::new();

    while let Some(batch) = batches.try_next().await? {
        let schema = batch.schema();
        if schema.fields().is_empty() {
            writeln!(tw.into_inner().unwrap(), "No columns to display.")?;
            return Ok(());
        }

        tw.flush()?;
        if !header_printed && batch.num_rows() > 0 {
            header_printed = true;

            let mut headers = schema.fields().iter().map(|f| f.name());
            write!(tw, "{}", headers.next().unwrap())?;
            for header in headers {
                write!(tw, "\t{}", header)?;
            }

            writeln!(tw)?;
        }

        let columns = batch.columns();
        let options = FormatOptions::default().with_null("(null)");
        let formatters: Vec<_> = columns
            .iter()
            .map(|col| ArrayFormatter::try_new(col.as_ref(), &options))
            .collect::<Result<_, _>>()?;

        for row in 0..batch.num_rows() {
            for (i, formatter) in formatters.iter().enumerate() {
                if i > 0 {
                    write!(tw, "\t")?;
                }

                let value = formatter.value(row);
                if truncate {
                    buf.clear();

                    write!(buf, "{}", formatter.value(row))?;
                    if buf.len() > TRUNCATE_TO_COLUMN_WIDTH {
                        truncation_occurred = true;
                        write!(tw, "{}...", &buf[..TRUNCATE_TO_COLUMN_WIDTH - 3])?;
                    } else {
                        write!(tw, "{buf}")?;
                    }
                } else {
                    write!(tw, "{value}")?;
                }
            }

            writeln!(tw)?;
        }
    }

    tw.flush()?;

    if !header_printed {
        eprintln!("No results!");
    }

    if truncation_occurred {
        eprintln!("\nNote: some values were truncated. Use --no-trunc to see full values.");
    }

    Ok(())
}

async fn print_json(
    mut batches: impl Stream<Item = FlightResult<RecordBatch>> + Unpin,
    job_id: &str,
) -> anyhow::Result<()> {
    let mut stdout = std::io::stdout().lock();

    // We want to stream query results and not buffer them into memory. We also
    // want to provide the job_id. This is the least painful way to do that.
    let job_id_escaped = serde_json::to_string(job_id)?;
    write!(stdout, r#"{{"job_id":{job_id_escaped},"results":"#,)?;

    let mut writer = arrow::json::ArrayWriter::new(&mut stdout);
    while let Some(batch) = batches.try_next().await? {
        writer.write(&batch)?;
    }

    // Close the object..
    writer.finish()?;
    writeln!(stdout, "}}")?;

    Ok(())
}
