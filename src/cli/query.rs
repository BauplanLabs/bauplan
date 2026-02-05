use std::{fmt::Write as _, io::Write, path::PathBuf, time};

use crate::cli::{Cli, KeyValue, OnOff, Output, Priority};
use anyhow::{Context as _, anyhow, bail};
use arrow::{
    array::RecordBatch,
    datatypes::Schema,
    util::display::{ArrayFormatter, FormatOptions},
};
use arrow_flight::error::Result as FlightResult;
use bauplan::flight::fetch_flight_results;
use bauplan::grpc::{self, generated as commanderpb};
use commanderpb::runner_event::Event as RunnerEvent;
use futures::{Stream, TryStreamExt, future};
use gethostname::gethostname;
use tabwriter::TabWriter;
use tracing::{debug, error};

#[derive(Debug, clap::Args)]
pub(crate) struct QueryArgs {
    /// Do not truncate output
    #[arg(long)]
    pub no_trunc: bool,
    /// Set the cache mode.
    #[arg(long)]
    pub cache: Option<OnOff>,
    /// Read query from file
    #[arg(short, long)]
    pub file: Option<PathBuf>,
    /// Arguments to pass to the job. Format: key=value
    #[arg(short, long, action = clap::ArgAction::Append)]
    pub arg: Vec<KeyValue>,
    /// Ref or branch name to run query against.
    #[arg(short, long)]
    pub r#ref: Option<String>,
    /// Limit number of returned rows. (use --all-rows to disable this)
    #[arg(long, default_value = "10")]
    pub max_rows: Option<u64>,
    /// Do not limit returned rows. Supercedes --max-rows
    #[arg(long)]
    pub all_rows: bool,
    /// Namespace to run the query in
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// Set the job priority (1-10, where 10 is highest priority)
    #[arg(long)]
    pub priority: Option<Priority>,
    /// Sql
    pub sql: Option<String>,
}

pub(crate) async fn handle(cli: &Cli, args: QueryArgs) -> anyhow::Result<()> {
    use yansi::Paint as _;

    let QueryArgs {
        no_trunc,
        cache,
        file,
        arg,
        r#ref,
        max_rows,
        namespace,
        all_rows,
        priority,
        sql,
    } = args;

    let timeout = cli.timeout.unwrap_or(time::Duration::from_secs(1800));

    let mut client = grpc::Client::new_lazy(&cli.profile, timeout)?;

    let sql_query = match (sql, file) {
        (None, Some(path)) => std::fs::read_to_string(&path)?,
        (Some(s), None) => s,
        _ => bail!("exactly one of either '--file' or inline SQL must be specified"),
    };

    let row_limit = if let Some(n) = max_rows
        && n > 0
        && !all_rows
    {
        Some(n)
    } else {
        None
    };

    let hostname = gethostname().to_string_lossy().into_owned();
    let args = arg.into_iter().map(KeyValue::into_strings).collect();

    let progress = cli.new_spinner().with_message("Planning query...");
    progress.enable_steady_tick(time::Duration::from_millis(100));

    let req = commanderpb::QueryRunRequest {
        job_request_common: Some(commanderpb::JobRequestCommon {
            module_version: Default::default(),
            hostname,
            args,
            debug: 0,
            priority: priority.map(|p| p.0 as _),
        }),
        r#ref,
        sql_query,
        cache: cache.unwrap_or(OnOff::On).to_string(),
        namespace,
    };

    let resp = match client.query_run(req).await {
        Ok(resp) => resp.into_inner(),
        Err(e) => {
            progress.finish_and_clear();
            return Err(anyhow!("{}", e.message()));
        }
    };

    let job_id = resp.job_response_common.as_ref().map(|c| &c.job_id);
    let Some(job_id) = job_id.to_owned() else {
        bail!("response missing job ID");
    };

    debug!(job_id, "successfully planned query");
    progress.set_message("Executing query...");

    let mut req = tonic::Request::new(commanderpb::SubscribeLogsRequest {
        job_id: job_id.clone(),
    });
    req.set_timeout(timeout);

    // This replaces the default handler, so we need to manually exit on
    // SIGINT from now on.
    let ctrl_c = tokio::signal::ctrl_c();
    futures::pin_mut!(ctrl_c);

    let mut client_clone = client.clone();
    let stream = client_clone.monitor_job(job_id.to_owned(), timeout);
    futures::pin_mut!(stream);

    // If we hit a timeout or SIGINT below, we'll call this closure.
    let mut kill_query = async |reason: &str| -> ! {
        error!(job_id, "{reason}, cancelling query");

        progress.set_message("Cancelling query...");
        if let Err(e) = client.cancel(job_id).await {
            error!(job_id, error = %e, "failed to cancel query");
            progress.finish_with_message(format!("Cancelling query... {}", "failed".red()));
        } else {
            debug!(job_id, "query successfully cancelled");
            progress.finish_with_message(format!("Cancelling query... {}", "done".green()));
        }

        std::process::exit(1)
    };

    let mut flight_event = None;
    loop {
        let res = match future::select(stream.try_next(), &mut ctrl_c).await {
            future::Either::Left((v, _)) => v,
            future::Either::Right(_) => kill_query("interrupt received").await,
        };

        let event = match res {
            Ok(Some(v)) => v,
            Ok(None) => break,
            Err(e)
                if e.code() == tonic::Code::Cancelled
                    || e.code() == tonic::Code::DeadlineExceeded =>
            {
                kill_query("execution timed out").await
            }
            Err(e) => return Err(e.into()),
        };

        match event {
            // Supposed to happen first.
            RunnerEvent::FlightServerStart(flight) => flight_event = Some(flight),
            RunnerEvent::JobCompletion(completion) => {
                if let Err(e) = grpc::interpret_outcome(completion.outcome) {
                    let suffix = match e {
                        grpc::JobError::Cancelled => "cancelled".red(),
                        grpc::JobError::Timeout => "timeout".red(),
                        _ => "failed".red(),
                    };

                    progress.finish_with_message(format!("Executing query... {suffix}"));
                    return Err(e.into());
                }

                break;
            }
            _ => (),
        }
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

    let fut = async {
        progress.set_message("Fetching results...");
        let (schema, batches) = fetch_flight_results(endpoint, magic_token, timeout, row_limit)
            .await
            .context("Failed to fetch query results")?;
        futures::pin_mut!(batches);

        progress.finish_with_message(format!("Fetching results... {}", "done".green()));
        match cli.global.output.unwrap_or_default() {
            Output::Tty => print_tty(schema, batches, !no_trunc).await,
            Output::Json => print_json(batches, job_id).await,
        }
    };

    futures::pin_mut!(fut);
    match future::select(fut, &mut ctrl_c).await {
        future::Either::Left((v, _)) => v,
        future::Either::Right(_) => std::process::exit(1),
    }
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
