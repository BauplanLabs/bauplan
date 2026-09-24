use crate::cli::{Cli, format_grpc_status};
use anyhow::Context as _;
use arrow::array::{RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Json, Router};
use bauplan::grpc::{self, generated as commanderpb};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path as StdPath;
use std::sync::{Arc, Mutex};
use std::time;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::oneshot;

pub(crate) async fn handle(cli: &Cli) -> anyhow::Result<()> {
    let mut client = grpc::Client::new_lazy(
        &cli.profile,
        cli.timeout.unwrap_or(time::Duration::from_secs(5)),
    )?;

    // Sanity check on credentials
    client
        .get_bauplan_info(cli.traced(commanderpb::GetBauplanInfoRequest::default()))
        .await
        .map_err(format_grpc_status)?
        .into_inner();

    // One file per session, keyed by pid so concurrent sessions don't interleave
    let otel_log_path =
        std::env::temp_dir().join(format!("bauplan-claude-otel-{}.jsonl", std::process::id()));
    let otel_log = Arc::new(Mutex::new(File::create(&otel_log_path)?));

    // Port 0 lets the OS pick a free port, which we read back for the endpoint
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let otel_port = listener.local_addr()?.port();

    // OTLP/HTTP exporters append /v1/{traces,logs,metrics} to the base endpoint
    let router = Router::new()
        .route("/v1/{signal}", post(record_otlp_export))
        .with_state(otel_log);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let otel_server = tokio::spawn(
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .into_future(),
    );

    // The terminal sends Ctrl+C to the whole foreground process group, so without
    // a registered handler we would die while claude keeps running. Registering
    // replaces the default action for the process lifetime; the child gets the
    // default back on exec
    let _sigint = signal(SignalKind::interrupt())?;

    // Chosen up front so we know it even if no telemetry reaches the collector
    let session_id = uuid::Uuid::new_v4();

    let status = Command::new("claude")
        //.args(args)
        .arg("--session-id")
        .arg(session_id.to_string())
        .env("CLAUDE_CODE_ENABLE_TELEMETRY", "1")
        // Traces are only exported with the beta flag on
        .env("CLAUDE_CODE_ENHANCED_TELEMETRY_BETA", "1")
        .env("OTEL_TRACES_EXPORTER", "otlp")
        .env("OTEL_LOGS_EXPORTER", "otlp")
        .env("OTEL_METRICS_EXPORTER", "otlp")
        // JSON so the collector can log payloads without decoding protobuf
        .env("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json")
        .env(
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            format!("http://127.0.0.1:{otel_port}"),
        )
        .env("OTEL_LOG_USER_PROMPTS", "1")
        .env("OTEL_LOG_TOOL_DETAILS", "1")
        .status()
        .await?;

    // Graceful shutdown lets exports already in flight finish writing
    let _ = shutdown_tx.send(());
    otel_server.await??;

    // ExitStatus Display gives "exit status: 0" or "signal: 9 (SIGKILL)"
    println!("claude exited: {status}");
    println!("claude session id: {session_id}");
    println!("otel log: {}", otel_log_path.display());

    // A failed conversion must not mask claude's exit status, so only warn
    let otel_parquet_path = otel_log_path.with_extension("parquet");
    match convert_otel_log_to_parquet(&otel_log_path, &otel_parquet_path) {
        Ok(()) => println!("otel parquet: {}", otel_parquet_path.display()),
        Err(err) => eprintln!("failed to convert otel log to parquet: {err:#}"),
    }

    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }

    Ok(())
}

/// Appends one OTLP/HTTP JSON export to the session log as a JSONL line tagged
/// with its signal (traces, logs or metrics).
async fn record_otlp_export(
    State(otel_log): State<Arc<Mutex<File>>>,
    Path(signal): Path<String>,
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let line = serde_json::json!({ "signal": signal, "payload": payload });
    // A poisoned lock or failed write only loses telemetry, never the claude session
    if let Ok(mut file) = otel_log.lock() {
        let _ = writeln!(file, "{line}");
    }
    // An empty object is a valid full-success Export*ServiceResponse
    Json(serde_json::json!({}))
}

/// Rewrites the session JSONL as a parquet file with two string columns,
/// `signal` and `payload`, keeping each OTLP export as raw JSON.
fn convert_otel_log_to_parquet(jsonl_path: &StdPath, parquet_path: &StdPath) -> anyhow::Result<()> {
    let mut signals = StringBuilder::new();
    let mut payloads = StringBuilder::new();
    for line in BufReader::new(File::open(jsonl_path)?).lines() {
        let mut record: serde_json::Value = serde_json::from_str(&line?)?;
        let signal = record["signal"]
            .as_str()
            .context("otel log line without a string signal")?
            .to_owned();
        signals.append_value(signal);
        payloads.append_value(record["payload"].take().to_string());
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("signal", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(signals.finish()), Arc::new(payloads.finish())],
    )?;

    let mut writer = ArrowWriter::try_new(File::create(parquet_path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
