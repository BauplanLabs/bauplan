mod cli;

use std::process::ExitCode;

use bauplan_api::error::GenericError;
use clap::Parser;
use tracing_indicatif::IndicatifWriter;
use tracing_subscriber::{EnvFilter, fmt};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            anstream::eprintln!("Error: {}", report(&err));
            ExitCode::FAILURE
        }
    }
}

/// An error with its causes, like anyhow prints them. We skip the generic
/// variant of the api errors because it only contains a fixed message, so we
/// show the error inside it.
fn report(err: &anyhow::Error) -> String {
    let mut chain = err.chain().filter(|err| {
        !err.source()
            .is_some_and(|source| source.is::<GenericError>())
    });
    let mut report = chain.next().map(ToString::to_string).unwrap_or_default();

    let causes: Vec<_> = chain.collect();
    match causes.as_slice() {
        [] => {}
        [cause] => report.push_str(&format!("\n\nCaused by:\n    {cause}")),
        causes => {
            report.push_str("\n\nCaused by:");
            for (n, cause) in causes.iter().enumerate() {
                report.push_str(&format!("\n    {n}: {cause}"));
            }
        }
    }

    let backtrace = err.backtrace();
    if backtrace.status() == std::backtrace::BacktraceStatus::Captured {
        report.push_str(&format!("\n\nStack backtrace:\n{backtrace}"));
    }
    report
}

fn run() -> anyhow::Result<()> {
    let args = cli::Args::parse();

    // Used by integration tests to validate argument parsing without executing.
    if cfg!(feature = "_check-parse") {
        return Ok(());
    }

    // Tracks global progress bar state. This is necessary so that indicatif
    // progress bars and tracing log lines play nicely with each other.
    let mp = indicatif::MultiProgress::new();

    init_logging(args.global.verbose, mp.clone())?;

    cli::run(args, mp)
}

fn init_logging(verbose: bool, mp: indicatif::MultiProgress) -> anyhow::Result<()> {
    let level = if verbose { "debug" } else { "info" };
    let filter = EnvFilter::builder()
        .with_default_directive(format!("bauplan={level}").parse()?)
        .from_env()?;

    let timer = fmt::time::ChronoUtc::new("%Y-%m-%dT%H:%M:%SZ".to_owned());
    let format = fmt::format()
        .with_target(false)
        .with_level(true)
        .with_timer(timer);

    let writer: IndicatifWriter<tracing_indicatif::writer::Stderr> = IndicatifWriter::new(mp);
    tracing_subscriber::fmt()
        .with_writer(writer)
        .event_format(format)
        .with_env_filter(filter)
        .init();

    Ok(())
}
