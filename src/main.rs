mod cli;

use clap::Parser;
use tracing_indicatif::IndicatifWriter;
use tracing_subscriber::{EnvFilter, fmt};

fn main() -> anyhow::Result<()> {
    let args = cli::Args::parse();

    // Apply color setting. Default is auto-detect (tty check).
    // --color forces on, --no-color forces off, last one wins.
    if args.global.color {
        colored::control::set_override(true);
    }

    // Tracks global progress bar state. This is necessary so that indicatif
    // progress bars and tracing log lines play nicely with each other.
    let mp = indicatif::MultiProgress::new();

    init_logging(args.global.verbose, mp.clone());

    cli::run(args, mp)
}

fn init_logging(verbose: bool, mp: indicatif::MultiProgress) {
    let level = if verbose { "debug" } else { "info" };
    let filter = EnvFilter::new(format!("bauplan={level}"));

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
}
