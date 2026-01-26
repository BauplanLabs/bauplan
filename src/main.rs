mod cli;

use clap::Parser;
use tracing_subscriber::{EnvFilter, fmt};

fn main() -> anyhow::Result<()> {
    let args = cli::Args::parse();

    init_logging(args.global.verbose);

    cli::run(args)
}

fn init_logging(verbose: bool) {
    let level = if verbose { "debug" } else { "info" };
    let filter = EnvFilter::new(format!("bauplan={level}"));

    let format = fmt::format().with_target(false).with_level(true);
    fmt().event_format(format).with_env_filter(filter).init();
}
