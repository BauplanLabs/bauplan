mod cli;

use clap::Parser;

fn main() -> anyhow::Result<()> {
    let e = env_logger::Env::default().default_filter_or("bauplan=info");
    env_logger::Builder::from_env(e).init();

    let args = cli::Args::parse();
    cli::run(args)
}
