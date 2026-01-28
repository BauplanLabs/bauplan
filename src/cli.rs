mod branch;
mod checkout;
mod commit;
mod config;
mod job;
mod namespace;
mod parameter;
mod query;
mod rerun;
mod run;
mod table;
mod tag;

use std::{
    io::{Write as _, stdout},
    str::FromStr,
    time,
};

use anyhow::bail;
use bauplan::{
    ApiError, ApiErrorKind, ApiRequest, ApiResponse, Profile,
    grpc::{self, generated as commanderpb},
};
use clap::{Parser, Subcommand};
use colored::Colorize as _;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};

#[derive(Debug, Parser)]
#[command(
    name = "bauplan",
    about = "The Bauplan CLI",
    version = env!("BPLN_VERSION"),
    propagate_version = true
)]
pub(crate) struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    #[command(subcommand)]
    pub command: Command,
}

/// How to format output.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Output {
    Json,
    #[default]
    Tty,
}

/// A priority for a job, from 1-10, where 10 is the highest.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct Priority(u32);

impl FromStr for Priority {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let priority = s.parse::<u32>()?;
        if !(1..=10).contains(&priority) {
            bail!("Invalid priority: {}", s);
        }

        Ok(Priority(priority))
    }
}

/// key=value string pairs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyValue(String, String);

impl KeyValue {
    fn as_strs(&self) -> (&str, &str) {
        (&self.0, &self.1)
    }

    fn into_strings(self) -> (String, String) {
        (self.0, self.1)
    }
}

impl FromStr for KeyValue {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Some((left, right)) = s.split_once('=') else {
            bail!("Invalid key=value pair: {}", s);
        };

        Ok(KeyValue(left.to_owned(), right.to_owned()))
    }
}

#[derive(Debug, clap::Args)]
pub(crate) struct GlobalArgs {
    /// Name of the profile to use
    #[arg(long, short = 'P', global = true)]
    pub profile: Option<String>,
    /// Output format (options: tty, json)
    #[arg(long, short = 'O', global = true)]
    pub output: Option<Output>,
    /// Timeout (in seconds) for client operations. (-1 = no timeout, default is command specific)
    #[arg(long, global = true)]
    pub client_timeout: Option<i64>,
    /// Print verbose logs
    #[arg(long, short = 'v', global = true)]
    pub verbose: bool,
    /// Enable colored output
    #[arg(long, global = true, overrides_with = "_no_color", hide = true)]
    pub color: bool,
    /// Disable colored output
    #[arg(long = "no-color", global = true)]
    pub _no_color: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Execute a bauplan run
    Run(run::RunArgs),
    /// Re-execute a previous bauplan run
    Rerun(rerun::RerunArgs),
    /// Manage branches
    Branch(branch::BranchArgs),
    /// Manage tags
    Tag(tag::TagArgs),
    /// Show commit history for a ref
    Commit(commit::CommitArgs),
    /// Manage namespaces
    Namespace(namespace::NamespaceArgs),
    /// Manage tables
    Table(table::TableArgs),
    /// Run an SQL query
    Query(query::QueryArgs),
    /// Manage project parameters
    Parameter(parameter::ParameterArgs),
    /// Configure Bauplan CLI settings
    Config(config::ConfigArgs),
    /// Print debug information about the current environment
    Info,
    /// Manage jobs
    Job(job::JobArgs),
    /// Set the active branch
    Checkout(checkout::CheckoutArgs),
}

pub(crate) struct Cli {
    pub(crate) profile: Profile,
    pub(crate) global: GlobalArgs,
    pub(crate) timeout: Option<time::Duration>,
    pub(crate) agent: ureq::Agent,
    pub(crate) multiprogress: indicatif::MultiProgress,
}

impl Cli {
    /// Creates a progress spinner that plays nicely with logging.
    fn new_spinner(&self) -> ProgressBar {
        fn elapsed_decimal(state: &ProgressState, w: &mut dyn std::fmt::Write) {
            let secs = state.elapsed().as_secs_f64();
            write!(w, "[{secs:.1}s]").unwrap()
        }
        fn current_timestamp(_state: &ProgressState, w: &mut dyn std::fmt::Write) {
            write!(w, "{}", chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ")).unwrap();
        }

        // This format aligns with the log output.
        let progress = ProgressBar::new_spinner().with_style(
            ProgressStyle::with_template(
                "{current_timestamp:.dim} {elapsed_decimal:.dim} {msg:.blue} {spinner:.cyan/blue}",
            )
            .unwrap()
            .with_key("elapsed_decimal", elapsed_decimal)
            .with_key("current_timestamp", current_timestamp)
            .tick_strings(&["⠋", "⠙", "⠚", "⠞", "⠖", "⠦", "⠴", "⠲", "⠳", "⠓"]),
        );

        progress.enable_steady_tick(time::Duration::from_millis(100));
        self.multiprogress.add(progress)
    }
}

pub(crate) fn run(args: Args, multiprogress: indicatif::MultiProgress) -> anyhow::Result<()> {
    let profile = if let Some(name) = args.global.profile.as_deref() {
        Profile::from_env(name)
    } else {
        Profile::from_default_env()
    };

    let profile = profile?.with_ua_product("bauplan-cli");

    // Allows error responses to be parsed.
    let mut cfg = ureq::config::Config::builder().http_status_as_error(false);
    let timeout = match args.global.client_timeout {
        Some(-1) | None => None,
        Some(v) if v > 0 => Some(time::Duration::from_secs(v as _)),
        Some(v) => bail!("Invalid timeout value: {v}"),
    };

    cfg = cfg.timeout_global(timeout);
    let agent = ureq::Agent::new_with_config(cfg.build());

    let cli = Cli {
        profile,
        global: args.global,
        timeout,
        agent,
        multiprogress,
    };

    match args.command {
        Command::Info => with_rt(get_info(&cli)),
        Command::Run(args) => run::handle(&cli, args),
        Command::Rerun(args) => rerun::handle(&cli, args),
        Command::Branch(args) => branch::handle(&cli, args),
        Command::Tag(args) => tag::handle(&cli, args),
        Command::Commit(args) => commit::handle(&cli, args),
        Command::Namespace(args) => namespace::handle(&cli, args),
        Command::Table(args) => table::handle(&cli, args),
        Command::Query(args) => with_rt(query::handle(&cli, args)),
        Command::Parameter(args) => parameter::handle(&cli, args),
        Command::Config(args) => config::handle(&cli, args),
        Command::Job(args) => job::handle(&cli, args),
        Command::Checkout(args) => checkout::handle(&cli, args),
    }
}

fn with_rt<T, F: Future<Output = anyhow::Result<T>>>(f: F) -> anyhow::Result<T> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let _guard = rt.enter();
    rt.block_on(f)
}

fn roundtrip<T: ApiRequest>(cli: &Cli, req: T) -> anyhow::Result<T::Response> {
    let resp = cli.agent.run(req.into_request(&cli.profile)?)?;
    let resp = <T::Response as ApiResponse>::from_response(resp.map(ureq::Body::into_reader))?;
    Ok(resp)
}

fn is_api_err_kind(e: &anyhow::Error, k: ApiErrorKind) -> bool {
    match e.downcast_ref() {
        Some(ApiError::ErrorResponse { kind, .. }) => *kind == k,
        _ => false,
    }
}

async fn get_info(cli: &Cli) -> anyhow::Result<()> {
    let mut stdout = stdout().lock();

    let mut client = grpc::Client::new_lazy(
        &cli.profile,
        cli.timeout.unwrap_or(time::Duration::from_secs(10)),
    )?;

    let resp = client
        .get_bauplan_info(commanderpb::GetBauplanInfoRequest::default())
        .await?
        .into_inner();

    let profile_name = &cli.profile.name;
    let active_branch = cli.profile.active_branch.as_deref().unwrap_or("main");

    writeln!(&mut stdout, "{:<35} {profile_name}", "Profile".green())?;
    writeln!(
        &mut stdout,
        "{:<35} {active_branch}",
        "Active branch".green()
    )?;

    writeln!(
        &mut stdout,
        "{:<35} {}",
        "Client Version".green(),
        env!("BPLN_VERSION"),
    )?;

    if let Some(user) = resp.user_info {
        writeln!(&mut stdout, "\n{}", "User".white().bold())?;
        writeln!(&mut stdout, "{:<35} {}", "ID".blue(), user.id)?;
        writeln!(&mut stdout, "{:<35} {}", "Username".blue(), user.username)?;
        writeln!(
            &mut stdout,
            "{:<35} {} {}",
            "Full Name".blue(),
            user.first_name,
            user.last_name
        )?;
    } else if !resp.user.is_empty() {
        writeln!(&mut stdout, "{:<35} {}", "Username".blue(), resp.user)?;
    }

    if let Some(org) = resp.organization_info {
        writeln!(&mut stdout, "\n{}", "Organization".white().bold())?;
        writeln!(&mut stdout, "{:<35} {}", "ID".blue(), org.id)?;
        writeln!(&mut stdout, "{:<35} {}", "Name".blue(), org.name)?;
        if let Some(key) = &org.default_parameter_secret_key {
            writeln!(&mut stdout, "{:<35} {key}", "Default Secret Key".blue())?;
            if let Some(pkey) = &org.default_parameter_secret_public_key {
                writeln!(
                    &mut stdout,
                    "{:<35} {pkey}",
                    "Default Secret Public Key".blue()
                )?;
            }
        }
    }

    writeln!(&mut stdout, "\n{}", "Runners".white().bold())?;
    for runner in resp.runners {
        writeln!(&mut stdout, "╰ {}", runner.hostname)?;
    }

    Ok(())
}
