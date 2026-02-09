mod branch;
mod checkout;
mod commit;
mod config;
mod job;
mod namespace;
mod parameter;
mod query;
mod run;
mod spinner;
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
use yansi::Paint as _;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum OnOff {
    On,
    Off,
}

impl std::fmt::Display for OnOff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OnOff::On => write!(f, "on"),
            OnOff::Off => write!(f, "off"),
        }
    }
}

impl From<OnOff> for bool {
    fn from(value: OnOff) -> Self {
        match value {
            OnOff::On => true,
            OnOff::Off => false,
        }
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
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Print version.
    Version,
    /// Print debug information about the current environment
    Info,
    /// Execute a bauplan run
    Run(run::RunArgs),
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

pub(crate) fn run(args: Args, multiprogress: indicatif::MultiProgress) -> anyhow::Result<()> {
    // Some commands don't require any config.
    if let Command::Version = args.command {
        println!("bauplan {}", env!("BPLN_VERSION"));
        return Ok(());
    }

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
        Command::Version => unreachable!(),
        Command::Parameter(args) => parameter::handle(&cli, args),
        Command::Info => with_rt(handle_info(&cli)),
        Command::Run(args) => run::handle(&cli, args),
        Command::Branch(args) => branch::handle(&cli, args),
        Command::Tag(args) => tag::handle(&cli, args),
        Command::Commit(args) => commit::handle(&cli, args),
        Command::Namespace(args) => namespace::handle(&cli, args),
        Command::Table(args) => table::handle(&cli, args),
        Command::Query(args) => with_rt(query::handle(&cli, args)),
        Command::Config(args) => config::handle(&cli, args),
        Command::Job(args) => with_rt(job::handle(&cli, args)),
        Command::Checkout(args) => checkout::handle(&cli, args),
    }
}

fn with_rt<T, F: Future<Output = T>>(f: F) -> T {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
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

async fn handle_info(cli: &Cli) -> anyhow::Result<()> {
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
