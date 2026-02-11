use std::{
    io::{Write, stdout},
    path::{Path, PathBuf},
    time,
};

use anyhow::{Context as _, anyhow, bail};
use bauplan::{
    grpc::{self, generated as commanderpb},
    project::{ParameterDefault, ParameterType, ParameterValue, ProjectFile},
};
use resolve_path::PathResolveExt as _;
use tabwriter::TabWriter;
use yansi::Paint;

use crate::cli::{Cli, format_grpc_status, with_rt, yaml};

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum ParameterTypeArg {
    Int,
    Float,
    Bool,
    Str,
    Secret,
}

impl From<ParameterTypeArg> for bauplan::project::ParameterType {
    fn from(t: ParameterTypeArg) -> Self {
        match t {
            ParameterTypeArg::Int => Self::Int,
            ParameterTypeArg::Float => Self::Float,
            ParameterTypeArg::Bool => Self::Bool,
            ParameterTypeArg::Str => Self::Str,
            ParameterTypeArg::Secret => Self::Secret,
        }
    }
}

#[derive(Debug, clap::Args)]
pub(crate) struct ParameterArgs {
    #[command(subcommand)]
    pub command: ParameterCommand,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum ParameterCommand {
    /// List all parameters in a project
    #[clap(alias = "list")]
    Ls(ParameterLsArgs),
    /// Remove a parameter from a project
    #[clap(alias = "delete")]
    Rm(ParameterRmArgs),
    /// Set a parameter value in a project
    Set(ParameterSetArgs),
}

#[derive(Debug, clap::Args)]
pub(crate) struct ParameterLsArgs {
    /// Path to the root Bauplan project directory.
    #[arg(short, long)]
    pub project_dir: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct ParameterRmArgs {
    /// Name of the parameter to remove
    pub name: String,
    /// Path to the root Bauplan project directory.
    #[arg(short, long)]
    pub project_dir: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct ParameterSetArgs {
    /// Name
    pub name: String,
    /// A default value.
    pub default_value: Option<String>,
    /// The type of the parameter
    #[arg(long)]
    pub r#type: Option<ParameterTypeArg>,
    /// A description of the parameter
    #[arg(long)]
    pub description: Option<String>,
    /// Mark the parameter as required
    #[arg(long)]
    pub required: bool,
    /// Mark the parameter as optional
    #[arg(long)]
    pub optional: bool,
    /// Read value from file
    #[arg(short, long)]
    pub file: Option<PathBuf>,
    /// Path to the root Bauplan project directory.
    #[arg(short, long)]
    pub project_dir: Option<PathBuf>,
}

pub(crate) fn handle(cli: &Cli, args: ParameterArgs) -> anyhow::Result<()> {
    match args.command {
        ParameterCommand::Ls(args) => list_parameters(args),
        ParameterCommand::Rm(args) => remove_parameter(args),
        ParameterCommand::Set(args) => set_parameter(cli, args),
    }
}

fn list_parameters(args: ParameterLsArgs) -> anyhow::Result<()> {
    let project_dir = resolve_project_dir(args.project_dir.as_deref())?;
    let project = ProjectFile::from_dir(&project_dir)?;

    // Validate that the parameters are valid.
    for (name, param) in &project.parameters {
        if param.default.is_some() {
            param
                .eval_default()
                .context(format!("parameter {name:?}"))?;
        }
    }

    print_parameters(&project)
}

fn remove_parameter(args: ParameterRmArgs) -> anyhow::Result<()> {
    validate_parameter_name(&args.name)?;

    let project_dir = resolve_project_dir(args.project_dir.as_deref())?;
    let mut project = ProjectFile::from_dir(&project_dir)?;

    if !project.parameters.contains_key(&args.name) {
        anyhow::bail!("parameter not found: {:?}", args.name);
    }

    yaml::edit(&project.path, |doc| write_parameter(doc, &args.name, None))
        .context("unable to update parameter in project file")?;

    project.parameters.remove(&args.name);
    print_parameters(&project)
}

fn set_parameter(cli: &Cli, args: ParameterSetArgs) -> anyhow::Result<()> {
    validate_parameter_name(&args.name)?;

    if args.default_value.is_some() && args.file.is_some() {
        anyhow::bail!("cannot set both value and file");
    }

    let default_value = if let Some(p) = &args.file {
        Some(std::fs::read_to_string(p)?)
    } else {
        args.default_value
    };

    let project_dir = resolve_project_dir(args.project_dir.as_deref())?;
    let mut project = ProjectFile::from_dir(&project_dir)?;

    let param_type = args.r#type.map(bauplan::project::ParameterType::from);
    let param = project
        .parameters
        .entry(args.name.clone())
        .or_insert(ParameterDefault {
            param_type: param_type.unwrap_or_default(),
            required: false,
            default: None,
            description: None,
            key: None,
        });

    if param.default.is_some() && param_type.is_some() && default_value.is_none() {
        bail!("cannot change the type of a parameter without also changing the default value");
    }

    if let Some(v) = default_value {
        if let Some(t) = param_type {
            param.param_type = t;
        }

        let value = match param.param_type {
            ParameterType::Secret => {
                // Fetch the org-wide public key from commander.
                let timeout = cli.timeout.unwrap_or(time::Duration::from_secs(5));
                let mut client = grpc::Client::new_lazy(&cli.profile, timeout)?;

                let req = cli.traced(commanderpb::GetBauplanInfoRequest::default());
                let (key_name, key) =
                    with_rt(client.org_default_public_key(req)).map_err(format_grpc_status)?;
                ParameterValue::encrypt_secret(key_name, &key, project.project.id, v)?
            }
            _ => parse_parameter(param.param_type, &v)?,
        };

        param.update_default(value)?;
    }

    if let Some(desc) = &args.description {
        let trimmed = desc.trim();
        param.description = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        };
    }

    if args.required {
        param.required = true;
    } else if args.optional {
        param.required = false;
    }

    yaml::edit(&project.path, |doc| {
        write_parameter(doc, &args.name, Some(&*param))
    })
    .context("unable to update parameter in project file")?;

    print_parameters(&project)
}

fn write_parameter(
    doc: &mut nondestructive::yaml::Document,
    name: &str,
    param: Option<&ParameterDefault>,
) -> anyhow::Result<()> {
    let Some(ParameterDefault {
        param_type,
        required,
        default,
        description,
        key,
    }) = param
    else {
        yaml::mapping_at_path(doc, &["parameters"])?.remove(name);
        return Ok(());
    };

    let mut entry = yaml::mapping_at_path(doc, &["parameters", name])?;

    yaml::upsert_str(&mut entry, "type", &param_type.to_string());
    match default {
        None => {
            entry.remove("default");
        }
        Some(serde_yaml::Value::String(s)) => yaml::upsert_str(&mut entry, "default", s),
        Some(serde_yaml::Value::Bool(b)) => yaml::upsert_bool(&mut entry, "default", *b),
        Some(serde_yaml::Value::Number(n)) if n.is_i64() => {
            yaml::upsert_i64(&mut entry, "default", n.as_i64().unwrap())
        }
        Some(serde_yaml::Value::Number(n)) if n.is_f64() => {
            yaml::upsert_f64(&mut entry, "default", n.as_f64().unwrap())
        }
        v => bail!("invalid type for parameter default: {v:?}"),
    }

    if let Some(k) = &key {
        yaml::upsert_str(&mut entry, "key", k)
    } else {
        entry.remove("key");
    }

    if let Some(d) = &description {
        yaml::upsert_str(&mut entry, "description", d)
    } else {
        entry.remove("description");
    }

    if let Some(mut v) = entry.get_mut("required") {
        v.set_bool(*required);
    } else if *required {
        entry.insert_bool("required", true);
    }

    Ok(())
}

pub(crate) fn resolve_project_dir(arg: Option<&Path>) -> std::io::Result<PathBuf> {
    if let Some(p) = arg {
        Ok(p.try_resolve()?.into_owned())
    } else {
        std::env::current_dir()
    }
}

pub(crate) fn validate_parameter_name(name: &str) -> anyhow::Result<()> {
    if name.trim().is_empty() {
        bail!("empty parameter name");
    }

    if name.contains(|c: char| c.is_whitespace() || c == '.') {
        bail!("invalid parameter name: {name:?}")
    }

    Ok(())
}

/// Parse a raw parameter string as a value. Should only be called for
/// non-secret parameters.
pub(crate) fn parse_parameter(
    param_type: ParameterType,
    value: &str,
) -> anyhow::Result<ParameterValue> {
    let ctx = || format!("invalid value {value:?} for {param_type}");
    let parsed = match param_type {
        ParameterType::Int => value.parse().map(ParameterValue::Int).with_context(ctx)?,
        ParameterType::Float => value.parse().map(ParameterValue::Float).with_context(ctx)?,
        ParameterType::Bool => parse_bool(value)
            .map(ParameterValue::Bool)
            .with_context(ctx)?,
        ParameterType::Str => ParameterValue::Str(value.to_string()),
        ParameterType::Vault => ParameterValue::Vault(value.to_string()),
        ParameterType::Secret => {
            panic!("parse_parameter called on secret")
        }
    };

    Ok(parsed)
}

fn parse_bool(s: &str) -> anyhow::Result<bool> {
    match s.to_lowercase().as_str() {
        "true" | "yes" | "1" | "on" => Ok(true),
        "false" | "no" | "0" | "off" => Ok(false),
        _ => Err(anyhow!("invalid boolean value: {s:?}")),
    }
}

fn print_parameters(project: &ProjectFile) -> anyhow::Result<()> {
    let mut tw = TabWriter::new(stdout().lock()).ansi(true);
    writeln!(&mut tw, "NAME\tTYPE\tREQUIRED\tDEFAULT\tDESCRIPTION")?;

    for (name, param) in &project.parameters {
        let required = if param.required {
            "required".blue()
        } else {
            "optional".dim()
        };

        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}",
            name.bold(),
            param.param_type,
            required,
            param.display_default(),
            param.description.as_deref().unwrap_or("-").dim(),
        )?;
    }

    tw.flush()?;
    Ok(())
}
