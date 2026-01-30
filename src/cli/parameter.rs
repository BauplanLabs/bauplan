use std::{
    io::{Write, stdout},
    path::{Path, PathBuf},
};

use anyhow::bail;
use bauplan::project::{Parameter, ProjectFile};
use resolve_path::PathResolveExt as _;
use tabwriter::TabWriter;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum ParameterType {
    Int,
    Float,
    Bool,
    Str,
    Secret,
}

impl From<ParameterType> for bauplan::project::ParameterType {
    fn from(t: ParameterType) -> Self {
        match t {
            ParameterType::Int => Self::Int,
            ParameterType::Float => Self::Float,
            ParameterType::Bool => Self::Bool,
            ParameterType::Str => Self::Str,
            ParameterType::Secret => Self::Secret,
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
    pub r#type: Option<ParameterType>,
    #[arg(long)]
    /// A description of the parameter
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

pub(crate) fn handle(args: ParameterArgs) -> anyhow::Result<()> {
    match args.command {
        ParameterCommand::Ls(a) => list_parameters(a),
        ParameterCommand::Rm(a) => remove_parameter(a),
        ParameterCommand::Set(a) => set_parameter(a),
    }
}

fn list_parameters(args: ParameterLsArgs) -> anyhow::Result<()> {
    let project_dir = project_dir(args.project_dir.as_deref())?;
    let project = ProjectFile::from_dir(&project_dir)?;

    print_parameters(&project)
}

fn remove_parameter(args: ParameterRmArgs) -> anyhow::Result<()> {
    validate_parameter_name(&args.name)?;

    let project_dir = project_dir(args.project_dir.as_deref())?;
    let mut project = ProjectFile::from_dir(&project_dir)?;

    if project.parameters.remove(&args.name).is_none() {
        anyhow::bail!("parameter not found: {:?}", args.name);
    }

    project.save()?;
    print_parameters(&project)
}

fn set_parameter(args: ParameterSetArgs) -> anyhow::Result<()> {
    validate_parameter_name(&args.name)?;

    if args.default_value.is_some() && args.file.is_some() {
        anyhow::bail!("cannot set both value and file");
    }

    let default_value = if let Some(p) = &args.file {
        Some(std::fs::read_to_string(p)?)
    } else {
        args.default_value
    };

    let project_dir = project_dir(args.project_dir.as_deref())?;
    let mut project = ProjectFile::from_dir(&project_dir)?;

    let param_type = args.r#type.map(bauplan::project::ParameterType::from);
    let param = project.parameters.entry(args.name).or_insert(Parameter {
        param_type: param_type.unwrap_or_default(),
        required: false,
        default: None,
        description: None,
        key: None,
    });

    if param.default.is_some() && param_type.is_some() && default_value.is_none() {
        bail!("cannot change the type of a parameter without also changing the default value");
    } else if let Some(v) = default_value {
        param.set_default_from_string(&v)?;
    }

    if let Some(t) = param_type {
        param.param_type = t;
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

    project.save()?;
    print_parameters(&project)
}

fn project_dir(arg: Option<&Path>) -> std::io::Result<PathBuf> {
    if let Some(p) = arg {
        Ok(p.try_resolve()?.into_owned())
    } else {
        std::env::current_dir()
    }
}

fn print_parameters(project: &ProjectFile) -> anyhow::Result<()> {
    let mut tw = TabWriter::new(stdout().lock());
    writeln!(&mut tw, "NAME\tTYPE\tREQUIRED\tDEFAULT\tDESCRIPTION")?;

    for (name, param) in &project.parameters {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}",
            name,
            param.param_type,
            param.required,
            param.display_default(),
            param.description.as_deref().unwrap_or("-"),
        )?;
    }

    tw.flush()?;
    Ok(())
}

fn validate_parameter_name(name: &str) -> anyhow::Result<()> {
    if name.trim().is_empty() {
        bail!("empty parameter name");
    }

    if name.contains(|c: char| c.is_whitespace() || c == '.') {
        bail!("invalid parameter name: {name:?}")
    }

    Ok(())
}
