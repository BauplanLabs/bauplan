use std::path::PathBuf;

use anyhow::{Context as _, bail};

use crate::cli::color::CliExamples;

#[derive(Debug, clap::Args)]
#[command(after_long_help = CliExamples("
  # Initialize a new project in the current directory
  bauplan init

  # Initialize in a specific directory
  bauplan init my_project

  # Initialize with a custom project name
  bauplan init --name my_pipeline
"))]
pub(crate) struct InitArgs {
    /// Directory to initialize. Defaults to the current directory.
    pub path: Option<PathBuf>,
    /// Project name. Defaults to the directory name.
    #[arg(long)]
    pub name: Option<String>,
}

pub(crate) fn handle(args: InitArgs) -> anyhow::Result<()> {
    let dir = match args.path {
        Some(p) => p,
        None => std::env::current_dir()?,
    };

    if !dir.exists() {
        std::fs::create_dir_all(&dir)
            .context(format!("failed to create directory {}", dir.display()))?;
    }

    let project_name = if let Some(name) = args.name {
        name
    } else {
        dir.canonicalize()
            .ok()
            .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
            .unwrap_or_else(|| "my_project".to_string())
    };

    let project_id = uuid::Uuid::new_v4();
    let project_yml = format!(
        "\
project:
  id: {project_id}
  name: {project_name}

defaults:
  python:
    version: \"3.13\"
"
    );

    let pyproject_toml = format!(
        "\
[project]
name = \"{project_name}\"
requires-python = \">=3.13\"
dependencies = [\"bauplan\"]
"
    );

    let models_py = "\
import bauplan


@bauplan.model()
@bauplan.python('3.13')
def my_model(passengers=bauplan.Model('titanic')):
    # Transform the data here.
    return passengers
";

    write_if_missing(&dir.join("bauplan_project.yml"), &project_yml)?;
    write_if_missing(&dir.join("pyproject.toml"), &pyproject_toml)?;
    write_if_missing(&dir.join("models.py"), models_py)?;

    eprintln!(
        "Initialized bauplan project {project_name:?} in {}",
        dir.display()
    );

    Ok(())
}

fn write_if_missing(path: &std::path::Path, content: &str) -> anyhow::Result<()> {
    if path.exists() {
        bail!("{} already exists", path.display());
    }

    std::fs::write(path, content).context(format!("failed to write {}", path.display()))?;

    Ok(())
}
