use std::{io::Write as _, path::PathBuf};

use anyhow::Context as _;
use bauplan::project::{ProjectFile, ProjectInfo};

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

    let dir = dir.canonicalize()?;
    let project_name = if let Some(name) = args.name {
        name
    } else if let Some(part) = dir.file_name() {
        part.to_string_lossy().into_owned()
    } else {
        "my_project".into()
    };

    // Create bauplan_project.yaml.
    let project_id = uuid::Uuid::new_v4();
    let project_file = ProjectFile {
        project: ProjectInfo {
            id: project_id,
            name: Some(project_name.clone()),
            description: None,
        },
        parameters: Default::default(),
        path: Default::default(), // unused
    };

    let project_yaml =
        serde_yaml::to_string(&project_file).context("failed to generate project file")?;

    // Fail if bauplan_project.yaml already exists.
    let project_path = dir.join("bauplan_project.yaml");
    let mut f = std::fs::File::create_new(&project_path)
        .context(format!("{} already exists", project_path.display()))?;
    f.write_all(project_yaml.as_bytes())
        .context(format!("failed to write {}", project_path.display()))?;

    // NB: tests/cli/init.rs has a test that runs this project.
    let models_py = include_str!("init/models.py");

    // Generate pyproject.toml. This works out to:
    //
    // [project]
    // name = "my_project"
    // requires-python = ">=3.12"
    // dependencies = ["bauplan=~x.x.x"]
    let bauplan_version = concat!("bauplan~=", env!("CARGO_PKG_VERSION")).into();

    let mut project = toml::Table::default();
    project.insert("name".into(), toml::Value::String(project_name.clone()));
    project.insert(
        "requires-python".into(),
        toml::Value::String("~=3.12".into()),
    );
    project.insert(
        "dependencies".into(),
        toml::Value::Array(vec![toml::Value::String(bauplan_version)]),
    );

    let mut pyproject = toml::Table::default();
    pyproject.insert("project".into(), toml::Value::Table(project));

    let pyproject_toml =
        toml::to_string_pretty(&pyproject).context("failed to serialize pyproject.toml")?;

    no_clobber(&dir.join("pyproject.toml"), &pyproject_toml)?;
    no_clobber(&dir.join("models.py"), models_py.trim_start())?;

    eprintln!(
        "Initialized bauplan project {project_name:?} in {}",
        dir.display()
    );

    Ok(())
}

fn no_clobber(path: &std::path::Path, content: &str) -> anyhow::Result<()> {
    match std::fs::File::create_new(path) {
        Ok(mut f) => f.write_all(content.as_bytes()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e),
    }
    .context(format!("failed to write {}", path.display()))?;

    Ok(())
}
