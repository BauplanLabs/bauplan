//! Helpers for managing bauplan projects.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors that can occur when working with project files.
#[derive(Debug, Error)]
#[allow(missing_docs)]
pub enum ProjectError {
    #[error("no bauplan_project.yaml found")]
    ProjectFileNotFound(PathBuf),
    #[error("both bauplan_project.yml and .yaml found in {0}; remove one to avoid ambiguity")]
    ProjectFileAmbiguous(PathBuf),
    #[error("failed to read project file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse project file: {0}")]
    Parse(#[from] serde_yaml::Error),
    #[error("project.id must not be empty")]
    EmptyProjectId,
    #[error("invalid value {0:?} of type {1}")]
    InvalidParameterValue(String, &'static str),
}

/// The type of a parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[allow(missing_docs)]
pub enum ParameterType {
    #[default]
    Str,
    Int,
    Float,
    Bool,
    Secret,
    Vault,
}

impl std::fmt::Display for ParameterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Str => write!(f, "str"),
            Self::Int => write!(f, "int"),
            Self::Float => write!(f, "float"),
            Self::Bool => write!(f, "bool"),
            Self::Secret => write!(f, "secret"),
            Self::Vault => write!(f, "vault"),
        }
    }
}

/// A parameter definition in a project file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Parameter {
    /// The type of the parameter.
    #[serde(rename = "type", default)]
    pub param_type: ParameterType,
    /// Whether the parameter is required to be passed when running models.
    #[serde(default)]
    pub required: bool,
    /// A default value for the parameter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_yaml::Value>,
    /// A description of the parameter for humans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The key used to decrypt the value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

struct DisplayDefaultValue<'a>(&'a Parameter);

impl std::fmt::Display for DisplayDefaultValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.param_type == ParameterType::Secret {
            write!(f, "***********")
        } else if let Some(v) = self.0.default.as_ref() {
            write!(f, "{}", serde_yaml::to_string(v).unwrap().trim())
        } else {
            write!(f, "-")
        }
    }
}

impl Parameter {
    /// Set the default value from a string, parsing it according to the type.
    pub fn set_default_from_string(&mut self, value: &str) -> Result<(), ProjectError> {
        let parsed = match self.param_type {
            ParameterType::Str | ParameterType::Secret | ParameterType::Vault => {
                serde_yaml::Value::String(value.to_string())
            }
            ParameterType::Int => {
                let n: i64 = value
                    .parse()
                    .map_err(|_| ProjectError::InvalidParameterValue(value.to_string(), "int"))?;
                serde_yaml::Value::Number(n.into())
            }
            ParameterType::Float => {
                let n: f64 = value
                    .parse()
                    .map_err(|_| ProjectError::InvalidParameterValue(value.to_string(), "float"))?;
                serde_yaml::Value::Number(n.into())
            }
            ParameterType::Bool => {
                let b = parse_bool(value).ok_or_else(|| {
                    ProjectError::InvalidParameterValue(value.to_string(), "bool")
                })?;
                serde_yaml::Value::Bool(b)
            }
        };

        self.default = Some(parsed);
        Ok(())
    }

    /// Create a `Display`able representation of the default value. This will
    /// obscure secret values.
    pub fn display_default(&self) -> impl std::fmt::Display {
        DisplayDefaultValue(self)
    }
}

fn parse_bool(s: &str) -> Option<bool> {
    match s.to_lowercase().as_str() {
        "true" | "yes" | "1" | "on" => Some(true),
        "false" | "no" | "0" | "off" => Some(false),
        _ => None,
    }
}

/// Project metadata.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectInfo {
    /// The project ID.
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// The name of the project.
    pub name: Option<String>,
    /// A description of the project.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// A bauplan project file (`bauplan_project.yml`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectFile {
    /// The project ID, name, and description.
    pub project: ProjectInfo,
    /// Parameters for models.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub parameters: BTreeMap<String, Parameter>,

    /// The location of the project file on disk.
    #[serde(skip)]
    pub path: PathBuf,
}

impl ProjectFile {
    /// Load a project file from a directory, looking for either `.yml` or `.yaml`.
    pub fn from_dir(dir: impl AsRef<Path>) -> Result<Self, ProjectError> {
        let dir = dir.as_ref();
        let yml_path = dir.join("bauplan_project.yml");
        let yaml_path = dir.join("bauplan_project.yaml");

        let path = match (yml_path.exists(), yaml_path.exists()) {
            (true, false) => yml_path,
            (false, true) => yaml_path,
            (true, true) => return Err(ProjectError::ProjectFileAmbiguous(dir.to_path_buf())),
            (false, false) => return Err(ProjectError::ProjectFileNotFound(dir.to_path_buf())),
        };

        Self::load(path)
    }

    /// Load a project file from a specific path.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ProjectError> {
        let path = path.as_ref().canonicalize()?;
        let content = std::fs::read_to_string(&path)?;
        let mut project: Self = serde_yaml::from_str(&content)?;

        if project.project.id.trim().is_empty() {
            return Err(ProjectError::EmptyProjectId);
        }

        project.path = path;
        Ok(project)
    }

    /// Write the project file back to disk.
    pub fn save(&self) -> Result<(), ProjectError> {
        let content = serde_yaml::to_string(self)?;
        std::fs::write(&self.path, content)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bool() {
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("True"), Some(true));
        assert_eq!(parse_bool("TRUE"), Some(true));
        assert_eq!(parse_bool("yes"), Some(true));
        assert_eq!(parse_bool("1"), Some(true));
        assert_eq!(parse_bool("on"), Some(true));

        assert_eq!(parse_bool("false"), Some(false));
        assert_eq!(parse_bool("False"), Some(false));
        assert_eq!(parse_bool("no"), Some(false));
        assert_eq!(parse_bool("0"), Some(false));
        assert_eq!(parse_bool("off"), Some(false));

        assert_eq!(parse_bool("invalid"), None);
        assert_eq!(parse_bool(""), None);
    }
}
