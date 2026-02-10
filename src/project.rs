//! Helpers for managing bauplan projects.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use base64::Engine;
use rsa::sha2::Sha256;
use rsa::{Oaep, RsaPublicKey};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

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
    #[error("failed to create archive: {0}")]
    Zip(#[from] zip::result::ZipError),
    #[error("encryption failed: {0}")]
    Encryption(#[from] rsa::Error),
    #[error("invalid value {0:?} of type {1}")]
    InvalidParameterValue(String, ParameterType),
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

/// A resolved parameter value.
#[derive(Clone, PartialEq)]
#[allow(missing_docs)]
pub enum ParameterValue {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Secret {
        key: String,
        encrypted_value: String,
    },
    Vault(String),
}

impl ParameterValue {
    /// Create an encrypted parameter value. The key_name is just metadata,
    /// usually an AWS KMS ARN.
    ///
    /// The encoded value takes the form {project_id}={value}, which pins the
    /// secret to the project so that users can't copy paste (for some reason).
    pub fn encrypt_secret(
        key_name: String,
        key: &RsaPublicKey,
        project_id: Uuid,
        value: impl AsRef<str>,
    ) -> Result<Self, ProjectError> {
        use base64::engine::general_purpose::STANDARD;

        let value = format!("{}={}", project_id.as_hyphenated(), value.as_ref());

        let padding = Oaep::new::<Sha256>();
        let secret = key.encrypt(&mut rand::thread_rng(), padding, value.as_bytes())?;
        let encrypted_value = STANDARD.encode(secret);
        Ok(ParameterValue::Secret {
            key: key_name,
            encrypted_value,
        })
    }
}

impl std::fmt::Debug for ParameterValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Str(s) => write!(f, "{s:?}"),
            Self::Int(i) => write!(f, "{i:?}"),
            Self::Float(v) => write!(f, "{v:?}"),
            Self::Bool(b) => write!(f, "{b:?}"),
            Self::Secret { .. } => write!(f, "***********"),
            Self::Vault(v) => write!(f, "{v}"),
        }
    }
}

impl std::fmt::Display for ParameterValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Str(s) => write!(f, "{}", s),
            Self::Int(i) => write!(f, "{}", i),
            Self::Float(v) => write!(f, "{}", v),
            Self::Bool(b) => write!(f, "{}", b),
            Self::Secret { .. } => write!(f, "***********"),
            Self::Vault(v) => write!(f, "{}", v),
        }
    }
}

/// A parameter definition in a project file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterDefault {
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

struct DisplayDefaultValue<'a>(&'a ParameterDefault);

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

impl ParameterDefault {
    /// Return the default as a [ParameterValue]. If the value in the file is
    /// not valid for the parameter type, an error is returned.
    pub fn eval_default(&self) -> Result<Option<ParameterValue>, ProjectError> {
        let Some(value) = self.default.as_ref() else {
            return Ok(None);
        };

        let err = || ProjectError::InvalidParameterValue(format!("{value:?}"), self.param_type);

        let v = match self.param_type {
            ParameterType::Str => ParameterValue::Str(value.as_str().ok_or_else(err)?.to_owned()),
            ParameterType::Int => ParameterValue::Int(value.as_i64().ok_or_else(err)?),
            ParameterType::Float => ParameterValue::Float(value.as_f64().ok_or_else(err)?),
            ParameterType::Bool => ParameterValue::Bool(value.as_bool().ok_or_else(err)?),
            ParameterType::Vault => {
                ParameterValue::Vault(value.as_str().ok_or_else(err)?.to_owned())
            }
            ParameterType::Secret => ParameterValue::Secret {
                key: self.key.clone().ok_or_else(err)?,
                encrypted_value: value.as_str().ok_or_else(err)?.to_owned(),
            },
        };

        Ok(Some(v))
    }

    /// Set the default value in YAML. The type of the value must match the
    /// [ParameterType].
    pub fn update_default(&mut self, value: ParameterValue) -> Result<(), ProjectError> {
        match (value, self.param_type) {
            (ParameterValue::Str(s), ParameterType::Str) => {
                self.default = Some(serde_yaml::Value::String(s));
                self.key = None;
            }
            (ParameterValue::Int(i), ParameterType::Int) => {
                self.default = Some(serde_yaml::Value::Number(i.into()));
                self.key = None;
            }
            (ParameterValue::Float(f), ParameterType::Float) => {
                self.default = Some(serde_yaml::Value::Number(f.into()));
                self.key = None;
            }
            (ParameterValue::Bool(b), ParameterType::Bool) => {
                self.default = Some(serde_yaml::Value::Bool(b));
                self.key = None;
            }
            (
                ParameterValue::Secret {
                    key,
                    encrypted_value: value,
                },
                ParameterType::Secret,
            ) => {
                self.default = Some(serde_yaml::Value::String(value));
                self.key = Some(key);
            }
            (ParameterValue::Vault(v), ParameterType::Vault) => {
                self.default = Some(serde_yaml::Value::String(v));
                self.key = None;
            }
            (v, t) => {
                return Err(ProjectError::InvalidParameterValue(v.to_string(), t));
            }
        }

        Ok(())
    }

    /// Create a `Display`able representation of the default value. This will
    /// obscure secret values.
    pub fn display_default(&self) -> impl std::fmt::Display {
        DisplayDefaultValue(self)
    }
}

/// Project metadata.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectInfo {
    /// The project ID.
    pub id: Uuid,
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
    pub parameters: BTreeMap<String, ParameterDefault>,

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

        project.path = path;
        Ok(project)
    }

    /// Create a zip archive of the project directory, including only relevant
    /// files (.py, .sql, requirements.txt, and the project file itself).
    pub fn create_code_snapshot(&self) -> Result<Vec<u8>, ProjectError> {
        let project_dir = self.path.parent().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid project file path",
            )
        })?;

        let mut buf = Vec::new();
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);

        let mut contents = Vec::new();
        for entry in std::fs::read_dir(project_dir)? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };

            if !include_in_snapshot(name) {
                continue;
            }

            contents.clear();
            let mut file = std::fs::File::open(&path)?;
            file.read_to_end(&mut contents)?;

            zip.start_file(name, options)?;
            zip.write_all(&contents)?;
        }

        zip.finish()?;
        Ok(buf)
    }
}

fn include_in_snapshot(name: &str) -> bool {
    name.ends_with(".py")
        || name.ends_with(".sql")
        || name == "requirements.txt"
        || name == "bauplan_project.yml"
        || name == "bauplan_project.yaml"
}
