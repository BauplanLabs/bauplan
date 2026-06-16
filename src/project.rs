//! Helpers for managing bauplan projects.

use std::collections::{BTreeMap, HashSet};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use base64::Engine;
use rsa::sha2::Sha256;
use rsa::{Oaep, RsaPublicKey};
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use thiserror::Error;
use uuid::Uuid;

use ignore::WalkBuilder;
use ignore::overrides::OverrideBuilder;

/// Errors that can occur when working with project files.
#[derive(Debug, Error)]
#[allow(missing_docs)]
pub enum ProjectError {
    #[error("no bauplan_project.yaml found in {0}")]
    ProjectFileNotFound(PathBuf),
    #[error("both bauplan_project.yml and .yaml found in {0}; remove one to avoid ambiguity")]
    ProjectFileAmbiguous(PathBuf),
    #[error("failed to read project file")]
    Io(#[from] std::io::Error),
    #[error("failed to parse project file")]
    Parse(#[from] serde_yaml::Error),
    #[error("failed to create archive")]
    Zip(#[from] zip::result::ZipError),
    #[error("encryption failed")]
    Encryption(#[from] rsa::Error),
    #[error("invalid glob pattern")]
    Glob(#[from] globset::Error),
    #[error("non-UTF8 glob pattern: {0}")]
    NonUtf8Pattern(PathBuf),
    #[error("failed to traverse directory")]
    Walk(#[from] walkdir::Error),
    #[error("failed to resolve include paths")]
    Ignore(#[from] ignore::Error),
    #[error("glob pattern not allowed: {0}")]
    GlobPatternNotAllowed(String),
    #[error("path not in base directory: {0}")]
    PathNotInBase(PathBuf),
    #[error("extension not allowed: {0}")]
    ExtensionNotAllowed(PathBuf),
    #[error("{0} is excluded by gitignore pattern {1:?} from {2}")]
    GitIgnoredFile(PathBuf, String, PathBuf),
    #[error("failed to extract relative path")]
    Prefix(#[from] std::path::StripPrefixError),
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
    /// Additional paths to include as glob expressions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_paths: Option<Vec<String>>,

    /// The location of the project file on disk.
    #[serde(skip)]
    pub path: PathBuf,
}

impl ProjectFile {
    /// Load a project file from a directory, looking for either `.yml` or `.yaml`.
    pub fn from_dir(dir: impl AsRef<Path>) -> Result<Self, ProjectError> {
        let dir = dir.as_ref();
        // Ensure the directory exists before looking for files.
        std::fs::metadata(dir)?;

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

        let mut files: HashSet<PathBuf> = HashSet::new();

        // Top level first, always included.
        for entry in std::fs::read_dir(project_dir)? {
            let path = entry?.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) && include_in_snapshot(name) {
                    files.insert(path);
            }
        }

        // Additional files, if specified.
        if let Some(additional_paths) = &self.include_paths {
            let additional = resolve_includes(project_dir, additional_paths)?;
            files.extend(additional);
        }
    

        let mut buf = Vec::new();
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);

        let mut contents = Vec::new();
        for path in files {
            // The zip spec mandates forward slashes in entry names, and serializing
            // the Path would produce backslashes on Windows; start_file_from_path
            // does the conversion
            let name = path.strip_prefix(project_dir)?;

            contents.clear();
            let mut file = std::fs::File::open(&path)?;
            file.read_to_end(&mut contents)?;

            zip.start_file_from_path(name, options)?;
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

fn include_paths_filter(file: &Path) -> bool {
    file.extension()
        .and_then(OsStr::to_str)
        .is_some_and(|ext| matches!(ext, "sql")) // Future: extend with "sql" | "py".
}

fn resolve_pattern(base_canonical: &Path, p: &String) -> Result<String, ProjectError>{

    // Users should be explicitly including file extensions, not globbing for all files.
    // Also, we do not enforce constraints on lower/upper case here.
    if !p.to_lowercase().ends_with(".sql") {
        return Err(ProjectError::GlobPatternNotAllowed(p.to_string()));
    }

    // Resolve the pattern lexically against base. Patterns may climb upward
    // as long as they resolve back inside base, e.g. ../<base dirname>/views/*.sql.
    // Components are walked manually instead of using join: on Windows the
    // canonicalized base is \\?\-prefixed and join would resolve `..` itself,
    // popping wildcards before the guard below can see them.
    let pattern = Path::new(p);

    let mut resolved = if pattern.is_absolute() {
        return Err(ProjectError::GlobPatternNotAllowed(format!("{} is an absolute path", p)));
    } else {
        base_canonical.to_path_buf()
    };

    for component in pattern.components() {
        match component {
            Component::CurDir => {}
            // Handle ".." by checking if there's a valid parent.
            Component::ParentDir => match resolved.file_name() {
                // Nothing left to pop: the pattern climbed past the filesystem root
                None => return Err(ProjectError::PathNotInBase(PathBuf::from(p))),
                Some(name) => {
                    // Popping a wildcard (e.g. views/**/../*.sql) would change the
                    // glob's meaning, only literal components can be climbed over
                    if name.to_string_lossy().contains(['*', '?', '[', '{']) {
                        return Err(ProjectError::GlobPatternNotAllowed(p.to_string()));
                    }
                    resolved.pop();
                }
            },
            other => resolved.push(other),
        }
    }

    // A pattern resolving outside base would silently never match the walk's
    // base-relative paths, so it fails here instead.
    let relative = resolved
        .strip_prefix(base_canonical)
        .map_err(|_| ProjectError::PathNotInBase(PathBuf::from(p)))?;

    // Rebuild the glob base-relative, joining with explicit `/`: serializing the
    // whole Path would produce backslashes on Windows, which glob syntax treats
    // as escapes.
    let mut glob = String::new();
    for component in relative.components() {
        let part = component
            .as_os_str()
            .to_str()
            .ok_or_else(|| ProjectError::NonUtf8Pattern(PathBuf::from(p)))?;
        if !glob.is_empty() {
            glob.push('/');
        }
        glob.push_str(part);
    }

    Ok(glob)

}

/// Include additional files in snapshot, based on user-provided glob patterns.
fn resolve_includes(base: &Path, patterns: &[String]) -> Result<Vec<PathBuf>, ProjectError> {
    let base_canonical = base.canonicalize()?;

    // We will only look for additional files in the overrides.
    let mut ob = OverrideBuilder::new(&base_canonical);
    for p in patterns {
        let resolved_pattern = resolve_pattern(&base_canonical, p)?;
        ob.add(&resolved_pattern).map_err(|_| ProjectError::GlobPatternNotAllowed(p.to_string()))?;
    }
    let overrides = ob.build()?;

    // Build the walker with gitignores
    let walker = WalkBuilder::new(&base_canonical)
        .overrides(overrides)
        .build();

    let mut paths = HashSet::new();

    for entry in walker {
        let entry_canonical = entry?.path().canonicalize()?;

        // Canonicalizing can land outside base when the entry is a symlink elsewhere.
        if !entry_canonical.starts_with(&base_canonical) {
            return Err(ProjectError::PathNotInBase(entry_canonical));
        }

        if !entry_canonical.is_file() {
            continue;
        }

        if !include_paths_filter(&entry_canonical) {
            return Err(ProjectError::ExtensionNotAllowed(entry_canonical));
        }

        paths.insert(entry_canonical);

    }

    Ok(paths.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use super::*;

    #[test]
    fn resolve_includes_accepts_upward_pattern_inside_base() {
        let tmp = tempfile::tempdir().unwrap();
        let proj = tmp.path().join("proj");
        std::fs::create_dir_all(proj.join("views")).unwrap();
        std::fs::write(proj.join("views").join("age.sql"), "SELECT 1").unwrap();

        // Climbs out of base but resolves back inside it
        let resolved = resolve_includes(&proj, &["../proj/views/*.sql".into()]).unwrap();
        assert_eq!(resolved.len(), 1);
        assert!(resolved[0].ends_with("views/age.sql"));
    }

    #[test]
    fn resolve_includes_rejects_pattern_escaping_base() {
        let tmp = tempfile::tempdir().unwrap();
        let proj = tmp.path().join("proj");
        std::fs::create_dir_all(&proj).unwrap();
        std::fs::write(tmp.path().join("age.sql"), "SELECT 1").unwrap();

        let err = resolve_includes(&proj, &["../*.sql".into()]).unwrap_err();
        assert_matches!(err, ProjectError::PathNotInBase(_));
    }

    #[test]
    fn resolve_includes_rejects_parent_dir_after_wildcard() {
        let tmp = tempfile::tempdir().unwrap();
        let proj = tmp.path().join("proj");
        std::fs::create_dir_all(&proj).unwrap();

        let err = resolve_includes(&proj, &["views/**/../*.sql".into()]).unwrap_err();
        assert_matches!(err, ProjectError::GlobPatternNotAllowed(_));
    }

    // #[test]
    // fn resolve_includes_fails_loud_on_gitignored_file() {
    //     let tmp = tempfile::tempdir().unwrap();
    //     // Repo root marker: discovery only checks that .git exists
    //     std::fs::create_dir(tmp.path().join(".git")).unwrap();
    //     // The .gitignore lives above base, like a repo-root one would
    //     std::fs::write(tmp.path().join(".gitignore"), "*.sql\n").unwrap();

    //     let proj = tmp.path().join("proj");
    //     std::fs::create_dir_all(proj.join("views")).unwrap();
    //     std::fs::write(proj.join("views").join("age.sql"), "SELECT 1").unwrap();

    //     let err = resolve_includes(&proj, &["views/*.sql".into()]).unwrap_err();
    //     assert_matches!(err, ProjectError::GitIgnoredFile(..));
    // }

    #[test]
    fn resolve_includes_respects_gitignore_whitelist() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir(tmp.path().join(".git")).unwrap();
        std::fs::write(tmp.path().join(".gitignore"), "*.sql\n").unwrap();

        let proj = tmp.path().join("proj");
        std::fs::create_dir_all(proj.join("views")).unwrap();
        std::fs::write(proj.join("views").join("age.sql"), "SELECT 1").unwrap();
        // The deeper .gitignore re-includes the file, taking precedence over the root one
        std::fs::write(proj.join("views").join(".gitignore"), "!age.sql\n").unwrap();

        let resolved = resolve_includes(&proj, &["views/*.sql".into()]).unwrap();
        assert_eq!(resolved.len(), 1);
    }

    #[test]
    fn resolve_includes_skips_gitignore_check_outside_git_repo() {
        let tmp = tempfile::tempdir().unwrap();
        // A .gitignore with no .git upward is not a repo, so it is not enforced
        std::fs::write(tmp.path().join(".gitignore"), "*.sql\n").unwrap();

        let proj = tmp.path().join("proj");
        std::fs::create_dir_all(proj.join("views")).unwrap();
        std::fs::write(proj.join("views").join("age.sql"), "SELECT 1").unwrap();

        let resolved = resolve_includes(&proj, &["views/*.sql".into()]).unwrap();
        assert_eq!(resolved.len(), 1);
    }

    #[test]
    fn code_snapshot_zip_entries_use_forward_slashes() {
        let tmp = tempfile::tempdir().unwrap();
        let proj = tmp.path().join("proj");
        std::fs::create_dir_all(proj.join("views")).unwrap();
        std::fs::write(proj.join("views").join("age.sql"), "SELECT 1").unwrap();
        std::fs::write(proj.join("models.py"), "import bauplan").unwrap();
        std::fs::write(
            proj.join("bauplan_project.yml"),
            "project:\n  id: 97e33cab-6805-4565-9df3-8ffa5e914574\n  name: zip_separators\ninclude_paths:\n  - views/*.sql\n",
        )
        .unwrap();

        let project = ProjectFile::load(proj.join("bauplan_project.yml")).unwrap();
        let snapshot = project.create_code_snapshot().unwrap();

        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(snapshot)).unwrap();
        let names: Vec<String> = (0..archive.len())
            .map(|i| archive.by_index(i).unwrap().name().to_owned())
            .collect();

        // The zip spec mandates `/` in entry names; a backslash entry written on
        // Windows would extract on unix as a single file literally named views\age.sql
        assert!(names.iter().all(|n| !n.contains('\\')), "{names:?}");

        // by_name is an exact match, so this fails if the separator is wrong
        let mut entry = archive.by_name("views/age.sql").unwrap();
        let mut content = String::new();
        entry.read_to_string(&mut content).unwrap();
        assert_eq!(content, "SELECT 1");
    }

    #[test]
    fn include_paths_filter_matches_only_sql_case_insensitive() {
        // (path, expected)
        let cases = [
            ("views/model.sql", true),
            ("model.sql", true),
            ("model.SQL", false),
            ("views/model.SQL", false),
            ("views/model.Sql", false),
            ("model.Sql", false),
            // py is not included yet
            ("script.py", false),
            ("script.PY", false),
            ("notes.txt", false),
            ("Justfile", false),
        ];

        for (name, expected) in cases {
            assert_eq!(
                include_paths_filter(Path::new(name)),
                expected,
                "wrong classification for extension of {name}"
            );
        }
    }
}
