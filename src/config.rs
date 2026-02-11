use std::{
    collections::BTreeMap,
    env,
    fs::File,
    io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use tracing::debug;

const DEFAULT_API_ENDPOINT: &str = "https://api.use1.aprod.bauplanlabs.com";

/// An error encountered while loading or resolving a configuration profile.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to load config file")]
    Io(#[from] io::Error),
    #[error("Invalid configuration")]
    Invalid(#[from] serde_yaml::Error),
    #[error("Profile '{0}' not found")]
    ProfileNotFound(String),
    #[error("API key contains invalid characters")]
    InvalidApiKey,
    #[error("No API key found")]
    NoApiKey,
    #[error("Invalid URI")]
    InvalidUri(#[from] http::uri::InvalidUri),
}

/// A fully resolved configuration profile for interacting with Bauplan.
#[derive(Clone, Serialize)]
pub struct Profile {
    /// The name of the profile.
    pub name: String,
    /// The API endpoint to use. Intended for internal use.
    #[serde(skip)]
    pub api_endpoint: http::Uri,
    /// The API key to use for authentication.
    pub api_key: String,
    /// The default branch for CLI operations. Set by `bauplan checkout`.
    /// Intended for internal use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_branch: Option<String>,
    /// The user-agent used on requests. Intended for internal use.
    #[serde(skip)]
    pub user_agent: String,
    /// The config file this profile was loaded from, or the canonical one if
    /// no config file exists.
    #[serde(skip)]
    pub config_path: PathBuf,
}

impl std::fmt::Debug for Profile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Profile")
            .field("name", &self.name)
            .field("api_endpoint", &self.api_endpoint)
            .field("api_key", &"********")
            .field("active_branch", &self.active_branch)
            .field("user_agent", &self.user_agent)
            .finish()
    }
}

/// A profile stored in the config file.
#[derive(Debug, Default, Clone, Deserialize)]
struct ConfigProfile {
    pub(crate) active_branch: Option<String>,
    pub(crate) api_endpoint: Option<String>,
    pub(crate) api_key: Option<String>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct Config {
    profiles: BTreeMap<String, ConfigProfile>,
}

impl Profile {
    /// Load the given profile from the Bauplan configuration file (usually
    /// ~/.config/bauplan.yaml). If no configuration file is present, then the
    /// configuration will be loaded solely from the environment.
    ///
    /// If `BAUPLAN_PROFILE` is set, that will be used to select the profile.
    /// Otherwise the profile `default` will be used.
    ///
    /// The following environment variables can override the corresponding
    /// values in the config file:
    ///
    /// | Environment Variable    | Config Value   |
    /// |-------------------------|----------------|
    /// | `BAUPLAN_API_KEY`       | `api_key`      |
    /// | `BAUPLAN_API_ENDPOINT`  | `api_endpoint` |
    pub fn from_default_env() -> Result<Self, Error> {
        if let Ok(s) = env::var("BAUPLAN_PROFILE") {
            Self::from_env(&s)
        } else {
            Self::from_env("default")
        }
    }

    /// Load the given profile from the Bauplan configuration file (usually
    /// ~/.config/bauplan.yaml). If no configuration file is present, then the
    /// configuration will be loaded solely from the environment.
    ///
    /// The following environment variables can override the corresponding
    /// values in the config file:
    ///
    /// | Environment Variable    | Config Value   |
    /// |-------------------------|----------------|
    /// | `BAUPLAN_API_KEY`       | `api_key`      |
    /// | `BAUPLAN_API_ENDPOINT`  | `api_endpoint` |
    pub fn from_env(name: &str) -> Result<Self, Error> {
        let api_key = env::var("BAUPLAN_API_KEY").ok();
        let api_endpoint = env::var("BAUPLAN_API_ENDPOINT").ok();

        let config_path = find_config()?;
        let profile = match read_profile(&config_path, name) {
            Ok(p) => p,
            Err(Error::Io(e)) if e.kind() == io::ErrorKind::NotFound => {
                debug!("no config file found");
                Default::default()
            }
            Err(e) => return Err(e),
        };

        let api_endpoint = api_endpoint
            .as_deref()
            .or(profile.api_endpoint.as_deref())
            .unwrap_or(DEFAULT_API_ENDPOINT)
            .parse()?;

        let Some(api_key) = api_key.or(profile.api_key) else {
            return Err(Error::NoApiKey);
        };

        if !api_key.is_ascii() {
            return Err(Error::InvalidApiKey);
        }

        Ok(Self {
            name: name.to_owned(),
            active_branch: profile.active_branch,
            api_endpoint,
            api_key,
            user_agent: make_ua(None),
            config_path,
        })
    }

    /// Modifies the user-agent to have a different prefix. Intended for
    /// internal use.
    #[doc(hidden)]
    pub fn with_ua_product(self, ua_product: &str) -> Self {
        Self {
            user_agent: make_ua(Some(ua_product)),
            ..self
        }
    }

    /// Load the given profile (or 'default') from the Bauplan configuration
    /// file (usually ~/.config/bauplan.yaml). Does not read any environment
    /// variables.
    ///
    /// Usually, you will want to use [Profile::from_env] instead.
    pub fn load(name: Option<&str>) -> Result<Self, Error> {
        let file = find_config()?;
        Self::read(&file, name)
    }

    /// Iterate through all profiles in the Bauplan configuration file (usually
    /// ~/.config/bauplan.yaml). Does not read any environment variables.
    pub fn load_all() -> Result<impl Iterator<Item = Self>, Error> {
        let path = find_config()?;
        let file = File::open(&path)?;
        let config: Config = serde_yaml::from_reader(file)?;
        let profiles: Result<Vec<_>, Error> = config
            .profiles
            .into_iter()
            .map(|(name, raw)| Profile::from_raw(raw, name, path.clone()))
            .collect();

        Ok(profiles?.into_iter())
    }

    /// Load the given profile (or 'default') from the given file, which must
    /// be a valid Bauplan configuration file. Does not read any environment
    /// variables.
    ///
    /// Usually, you will want to use [Profile::from_env] instead.
    pub fn read(path: impl AsRef<Path>, name: Option<&str>) -> Result<Self, Error> {
        let path = path.as_ref();
        let name = name.unwrap_or("default").to_owned();
        let profile = read_profile(path, &name)?;
        Self::from_raw(profile, name, path.to_owned())
    }

    /// Read all profiles from the given file, which must be a valid Bauplan
    /// configuration file. Does not read any environment variables.
    pub fn read_all(path: impl AsRef<Path>) -> Result<impl Iterator<Item = Self>, Error> {
        let path = path.as_ref();
        let file = File::open(path)?;
        let config: Config = serde_yaml::from_reader(file)?;

        let profiles: Result<Vec<_>, Error> = config
            .profiles
            .into_iter()
            .map(|(name, raw)| Profile::from_raw(raw, name, path.to_owned()))
            .collect();

        Ok(profiles?.into_iter())
    }

    fn from_raw(raw: ConfigProfile, name: String, path: PathBuf) -> Result<Self, Error> {
        let ConfigProfile {
            active_branch,
            api_endpoint,
            api_key,
        } = raw;

        let api_endpoint = api_endpoint
            .unwrap_or(DEFAULT_API_ENDPOINT.to_string())
            .parse()?;
        let api_key = api_key.ok_or(Error::NoApiKey)?;
        if !api_key.is_ascii() {
            return Err(Error::InvalidApiKey);
        }

        Ok(Self {
            name,
            active_branch,
            api_endpoint,
            api_key,
            user_agent: make_ua(None),
            config_path: path.to_owned(),
        })
    }
}

fn find_config() -> Result<PathBuf, Error> {
    let Some(home) = env::home_dir() else {
        return Err(Error::Io(io::Error::other(
            "No $HOME found for the current user",
        )));
    };

    let canonical = home.join(".config/bauplan.yaml");
    if canonical.exists() {
        return Ok(canonical);
    }

    // Try some fallback paths, and if that doesn't work, return the error from
    // the canonical location.
    for fallback in [
        ".config/bauplan.yml",
        ".bauplan/config.yaml",
        ".bauplan/config.yml",
    ] {
        let path = home.join(fallback);
        if path.exists() {
            return Ok(path);
        }
    }

    Ok(canonical)
}

fn read_profile(p: &Path, name: &str) -> Result<ConfigProfile, Error> {
    let file = File::open(p)?;
    let mut config: Config = serde_yaml::from_reader(file).map_err(Error::Invalid)?;
    let Some(config_profile) = config.profiles.remove(name) else {
        return Err(Error::ProfileNotFound(name.to_string()));
    };

    debug!(path = %p.display(), "loaded config file");

    Ok(config_profile)
}

fn make_ua(product: Option<&str>) -> String {
    format!("{}/{}", product.unwrap_or("default"), env!("BPLN_VERSION"))
}
