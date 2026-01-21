use std::{collections::BTreeMap, env, fs::File, io};

use serde::Deserialize;

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
#[derive(Clone)]
pub struct Profile {
    /// The name of the profile.
    pub name: String,
    /// The API endpoint to use. Intended for internal use.
    pub api_endpoint: http::Uri,
    /// The API key to use for authentication.
    pub api_key: String,
    /// The default branch for CLI operations. Set by `bauplan checkout`.
    /// Intended for internal use.
    pub active_branch: Option<String>,
    /// The user-agent used on requests. Intended for internal use.
    pub user_agent: String,
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
        let file = find_config()?;
        let ConfigProfile {
            api_endpoint,
            api_key,
            active_branch,
        } = read_profile(&file, name).unwrap_or_default();

        let api_endpoint = api_endpoint
            .or_else(|| env::var("BAUPLAN_API_ENDPOINT").ok())
            .unwrap_or(DEFAULT_API_ENDPOINT.to_string())
            .parse()?;

        let api_key = api_key
            .or_else(|| env::var("BAUPLAN_API_KEY").ok())
            .ok_or(Error::NoApiKey)?;
        if !api_key.is_ascii() {
            return Err(Error::InvalidApiKey);
        }

        Ok(Self {
            name: name.to_owned(),
            active_branch,
            api_endpoint,
            api_key,
            user_agent: make_ua(None),
        })
    }

    /// Modifies the user-agent to have a different prefix. Intended for
    /// internal use.
    #[doc(hidden)]
    pub fn with_ua_product(self, ua_product: &str) -> Self {
        Self {
            name: self.name,
            active_branch: self.active_branch,
            api_endpoint: self.api_endpoint,
            api_key: self.api_key,
            user_agent: make_ua(Some(ua_product)),
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

    /// Load the given profile (or 'default') from the given file, which must
    /// be a valid Bauplan configuration file. Does not read any environment
    /// variables.
    ///
    /// Usually, you will want to use [Profile::from_env] instead.
    pub fn read(file: &File, name: Option<&str>) -> Result<Self, Error> {
        let name = name.unwrap_or("default").to_owned();
        let ConfigProfile {
            api_endpoint,
            api_key,
            active_branch,
        } = read_profile(file, &name)?;

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
        })
    }
}

fn find_config() -> Result<File, Error> {
    let Some(home) = env::home_dir() else {
        return Err(Error::Io(io::Error::other(
            "No $HOME found for the current user",
        )));
    };

    let res = match File::open(home.join(".config/bauplan.yaml")) {
        Ok(f) => return Ok(f),
        Err(e) => Err(e.into()),
    };

    // Try some fallback paths, and if that doesn't work, return the error from
    // the canonical location.
    for fallback in [
        ".config/bauplan.yml",
        ".bauplan/config.yaml",
        ".bauplan/config.yml",
    ] {
        if let Ok(f) = File::open(home.join(fallback)) {
            return Ok(f);
        }
    }

    res
}

fn read_profile(file: &File, name: &str) -> Result<ConfigProfile, Error> {
    let mut config: Config = serde_yaml::from_reader(file).map_err(Error::Invalid)?;
    let Some(config_profile) = config.profiles.remove(name) else {
        return Err(Error::ProfileNotFound(name.to_string()));
    };

    Ok(config_profile)
}

fn make_ua(product: Option<&str>) -> String {
    format!("{}/{}", product.unwrap_or("default"), env!("BPLN_VERSION"))
}
