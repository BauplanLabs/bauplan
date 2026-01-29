use std::{fmt::Display, str::FromStr};

use serde::{Deserialize, Serialize};

/// A ref returned by the API.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CatalogRef {
    /// A branch.
    Branch {
        /// The branch name.
        name: String,
        /// The commit hash.
        hash: String,
    },
    /// A tag.
    Tag {
        /// The tag name.
        name: String,
        /// The commit hash.
        hash: String,
    },
    /// A detached ref (a specific commit, not on any branch).
    Detached {
        /// The commit hash.
        hash: String,
    },
}

impl Display for CatalogRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogRef::Branch { name, hash } | CatalogRef::Tag { name, hash } => {
                write!(f, "{}@{}", name, hash)
            }
            CatalogRef::Detached { hash } => write!(f, "@{}", hash),
        }
    }
}

/// The string was not a valid catalog ref.
#[derive(Debug, Clone, thiserror::Error)]
#[error("Invalid ref: {0}")]
pub struct InvalidRef(String);

impl FromStr for CatalogRef {
    type Err = InvalidRef;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match_ref(s).ok_or(InvalidRef(s.to_string()))
    }
}

fn match_ref(s: &str) -> Option<CatalogRef> {
    let regex = regex::Regex::new(r#"\A([\w+]?)(:?@([^@]+))?\z"#).unwrap();
    let caps = regex.captures(s)?;
    let hash = caps.get(3)?.as_str();

    Some(CatalogRef::Detached {
        hash: hash.to_string(),
    })
}
