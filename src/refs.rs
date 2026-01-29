use std::{fmt::Display, str::FromStr, sync::LazyLock};

use serde::{Deserialize, Serialize};

/// A ref returned by the API.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
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

// Matches refs like "main@abc123" or "@abc123" (detached).
static REF_REGEX: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"\A([^@]+)?@([^@]+)\z").unwrap());

fn match_ref(s: &str) -> Option<CatalogRef> {
    let caps = REF_REGEX.captures(s)?;
    let hash = caps.get(2)?.as_str().to_string();

    Some(CatalogRef::Detached { hash })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_ref() {
        let cases: &[(&str, Option<CatalogRef>)] = &[
            (
                "main@abc123",
                Some(CatalogRef::Detached {
                    hash: "abc123".into(),
                }),
            ),
            (
                "user.feature-branch@deadbeef",
                Some(CatalogRef::Detached {
                    hash: "deadbeef".into(),
                }),
            ),
            (
                "@abc123",
                Some(CatalogRef::Detached {
                    hash: "abc123".into(),
                }),
            ),
            ("main", None),
            ("", None),
            ("main@abc@def", None),
        ];

        for (input, expected) in cases {
            let result = input.parse::<CatalogRef>().ok();
            assert_eq!(&result, expected, "parsing {input:?}");
        }
    }
}
