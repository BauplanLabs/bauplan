//! Types for modifying commit bodies and properties.
use std::collections::BTreeMap;

use serde::Serialize;

/// Options for modifying a commit operation.
#[derive(Default, Debug, Clone, Serialize)]
pub struct CommitOptions<'a> {
    /// Override the commit body.
    #[serde(rename = "commit_body")]
    pub body: Option<&'a str>,

    /// Additional commit properties.
    #[serde(rename = "commit_properties")]
    pub properties: BTreeMap<&'a str, &'a str>,
}

impl<'a> CommitOptions<'a> {
    /// Create a new commit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the commit body.
    pub fn body(mut self, body: &'a str) -> Self {
        self.body = Some(body);
        self
    }

    /// Add a custom property to the commit.
    pub fn property(mut self, key: &'a str, value: &'a str) -> Self {
        self.properties.insert(key, value);
        self
    }
}
