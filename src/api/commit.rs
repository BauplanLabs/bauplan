//! Types for commit operations.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{CatalogRef, PaginatedResponse, api::ApiRequest, branch::Branch};

/// An actor (author or committer) in a commit.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(name = "Actor", module = "bauplan", from_py_object, get_all)
)]
pub struct Actor {
    /// The actor's name.
    pub name: String,
    /// The actor's email address.
    pub email: Option<String>,
}

/// A commit in the catalog.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(name = "Commit", module = "bauplan", from_py_object, get_all)
)]
pub struct Commit {
    /// The ref (branch, tag, or detached) this commit is on.
    pub r#ref: CatalogRef,
    /// The commit message.
    pub message: Option<String>,
    /// The authors of the commit.
    pub authors: Vec<Actor>,
    /// The date the commit was authored.
    pub authored_date: DateTime<Utc>,
    /// The committer of the commit.
    pub committer: Actor,
    /// The date the commit was committed.
    pub committed_date: DateTime<Utc>,
    /// The parent ref.
    pub parent_ref: CatalogRef,
    /// The parent commit hashes.
    pub parent_hashes: Vec<String>,
    /// Custom properties on the commit.
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    /// Actors who signed off on the commit.
    #[serde(default)]
    pub signed_off_by: Vec<Actor>,
}

impl Commit {
    /// Returns the first author of the commit.
    pub fn author(&self) -> Option<&Actor> {
        self.authors.first()
    }

    /// Returns the commit hash from the ref.
    pub fn hash(&self) -> &str {
        match &self.r#ref {
            CatalogRef::Branch { hash, .. } => hash,
            CatalogRef::Tag { hash, .. } => hash,
            CatalogRef::Detached { hash } => hash,
        }
    }

    /// Returns just the subject line of the commit message.
    pub fn subject(&self) -> Option<&str> {
        self.message.as_ref().and_then(|m| {
            let s = m.trim().lines().next()?.trim();
            if s.is_empty() { None } else { Some(s) }
        })
    }

    /// Returns the body of the commit message (everything after the subject).
    pub fn body(&self) -> Option<&str> {
        self.message.as_ref().and_then(|m| {
            let trimmed = m.trim();
            let newline_pos = trimmed.find('\n')?;
            let body = trimmed[newline_pos + 1..].trim();
            if body.is_empty() { None } else { Some(body) }
        })
    }

    /// For merge commits, returns a Branch pointing to the second parent.
    pub fn parent_merge_ref(&self) -> Option<Branch> {
        if self.parent_hashes.len() > 1
            && let CatalogRef::Branch { name, .. } = &self.parent_ref
        {
            return Some(Branch {
                name: name.clone(),
                hash: self.parent_hashes[1].clone(),
            });
        }

        None
    }
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl Commit {
    fn __repr__(&self) -> String {
        let hash = self.hash();
        let short_hash = &hash[..hash.len().min(8)];
        let subject = self.subject().unwrap_or_default();
        let author = self.author().map(|a| a.name.as_str()).unwrap_or_default();
        format!("Commit(hash={short_hash:?}, author={author:?}, message={subject:?})")
    }

    #[getter(author)]
    fn py_author(&self) -> Option<Actor> {
        self.author().cloned()
    }

    #[getter(subject)]
    fn py_subject(&self) -> Option<&str> {
        self.subject()
    }

    #[getter(body)]
    fn py_body(&self) -> Option<&str> {
        self.body()
    }

    #[getter(parent_merge_ref)]
    fn py_parent_merge_ref(&self) -> Option<Branch> {
        self.parent_merge_ref()
    }
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl Actor {
    fn __repr__(&self) -> String {
        if let Some(email) = &self.email {
            format!("Actor(name={:?}, email={:?})", self.name, email)
        } else {
            format!("Actor(name={:?})", self.name)
        }
    }
}

/// Options for modifying a commit operation.
#[derive(Default, Debug, Clone, Serialize)]
pub struct CommitOptions<'a> {
    /// Override the commit body.
    #[serde(rename = "commit_body", skip_serializing_if = "Option::is_none")]
    pub body: Option<&'a str>,

    /// Additional commit properties.
    #[serde(
        rename = "commit_properties",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub properties: BTreeMap<&'a str, &'a str>,
}

/// List commits for a ref.
#[derive(Debug, Clone)]
pub struct GetCommits<'a> {
    /// The ref to get commits from.
    pub at_ref: &'a str,
    /// Filter commits by message content.
    pub filter_by_message: Option<&'a str>,
    /// Filter commits by author username.
    pub filter_by_author_username: Option<&'a str>,
    /// Filter commits by author name.
    pub filter_by_author_name: Option<&'a str>,
    /// Filter commits by author email.
    pub filter_by_author_email: Option<&'a str>,
    /// Filter commits by exact authored date.
    pub filter_by_authored_date: Option<&'a str>,
    /// Filter commits authored after this date.
    pub filter_by_authored_date_start_at: Option<&'a str>,
    /// Filter commits authored before this date.
    pub filter_by_authored_date_end_at: Option<&'a str>,
    /// Filter commits by parent hash.
    pub filter_by_parent_hash: Option<&'a str>,
    /// Filter commits by properties.
    pub filter_by_properties: Option<&'a BTreeMap<String, String>>,
    /// CEL filter expression.
    pub filter: Option<&'a str>,
}

#[derive(Serialize)]
struct GetCommitsQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_by_message: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_by_author_username: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_by_author_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_by_author_email: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_by_authored_date: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_by_authored_date_start_at: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_by_authored_date_end_at: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_by_parent_hash: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_by_properties: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<&'a str>,
}

impl ApiRequest for GetCommits<'_> {
    type Response = PaginatedResponse<Commit>;

    fn path(&self) -> String {
        format!("/catalog/v0/refs/{}/commits", self.at_ref)
    }

    fn query(&self) -> Option<impl Serialize> {
        Some(GetCommitsQuery {
            filter_by_message: self.filter_by_message,
            filter_by_author_username: self.filter_by_author_username,
            filter_by_author_name: self.filter_by_author_name,
            filter_by_author_email: self.filter_by_author_email,
            filter_by_authored_date: self.filter_by_authored_date,
            filter_by_authored_date_start_at: self.filter_by_authored_date_start_at,
            filter_by_authored_date_end_at: self.filter_by_authored_date_end_at,
            filter_by_parent_hash: self.filter_by_parent_hash,
            filter_by_properties: self
                .filter_by_properties
                .map(|p| serde_json::to_string(p).unwrap_or_default()),
            filter: self.filter,
        })
    }
}

#[cfg(all(test, feature = "_integration-tests"))]
mod test {
    use super::*;
    use crate::{ApiError, ApiErrorKind, api::testutil::roundtrip, paginate};

    #[test]
    fn get_commits() -> anyhow::Result<()> {
        let req = GetCommits {
            at_ref: "main",
            filter_by_message: None,
            filter_by_author_username: None,
            filter_by_author_name: None,
            filter_by_author_email: None,
            filter_by_authored_date: None,
            filter_by_authored_date_start_at: None,
            filter_by_authored_date_end_at: None,
            filter_by_parent_hash: None,
            filter_by_properties: None,
            filter: None,
        };

        let commits =
            paginate(req, Some(5), |r| roundtrip(r))?.collect::<Result<Vec<Commit>, ApiError>>()?;

        assert!(!commits.is_empty());
        for commit in &commits {
            assert!(!commit.hash().is_empty());
            assert!(!commit.authors.is_empty());
        }

        Ok(())
    }

    #[test]
    fn get_commits_with_filter() -> anyhow::Result<()> {
        let req = GetCommits {
            at_ref: "main",
            filter_by_message: Some("Update"),
            filter_by_author_username: None,
            filter_by_author_name: None,
            filter_by_author_email: None,
            filter_by_authored_date: None,
            filter_by_authored_date_start_at: None,
            filter_by_authored_date_end_at: None,
            filter_by_parent_hash: None,
            filter_by_properties: None,
            filter: None,
        };

        let commits =
            paginate(req, Some(5), |r| roundtrip(r))?.collect::<Result<Vec<Commit>, ApiError>>()?;

        // All commits should contain "Update" in their message
        for commit in &commits {
            if let Some(msg) = &commit.message {
                assert!(
                    msg.contains("Update"),
                    "Expected message to contain 'Update', got: {}",
                    msg
                );
            }
        }

        Ok(())
    }

    #[test]
    fn get_commits_ref_not_found() -> anyhow::Result<()> {
        let req = GetCommits {
            at_ref: "nonexistent_branch_12345",
            filter_by_message: None,
            filter_by_author_username: None,
            filter_by_author_name: None,
            filter_by_author_email: None,
            filter_by_authored_date: None,
            filter_by_authored_date_start_at: None,
            filter_by_authored_date_end_at: None,
            filter_by_parent_hash: None,
            filter_by_properties: None,
            filter: None,
        };

        let Err(ApiError::ErrorResponse {
            kind: ApiErrorKind::RefNotFound { input_ref },
            ..
        }) = roundtrip(req)
        else {
            panic!("expected RefNotFound");
        };

        assert_eq!(input_ref, "nonexistent_branch_12345");

        Ok(())
    }
}
