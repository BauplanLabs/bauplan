//! Ref and commit endpoints.
//!
//! A ref names a commit: a branch name, a tag name, or a commit hash.

use gropius::{Path, Query, Request};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::error::{ErrorEnvelope, GenericError};
use crate::model::{AtCommit, Commit, Entity};
use crate::pagination::{ListQuery, Page};

/// A table or a namespace that differs between the two refs, and how.
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DiffEntry {
    /// The table or the namespace.
    pub entity: Entity,
    /// How it changes from `ref` to `to_ref`.
    pub change: DiffChange,
}

/// How an entity changes from `ref` to `to_ref`.
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiffChange {
    /// Only at `to_ref`.
    Added,
    /// Only at `ref`.
    Removed,
    /// At both refs, but it changed.
    Changed,
    /// At `to_ref` with a new name.
    Renamed {
        /// The name at `ref`.
        from: String,
    },
    /// A kind of change this client does not know yet.
    #[serde(untagged)]
    #[schemars(skip)]
    Unknown(serde_json::Value),
}

/// An object with the hashes of the two commits it compares.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "{T}BetweenCommits")]
pub struct BetweenCommits<T> {
    /// The object.
    #[serde(flatten)]
    pub inner: T,
    /// The hash of the commit at `ref`.
    pub from_commit_hash: String,
    /// The hash of the commit at `to_ref`.
    pub to_commit_hash: String,
}

/// API error returned by `list_history`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum ListHistoryError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Ref '{ref_name}' not found.
    #[api_error(404)]
    RefNotFound { ref_name: String },
    /// A generic error.
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `diff_refs`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum DiffRefsError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Ref '{ref_name}' not found.
    #[api_error(404)]
    RefNotFound { ref_name: String },
    /// A generic error.
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// Ref and commit operations.
#[gropius::api(tags = ["refs"], client(cfg(any(feature = "client-reqwest", feature = "client-ureq"))))]
pub trait RefApi {
    /// List the commit history at a ref, newest first.
    #[endpoint(GET, "/v1/catalogs/{catalog}/refs/{ref}/history")]
    async fn list_history(
        &self,
        path: Path<(String, String)>,
        query: Query<ListQuery>,
        req: Request,
    ) -> Result<AtCommit<Page<Commit>>, ListHistoryError>;

    /// List the tables and namespaces that differ from `ref` to `to_ref`, in
    /// name order.
    ///
    /// To read the next pages at the same commits, pass `from_commit_hash` and
    /// `to_commit_hash` as the refs.
    #[endpoint(GET, "/v1/catalogs/{catalog}/refs/{ref}/diff/{to_ref}")]
    async fn diff_refs(
        &self,
        path: Path<(String, String, String)>,
        query: Query<ListQuery>,
        req: Request,
    ) -> Result<BetweenCommits<Page<DiffEntry>>, DiffRefsError>;
}
