//! Branch endpoints.
//!
//! A branch is a named line of history whose head advances with each commit.

use chrono::{DateTime, Utc};
use gropius::{Body, EmptyResponse, Path, Query, Request};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::error::{ErrorEnvelope, GenericError};
use crate::model::{Commit, ExpectedHead};
use crate::pagination::{ListQuery, Page};

/// A branch.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Branch {
    /// The branch name.
    pub name: String,
    /// The hash of the commit at the head of the branch.
    pub commit_hash: String,
    /// When the branch was created.
    pub created_at: DateTime<Utc>,
    /// When the branch was last updated. This is the later of two values:
    /// - The creation time of the branch's HEAD commit
    /// - The creation time of the branch itself
    pub updated_at: DateTime<Utc>,
    /// How long the branch lives, in seconds.
    pub ttl_seconds: Option<u64>,
}

/// Filters for the list of branches.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct BranchFilters {
    /// Only the branches owned by the current user.
    #[serde(
        default,
        skip_serializing_if = "std::ops::Not::not",
        deserialize_with = "crate::pagination::query_bool"
    )]
    pub owned: bool,
    /// Keeps the branches whose name contains it, `^` and `$` anchor it to
    /// the start or the end.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Request body for creating a branch.
#[derive(Debug, Serialize, Deserialize, JsonSchema, garde::Validate)]
pub struct CreateBranchBody {
    /// Name for the new branch.
    #[garde(custom(crate::model::ref_syntax))]
    pub name: String,
    /// The ref to branch from.
    #[garde(skip)]
    pub from_ref: String,
    /// If set, the branch should be automatically deleted after the given duration.
    #[serde(default)]
    #[garde(range(min = 1))]
    pub ttl_seconds: Option<u64>,
}

/// Request body for renaming a branch.
#[derive(Debug, Serialize, Deserialize, JsonSchema, garde::Validate)]
pub struct RenameBranchBody {
    /// The new branch name.
    #[garde(custom(crate::model::ref_syntax))]
    pub name: String,
}

/// Request body for setting the TTL of a branch.
#[derive(Debug, Serialize, Deserialize, JsonSchema, garde::Validate)]
pub struct SetBranchTtlBody {
    /// How long the branch lives, in seconds.
    #[garde(range(min = 1))]
    pub ttl_seconds: u64,
}

/// Request body for merging into a branch.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct MergeBranchBody {
    /// The ref to merge, a branch, a tag or a commit hash.
    pub from_ref: String,
    /// The merge strategy, a squash by default.
    #[serde(default)]
    pub strategy: MergeStrategy,
    #[serde(flatten)]
    pub expected_head: ExpectedHead,
}

/// The merge strategy.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MergeStrategy {
    /// One commit with all the changes of the source.
    Squash {
        /// The commit message. Defaults to `Merge {from_ref} into {branch}`.
        #[serde(default)]
        message: Option<String>,
    },
    /// The commits of the source are replayed one by one on the branch.
    Rebase,
    /// The branch is moved to the source wholesale. The common ancestor must
    /// be the tip of the branch.
    FastForward,
}

impl Default for MergeStrategy {
    fn default() -> Self {
        Self::Squash { message: None }
    }
}

/// An entity the merge stops on.
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Conflict {
    /// A table, `namespace.table`.
    Table(String),
    /// A namespace.
    Namespace(String),
    /// A kind of entity this client does not know yet.
    #[serde(untagged)]
    #[schemars(skip)]
    Unknown(serde_json::Value),
}

/// API error returned by `list_branches`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum ListBranchesError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// A generic error.
    #[displaydoc("{0}")]
    #[error(transparent)]
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `create_branch`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum CreateBranchError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Branch '{name}' already exists.
    #[api_error(409)]
    BranchNameConflict { name: String },
    /// Name '{name}' is already used by a tag.
    #[api_error(409)]
    TagNameConflict { name: String },
    /// Source ref '{from_ref}' not found.
    #[api_error(404)]
    FromRefNotFound { from_ref: String },
    /// Not implemented.
    #[api_error(501)]
    NotImplemented,
    /// A generic error.
    #[displaydoc("{0}")]
    #[error(transparent)]
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `resolve_branch`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum ResolveBranchError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Branch '{branch_name}' not found.
    #[api_error(404)]
    BranchNotFound { branch_name: String },
    /// A generic error.
    #[displaydoc("{0}")]
    #[error(transparent)]
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `rename_branch`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum RenameBranchError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Branch 'main' cannot be renamed or deleted.
    #[api_error(403)]
    MainBranch,
    /// Branch '{branch_name}' not found.
    #[api_error(404)]
    BranchNotFound { branch_name: String },
    /// Branch '{name}' already exists.
    #[api_error(409)]
    BranchNameConflict { name: String },
    /// Name '{name}' is already used by a tag.
    #[api_error(409)]
    TagNameConflict { name: String },
    /// Branch '{branch_name}' head has moved to '{commit_hash}'.
    #[api_error(409)]
    BranchHeadChanged {
        branch_name: String,
        commit_hash: String,
    },
    /// A generic error.
    #[displaydoc("{0}")]
    #[error(transparent)]
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `set_branch_ttl`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum SetBranchTtlError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Branch 'main' cannot have a TTL.
    #[api_error(403)]
    MainBranch,
    /// Branch '{branch_name}' not found.
    #[api_error(404)]
    BranchNotFound { branch_name: String },
    /// Not implemented.
    #[api_error(501)]
    NotImplemented,
    /// A generic error.
    #[displaydoc("{0}")]
    #[error(transparent)]
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `remove_branch_ttl`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum RemoveBranchTtlError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Branch 'main' cannot have a TTL.
    #[api_error(403)]
    MainBranch,
    /// Branch '{branch_name}' not found.
    #[api_error(404)]
    BranchNotFound { branch_name: String },
    /// Not implemented.
    #[api_error(501)]
    NotImplemented,
    /// A generic error.
    #[displaydoc("{0}")]
    #[error(transparent)]
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `delete_branch`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum DeleteBranchError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Branch 'main' cannot be renamed or deleted.
    #[api_error(403)]
    MainBranch,
    /// Branch '{branch_name}' not found.
    #[api_error(404)]
    BranchNotFound { branch_name: String },
    /// Branch '{branch_name}' head has moved to '{commit_hash}'.
    #[api_error(409)]
    BranchHeadChanged {
        branch_name: String,
        commit_hash: String,
    },
    /// A generic error.
    #[displaydoc("{0}")]
    #[error(transparent)]
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `merge_branch`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum MergeBranchError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Branch '{branch_name}' not found.
    #[api_error(404)]
    BranchNotFound { branch_name: String },
    /// Source ref '{from_ref}' not found.
    #[api_error(404)]
    FromRefNotFound { from_ref: String },
    /// Branch '{branch_name}' head has moved to '{commit_hash}'.
    #[api_error(409)]
    BranchHeadChanged {
        branch_name: String,
        commit_hash: String,
    },
    /// The merge on '{commit_hash}' has conflicts.
    #[api_error(409)]
    MergeConflict {
        commit_hash: String,
        conflicts: Vec<Conflict>,
    },
    /// Not implemented.
    #[api_error(501)]
    NotImplemented,
    /// A generic error.
    #[displaydoc("{0}")]
    #[error(transparent)]
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// Branch operations.
#[gropius::api(tags = ["branches"], client(cfg(feature = "client")))]
pub trait BranchApi {
    /// List branches.
    #[endpoint(GET, "/v1/catalogs/{catalog}/branches")]
    async fn list_branches(
        &self,
        path: Path<String>,
        query: Query<ListQuery<BranchFilters>>,
        req: Request,
    ) -> Result<Page<Branch>, ListBranchesError>;

    /// Create a branch pointing at the commit `from_ref` resolves to.
    #[endpoint(POST, "/v1/catalogs/{catalog}/branches")]
    async fn create_branch(
        &self,
        path: Path<String>,
        body: Body<CreateBranchBody>,
        req: Request,
    ) -> Result<Branch, CreateBranchError>;

    /// Resolve a branch to its head commit hash.
    #[endpoint(GET, "/v1/catalogs/{catalog}/branches/{branch}")]
    async fn resolve_branch(
        &self,
        path: Path<(String, String)>,
        req: Request,
    ) -> Result<Branch, ResolveBranchError>;

    /// Rename a branch.
    #[endpoint(POST, "/v1/catalogs/{catalog}/branches/{branch}/rename")]
    async fn rename_branch(
        &self,
        path: Path<(String, String)>,
        body: Body<RenameBranchBody>,
        req: Request,
    ) -> Result<Branch, RenameBranchError>;

    /// Set the TTL of a branch.
    #[endpoint(POST, "/v1/catalogs/{catalog}/branches/{branch}/ttl")]
    async fn set_branch_ttl(
        &self,
        path: Path<(String, String)>,
        body: Body<SetBranchTtlBody>,
        req: Request,
    ) -> Result<Branch, SetBranchTtlError>;

    /// Remove the TTL of a branch.
    #[endpoint(DELETE, "/v1/catalogs/{catalog}/branches/{branch}/ttl")]
    async fn remove_branch_ttl(
        &self,
        path: Path<(String, String)>,
        req: Request,
    ) -> Result<Branch, RemoveBranchTtlError>;

    /// Merge a ref into a branch.
    ///
    /// ## Commit
    ///
    /// A squash produces a commit on the branch, or returns `null` when there is
    /// nothing to merge; `expected_commit_hash` enforces optimistic locking.
    #[endpoint(POST, "/v1/catalogs/{catalog}/branches/{branch}/merge")]
    async fn merge_branch(
        &self,
        path: Path<(String, String)>,
        body: Body<MergeBranchBody>,
        req: Request,
    ) -> Result<Option<Commit>, MergeBranchError>;

    /// Delete a branch.
    #[endpoint(DELETE, "/v1/catalogs/{catalog}/branches/{branch}")]
    async fn delete_branch(
        &self,
        path: Path<(String, String)>,
        body: Option<Body<ExpectedHead>>,
        req: Request,
    ) -> Result<EmptyResponse, DeleteBranchError>;
}
