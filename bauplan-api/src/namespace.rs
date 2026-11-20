//! Namespace endpoints.
//!
//! Namespaces are standalone objects: they can be empty, and create/delete
//! each produce a commit. A namespace name never has a dot, the dot splits a
//! namespace from a table name. We validate a name only when we create it,
//! so an old name we would not create today still works.

use gropius::{Body, Path, Query, Request};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::error::{ErrorEnvelope, GenericError};
use crate::model::{AtCommit, Commit, CommitOptions, WithCommit, is_identifier};
use crate::pagination::{ListQuery, Page};

/// A namespace.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Namespace {
    /// The namespace name.
    pub name: String,
}

/// A namespace in a list.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct NamespaceIdentifier {
    /// The namespace name.
    pub name: String,
}

/// Filters for the list of namespaces.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct NamespaceFilters {
    /// Keeps the namespaces whose name contains it, `^` and `$` anchor it to
    /// the start or the end.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// A valid namespace name.
fn namespace_name(value: &str, _: &()) -> garde::Result {
    if is_identifier(value) {
        Ok(())
    } else {
        Err(garde::Error::new("not a valid namespace name"))
    }
}

/// Request body for creating a namespace.
#[derive(Debug, Serialize, Deserialize, JsonSchema, garde::Validate)]
pub struct CreateNamespaceBody {
    /// The namespace name.
    #[garde(custom(namespace_name))]
    pub name: String,
    /// Commit options for the commit this creates.
    #[serde(default)]
    #[garde(dive)]
    pub commit: CommitOptions,
}

/// Request body for deleting a namespace.
#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, garde::Validate)]
pub struct DeleteNamespaceBody {
    /// Commit options for the commit this creates.
    #[serde(default)]
    #[garde(dive)]
    pub commit: CommitOptions,
}

/// API error returned by `list_namespaces`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum ListNamespacesError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Ref '{ref_name}' not found.
    #[api_error(404)]
    RefNotFound { ref_name: String },
    /// A generic error.
    #[displaydoc("{0}")]
    #[error(transparent)]
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `create_namespace`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum CreateNamespaceError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Ref '{ref_name}' not found.
    #[api_error(404)]
    RefNotFound { ref_name: String },
    /// Ref '{ref_name}' is not a branch.
    #[api_error(400)]
    NotABranch { ref_name: String },
    /// Name '{name}' is already in use.
    #[api_error(409)]
    NameConflict { name: String },
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

/// API error returned by `delete_namespace`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum DeleteNamespaceError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Ref '{ref_name}' not found.
    #[api_error(404)]
    RefNotFound { ref_name: String },
    /// Ref '{ref_name}' is not a branch.
    #[api_error(400)]
    NotABranch { ref_name: String },
    /// Namespace '{namespace_name}' not found.
    #[api_error(404)]
    NamespaceNotFound { namespace_name: String },
    /// Namespace '{namespace_name}' is not empty.
    #[api_error(409)]
    NamespaceNotEmpty { namespace_name: String },
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

/// Namespace operations.
#[gropius::api(tags = ["namespaces"], client(cfg(feature = "client")))]
pub trait NamespaceApi {
    /// List namespaces at a ref.
    #[endpoint(GET, "/v1/catalogs/{catalog}/refs/{ref}/namespaces")]
    async fn list_namespaces(
        &self,
        path: Path<(String, String)>,
        query: Query<ListQuery<NamespaceFilters>>,
        req: Request,
    ) -> Result<AtCommit<Page<NamespaceIdentifier>>, ListNamespacesError>;

    /// Create a namespace.
    ///
    /// ## Commit
    ///
    /// Produces a commit on the target branch; `commit.expected_commit_hash`
    /// enforces optimistic locking.
    #[endpoint(POST, "/v1/catalogs/{catalog}/refs/{ref}/namespaces")]
    async fn create_namespace(
        &self,
        path: Path<(String, String)>,
        body: Body<CreateNamespaceBody>,
        req: Request,
    ) -> Result<WithCommit<Namespace>, CreateNamespaceError>;

    /// Delete a namespace. Only empty namespaces can be deleted.
    ///
    /// ## Commit
    ///
    /// Produces a commit on the target branch; `commit.expected_commit_hash`
    /// enforces optimistic locking.
    #[endpoint(DELETE, "/v1/catalogs/{catalog}/refs/{ref}/namespaces/{namespace}")]
    async fn delete_namespace(
        &self,
        path: Path<(String, String, String)>,
        body: Option<Body<DeleteNamespaceBody>>,
        req: Request,
    ) -> Result<Commit, DeleteNamespaceError>;
}
