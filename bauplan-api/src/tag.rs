//! Tag endpoints.
//!
//! A tag is a name pointing at a commit.

use chrono::{DateTime, Utc};
use gropius::{Body, EmptyResponse, Path, Query, Request};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::error::{ErrorEnvelope, GenericError};
use crate::pagination::{ListQuery, Page};

/// A tag.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Tag {
    /// The tag name.
    pub name: String,
    /// The hash of the commit the tag points at.
    pub commit_hash: String,
    /// When the tag was created.
    pub created_at: DateTime<Utc>,
    /// When the tag was last updated.
    pub updated_at: DateTime<Utc>,
}

/// Request body for creating a tag.
#[derive(Debug, Serialize, Deserialize, JsonSchema, garde::Validate)]
pub struct CreateTagBody {
    /// Name for the new tag.
    #[garde(custom(crate::model::ref_syntax))]
    pub name: String,
    /// The ref to tag.
    #[garde(skip)]
    pub from_ref: String,
}

/// Request body for renaming a tag.
#[derive(Debug, Serialize, Deserialize, JsonSchema, garde::Validate)]
pub struct RenameTagBody {
    /// The new tag name.
    #[garde(custom(crate::model::ref_syntax))]
    pub name: String,
}

/// API error returned by `list_tags`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum ListTagsError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// A generic error.
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `create_tag`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum CreateTagError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Tag '{name}' already exists.
    #[api_error(409)]
    TagNameConflict { name: String },
    /// Name '{name}' is already used by a branch.
    #[api_error(409)]
    BranchNameConflict { name: String },
    /// Source ref '{from_ref}' not found.
    #[api_error(404)]
    FromRefNotFound { from_ref: String },
    /// A generic error.
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `resolve_tag`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum ResolveTagError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Tag '{tag_name}' not found.
    #[api_error(404)]
    TagNotFound { tag_name: String },
    /// A generic error.
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `rename_tag`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum RenameTagError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Tag '{tag_name}' not found.
    #[api_error(404)]
    TagNotFound { tag_name: String },
    /// Tag '{name}' already exists.
    #[api_error(409)]
    TagNameConflict { name: String },
    /// Name '{name}' is already used by a branch.
    #[api_error(409)]
    BranchNameConflict { name: String },
    /// Tag '{tag_name}' changed during the request.
    #[api_error(409)]
    TagChanged { tag_name: String },
    /// A generic error.
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `delete_tag`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum DeleteTagError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Tag '{tag_name}' not found.
    #[api_error(404)]
    TagNotFound { tag_name: String },
    /// Tag '{tag_name}' changed during the request.
    #[api_error(409)]
    TagChanged { tag_name: String },
    /// A generic error.
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// Tag operations.
#[gropius::api(tags = ["tags"], client(cfg(feature = "client")))]
pub trait TagApi {
    /// List tags.
    #[endpoint(GET, "/v1/catalogs/{catalog}/tags")]
    async fn list_tags(
        &self,
        path: Path<String>,
        query: Query<ListQuery>,
        req: Request,
    ) -> Result<Page<Tag>, ListTagsError>;

    /// Create a tag pointing at the commit `from_ref` resolves to.
    #[endpoint(POST, "/v1/catalogs/{catalog}/tags")]
    async fn create_tag(
        &self,
        path: Path<String>,
        body: Body<CreateTagBody>,
        req: Request,
    ) -> Result<Tag, CreateTagError>;

    /// Resolve a tag to the commit hash it points at.
    #[endpoint(GET, "/v1/catalogs/{catalog}/tags/{tag}")]
    async fn resolve_tag(
        &self,
        path: Path<(String, String)>,
        req: Request,
    ) -> Result<Tag, ResolveTagError>;

    /// Rename a tag.
    #[endpoint(POST, "/v1/catalogs/{catalog}/tags/{tag}/rename")]
    async fn rename_tag(
        &self,
        path: Path<(String, String)>,
        body: Body<RenameTagBody>,
        req: Request,
    ) -> Result<Tag, RenameTagError>;

    /// Delete a tag.
    #[endpoint(DELETE, "/v1/catalogs/{catalog}/tags/{tag}")]
    async fn delete_tag(
        &self,
        path: Path<(String, String)>,
        req: Request,
    ) -> Result<EmptyResponse, DeleteTagError>;
}
