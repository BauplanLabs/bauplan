//! Ref and commit endpoints.
//!
//! A ref names a commit: a branch name, a tag name, or a commit hash.

use gropius::{Path, Query, Request};

use crate::error::{ErrorEnvelope, GenericError};
use crate::model::{AtCommit, Commit};
use crate::pagination::{ListQuery, Page};

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
    #[displaydoc("{0}")]
    #[error(transparent)]
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// Ref and commit operations.
#[gropius::api(tags = ["refs"], client(cfg(feature = "client")))]
pub trait RefApi {
    /// List the commit history at a ref, newest first.
    #[endpoint(GET, "/v1/catalogs/{catalog}/refs/{ref}/history")]
    async fn list_history(
        &self,
        path: Path<(String, String)>,
        query: Query<ListQuery>,
        req: Request,
    ) -> Result<AtCommit<Page<Commit>>, ListHistoryError>;
}
