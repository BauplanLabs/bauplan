//! Catalog endpoints.

use gropius::{Body, EmptyResponse, Path, Query, Request};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::error::{ErrorEnvelope, GenericError};
use crate::pagination::{ListQuery, Page};

/// A catalog.
#[derive(Debug, Serialize, JsonSchema)]
pub struct Catalog {
    /// The catalog name.
    pub name: String,
}

/// Request body for creating a catalog.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateCatalogBody {
    /// The catalog name.
    #[schemars(
        length(max = 64),
        regex(pattern = "^[A-Za-z](?:[A-Za-z0-9_-]*[A-Za-z0-9])?$")
    )]
    pub name: String,
}

/// API error returned by `list_catalogs`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum ListCatalogsError {
    /// A generic error.
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `create_catalog`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum CreateCatalogError {
    /// Catalog '{catalog_name}' already exists.
    #[api_error(409)]
    CatalogExists { catalog_name: String },
    /// Not implemented.
    #[api_error(501)]
    NotImplemented,
    /// A generic error.
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `delete_catalog`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum DeleteCatalogError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Not implemented.
    #[api_error(501)]
    NotImplemented,
    /// A generic error.
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// Catalog operations.
#[gropius::api(tags = ["catalogs"])]
pub trait CatalogApi {
    /// List catalogs.
    #[endpoint(GET, "/v1/catalogs")]
    async fn list_catalogs(
        &self,
        query: Query<ListQuery>,
        req: Request,
    ) -> Result<Page<Catalog>, ListCatalogsError>;

    /// Create a catalog.
    #[endpoint(POST, "/v1/catalogs")]
    async fn create_catalog(
        &self,
        body: Body<CreateCatalogBody>,
        req: Request,
    ) -> Result<Catalog, CreateCatalogError>;

    /// Delete a catalog.
    #[endpoint(DELETE, "/v1/catalogs/{catalog}")]
    async fn delete_catalog(
        &self,
        path: Path<String>,
        req: Request,
    ) -> Result<EmptyResponse, DeleteCatalogError>;
}
