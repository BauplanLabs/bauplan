//! Catalog endpoints.

use std::sync::LazyLock;

use gropius::{Body, EmptyResponse, Path, Query, Request};
use regex::Regex;
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

const CATALOG_NAME_PATTERN: &str = "^[A-Za-z](?:[A-Za-z0-9_-]*[A-Za-z0-9])?$";
static CATALOG_NAME: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(CATALOG_NAME_PATTERN).expect("static regex"));

/// A valid catalog name.
fn catalog_name(value: &str, _: &()) -> garde::Result {
    if CATALOG_NAME.is_match(value) {
        Ok(())
    } else {
        Err(garde::Error::new("not a valid catalog name"))
    }
}

/// Request body for creating a catalog.
#[derive(Debug, Deserialize, JsonSchema, garde::Validate)]
pub struct CreateCatalogBody {
    /// The catalog name.
    #[garde(length(max = 64), custom(catalog_name))]
    #[schemars(regex(pattern = CATALOG_NAME_PATTERN))]
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn validates_catalog_names() {
        let validate = |name: &str| {
            garde::Validate::validate(&CreateCatalogBody {
                name: name.to_owned(),
            })
            .is_ok()
        };

        for valid in ["default", "a", "big-catalog_1"] {
            assert!(validate(valid), "{valid:?}");
        }

        for rejected in [
            "", " default", "default ", "9lead", "_lead", "lead_", "lead-", "a.b", "a/b", "a b",
        ] {
            assert!(!validate(rejected), "{rejected:?}");
        }

        assert!(validate(&format!("a{}", "b".repeat(63))));
        assert!(!validate(&format!("a{}", "b".repeat(64))));
    }
}
