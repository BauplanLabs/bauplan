//! Request and response implementations for iceberg catalog types. The Bauplan
//! implements an iceberg catalog to spec at /iceberg/v1.
//!
//! Not all catalog operations are represented here; only the ones which are
//! presently used in the CLI or PySDK.

use iceberg_catalog_rest::{LoadTableResult as IcebergTable, RegisterTableRequest};

use crate::{ApiRequest, ApiResponse};

/// Register a table in a namespace on a branch, using an existing metadata
/// file.
#[derive(Debug, Clone)]
pub struct RegisterTable<'a> {
    /// The table name.
    pub name: &'a str,
    /// The metadata file location.
    pub metadata_location: &'a str,
    /// Whether to overwrite the table if it already exists.
    pub overwrite: bool,
    /// The branch to register the table on.
    pub branch: &'a str,
    /// The namespace to register the table in.
    pub namespace: &'a str,
}

impl ApiRequest for RegisterTable<'_> {
    type Response = IcebergTable;

    fn method(&self) -> http::Method {
        http::Method::POST
    }

    fn path(&self) -> String {
        format!(
            "/iceberg/v1/{}/namespaces/{}/register",
            self.branch, self.namespace,
        )
    }

    fn body(&self) -> Option<impl serde::Serialize> {
        Some(RegisterTableRequest {
            name: self.name.to_string(),
            metadata_location: self.metadata_location.to_string(),
            overwrite: Some(self.overwrite),
        })
    }
}

impl ApiResponse for IcebergTable {
    fn from_response_parts(
        parts: http::response::Parts,
        body: impl std::io::Read,
    ) -> Result<Self, super::ApiError> {
        if parts.status.is_success() {
            serde_json::from_reader(body).map_err(|e| {
                tracing::error!("Failed to parse iceberg response: {e:#?}");
                super::ApiError::InvalidResponse(parts.status)
            })
        } else {
            let raw: super::RawApiResponse<serde_json::Value> =
                serde_json::from_reader(body).map_err(|_| super::ApiError::Other(parts.status))?;

            match raw {
                super::RawApiResponse::Error { error } => {
                    Err(super::ApiError::from_raw(parts.status, error))
                }
                _ => Err(super::ApiError::Other(parts.status)),
            }
        }
    }
}
