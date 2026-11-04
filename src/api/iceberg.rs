//! Request and response implementations for iceberg catalog types. The Bauplan
//! implements an iceberg catalog to spec at /iceberg/v1.
//!
//! Not all catalog operations are represented here; only the ones which are
//! presently used in the CLI or PySDK.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    ApiRequest, ApiResponse,
    api::{PathArgs, urlformat},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
struct RegisterTableRequest<'a> {
    name: &'a str,
    metadata_location: &'a str,
    overwrite: bool,
}

/// The response to a register table request, only the fields we use.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct LoadTableResult {
    /// The table metadata.
    pub metadata: TableMetadata,
}

/// The table metadata in a [`LoadTableResult`].
#[derive(Debug, PartialEq, Deserialize, Eq, Clone, Copy, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    /// The table UUID, or None if the metadata doesn't have one.
    pub table_uuid: Option<Uuid>,
}

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
    type Response = LoadTableResult;

    fn method(&self) -> http::Method {
        http::Method::POST
    }

    fn path(&self) -> PathArgs {
        urlformat!(
            "/iceberg/v1/{}/namespaces/{}/register",
            self.branch,
            self.namespace,
        )
    }

    fn body(&self) -> Option<impl Serialize> {
        Some(RegisterTableRequest {
            name: self.name,
            metadata_location: self.metadata_location,
            overwrite: self.overwrite,
        })
    }
}

impl ApiResponse for LoadTableResult {
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
            let raw: super::RawApiResponse<serde_json::Value> = serde_json::from_reader(body)
                .map_err(|_| super::ApiError::Other {
                    status: parts.status,
                    kind: None,
                    message: None,
                })?;

            match raw {
                super::RawApiResponse::Error { error } => {
                    Err(super::ApiError::from_raw(parts.status, error))
                }
                _ => Err(super::ApiError::Other {
                    status: parts.status,
                    kind: None,
                    message: None,
                }),
            }
        }
    }
}
