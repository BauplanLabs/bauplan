//! Request and response implementations for iceberg catalog types. The Bauplan
//! implements an iceberg catalog to spec at /iceberg/v1.
//!
//! Not all catalog operations are represented here; only the ones which are
//! presently used in the CLI or PySDK.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    ApiRequest, ApiResponse,
    api::{PathArgs, urlformat},
};

/// Copy `schema` under a new `schema_id`, replacing the `doc` on every field
/// named in `docs`. A blank doc clears the field's documentation.
///
/// Iceberg schemas are immutable, so changing column documentation means adding
/// a whole new schema and making it current; see [`CommitTable`].
pub fn with_column_docs(
    schema: &Schema,
    schema_id: i32,
    docs: &BTreeMap<String, String>,
) -> Schema {
    let fields = schema
        .fields
        .iter()
        .map(|field| match docs.get(&field.name) {
            None => field.clone(),
            Some(doc) => SchemaField {
                doc: crate::table::clean_doc(Some(doc)).map(str::to_owned),
                ..field.clone()
            },
        })
        .collect();

    Schema {
        schema_id,
        fields,
        ..schema.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
struct RegisterTableRequest<'a> {
    name: &'a str,
    metadata_location: &'a str,
    overwrite: bool,
}

/// The response to a load table or register table request, only the fields we
/// use.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct LoadTableResult {
    /// The table metadata.
    pub metadata: TableMetadata,
}

/// The table metadata in a [`LoadTableResult`], only the fields we use.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    /// The table UUID, or None if the metadata doesn't have one.
    pub table_uuid: Option<Uuid>,
    /// The ID of the current schema in `schemas`.
    #[serde(default)]
    pub current_schema_id: Option<i32>,
    /// Every schema the table has had.
    #[serde(default)]
    pub schemas: Vec<Schema>,
    /// The table properties. Table documentation is the `comment` property.
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

/// An Iceberg schema.
///
/// Only the top-level fields are modeled; everything else (the `type` tag,
/// nested field types, and so on) is kept verbatim in `extra`, so a schema
/// read from the catalog serializes back unchanged in an `add-schema` update.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct Schema {
    /// The schema ID.
    pub schema_id: i32,
    /// The top-level fields of the schema.
    pub fields: Vec<SchemaField>,
    /// Any other schema attributes, passed through unchanged.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// A top-level field in a [`Schema`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct SchemaField {
    /// The field ID.
    pub id: i32,
    /// The field name.
    pub name: String,
    /// The field documentation, if it has any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// Any other field attributes (`type`, `required`, ...), passed through
    /// unchanged.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// A metadata update in a [`CommitTable`] request; only the actions we use.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "action",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
pub enum TableUpdate {
    /// Set (add or overwrite) table properties.
    SetProperties {
        /// The properties to set.
        updates: BTreeMap<String, String>,
    },
    /// Remove table properties.
    RemoveProperties {
        /// The property keys to remove.
        removals: Vec<String>,
    },
    /// Add a schema to the table, without making it current.
    AddSchema {
        /// The schema to add.
        schema: Schema,
    },
    /// Make a schema current. `-1` means the schema added last in the same
    /// commit.
    SetCurrentSchema {
        /// The ID of the schema to make current.
        schema_id: i32,
    },
}

/// A precondition in a [`CommitTable`] request; only the ones we use.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
pub enum TableRequirement {
    /// The table UUID must match.
    AssertTableUuid {
        /// The expected table UUID.
        uuid: Uuid,
    },
    /// The current schema ID must match.
    AssertCurrentSchemaId {
        /// The expected current schema ID.
        current_schema_id: i32,
    },
}

/// A table identifier shaped to match the Iceberg REST spec
/// (`{"namespace": [...], "name": "..."}`), not Bauplan's own `namespace.table`
/// form. Iceberg allows multi-level namespaces, but Bauplan namespaces are a
/// single level.
#[derive(Debug, Clone, Serialize)]
struct TableIdentifier<'a> {
    namespace: [&'a str; 1],
    name: &'a str,
}

#[derive(Debug, Clone, Serialize)]
struct CommitTableRequest<'a> {
    identifier: TableIdentifier<'a>,
    requirements: &'a [TableRequirement],
    updates: &'a [TableUpdate],
}

/// The response to a [`CommitTable`] request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTableResponse {
    /// The location of the new metadata file.
    pub metadata_location: String,
    /// The table metadata after the commit.
    pub metadata: TableMetadata,
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

/// Load the Iceberg metadata for a table at a ref.
///
/// Unlike [`crate::table::GetTable`], which goes through the Bauplan catalog
/// service, this reads the table straight from the Iceberg catalog. Table
/// documentation is the `comment` entry in the metadata properties; column
/// documentation is the `doc` on each field of the current schema.
#[derive(Debug, Clone)]
pub struct LoadTable<'a> {
    /// The table name, without a namespace.
    pub name: &'a str,
    /// The namespace the table is in.
    pub namespace: &'a str,
    /// The ref (branch, tag, etc) at which to read the table.
    pub at_ref: &'a str,
}

impl ApiRequest for LoadTable<'_> {
    type Response = LoadTableResult;

    fn path(&self) -> PathArgs {
        urlformat!(
            "/iceberg/v1/{}/namespaces/{}/tables/{}",
            self.at_ref,
            self.namespace,
            self.name,
        )
    }
}

/// Commit metadata updates to a table on a branch.
///
/// Requirements are preconditions checked before the commit lands; an empty
/// list makes the write unconditional.
#[derive(Debug, Clone)]
pub struct CommitTable<'a> {
    /// The table name, without a namespace.
    pub name: &'a str,
    /// The namespace the table is in.
    pub namespace: &'a str,
    /// The branch to commit to.
    pub branch: &'a str,
    /// Preconditions that must hold for the commit to land.
    pub requirements: Vec<TableRequirement>,
    /// The metadata updates to apply.
    pub updates: Vec<TableUpdate>,
}

impl ApiRequest for CommitTable<'_> {
    type Response = CommitTableResponse;

    fn method(&self) -> http::Method {
        http::Method::POST
    }

    fn path(&self) -> PathArgs {
        urlformat!(
            "/iceberg/v1/{}/namespaces/{}/tables/{}",
            self.branch,
            self.namespace,
            self.name,
        )
    }

    fn body(&self) -> Option<impl Serialize> {
        Some(CommitTableRequest {
            identifier: TableIdentifier {
                namespace: [self.namespace],
                name: self.name,
            },
            requirements: &self.requirements,
            updates: &self.updates,
        })
    }
}

/// Parse a response from the iceberg endpoint, which returns bare Iceberg JSON
/// on success and a Bauplan-shaped error envelope on failure.
fn iceberg_response<T: serde::de::DeserializeOwned>(
    parts: http::response::Parts,
    body: impl std::io::Read,
) -> Result<T, super::ApiError> {
    if parts.status.is_success() {
        serde_json::from_reader(body).map_err(|e| {
            tracing::error!("Failed to parse iceberg response: {e:#?}");
            super::ApiError::InvalidResponse(parts.status)
        })
    } else {
        let raw: super::RawApiResponse<serde_json::Value> =
            serde_json::from_reader(body).map_err(|_| super::ApiError::Other {
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

impl ApiResponse for LoadTableResult {
    fn from_response_parts(
        parts: http::response::Parts,
        body: impl std::io::Read,
    ) -> Result<Self, super::ApiError> {
        iceberg_response(parts, body)
    }
}

impl ApiResponse for CommitTableResponse {
    fn from_response_parts(
        parts: http::response::Parts,
        body: impl std::io::Read,
    ) -> Result<Self, super::ApiError> {
        iceberg_response(parts, body)
    }
}
