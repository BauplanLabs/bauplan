//! Request and response implementations for iceberg catalog types. The Bauplan
//! implements an iceberg catalog to spec at /iceberg/v1.
//!
//! Not all catalog operations are represented here; only the ones which are
//! presently used in the CLI or PySDK.

use std::{collections::BTreeMap, sync::Arc};

use iceberg::{
    NamespaceIdent, TableIdent, TableRequirement, TableUpdate,
    spec::{NestedField, Schema},
};
use iceberg_catalog_rest::{
    CommitTableRequest, CommitTableResponse, LoadTableResult as IcebergTable, RegisterTableRequest,
};

use crate::{
    ApiRequest, ApiResponse,
    api::{PathArgs, urlformat},
};

/// Iceberg joins the levels of a multi-level namespace with the unit separator.
fn encode_namespace(namespace: &str) -> String {
    namespace.replace('.', "\u{1f}")
}

/// Copy `schema` under a new `schema_id`, replacing the `doc` on every field
/// named in `docs`. A blank doc clears the field's documentation.
///
/// Iceberg schemas are immutable, so changing column documentation means adding
/// a whole new schema and making it current; see [`CommitTable`].
pub fn with_column_docs(
    schema: &Schema,
    schema_id: i32,
    docs: &BTreeMap<String, String>,
) -> Result<Schema, iceberg::Error> {
    let fields = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| match docs.get(&field.name) {
            None => field.clone(),
            Some(doc) => {
                let mut field = NestedField::clone(field);
                field.doc = crate::table::clean_doc(Some(doc)).map(str::to_owned);
                Arc::new(field)
            }
        })
        .collect::<Vec<_>>();

    Schema::builder()
        .with_schema_id(schema_id)
        .with_identifier_field_ids(schema.identifier_field_ids())
        .with_fields(fields)
        .build()
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
    type Response = IcebergTable;

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

    fn body(&self) -> Option<impl serde::Serialize> {
        Some(RegisterTableRequest {
            name: self.name.to_string(),
            metadata_location: self.metadata_location.to_string(),
            overwrite: Some(self.overwrite),
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
    type Response = IcebergTable;

    fn path(&self) -> PathArgs {
        urlformat!(
            "/iceberg/v1/{}/namespaces/{}/tables/{}",
            self.at_ref,
            &encode_namespace(self.namespace),
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
            &encode_namespace(self.namespace),
            self.name,
        )
    }

    fn body(&self) -> Option<impl serde::Serialize> {
        // `from_vec` only rejects an empty namespace, and `split` always yields
        // at least one level.
        let namespace = NamespaceIdent::from_vec(
            self.namespace.split('.').map(str::to_owned).collect(),
        )
        .expect("split yields at least one namespace level");

        Some(CommitTableRequest {
            identifier: Some(TableIdent::new(namespace, self.name.to_string())),
            requirements: self.requirements.clone(),
            updates: self.updates.clone(),
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

impl ApiResponse for IcebergTable {
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
