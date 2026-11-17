//! Table endpoints.
//!
//! A full table name is `namespace.table`, with one dot only: the two parts
//! never have one. We validate a name only when we create it, so an old name
//! we would not create today still works.

use chrono::{DateTime, Utc};
use gropius::{Body, Path, Query, Request};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::arrow_schema::ArrowField;
use crate::error::{ErrorEnvelope, GenericError};
use crate::model::{AtCommit, Commit, CommitOptions, WithCommit, is_identifier};
use crate::pagination::{ListQuery, Page};

/// Splits a full table name `namespace.table` by the dot. We do not validate
/// the identifiers here, a name is validated only when it is created.
pub fn split_table_name(name: &str) -> Option<(&str, &str)> {
    let (namespace, table) = name.split_once('.')?;
    if namespace.is_empty() || table.is_empty() || table.contains('.') {
        return None;
    }

    Some((namespace, table))
}

/// A valid table name is made of two valid identifiers.
fn table_name(value: &str, _: &()) -> garde::Result {
    match split_table_name(value) {
        Some((namespace, table)) if is_identifier(namespace) && is_identifier(table) => Ok(()),
        _ => Err(garde::Error::new("not a valid table name")),
    }
}

/// A table in a list.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct TableIdentifier {
    /// The table name, `namespace.table`.
    pub name: String,
}

/// A table.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Table {
    /// The table name, `namespace.table`.
    pub name: String,
    /// The kind of table, with its kind-specific details.
    pub kind: TableKind,
    /// The documentation of the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// The number of records, if the current snapshot has the totals.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records: Option<u64>,
    /// The size of the data files in bytes, if the current snapshot has the
    /// totals.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// The columns of the table, in arrow terms.
    pub columns: Vec<ArrowField>,
    /// The partition fields.
    pub partitions: Vec<PartitionField>,
    /// When the table was created, only for the tables that record it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// When the table was last written.
    pub updated_at: DateTime<Utc>,
}

/// The kind of a table: its format and who owns its files.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TableKind {
    /// An iceberg table whose files bauplan writes and owns.
    IcebergManaged {
        /// The location of the current metadata file.
        metadata_location: String,
    },
    /// A read-only iceberg table whose metadata and data files live outside
    /// bauplan.
    IcebergExternal {
        /// The location of the current metadata file.
        metadata_location: String,
    },
    /// A read-only iceberg table whose metadata bauplan writes, over parquet
    /// files that live outside bauplan.
    IcebergExternalParquet {
        /// The location of the current metadata file.
        metadata_location: String,
    },
    /// A kind this client does not know yet.
    #[serde(untagged, skip_serializing)]
    #[schemars(skip)]
    Unknown(serde_json::Value),
}

/// A partition field of the table.
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PartitionField {
    /// The field the table is partitioned by.
    pub name: String,
    /// The partition transform.
    pub transform: PartitionTransform,
}

/// A partition transform: a string for the ones with no parameter, an
/// object with one key for the others.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket(u32),
    Truncate(u32),
}

/// Request body for creating a table.
#[derive(Debug, Serialize, Deserialize, JsonSchema, garde::Validate)]
pub struct CreateTableBody {
    /// The table name, `namespace.table`. The namespace must exist.
    #[garde(custom(table_name))]
    pub name: String,
    /// The documentation of the table.
    #[serde(default)]
    #[garde(skip)]
    pub doc: Option<String>,
    /// The columns of the table, in arrow terms.
    #[garde(length(min = 1))]
    pub columns: Vec<ArrowField>,
    /// The partition fields.
    #[serde(default)]
    #[garde(skip)]
    pub partitions: Vec<PartitionField>,
    /// Commit options for the commit this creates.
    #[serde(default)]
    #[garde(dive)]
    pub commit: CommitOptions,
}

/// Request body for cloning a table.
#[derive(Debug, Serialize, Deserialize, JsonSchema, garde::Validate)]
pub struct CloneTableBody {
    /// The name of the clone, `namespace.table`. The namespace must exist.
    #[garde(custom(table_name))]
    pub name: String,
    /// The ref to clone from, a branch, a tag or a commit hash.
    #[garde(skip)]
    pub from_ref: String,
    /// The table to clone, `namespace.table`, as it is at `from_ref`.
    #[garde(skip)]
    pub from_table: String,
    /// Commit options for the commit this creates.
    #[serde(default)]
    #[garde(dive)]
    pub commit: CommitOptions,
}

/// Request body for renaming a table.
#[derive(Debug, Serialize, Deserialize, JsonSchema, garde::Validate)]
pub struct RenameTableBody {
    /// The new name, `namespace.table`. The namespace must exist.
    #[garde(custom(table_name))]
    pub name: String,
    /// Commit options for the commit this creates.
    #[serde(default)]
    #[garde(dive)]
    pub commit: CommitOptions,
}

/// Request body for deleting a table.
#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, garde::Validate)]
pub struct DeleteTableBody {
    /// Commit options for the commit this creates.
    #[serde(default)]
    #[garde(dive)]
    pub commit: CommitOptions,
}

/// API error returned by `list_tables`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum ListTablesError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Ref '{ref_name}' not found.
    #[api_error(404)]
    RefNotFound { ref_name: String },
    /// A generic error.
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `create_table`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum CreateTableError {
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
    /// Name '{name}' is already in use.
    #[api_error(409)]
    NameConflict { name: String },
    /// Branch '{branch_name}' head has moved to '{commit_hash}'.
    #[api_error(409)]
    BranchHeadChanged {
        branch_name: String,
        commit_hash: String,
    },
    /// Not implemented.
    #[api_error(501)]
    NotImplemented,
    /// A generic error.
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `clone_table`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum CloneTableError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Ref '{ref_name}' not found.
    #[api_error(404)]
    RefNotFound { ref_name: String },
    /// Ref '{ref_name}' is not a branch.
    #[api_error(400)]
    NotABranch { ref_name: String },
    /// Source ref '{from_ref}' not found.
    #[api_error(404)]
    FromRefNotFound { from_ref: String },
    /// Source table '{from_table}' not found at '{from_ref}'.
    #[api_error(404)]
    FromTableNotFound {
        from_ref: String,
        from_table: String,
    },
    /// Table '{from_table}' cannot be cloned.
    #[api_error(400)]
    UnsupportedTableKind { from_table: String },
    /// Namespace '{namespace_name}' not found.
    #[api_error(404)]
    NamespaceNotFound { namespace_name: String },
    /// Name '{name}' is already in use.
    #[api_error(409)]
    NameConflict { name: String },
    /// Branch '{branch_name}' head has moved to '{commit_hash}'.
    #[api_error(409)]
    BranchHeadChanged {
        branch_name: String,
        commit_hash: String,
    },
    /// Not implemented.
    #[api_error(501)]
    NotImplemented,
    /// A generic error.
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `get_table`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum GetTableError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Ref '{ref_name}' not found.
    #[api_error(404)]
    RefNotFound { ref_name: String },
    /// Table '{table_name}' not found.
    #[api_error(404)]
    TableNotFound { table_name: String },
    /// A generic error.
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `rename_table`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum RenameTableError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Ref '{ref_name}' not found.
    #[api_error(404)]
    RefNotFound { ref_name: String },
    /// Ref '{ref_name}' is not a branch.
    #[api_error(400)]
    NotABranch { ref_name: String },
    /// Table '{table_name}' not found.
    #[api_error(404)]
    TableNotFound { table_name: String },
    /// Name '{name}' is already in use.
    #[api_error(409)]
    NameConflict { name: String },
    /// Namespace '{namespace_name}' not found.
    #[api_error(404)]
    NamespaceNotFound { namespace_name: String },
    /// Branch '{branch_name}' head has moved to '{commit_hash}'.
    #[api_error(409)]
    BranchHeadChanged {
        branch_name: String,
        commit_hash: String,
    },
    /// A generic error.
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// API error returned by `delete_table`.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum DeleteTableError {
    /// Catalog '{catalog_name}' not found.
    #[api_error(404)]
    CatalogNotFound { catalog_name: String },
    /// Ref '{ref_name}' not found.
    #[api_error(404)]
    RefNotFound { ref_name: String },
    /// Ref '{ref_name}' is not a branch.
    #[api_error(400)]
    NotABranch { ref_name: String },
    /// Table '{table_name}' not found.
    #[api_error(404)]
    TableNotFound { table_name: String },
    /// Branch '{branch_name}' head has moved to '{commit_hash}'.
    #[api_error(409)]
    BranchHeadChanged {
        branch_name: String,
        commit_hash: String,
    },
    /// A generic error.
    #[api_error(transparent)]
    Generic(#[from] GenericError),
}

/// Table operations.
#[gropius::api(tags = ["tables"], client(cfg(feature = "client")))]
pub trait TableApi {
    /// List tables at a ref.
    #[endpoint(GET, "/v1/catalogs/{catalog}/refs/{ref}/tables")]
    async fn list_tables(
        &self,
        path: Path<(String, String)>,
        query: Query<ListQuery>,
        req: Request,
    ) -> Result<AtCommit<Page<TableIdentifier>>, ListTablesError>;

    /// Create an empty table.
    ///
    /// The name is namespace-qualified (`namespace.table`), and the
    /// namespace must exist.
    ///
    /// ## Commit
    ///
    /// Produces a commit on the target branch; `commit.expected_commit_hash`
    /// enforces optimistic locking.
    #[endpoint(POST, "/v1/catalogs/{catalog}/refs/{ref}/tables")]
    async fn create_table(
        &self,
        path: Path<(String, String)>,
        body: Body<CreateTableBody>,
        req: Request,
    ) -> Result<WithCommit<Table>, CreateTableError>;

    /// Clone a table.
    ///
    /// Creates `name` with the content `from_table` has at `from_ref`, without
    /// copying any data. From then on the two tables are independent.
    ///
    /// ## Commit
    ///
    /// Produces a commit on the target branch; `commit.expected_commit_hash`
    /// enforces optimistic locking.
    #[endpoint(POST, "/v1/catalogs/{catalog}/refs/{ref}/tables/clone")]
    async fn clone_table(
        &self,
        path: Path<(String, String)>,
        body: Body<CloneTableBody>,
        req: Request,
    ) -> Result<WithCommit<Table>, CloneTableError>;

    /// Get a table at a ref.
    ///
    /// `table` is namespace-qualified (`namespace.table`).
    #[endpoint(GET, "/v1/catalogs/{catalog}/refs/{ref}/tables/{table}")]
    async fn get_table(
        &self,
        path: Path<(String, String, String)>,
        req: Request,
    ) -> Result<WithCommit<Table>, GetTableError>;

    /// Rename a table.
    ///
    /// `table` and the new name are namespace-qualified
    /// (`namespace.table`), and the destination namespace must exist.
    ///
    /// ## Commit
    ///
    /// Produces a commit on the target branch, except for a no-op;
    /// `commit.expected_commit_hash` enforces optimistic locking.
    #[endpoint(POST, "/v1/catalogs/{catalog}/refs/{ref}/tables/{table}/rename")]
    async fn rename_table(
        &self,
        path: Path<(String, String, String)>,
        body: Body<RenameTableBody>,
        req: Request,
    ) -> Result<WithCommit<Table>, RenameTableError>;

    /// Delete a table.
    ///
    /// `table` is namespace-qualified (`namespace.table`).
    ///
    /// ## Commit
    ///
    /// Produces a commit on the target branch; `commit.expected_commit_hash`
    /// enforces optimistic locking.
    #[endpoint(DELETE, "/v1/catalogs/{catalog}/refs/{ref}/tables/{table}")]
    async fn delete_table(
        &self,
        path: Path<(String, String, String)>,
        body: Option<Body<DeleteTableBody>>,
        req: Request,
    ) -> Result<Commit, DeleteTableError>;
}
