//! API operations concerning tables in the lake.

use std::time;

use http::uri::{InvalidUri, PathAndQuery};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    CatalogRef,
    api::{ApiRequest, DataResponse, commit::CommitOptions},
};

/// Load the schema and other metadata for a table.
#[derive(Debug, Clone)]
pub struct GetTable<'a> {
    /// The name of the table to fetch information for. Can be with or without
    /// an explicit namespace ('taxi_fhvhv' or 'bauplan.taxi_fhvhv').
    pub name: &'a str,

    /// The ref (branch, tag, etc) at which to read the table. Defaults to
    /// `main`.
    pub at_ref: Option<&'a str>,

    /// The namespace to search for the table. If specified, the table name
    /// should not include a namespace.
    pub namespace: Option<&'a str>,
}

/// A field in a table schema.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TableField {
    /// The field ID.
    pub id: i32,
    /// The field name.
    pub name: String,
    /// Whether the field is required.
    pub required: bool,
    /// The field type.
    pub r#type: String,
}

/// A table in the lake.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TableWithMetadata {
    /// The table ID.
    pub id: Uuid,
    /// The table name.
    pub name: String,
    /// The table namespace.
    pub namespace: String,
    /// The number of records in the table.
    pub records: Option<u64>,
    /// The size of the table.
    pub size: Option<u64>,
    /// The timestamp when the table was last updated.
    #[serde(rename = "last_updated_ms", deserialize_with = "deserialize_epoch_ms")]
    pub last_updated_at: time::SystemTime,
    /// The fields in the table schema.
    pub fields: Vec<TableField>,
    /// The number of snapshots.
    pub snapshots: Option<u32>,
}

impl ApiRequest for GetTable<'_> {
    type Response = TableWithMetadata;

    fn path_and_query(&self) -> Result<PathAndQuery, InvalidUri> {
        let Self {
            name,
            at_ref,
            namespace,
        } = &self;
        let at_ref = at_ref.as_deref().unwrap_or("main");
        if let Some(namespace) = namespace {
            format!("/catalog/v0/refs/{at_ref}/tables/{name}?namespace={namespace}").parse()
        } else {
            format!("/catalog/v0/refs/{at_ref}/tables/{name}").parse()
        }
    }
}

impl DataResponse for TableWithMetadata {}

/// Revert a table to a previous ref.
#[derive(Debug, Clone)]
pub struct RevertTable<'a> {
    /// The name of the table to copy. Can be with or without an explicit
    /// namespace ('taxi_fhvhv' or 'bauplan.taxi_fhvhv').
    pub name: &'a str,

    /// The source ref to "read" the table state from.
    pub source_ref: &'a str,

    /// The branch to commit to.
    pub into_branch: &'a str,

    /// If set, overwrite the table in the destination branch.
    pub replace: bool,

    /// The namespace that the table is in. If specified, the table name
    /// should not include a namespace.
    pub namespace: Option<&'a str>,

    /// Override the commit body or add custom properties.
    pub commit: CommitOptions<'a>,
}

#[derive(Debug, Clone, Serialize)]
struct RevertTableBody<'a> {
    replace: bool,
    #[serde(flatten)]
    commit: CommitOptions<'a>,
}

impl ApiRequest for RevertTable<'_> {
    type Response = CatalogRef;

    fn method(&self) -> http::Method {
        http::Method::POST
    }

    fn path_and_query(&self) -> Result<PathAndQuery, InvalidUri> {
        let Self {
            name,
            source_ref,
            into_branch,
            ..
        } = self;

        format!("/catalog/v0/refs/{source_ref}/tables/{name}/revert/{into_branch}").parse()
    }

    fn into_body(self) -> Option<impl Serialize> {
        Some(RevertTableBody {
            replace: self.replace,
            commit: self.commit,
        })
    }
}

fn deserialize_epoch_ms<'de, D>(deserializer: D) -> Result<time::SystemTime, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let millis: u64 = Deserialize::deserialize(deserializer)?;
    Ok(time::UNIX_EPOCH + time::Duration::from_millis(millis))
}
