//! API operations concerning tables in the lake.

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    CatalogRef, PaginatedResponse,
    api::{ApiRequest, DataResponse, commit::CommitOptions},
};

/// A field in a table schema.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(name = "TableField", module = "bauplan", get_all)
)]
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

/// The kind of table entry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[cfg_attr(feature = "python", pyo3::pyclass(eq, eq_int, module = "bauplan"))]
pub enum TableKind {
    /// A managed table.
    #[default]
    Table,
    /// An external table.
    ExternalTable,
}

impl std::fmt::Display for TableKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableKind::Table => write!(f, "TABLE"),
            TableKind::ExternalTable => write!(f, "EXTERNAL_TABLE"),
        }
    }
}

/// A table in the lake.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(name = "Table", module = "bauplan", get_all)
)]
pub struct Table {
    /// The table ID.
    pub id: Uuid,
    /// The table name.
    pub name: String,
    /// The table namespace.
    pub namespace: String,
    /// The table type.
    #[serde(default)]
    pub kind: TableKind,
    /// The number of records in the table.
    pub records: Option<u64>,
    /// The size of the table.
    pub size: Option<u64>,
    /// The timestamp when the table was last updated.
    #[serde(alias = "last_updated_ms", deserialize_with = "deserialize_epoch_ms")]
    pub last_updated_at: DateTime<Utc>,
    /// The fields in the table schema.
    pub fields: Vec<TableField>,
    /// The number of snapshots.
    pub snapshots: Option<u32>,
}

/// Load the schema and other metadata for a table.
#[derive(Debug, Clone)]
pub struct GetTable<'a> {
    /// The name of the table to fetch information for. Can be with or without
    /// an explicit namespace ('taxi_fhvhv' or 'bauplan.taxi_fhvhv').
    pub name: &'a str,

    /// The ref (branch, tag, etc) at which to read the table. Defaults to
    /// `main`.
    pub at_ref: &'a str,

    /// The namespace to search for the table. If specified, the table name
    /// should not include a namespace.
    pub namespace: Option<&'a str>,
}

#[derive(Serialize)]
struct GetTableQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<&'a str>,
}

impl ApiRequest for GetTable<'_> {
    type Response = Table;

    fn path(&self) -> String {
        format!("/catalog/v0/refs/{}/tables/{}", self.at_ref, self.name)
    }

    fn query(&self) -> Option<impl Serialize> {
        Some(GetTableQuery {
            namespace: self.namespace,
        })
    }
}

impl DataResponse for Table {}

/// List tables in a ref.
#[derive(Debug, Clone)]
pub struct GetTables<'a> {
    /// The ref (branch, tag, etc) at which to list tables. Defaults to `main`.
    pub at_ref: &'a str,

    /// Filter tables by name pattern.
    pub filter_by_name: Option<&'a str>,

    /// Filter tables by namespace.
    pub filter_by_namespace: Option<&'a str>,
}

#[derive(Serialize)]
struct GetTablesQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_by_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_by_namespace: Option<&'a str>,
}

impl ApiRequest for GetTables<'_> {
    type Response = PaginatedResponse<Table>;

    fn path(&self) -> String {
        format!("/catalog/v0/refs/{}/tables", self.at_ref)
    }

    fn query(&self) -> Option<impl Serialize> {
        Some(GetTablesQuery {
            filter_by_name: self.filter_by_name,
            filter_by_namespace: self.filter_by_namespace,
        })
    }
}

/// Delete a table from a branch.
#[derive(Debug, Clone)]
pub struct DeleteTable<'a> {
    /// The name of the table to delete. Can be with or without an explicit
    /// namespace ('taxi_fhvhv' or 'bauplan.taxi_fhvhv').
    pub name: &'a str,

    /// The branch to delete the table from.
    pub branch: &'a str,

    /// The namespace that the table is in. If specified, the table name
    /// should not include a namespace.
    pub namespace: Option<&'a str>,

    /// Override the commit body or add custom properties.
    pub commit: CommitOptions<'a>,
}

#[derive(Serialize)]
struct DeleteTableQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<&'a str>,
}

#[derive(Debug, Clone, Serialize)]
struct DeleteTableBody<'a> {
    #[serde(flatten)]
    commit: CommitOptions<'a>,
}

impl ApiRequest for DeleteTable<'_> {
    type Response = CatalogRef;

    fn method(&self) -> http::Method {
        http::Method::DELETE
    }

    fn path(&self) -> String {
        let DeleteTable { branch, name, .. } = self;
        format!("/catalog/v0/branches/{branch}/tables/{name}")
    }

    fn query(&self) -> Option<impl Serialize> {
        Some(DeleteTableQuery {
            namespace: self.namespace,
        })
    }

    fn body(&self) -> Option<impl Serialize> {
        Some(DeleteTableBody {
            commit: self.commit.clone(),
        })
    }
}

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

    fn path(&self) -> String {
        let Self {
            name,
            source_ref,
            into_branch,
            ..
        } = self;

        format!("/catalog/v0/refs/{source_ref}/tables/{name}/revert/{into_branch}")
    }

    fn body(&self) -> Option<impl Serialize> {
        Some(RevertTableBody {
            replace: self.replace,
            commit: self.commit.clone(),
        })
    }
}

fn deserialize_epoch_ms<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let millis: i64 = Deserialize::deserialize(deserializer)?;
    Utc.timestamp_millis_opt(millis)
        .single()
        .ok_or_else(|| serde::de::Error::custom("invalid timestamp"))
}

#[cfg(all(test, feature = "_integration_tests"))]
mod test {
    use assert_matches::assert_matches;

    use super::*;
    use crate::{ApiError, ApiErrorKind, api::testutil::roundtrip};

    #[test]
    fn get_table() -> anyhow::Result<()> {
        let req = GetTable {
            name: "titanic",
            at_ref: "main",
            namespace: Some("bauplan"),
        };

        let table: Table = roundtrip(req)?;

        assert_eq!(table.name, "titanic");
        assert_eq!(table.namespace, "bauplan");
        assert!(table.records.unwrap_or(0) > 0);
        assert!(!table.fields.is_empty());

        Ok(())
    }

    #[test]
    fn get_table_namespace_included() -> anyhow::Result<()> {
        let req = GetTable {
            name: "bauplan.titanic",
            at_ref: "main",
            namespace: None,
        };

        let table: Table = roundtrip(req)?;

        assert_eq!(table.name, "titanic");
        assert_eq!(table.namespace, "bauplan");
        assert!(table.records.unwrap_or(0) > 0);
        assert!(!table.fields.is_empty());

        Ok(())
    }

    #[test]
    fn get_table_not_found() -> anyhow::Result<()> {
        let req = GetTable {
            name: "nonexistent_table_12345",
            at_ref: "main",
            namespace: Some("bauplan"),
        };

        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::TableNotFound,
                ..
            })
        );

        Ok(())
    }

    #[test]
    fn get_table_ref_not_found() -> anyhow::Result<()> {
        let req = GetTable {
            name: "titanic",
            at_ref: "nonexistent_branch_12345",
            namespace: Some("bauplan"),
        };

        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::RefNotFound,
                ..
            })
        );

        Ok(())
    }

    #[test]
    fn get_tables() -> anyhow::Result<()> {
        let req = GetTables {
            at_ref: "main",
            filter_by_name: None,
            filter_by_namespace: Some("bauplan"),
        };

        let tables = crate::paginate(req, None, |r| roundtrip(r))?
            .collect::<Result<Vec<Table>, ApiError>>()?;

        let titanic = tables.iter().find(|t| t.name == "titanic");
        assert!(titanic.is_some());

        Ok(())
    }

    #[test]
    fn get_tables_limit() -> anyhow::Result<()> {
        let req = GetTables {
            at_ref: "main",
            filter_by_name: None,
            filter_by_namespace: Some("bauplan"),
        };

        let tables = crate::paginate(req, Some(7), |r| roundtrip(r))?
            .collect::<Result<Vec<Table>, ApiError>>()?;

        assert_eq!(tables.len(), 7);

        Ok(())
    }

    #[test]
    fn get_tables_with_filter() -> anyhow::Result<()> {
        let req = GetTables {
            at_ref: "main",
            filter_by_name: Some("titanic"),
            filter_by_namespace: Some("bauplan"),
        };

        let tables = crate::paginate(req, Some(7), |r| roundtrip(r))?
            .collect::<Result<Vec<Table>, ApiError>>()?;
        assert!(!tables.is_empty());
        assert!(tables.iter().all(|t| t.name.contains("titanic")));

        Ok(())
    }

    #[test]
    fn get_tables_ref_not_found() -> anyhow::Result<()> {
        let req = GetTables {
            at_ref: "nonexistent_branch_12345",
            filter_by_name: None,
            filter_by_namespace: None,
        };

        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::RefNotFound,
                ..
            })
        );

        Ok(())
    }

    #[test]
    fn delete_table() -> anyhow::Result<()> {
        use crate::api::testutil::TestBranch;

        let branch = TestBranch::new("test_table_delete")?;

        // The branch is a copy of main, so it already has the titanic table.
        let req = GetTable {
            name: "titanic",
            at_ref: &branch.name,
            namespace: Some("bauplan"),
        };
        let table = roundtrip(req)?;
        assert_eq!(table.name, "titanic");

        // Delete it.
        let req = DeleteTable {
            name: "titanic",
            branch: &branch.name,
            namespace: Some("bauplan"),
            commit: Default::default(),
        };
        roundtrip(req)?;

        // Verify it's gone.
        let req = GetTable {
            name: "titanic",
            at_ref: &branch.name,
            namespace: Some("bauplan"),
        };
        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::TableNotFound,
                ..
            })
        );

        Ok(())
    }
}
