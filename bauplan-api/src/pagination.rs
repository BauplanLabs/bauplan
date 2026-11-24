//! Pagination of the list endpoints.
//!
//! A list endpoint takes `pagination_token` and `limit` in its query type and
//! returns [`Page<T>`]. Filters are sent with every request; the opaque token
//! only carries the resume position. A list at a ref also returns its
//! `commit_hash`. Pass it as the ref of the next pages to read the same state.

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};

/// Query for a list endpoint: the cursor, the page size and the
/// endpoint-specific filters.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, garde::Validate)]
pub struct ListQuery<T = ()> {
    /// Opaque cursor from the previous page.
    #[serde(default)]
    #[garde(skip)]
    pub pagination_token: Option<String>,
    /// The maximum number of items in a page. The server may return fewer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(range(min = 1))]
    pub limit: Option<u32>,
    /// The endpoint-specific filters.
    #[serde(flatten)]
    #[garde(skip)]
    pub filters: T,
}

/// Reads a boolean filter of a list query.
///
/// The filters are flattened into the query, and serde reads a flattened
/// value as text.
pub(crate) fn query_bool<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        Bool(bool),
        Text(String),
    }

    match Value::deserialize(deserializer)? {
        Value::Bool(value) => Ok(value),
        Value::Text(text) => text.parse().map_err(de::Error::custom),
    }
}

/// One page of results plus the cursor for the next page.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "{T}Page")]
pub struct Page<T> {
    /// The items in this page.
    pub items: Vec<T>,
    /// Opaque cursor for the next page, absent when there are no more results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination_token: Option<String>,
}
