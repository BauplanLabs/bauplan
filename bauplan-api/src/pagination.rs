//! Keyset pagination primitives.
//!
//! A list endpoint takes `pagination_token` and `limit` in its query type and
//! returns [`Page<T>`]. Filters are sent with every request; the opaque token
//! only carries the resume position.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Query for a list endpoint: the cursor, the page size and the
/// endpoint-specific filters.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, garde::Validate)]
pub struct ListQuery<T = ()> {
    /// Opaque cursor from the previous page.
    #[serde(default)]
    #[garde(skip)]
    pub pagination_token: Option<String>,
    /// The maximum number of items to return.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(range(min = 1))]
    pub limit: Option<u32>,
    /// The endpoint-specific filters.
    #[serde(flatten)]
    #[garde(skip)]
    pub filters: T,
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
