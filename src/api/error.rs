use serde::Deserialize;

use crate::CatalogRef;

/// A typed API error kind, deserialized from the `type` and `context` fields
/// of an error response.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", content = "context", rename_all = "SCREAMING_SNAKE_CASE")]
#[cfg_attr(feature = "python", pyo3::pyclass(module = "bauplan.exceptions"))]
pub enum ApiErrorKind {
    // 400
    InvalidRef {
        input_ref: String,
    },
    NotABranchRef {
        input_ref: String,
    },
    NotATagRef {
        input_ref: String,
    },
    NotAWriteBranchRef {
        input_ref: String,
    },
    SameRef {
        input_ref: CatalogRef,
        #[serde(rename = "ref")]
        catalog_ref: CatalogRef,
    },

    // 403
    CreateBranchForbidden {
        branch_name: String,
    },
    CreateNamespaceForbidden {
        namespace_name: String,
    },
    CreateTagForbidden {
        tag_name: String,
    },
    DeleteBranchForbidden {
        branch_name: String,
    },
    DeleteNamespaceForbidden {
        namespace_name: String,
    },
    DeleteTableForbidden {
        table_name: String,
    },
    DeleteTagForbidden {
        tag_name: String,
    },
    MergeForbidden {
        source_ref: String,
        destination_branch: String,
    },
    RenameBranchForbidden {
        old_branch_name: String,
        new_branch_name: String,
    },
    RenameTagForbidden {
        old_tag_name: String,
        new_tag_name: String,
    },
    RevertTableForbidden {
        table_name: String,
        source_ref: String,
        destination_branch: String,
    },

    // 404
    BranchNotFound {
        branch_name: String,
    },
    NamespaceNotFound {
        namespace_name: String,
        input_ref: String,
        #[serde(rename = "ref")]
        catalog_ref: CatalogRef,
    },
    RefNotFound {
        input_ref: String,
    },
    TableNotFound {
        table_name: String,
        input_ref: String,
        #[serde(rename = "ref")]
        catalog_ref: CatalogRef,
    },
    TagNotFound {
        tag_name: String,
    },

    // 409
    BranchExists {
        branch_name: String,
        #[serde(rename = "ref")]
        catalog_ref: CatalogRef,
    },
    BranchHeadChanged {
        input_ref: CatalogRef,
        head_ref: CatalogRef,
    },
    MergeConflict {
        source_ref: CatalogRef,
        destination_ref: CatalogRef,
    },
    NamespaceExists {
        namespace_name: String,
        #[serde(rename = "ref")]
        catalog_ref: CatalogRef,
    },
    NamespaceIsNotEmpty {
        namespace_name: String,
        branch_name: String,
    },
    NamespaceUnresolved {
        table_name: String,
        namespace_name: String,
    },
    RevertDestinationTableExists {
        source_table_name: String,
        destination_table_name: String,
    },
    RevertIdenticalTable {
        source_table_name: String,
        destination_table_name: String,
    },
    TagExists {
        tag_name: String,
        #[serde(rename = "ref")]
        catalog_ref: CatalogRef,
    },
}

impl std::fmt::Display for ApiErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::BranchExists { .. } => "BRANCH_EXISTS",
            Self::BranchHeadChanged { .. } => "BRANCH_HEAD_CHANGED",
            Self::BranchNotFound { .. } => "BRANCH_NOT_FOUND",
            Self::CreateBranchForbidden { .. } => "CREATE_BRANCH_FORBIDDEN",
            Self::CreateNamespaceForbidden { .. } => "CREATE_NAMESPACE_FORBIDDEN",
            Self::CreateTagForbidden { .. } => "CREATE_TAG_FORBIDDEN",
            Self::DeleteBranchForbidden { .. } => "DELETE_BRANCH_FORBIDDEN",
            Self::DeleteNamespaceForbidden { .. } => "DELETE_NAMESPACE_FORBIDDEN",
            Self::DeleteTableForbidden { .. } => "DELETE_TABLE_FORBIDDEN",
            Self::DeleteTagForbidden { .. } => "DELETE_TAG_FORBIDDEN",
            Self::InvalidRef { .. } => "INVALID_REF",
            Self::MergeConflict { .. } => "MERGE_CONFLICT",
            Self::MergeForbidden { .. } => "MERGE_FORBIDDEN",
            Self::NamespaceExists { .. } => "NAMESPACE_EXISTS",
            Self::NamespaceIsNotEmpty { .. } => "NAMESPACE_IS_NOT_EMPTY",
            Self::NamespaceNotFound { .. } => "NAMESPACE_NOT_FOUND",
            Self::NamespaceUnresolved { .. } => "NAMESPACE_UNRESOLVED",
            Self::NotABranchRef { .. } => "NOT_A_BRANCH_REF",
            Self::NotATagRef { .. } => "NOT_A_TAG_REF",
            Self::NotAWriteBranchRef { .. } => "NOT_A_WRITE_BRANCH_REF",
            Self::RefNotFound { .. } => "REF_NOT_FOUND",
            Self::RenameBranchForbidden { .. } => "RENAME_BRANCH_FORBIDDEN",
            Self::RenameTagForbidden { .. } => "RENAME_TAG_FORBIDDEN",
            Self::RevertDestinationTableExists { .. } => "REVERT_DESTINATION_TABLE_EXISTS",
            Self::RevertIdenticalTable { .. } => "REVERT_IDENTICAL_TABLE",
            Self::RevertTableForbidden { .. } => "REVERT_TABLE_FORBIDDEN",
            Self::SameRef { .. } => "SAME_REF",
            Self::TableNotFound { .. } => "TABLE_NOT_FOUND",
            Self::TagExists { .. } => "TAG_EXISTS",
            Self::TagNotFound { .. } => "TAG_NOT_FOUND",
        };

        f.write_str(s)
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct RawApiError {
    pub message: Option<String>,
    #[serde(flatten)]
    pub kind: ApiErrorKind,
}

/// An error response from the API.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// The API responded with an application-level error code.
    ErrorResponse {
        /// The HTTP status on the overall response.
        status: http::StatusCode,
        /// The error code from the API, along with any context provided.
        kind: ApiErrorKind,
        /// A longer description of the error encountered.
        message: Option<String>,
    },
    /// The API response did not contain a code, but the HTTP status was non-200.
    Other(http::StatusCode),
    /// The API response was invalid.
    InvalidResponse(http::StatusCode),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::ErrorResponse { kind, message, .. } => {
                write!(f, "{kind}")?;
                if let Some(message) = &message {
                    write!(f, ": {message}")?;
                }
            }
            ApiError::Other(status) => {
                write!(f, "{status}")?;
            }
            ApiError::InvalidResponse(status) => {
                write!(f, "Invalid response ({status})")?;
            }
        }

        Ok(())
    }
}

impl ApiError {
    pub(crate) fn from_raw(status: http::StatusCode, raw: RawApiError) -> Self {
        ApiError::ErrorResponse {
            status,
            kind: raw.kind,
            message: raw.message,
        }
    }

    /// The HTTP status code of the response.
    pub fn status(&self) -> http::StatusCode {
        match self {
            ApiError::ErrorResponse { status, .. }
            | ApiError::Other(status)
            | ApiError::InvalidResponse(status) => *status,
        }
    }

    /// Extract server error context, if any is available.
    pub fn kind(&self) -> Option<&ApiErrorKind> {
        match self {
            ApiError::ErrorResponse { kind, .. } => Some(kind),
            _ => None,
        }
    }
}
