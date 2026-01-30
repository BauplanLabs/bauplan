use std::str::FromStr as _;

use serde::Deserialize;

/// An error response from the API.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// The API responded with an application-level error code.
    ErrorResponse {
        /// The HTTP status on the overall response.
        status: http::StatusCode,
        /// The error code from the API.
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

/// Indicates that the error code was unrecognized.
#[derive(Debug, Clone, thiserror::Error)]
#[error("Invalid error kind: {0}")]
pub struct InvalidErrorKind(String);

macro_rules! api_error_kinds {
    ($($code:literal => $variant:ident),* $(,)?) => {
        /// An error code from the API.
        #[derive(Debug, Clone, PartialEq, Eq)]
        #[non_exhaustive]
        pub enum ApiErrorKind {
            $(
                #[doc = $code]
                $variant,
            )*
            /// An unknown error code.
            Unknown(String),
        }

        impl std::fmt::Display for ApiErrorKind {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(match self {
                    $(ApiErrorKind::$variant => $code,)*
                    ApiErrorKind::Unknown(kind) => kind,
                })
            }
        }

        impl std::str::FromStr for ApiErrorKind {
            type Err = InvalidErrorKind;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(match s {
                    $($code => ApiErrorKind::$variant,)*
                    _ => return Err(InvalidErrorKind(s.to_string())),
                })
            }
        }
    };
}

api_error_kinds! {
    "BRANCH_EXISTS" => BranchExists,
    "BRANCH_HEAD_CHANGED" => BranchHeadChanged,
    "BRANCH_NOT_FOUND" => BranchNotFound,
    "CREATE_BRANCH_FORBIDDEN" => CreateBranchForbidden,
    "CREATE_NAMESPACE_FORBIDDEN" => CreateNamespaceForbidden,
    "CREATE_TAG_FORBIDDEN" => CreateTagForbidden,
    "DELETE_BRANCH_FORBIDDEN" => DeleteBranchForbidden,
    "DELETE_NAMESPACE_FORBIDDEN" => DeleteNamespaceForbidden,
    "DELETE_TABLE_FORBIDDEN" => DeleteTableForbidden,
    "DELETE_TAG_FORBIDDEN" => DeleteTagForbidden,
    "INVALID_REF" => InvalidRef,
    "MERGE_CONFLICT" => MergeConflict,
    "MERGE_FORBIDDEN" => MergeForbidden,
    "NAMESPACE_UNRESOLVED" => NamespaceUnresolved,
    "NAMESPACE_EXISTS" => NamespaceExists,
    "NAMESPACE_IS_NOT_EMPTY" => NamespaceIsNotEmpty,
    "NAMESPACE_NOT_FOUND" => NamespaceNotFound,
    "NOT_A_BRANCH_REF" => NotABranchRef,
    "NOT_A_TAG_REF" => NotATagRef,
    "NOT_A_WRITE_BRANCH_REF" => NotAWriteBranchRef,
    "REF_NOT_FOUND" => RefNotFound,
    "RENAME_BRANCH_FORBIDDEN" => RenameBranchForbidden,
    "RENAME_TAG_FORBIDDEN" => RenameTagForbidden,
    "REVERT_DESTINATION_TABLE_EXISTS" => RevertDestinationTableExists,
    "REVERT_IDENTICAL_TABLE" => RevertIdenticalTable,
    "REVERT_TABLE_FORBIDDEN" => RevertTableForbidden,
    "SAME_REF" => SameRef,
    "TABLE_NOT_FOUND" => TableNotFound,
    "TAG_EXISTS" => TagExists,
    "TAG_NOT_FOUND" => TagNotFound,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RawApiError {
    r#type: String,
    message: Option<String>,
}

impl ApiError {
    pub(crate) fn from_raw(status: http::StatusCode, raw: RawApiError) -> Self {
        let kind = ApiErrorKind::from_str(&raw.r#type).unwrap_or(ApiErrorKind::Unknown(raw.r#type));

        ApiError::ErrorResponse {
            status,
            kind,
            message: raw.message,
        }
    }
}
