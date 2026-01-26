use std::{fmt::Display, io::Read};

use serde::{Deserialize, Serialize};

use crate::Profile;

pub mod branch;
pub mod commit;
mod error;
pub mod namespace;
mod paginate;
pub mod table;
pub mod tag;

#[cfg(all(test, feature = "_integration_tests"))]
pub(crate) mod testutil;

pub use error::*;
pub use paginate::*;

/// A ref returned by the API.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CatalogRef {
    /// A branch.
    Branch {
        /// The branch name.
        name: String,
        /// The commit hash.
        hash: String,
    },
    /// A tag.
    Tag {
        /// The tag name.
        name: String,
        /// The commit hash.
        hash: String,
    },
    /// A detached ref (a specific commit, not on any branch).
    Detached {
        /// The commit hash.
        hash: String,
    },
}

impl Display for CatalogRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogRef::Branch { name, hash } | CatalogRef::Tag { name, hash } => {
                write!(f, "{}@{}", name, hash)
            }
            CatalogRef::Detached { hash } => write!(f, "@{}", hash),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawMetadata {
    pagination_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawApiResponse<T> {
    Error {
        error: RawApiError,
    },
    Data {
        data: T,
        #[serde(default)]
        r#ref: Option<CatalogRef>,
        metadata: RawMetadata,
    },
}

/// Implemented by types that can be sent as requests to the Bauplan API.
pub trait ApiRequest: Sized {
    /// The corresponding response type.
    type Response: ApiResponse;

    /// The path that the request should take.
    fn path(&self) -> String;

    /// The method to use.
    fn method(&self) -> http::Method {
        http::Method::GET
    }

    /// The serializable request body.
    fn body(&self) -> Option<impl Serialize> {
        None::<&()>
    }

    /// The serializable query string.
    fn query(&self) -> Option<impl Serialize> {
        None::<&()>
    }

    /// Consume the request and return an [http::Request] suitable for passing
    /// to your favorite HTTP client.
    fn into_request(self, profile: &Profile) -> Result<http::Request<String>, http::Error> {
        let method = self.method();
        let mut path = self.path();
        let mut parts = profile.api_endpoint.clone().into_parts();

        if let Some(qs) = self.query() {
            path.push('?');

            // SAFETY: query strings should only be valid UTF-8.
            unsafe {
                serde_qs::to_writer(&qs, &mut path.as_mut_vec())
                    .expect("query string serialization should be infallible");
            }
        }

        parts.path_and_query = Some(path.parse()?);

        let uri = http::Uri::from_parts(parts).unwrap();
        let req = http::Request::builder()
            .method(method)
            .uri(uri)
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer {}", profile.api_key),
            )
            .header(http::header::USER_AGENT, &profile.user_agent);

        if let Some(body) = self.body() {
            let body_str =
                serde_json::to_string(&body).expect("JSON serialization should be infallible");
            req.header(http::header::CONTENT_TYPE, "application/json")
                .header(http::header::CONTENT_LENGTH, body_str.len())
                .body(body_str)
        } else {
            req.body("".to_string())
        }
    }

    /// Add a pagination token to the request.
    fn paginate(
        self,
        pagination_token: Option<&str>,
        limit: Option<usize>,
    ) -> PaginatedRequest<'_, Self> {
        PaginatedRequest {
            base: self,
            pagination_token,
            limit,
        }
    }
}

/// Implemented by types that can be read as responses from the Bauplan API.
pub trait ApiResponse: Sized {
    /// Read the response from an [http::Response] object.
    fn from_response(resp: http::Response<impl Read>) -> Result<Self, ApiError> {
        let (parts, body) = resp.into_parts();
        Self::from_response_parts(parts, body)
    }

    /// Read the response from pre-parsed parts. Useful for async HTTP clients
    /// where the body must be collected before parsing.
    fn from_response_parts(parts: http::response::Parts, body: impl Read)
    -> Result<Self, ApiError>;
}

/// A private trait for types that deserialize json from the `data` field of
/// the generic response.
pub(crate) trait DataResponse: for<'de> Deserialize<'de> {}

impl<T: DataResponse> ApiResponse for T {
    fn from_response_parts(
        parts: http::response::Parts,
        body: impl Read,
    ) -> Result<Self, ApiError> {
        let raw: RawApiResponse<Self> = serde_json::from_reader(body).map_err(|e| {
            tracing::error!("Failed to parse API response: {e:#?}");
            ApiError::InvalidResponse(parts.status)
        })?;

        match raw {
            RawApiResponse::Data { data, .. } => Ok(data),
            RawApiResponse::Error { error } => Err(ApiError::from_raw(parts.status, error)),
        }
    }
}

// For API methods that just return a ref and no data.
impl ApiResponse for CatalogRef {
    fn from_response_parts(
        parts: http::response::Parts,
        body: impl Read,
    ) -> Result<Self, ApiError> {
        let raw: RawApiResponse<serde_json::Value> =
            serde_json::from_reader(body).map_err(|e| {
                tracing::error!("Failed to parse API response: {e:#?}");
                ApiError::InvalidResponse(parts.status)
            })?;

        match raw {
            RawApiResponse::Data { r#ref: Some(r), .. } => Ok(r),
            RawApiResponse::Data { r#ref: None, .. } => {
                Err(ApiError::InvalidResponse(parts.status))
            }
            RawApiResponse::Error { error } => Err(ApiError::from_raw(parts.status, error)),
        }
    }
}
