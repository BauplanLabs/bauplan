// ApiError is just over the threshold.
#![allow(clippy::result_large_err)]

use std::borrow::Cow;
use std::io::Read;

use percent_encoding::{AsciiSet, CONTROLS, PercentEncode, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{CatalogRef, Profile};

pub mod branch;
pub mod commit;
mod error;
pub mod iceberg;
pub mod namespace;
mod paginate;
pub mod table;
pub mod tag;

#[cfg(all(test, feature = "_integration-tests"))]
pub(crate) mod testutil;

pub use error::*;
pub use paginate::*;

/// A percent-encoded URL path for an API request.
#[derive(Debug)]
pub struct PathArgs(Cow<'static, str>);

fn encode_segment(s: &str) -> PercentEncode<'_> {
    // WHATWG path percent-encode set (https://url.spec.whatwg.org/#path-percent-encode-set)
    // extended with `/` and `%` to treat the input as a single segment.
    const SEGMENT: &AsciiSet = &CONTROLS
        .add(b' ').add(b'"').add(b'<').add(b'>').add(b'`')
        .add(b'#').add(b'?').add(b'{').add(b'}')
        .add(b'/').add(b'%');
    utf8_percent_encode(s, SEGMENT)
}

macro_rules! urlformat {
    ($fmt:literal) => {
        $crate::api::PathArgs(::std::borrow::Cow::Borrowed($fmt))
    };
    ($fmt:literal, $($arg:expr),+ $(,)?) => {
        $crate::api::PathArgs(::std::borrow::Cow::Owned(format!(
            $fmt, $($crate::api::encode_segment($arg)),+
        )))
    };
}
pub(crate) use urlformat;

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

    /// The path that the request should take. Construct with [`urlformat!`].
    fn path(&self) -> PathArgs;

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
        let path = self.path().0;
        let mut parts = profile.api_endpoint.clone().into_parts();

        let path = if let Some(qs) = self.query() {
            let mut path = path.into_owned();
            path.push('?');

            // SAFETY: query strings should only be valid UTF-8.
            unsafe {
                serde_qs::to_writer(&qs, &mut path.as_mut_vec())
                    .expect("query string serialization should be infallible");
            }

            Cow::Owned(path)
        } else {
            path
        };

        parts.path_and_query = Some(path.parse()?);

        let uri = http::Uri::from_parts(parts).unwrap();
        let mut req = http::Request::builder()
            .method(method)
            .uri(uri)
            .header(http::header::USER_AGENT, &profile.user_agent);

        if let Some(key) = &profile.api_key {
            req = req.header(http::header::AUTHORIZATION, format!("Bearer {}", key));
        } else {
            warn!("no API key provided");
        }

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
        let raw: RawApiResponse<serde_json::Value> =
            serde_json::from_reader(body).map_err(|e| {
                tracing::error!("Failed to parse API response: {e}");
                ApiError::InvalidResponse(parts.status)
            })?;

        match raw {
            RawApiResponse::Data { data, .. } => {
                serde_path_to_error::deserialize(data).map_err(|e| {
                    tracing::error!("Failed to parse API response data: {e}");
                    ApiError::InvalidResponse(parts.status)
                })
            }
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

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    #[test]
    fn urlformat_static_is_borrowed() {
        let p = urlformat!("/catalog/v0/branches").0;
        assert!(matches!(p, Cow::Borrowed("/catalog/v0/branches")));
    }

    #[test]
    fn urlformat_parameterized_is_owned_and_encoded() {
        let p = urlformat!("/catalog/v0/branches/{}", "feature/foo").0;
        assert!(matches!(p, Cow::Owned(s) if s == "/catalog/v0/branches/feature%2Ffoo"));
    }

    #[test]
    fn urlformat_encodes_expected_chars() {
        assert_eq!(urlformat!("/{}", "main").0, "/main");
        assert_eq!(urlformat!("/{}", "main@abc123").0, "/main@abc123");
        assert_eq!(urlformat!("/{}", "a b").0, "/a%20b");
        assert_eq!(urlformat!("/{}", "100%").0, "/100%25");
        assert_eq!(urlformat!("/{}", "café").0, "/caf%C3%A9");
        // Iceberg multi-level namespace separator (U+001F) must encode to %1F.
        assert_eq!(urlformat!("/{}", "a\u{1f}b").0, "/a%1Fb");
        // Multi-arg: each segment is encoded independently.
        assert_eq!(
            urlformat!("/refs/{}/namespaces/{}", "feature/foo", "a b").0,
            "/refs/feature%2Ffoo/namespaces/a%20b",
        );
    }
}
