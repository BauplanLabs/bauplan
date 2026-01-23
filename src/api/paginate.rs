use std::{fmt, io::Read};

use serde::{Deserialize, Serialize};

use crate::ApiRequest;

use super::{ApiError, ApiResponse, RawApiResponse};

/// A request with a pagination token and limit attached.
pub struct PaginatedRequest<'a, T> {
    /// The inner request.
    pub base: T,
    /// The pagination token, if any.
    pub pagination_token: Option<&'a str>,
    /// The maximum number of records to request from the server.
    pub limit: Option<usize>,
}

#[derive(Serialize)]
struct PaginateQuery<'a, T> {
    #[serde(flatten)]
    inner: T,
    pagination_token: Option<&'a str>,
    max_records: Option<usize>,
}

impl<T> fmt::Debug for PaginatedRequest<'_, T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PaginatedResponse")
            .field("base", &self.base)
            .field("pagination_token", &self.pagination_token)
            .field("limit", &self.limit)
            .finish()
    }
}

impl<T: ApiRequest> ApiRequest for PaginatedRequest<'_, T> {
    type Response = T::Response;

    fn path(&self) -> String {
        self.base.path()
    }

    fn method(&self) -> http::Method {
        self.base.method()
    }

    fn body(&self) -> Option<impl Serialize> {
        self.base.body()
    }

    fn query(&self) -> Option<impl Serialize> {
        Some(PaginateQuery {
            inner: self.base.query(),
            pagination_token: self.pagination_token,
            max_records: self.limit,
        })
    }
}

/// A possibly partial response, with a pagination token.
pub struct PaginatedResponse<T> {
    /// One page of results.
    pub page: Vec<T>,
    /// The pagination token for the next page. Will be unset if there are no
    /// more results.
    pub pagination_token: Option<String>,
}

impl<T> fmt::Debug for PaginatedResponse<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PaginatedResponse")
            .field("page", &self.page)
            .field("pagination_token", &self.pagination_token)
            .finish()
    }
}

impl<T> ApiResponse for PaginatedResponse<T>
where
    T: for<'de> Deserialize<'de>,
{
    fn from_response_parts(
        parts: http::response::Parts,
        body: impl Read,
    ) -> Result<Self, ApiError> {
        let raw: RawApiResponse<Vec<T>> = serde_json::from_reader(body).map_err(|e| {
            log::error!("Failed to parse API response: {e:#?}");
            ApiError::InvalidResponse(parts.status, e)
        })?;

        match raw {
            RawApiResponse::Data { data, metadata, .. } => Ok(PaginatedResponse {
                page: data,
                pagination_token: metadata.pagination_token,
            }),
            RawApiResponse::Error { error } => Err(ApiError::from_raw(parts.status, error)),
        }
    }
}

struct Paginator<F, E, R, T>
where
    F: Fn(PaginatedRequest<'_, R>) -> Result<R::Response, E>,
    E: From<ApiError>,
    R: ApiRequest<Response = PaginatedResponse<T>> + Clone,
{
    base_req: R,
    fetch_batch: F,
    batch: <Vec<T> as IntoIterator>::IntoIter,
    next_pagination_token: Option<String>,
    off: usize,
    limit: Option<usize>,
}

impl<F, E, R, T> Iterator for Paginator<F, E, R, T>
where
    F: Fn(PaginatedRequest<'_, R>) -> Result<R::Response, E>,
    E: From<ApiError>,
    R: ApiRequest<Response = PaginatedResponse<T>> + Clone,
{
    type Item = Result<T, E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.limit.is_some_and(|l| self.off >= l) {
            return None;
        }

        if let Some(v) = self.batch.next() {
            self.off += 1;
            return Some(Ok(v));
        }

        let token = self.next_pagination_token.take()?;
        let limit = self.limit.map(|l| l - self.off);
        let page_req = self.base_req.clone().paginate(Some(&token), limit);

        let PaginatedResponse {
            page,
            pagination_token,
        } = match (self.fetch_batch)(page_req) {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };

        self.batch = page.into_iter();
        self.next_pagination_token = pagination_token;

        if let Some(v) = self.batch.next() {
            self.off += 1;
            Some(Ok(v))
        } else {
            None
        }
    }
}

/// Repeatedly make a request, fetching more results continuously by calling
/// `fetch_batch`.
pub fn paginate<F, E, R, T>(
    base_req: R,
    limit: Option<usize>,
    fetch_batch: F,
) -> Result<impl Iterator<Item = Result<T, E>>, E>
where
    F: Fn(PaginatedRequest<'_, R>) -> Result<R::Response, E>,
    E: From<ApiError>,
    R: ApiRequest<Response = PaginatedResponse<T>> + Clone,
{
    let PaginatedResponse {
        page,
        pagination_token: next_pagination_token,
    } = fetch_batch(base_req.clone().paginate(None, limit))?;

    Ok(Paginator {
        fetch_batch,
        base_req,
        batch: page.into_iter(),
        next_pagination_token,
        off: 0,
        limit,
    })
}
