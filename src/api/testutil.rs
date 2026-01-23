//! Test utilities for API integration tests.

use crate::{ApiError, ApiRequest, ApiResponse, Profile};

/// Execute an API request and parse the response.
pub fn roundtrip<T: ApiRequest>(req: T) -> Result<T::Response, ApiError> {
    let agent = ureq::Agent::new_with_config(
        ureq::config::Config::builder()
            .http_status_as_error(false)
            .build(),
    );

    let profile = Profile::from_default_env()
        .expect("Failed to load test profile. Did you forget to set BAUPLAN_PROFILE?");
    let req = req
        .into_request(&profile)
        .expect("Failed to create request");
    let resp = agent.run(req).expect("HTTP Error");
    T::Response::from_response(resp.map(ureq::Body::into_reader))
}
