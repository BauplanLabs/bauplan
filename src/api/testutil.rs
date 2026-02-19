//! Test utilities for API integration tests.

use crate::{ApiError, ApiRequest, ApiResponse, Profile};
use std::{
    hash::{BuildHasher, Hasher},
    sync::OnceLock,
    time,
};

fn test_profile() -> &'static Profile {
    static PROFILE: OnceLock<Profile> = OnceLock::new();
    PROFILE.get_or_init(|| {
        Profile::from_default_env()
            .expect("Failed to load test profile. Did you forget to set BAUPLAN_PROFILE?")
    })
}

/// Execute an API request and parse the response.
pub(crate) fn roundtrip<T: ApiRequest>(req: T) -> Result<T::Response, ApiError> {
    let agent = ureq::Agent::new_with_config(
        ureq::config::Config::builder()
            .http_status_as_error(false)
            .build(),
    );

    let profile = test_profile();
    let req = req.into_request(profile).expect("Failed to create request");
    let resp = agent.run(req).expect("HTTP Error");
    T::Response::from_response(resp.map(ureq::Body::into_reader))
}

/// Get the username for the test profile via the gRPC info endpoint.
pub(crate) fn test_username() -> &'static str {
    use crate::grpc::{self, generated::GetBauplanInfoRequest};

    static USERNAME: OnceLock<String> = OnceLock::new();
    USERNAME.get_or_init(|| {
        let profile = test_profile();

        let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

        rt.block_on(async {
            let mut client = grpc::Client::new_lazy(profile, time::Duration::from_secs(30))
                .expect("Failed to create gRPC client");

            let resp = client
                .get_bauplan_info(GetBauplanInfoRequest::default())
                .await
                .expect("Failed to get bauplan info");

            resp.into_inner()
                .user_info
                .expect("No user info in response")
                .username
        })
    })
}

/// Generate a unique name for test resources.
pub(crate) fn test_name(prefix: &str) -> String {
    let ts = time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let rand: u32 = std::hash::RandomState::new().build_hasher().finish() as u32;

    format!("{prefix}_{ts}_{rand:08x}")
}

/// A temporary branch that is deleted when dropped.
pub(crate) struct TestBranch {
    pub name: String,
}

impl TestBranch {
    /// Create a new temporary branch from main.
    pub(crate) fn new(prefix: &str) -> Result<Self, ApiError> {
        let username = test_username();
        let name = format!("{username}.{}", test_name(prefix));
        let req = crate::branch::CreateBranch {
            name: &name,
            from_ref: "main",
        };
        roundtrip(req)?;

        Ok(Self { name })
    }
}

impl Drop for TestBranch {
    fn drop(&mut self) {
        let req = crate::branch::DeleteBranch { name: &self.name };
        if let Err(e) = roundtrip(req) {
            eprintln!("Warning: failed to delete test branch {}: {e}", self.name);
        }
    }
}

/// A temporary tag that is deleted when dropped.
pub(crate) struct TestTag {
    pub name: String,
}

impl TestTag {
    /// Create a new temporary tag from main.
    pub(crate) fn new(prefix: &str) -> Result<Self, ApiError> {
        let name = test_name(prefix);
        let req = crate::tag::CreateTag {
            name: &name,
            from_ref: "main",
        };
        roundtrip(req)?;

        Ok(Self { name })
    }
}

impl Drop for TestTag {
    fn drop(&mut self) {
        let req = crate::tag::DeleteTag { name: &self.name };
        if let Err(e) = roundtrip(req) {
            eprintln!("Warning: failed to delete test tag {}: {e}", self.name);
        }
    }
}
