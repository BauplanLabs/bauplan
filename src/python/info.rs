//! Info operations.

use std::time::Duration;

use pyo3::prelude::*;
use tonic::Request;

use crate::{
    grpc::generated::{GetBauplanInfoRequest, GetBauplanInfoResponse},
    python::exceptions::BauplanError,
};

use super::Client;

#[pyclass(name = "RunnerNodeInfo", module = "bauplan")]
#[derive(Debug, Clone)]
pub(crate) struct PyRunnerNodeInfo {
    #[pyo3(get)]
    hostname: String,
}

#[pyclass(name = "OrganizationInfo", module = "bauplan")]
#[derive(Debug, Clone)]
pub(crate) struct PyOrganizationInfo {
    #[pyo3(get)]
    id: String,
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    slug: String,
    #[pyo3(get)]
    default_parameter_secret_key: Option<String>,
    #[pyo3(get)]
    default_parameter_secret_public_key: Option<String>,
}

#[pyclass(name = "UserInfo", module = "bauplan")]
#[derive(Debug, Clone)]
pub(crate) struct PyUserInfo {
    #[pyo3(get)]
    id: String,
    #[pyo3(get)]
    username: String,
    #[pyo3(get)]
    first_name: String,
    #[pyo3(get)]
    last_name: String,
}

#[pymethods]
impl PyUserInfo {
    #[getter]
    fn full_name(&self) -> String {
        format!("{} {}", self.first_name, self.last_name)
    }
}

#[pyclass(name = "InfoState", module = "bauplan")]
#[derive(Debug, Clone)]
pub(crate) struct PyInfoState {
    #[pyo3(get)]
    client_version: String,
    #[pyo3(get)]
    organization: Option<PyOrganizationInfo>,
    #[pyo3(get)]
    user: Option<PyUserInfo>,
    #[pyo3(get)]
    runners: Vec<PyRunnerNodeInfo>,
}

impl From<GetBauplanInfoResponse> for PyInfoState {
    fn from(resp: GetBauplanInfoResponse) -> Self {
        let organization = resp.organization_info.map(|org| PyOrganizationInfo {
            id: org.id,
            name: org.name,
            slug: org.slug,
            default_parameter_secret_key: org.default_parameter_secret_key,
            default_parameter_secret_public_key: org.default_parameter_secret_public_key,
        });

        let user = resp.user_info.map(|u| PyUserInfo {
            id: u.id,
            username: u.username,
            first_name: u.first_name,
            last_name: u.last_name,
        });

        let runners: Vec<PyRunnerNodeInfo> = resp
            .runners
            .into_iter()
            .map(|r| PyRunnerNodeInfo {
                hostname: r.hostname,
            })
            .collect();

        Self {
            client_version: resp.client_version,
            organization,
            user,
            runners,
        }
    }
}

#[pymethods]
impl Client {
    /// Fetch organization & account information.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// info = client.info()
    /// print(info.user.username)
    /// print(info.organization.name)
    /// ```
    ///
    /// Parameters:
    ///     client_timeout: timeout in seconds.
    ///
    /// Returns:
    ///     An `InfoState` object containing organization, user, and runner information.
    #[pyo3(signature = (*, client_timeout: "int | None" = None) -> "InfoState")]
    fn info(&mut self, client_timeout: Option<u64>) -> PyResult<PyInfoState> {
        let mut request = Request::new(GetBauplanInfoRequest::default());
        request.set_timeout(
            client_timeout
                .map(Duration::from_secs)
                .unwrap_or(self.client_timeout),
        );

        let rt = super::rt();
        let info = rt
            .block_on(self.grpc.get_bauplan_info(request))
            .map_err(|e| BauplanError::new_err(e.to_string()))?;

        Ok(info.into_inner().into())
    }
}
