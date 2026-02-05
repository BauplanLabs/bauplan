//! Python bindings for the Bauplan client.

use std::{fs::File, sync::OnceLock, time};

use pyo3::{exceptions::PyValueError, prelude::*};
use tokio::runtime::Runtime;

mod branch;
mod commit;
mod exceptions;
mod info;
pub(crate) mod job;
mod namespace;
mod paginate;
mod query;
mod refs;
mod run;
mod table;
mod tag;

use crate::{
    ApiError, ApiErrorKind, ApiRequest, ApiResponse, Profile, grpc,
    python::exceptions::BauplanError,
};

#[derive(Debug, thiserror::Error)]
enum ClientError {
    #[error("error building request")]
    Validation(#[from] http::Error),
    #[error("transport error: {0}")]
    Transport(#[from] ureq::Error),
    #[error(transparent)]
    Api(#[from] ApiError),
}

impl ClientError {
    fn is_api_err(&self, k: ApiErrorKind) -> bool {
        match self {
            ClientError::Api(ApiError::ErrorResponse { kind, .. }) => k == *kind,
            _ => false,
        }
    }
}

/// A client for the Bauplan API.
///
/// #### Using the client
///
/// ```python
/// import bauplan
/// client = bauplan.Client()
///
/// # query the table and return result set as an arrow Table
/// my_table = client.query('SELECT avg(age) AS average_age FROM bauplan.titanic limit 1', ref='main')
///
/// # efficiently cast the table to a pandas DataFrame
/// df = my_table.to_pandas()
/// ```
///
/// #### Notes on authentication
///
/// ```python notest
/// # by default, authenticate from BAUPLAN_API_KEY >> BAUPLAN_PROFILE >> ~/.config/bauplan.yml
/// client = bauplan.Client()
/// # client used ~/.config/bauplan.yml profile 'default'
///
/// os.environ['BAUPLAN_PROFILE'] = "someprofile"
/// client = bauplan.Client()
/// # >> client now uses profile 'someprofile'
///
/// os.environ['BAUPLAN_API_KEY'] = "mykey"
/// client = bauplan.Client()
/// # >> client now authenticates with api_key value "mykey", because api key > profile
///
/// # specify authentication directly - this supercedes BAUPLAN_API_KEY in the environment
/// client = bauplan.Client(api_key='MY_KEY')
///
/// # specify a profile from ~/.config/bauplan.yml - this supercedes BAUPLAN_PROFILE in the environment
/// client = bauplan.Client(profile='default')
/// ```
///
/// #### Handling Exceptions
///
/// Catalog operations (branch/table methods) raise a subclass of `bauplan.exceptions.BauplanError` that mirror HTTP status codes.
///     - 400: `bauplan.exceptions.InvalidDataError`
///     - 401: `bauplan.exceptions.UnauthorizedError`
///     - 403: `bauplan.exceptions.AccessDeniedError`
///     - 404: `bauplan.exceptions.ResourceNotFoundError` e.g .ID doesn't match any records
///     - 404: `bauplan.exceptions.ApiRouteError` e.g. the given route doesn't exist
///     - 405: `bauplan.exceptions.ApiMethodError` e.g. POST on a route with only GET defined
///     - 409: `bauplan.exceptions.UpdateConflictError` e.g. creating a record with a name that already exists
///     - 429: `bauplan.exceptions.TooManyRequestsError`
///
/// Run/Query/Scan/Import operations raise a subclass of `bauplan.exceptions.BauplanError` that represents, and also return a `bauplan.state.RunState` object containing details and logs:
///     - `bauplan.exceptions.JobError` e.g. something went wrong in a run/query/import/scan; includes error details
///
/// Run/import operations also return a state object that includes a `job_status` and other details.
/// There are two ways to check status for run/import operations:
///     1. try/except `bauplan.exceptions.JobError`
///     2. check the `state.job_status` attribute
///
/// ## Examples
///
/// ```python notest
/// state = client.run(...)
/// if state.job_status != "SUCCESS":
///     ...
/// ```
///
/// Parameters:
///     profile: The Bauplan config profile name to use to determine api_key.
///     api_key: Your unique Bauplan API key; mutually exclusive with `profile`. If not provided, fetch precedence is 1) environment `BAUPLAN_API_KEY` 2) .bauplan/config.yml
///     client_timeout: The client timeout in seconds for all the requests.
///     config_file_path: The path to the Bauplan config file to use. If not provided, ~/.config/bauplan.yaml will be used. Note that this disables any environment-based configuration.
///
///     feature_flags: A dictionary of feature flags to enable or disable during the use of this client instance.
#[pyclass]
pub(crate) struct Client {
    pub(crate) profile: Profile,
    pub(crate) agent: ureq::Agent,
    pub(crate) grpc: grpc::Client,
    pub(crate) client_timeout: time::Duration,
}

#[pymethods]
impl Client {
    #[new]
    #[pyo3(signature = (
            profile = None,
            api_key = None,
            client_timeout = None,
            config_file_path = None,
        ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        profile: Option<&str>,
        api_key: Option<String>,
        client_timeout: Option<u64>,
        config_file_path: Option<&str>,
    ) -> PyResult<Self> {
        let profile = if let Some(p) = config_file_path {
            Profile::read(&File::open(p)?, profile)
        } else if let Some(name) = profile {
            Profile::from_env(name)
        } else {
            Profile::from_default_env()
        };

        let mut profile = profile
            .map_err(|e| PyValueError::new_err(e.to_string()))?
            .with_ua_product("bauplan-pysdk");
        if let Some(api_key) = api_key {
            profile.api_key = api_key;
        }

        // TODO: The old PySDK had DEFAULT_API_CALL_TIMEOUT_SECONDS = 30.
        // This is almost certainly way too long.
        let client_timeout = client_timeout
            .map(time::Duration::from_secs)
            .unwrap_or(time::Duration::from_secs(30));

        let cfg = ureq::config::Config::builder()
            .http_status_as_error(false)
            .timeout_global(Some(client_timeout));
        let agent = ureq::Agent::new_with_config(cfg.build());

        let grpc = {
            let rt = rt();
            let _guard = rt.enter();
            grpc::Client::new_lazy(&profile, client_timeout)
                .map_err(|e| BauplanError::new_err(e.to_string()))?
        };

        Ok(Self {
            profile,
            agent,
            grpc,
            client_timeout,
        })
    }
}

fn roundtrip<T: ApiRequest>(
    req: T,
    profile: &Profile,
    agent: &ureq::Agent,
) -> Result<T::Response, ClientError> {
    let req = req.into_request(profile)?;
    let resp = agent.run(req)?;
    let resp = <T::Response as ApiResponse>::from_response(resp.map(ureq::Body::into_reader))?;
    Ok(resp)
}

fn optional_on_off<'a>(name: &'static str, v: Option<&'a str>) -> PyResult<Option<&'a str>> {
    match v {
        None | Some("on") | Some("off") => Ok(v),
        Some(_) => Err(PyValueError::new_err(format!(
            "{name} must be 'on' or 'off'"
        ))),
    }
}

#[pymodule]
mod _internal {
    // Client
    #[pymodule_export]
    use super::Client;
    #[pymodule_export]
    use super::exceptions::exceptions;

    // Refs
    #[pymodule_export]
    use super::refs::PyBranch as Branch;
    #[pymodule_export]
    use super::refs::PyDetachedRef as DetachedRef;
    #[pymodule_export]
    use super::refs::PyRef as Ref;
    #[pymodule_export]
    use super::refs::PyRefType;
    #[pymodule_export]
    use super::refs::PyTag as Tag;

    // Commits
    #[pymodule_export]
    use crate::commit::Actor;
    #[pymodule_export]
    use crate::commit::Commit;

    // Catalog
    #[pymodule_export]
    use crate::namespace::Namespace;
    #[pymodule_export]
    use crate::table::Table;
    #[pymodule_export]
    use crate::table::TableField;
    #[pymodule_export]
    use crate::table::TableKind;

    // Jobs
    #[pymodule_export]
    use super::job::JobContext;
    #[pymodule_export]
    use super::job::JobLogEvent;
    #[pymodule_export]
    use super::job::JobLogLevel;
    #[pymodule_export]
    use super::job::JobLogList;
    #[pymodule_export]
    use super::job::JobLogStream;
    #[pymodule_export]
    use crate::grpc::job::Job;
    #[pymodule_export]
    use crate::grpc::job::JobKind;
    #[pymodule_export]
    use crate::grpc::job::JobState;

    // Run state
    #[pymodule_export]
    use super::run::state::RunExecutionContext;
    #[pymodule_export]
    use super::run::state::RunState;

    // Info
    #[pymodule_export]
    use super::info::PyInfoState as InfoState;
    #[pymodule_export]
    use super::info::PyOrganizationInfo as OrganizationInfo;
    #[pymodule_export]
    use super::info::PyRunnerNodeInfo as RunnerNodeInfo;
    #[pymodule_export]
    use super::info::PyUserInfo as UserInfo;
}

// Copied from delta-rs:
// https://github.com/delta-io/delta-rs/blob/d4d75cc06dcdc02338a8a5222a3949312f330d8f/python/src/utils.rs#L14
#[inline]
pub(crate) fn rt() -> &'static Runtime {
    static TOKIO_RT: OnceLock<Runtime> = OnceLock::new();
    static PID: OnceLock<u32> = OnceLock::new();
    let pid = std::process::id();
    let runtime_pid = *PID.get_or_init(|| pid);
    if pid != runtime_pid {
        panic!(
            "Forked process detected - current PID is {pid} but the tokio runtime was created by {runtime_pid}. The tokio \
            runtime does not support forked processes https://github.com/tokio-rs/tokio/issues/4301. If you are \
            seeing this message while using Python multithreading make sure to use the `spawn` or `forkserver` \
            mode.",
        );
    }

    TOKIO_RT.get_or_init(|| Runtime::new().expect("Failed to create a tokio runtime."))
}
