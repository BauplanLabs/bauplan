//! Python bindings for the Bauplan client.

use std::{
    sync::{Arc, OnceLock},
    time,
};

use pyo3::{exceptions::PyValueError, marker::Ungil, prelude::*};
use tokio::runtime::Runtime;
use ureq::middleware::MiddlewareNext;

mod arrow_schema;
mod branch;
mod commit;
mod diff;
mod exceptions;
mod info;
pub(crate) mod job;
mod namespace;
mod paginate;
mod query;
mod run;
mod schema;
mod state;
mod table;
mod tag;

use bauplan_api::{
    branch::BranchApiClient, catalog_ref::RefApiClient, namespace::NamespaceApiClient,
    table::TableApiClient, tag::TagApiClient,
};

use crate::{
    Profile, grpc,
    python::exceptions::{BauplanError, BauplanJobError},
};

pub(crate) fn job_err(e: impl std::fmt::Display) -> PyErr {
    BauplanJobError::new_err(e.to_string())
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
/// my_table = client.query('SELECT avg(Age) AS average_age FROM bauplan.titanic limit 1', ref='main')
///
/// # efficiently cast the table to a pandas DataFrame
/// df = my_table.to_pandas()
/// ```
///
/// #### Notes on authentication
///
/// ```python
/// # by default, authenticate from BAUPLAN_API_KEY >> BAUPLAN_PROFILE >> ~/.bauplan/config.yaml
/// client = bauplan.Client()
/// # client used ~/.bauplan/config.yaml profile 'default'
///
/// import os
/// os.environ['BAUPLAN_PROFILE'] = "someprofile"
/// client = bauplan.Client()
/// # >> client now uses profile 'someprofile'
///
/// os.environ['BAUPLAN_API_KEY'] = "mykey"
/// client = bauplan.Client()
/// # >> client now authenticates with api_key value "mykey", because api key > profile
///
/// # specify authentication directly - this supersedes BAUPLAN_API_KEY in the environment
/// client = bauplan.Client(api_key='MY_KEY')
///
/// # specify a profile from ~/.bauplan/config.yaml - this supersedes BAUPLAN_PROFILE in the environment
/// client = bauplan.Client(profile='default')
/// ```
///
/// #### Handling Exceptions
///
/// Catalog operations (branch, tag, namespace and table methods) raise a subclass of
/// `bauplan.exceptions.BauplanAPIError`, one for each type of error, like
/// `TableNotFoundError`. Each method lists the ones it raises, and every method can
/// also raise `InternalError` and `ServiceUnavailableError`.
///
/// Every HTTP error is a `bauplan.exceptions.BauplanStatusError`, with the `status` of
/// the response. For an error without a type we know, like a 502 from a proxy or an
/// error added after this release, we raise `BauplanStatusError` itself. To retry a
/// temporary error, catch `BauplanStatusError` and check its `status`, like 502, 503
/// or 504.
///
/// Jobs like `run` and `import_data` return a state object instead. A failed job doesn't
/// raise; check `state.job_status` and `state.error`. `bauplan.exceptions.BauplanJobError`
/// is only raised if the job can't be submitted or times out.
///
/// `query`, `scan` and `query_to_*` return the results directly and raise
/// `bauplan.exceptions.BauplanQueryError` if the query fails.
///
/// #### Logging
///
/// You can use python's standard logging apparatus to tap logs from the client:
///
/// ```python
/// import logging
///
/// # Enable debug logs from the Bauplan client.
/// logging.basicConfig()
/// logging.getLogger("bauplan").setLevel(logging.DEBUG)
///
/// client = bauplan.Client()
/// client.query("SELECT count(*) FROM titanic", ref="main")
/// ```
///
/// #### Pagination
///
/// Methods like `list_branches` and `list_tables` return a lazy paginating iterator:
///
/// ```python
/// #! client = bauplan.Client()
/// for branch in client.list_branches():
///     ...
/// ```
///
/// As a general rule, you should try to avoid eagerly evaluating them, as the lists can be very large.
///
/// ```python
/// #! client = bauplan.Client()
/// # Don't do this!
/// branches = list(client.list_branches())
/// ```
///
/// ## Examples
///
/// ```python
/// #! client = bauplan.Client()
/// state = client.run("./my_pipeline")
/// if state.job_status != "SUCCESS":
///     ...
/// ```
///
/// Parameters:
///     profile: The Bauplan config profile name to use to determine api_key.
///     api_key: Your unique Bauplan API key; mutually exclusive with `profile`. If not provided, fetch precedence is 1) environment `BAUPLAN_API_KEY` 2) ~/.bauplan/config.yaml
///     client_timeout: The client timeout in seconds for all the requests.
///     config_file_path: The path to the Bauplan config file to use. If not provided, ~/.bauplan/config.yaml will be used. Note that this disables any environment-based configuration.
///     catalog: The name of the catalog to use. If not provided, fetch precedence is 1) environment `BAUPLAN_CATALOG` 2) ~/.bauplan/config.yaml 3) `default`
#[pyclass(module = "bauplan")]
pub(crate) struct Client {
    pub(crate) profile: Profile,
    /// Every request through it carries the api key.
    pub(crate) agent: ureq::Agent,
    pub(crate) grpc: grpc::Client,
    pub(crate) client_timeout: time::Duration,
    /// NB: we don't ever call `endpoint.close()`, because there's no good time
    /// to do that. It's probably fine; in normal use all connections will have
    /// finished out long before we drop the client. If any are still open when
    /// we drop, then the server will have to wait for the idle timeout, but
    /// that's not that tragic.
    pub(crate) longbow_endpoint: Arc<tokio::sync::OnceCell<bauplan_longbow::iroh::Endpoint>>,
}

#[pymethods]
impl Client {
    #[new]
    #[pyo3(signature = (
            profile = None,
            api_key = None,
            client_timeout = None,
            config_file_path = None,
            catalog = None,
        ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        profile: Option<&str>,
        api_key: Option<String>,
        client_timeout: Option<u64>,
        config_file_path: Option<&str>,
        catalog: Option<String>,
    ) -> PyResult<Self> {
        let profile = if let Some(p) = config_file_path {
            Profile::read(p, profile)
        } else if let Some(name) = profile {
            Profile::from_env(name)
        } else {
            Profile::from_default_env()
        };

        let mut profile = profile
            .map_err(|e| PyValueError::new_err(e.to_string()))?
            .with_ua_product("bauplan-pysdk");
        if let Some(api_key) = api_key {
            profile = profile.with_api_key(api_key);
        }
        if let Some(catalog) = catalog {
            profile = profile.with_catalog(catalog);
        }

        profile
            .validate()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        // TODO: The old PySDK had DEFAULT_API_CALL_TIMEOUT_SECONDS = 30.
        // This is almost certainly way too long.
        let client_timeout = client_timeout
            .map(time::Duration::from_secs)
            .unwrap_or(time::Duration::from_secs(30));

        let authorization = profile.authorization().map_err(BauplanError::new_err)?;

        let middleware = move |mut req: http::Request<ureq::SendBody<'_>>,
                               next: MiddlewareNext<'_>| {
            if let Some(authorization) = &authorization {
                req.headers_mut()
                    .insert(http::header::AUTHORIZATION, authorization.clone());
            }
            next.handle(req)
        };

        let cfg = ureq::config::Config::builder()
            // Allows error responses to be parsed.
            .http_status_as_error(false)
            .tls_config(
                ureq::tls::TlsConfig::builder()
                    .root_certs(ureq::tls::RootCerts::PlatformVerifier)
                    .build(),
            )
            .user_agent(&profile.user_agent)
            .timeout_global(Some(client_timeout))
            .middleware(middleware);
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
            longbow_endpoint: Arc::new(tokio::sync::OnceCell::new()),
        })
    }
}

impl Client {
    /// A client for the branch endpoints of the api.
    fn branches(&self) -> BranchApiClient {
        BranchApiClient::with_transport(
            self.profile.new_api_endpoint.to_string(),
            self.agent.clone(),
        )
    }

    /// A client for the namespace endpoints of the api.
    fn namespaces(&self) -> NamespaceApiClient {
        NamespaceApiClient::with_transport(
            self.profile.new_api_endpoint.to_string(),
            self.agent.clone(),
        )
    }

    /// A client for the table endpoints of the api.
    fn tables(&self) -> TableApiClient {
        TableApiClient::with_transport(
            self.profile.new_api_endpoint.to_string(),
            self.agent.clone(),
        )
    }

    /// A client for the tag endpoints of the api.
    fn tags(&self) -> TagApiClient {
        TagApiClient::with_transport(
            self.profile.new_api_endpoint.to_string(),
            self.agent.clone(),
        )
    }

    /// A client for the ref endpoints of the api.
    fn refs(&self) -> RefApiClient {
        RefApiClient::with_transport(
            self.profile.new_api_endpoint.to_string(),
            self.agent.clone(),
        )
    }
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
    use pyo3::prelude::*;

    // Client
    #[pymodule_export]
    use super::Client;

    // Submodules
    #[pymodule_export]
    use super::exceptions::exceptions;
    #[pymodule_export]
    use super::schema::schema;
    #[pymodule_export]
    use super::state::state;

    // Info
    #[pymodule_export]
    use super::info::PyInfoState as InfoState;
    #[pymodule_export]
    use super::info::PyOrganizationInfo as OrganizationInfo;
    #[pymodule_export]
    use super::info::PyRunnerNodeInfo as RunnerNodeInfo;
    #[pymodule_export]
    use super::info::PyUserInfo as UserInfo;

    // Register submodules in sys.modules so that
    // `from bauplan._internal.schema import X` works.
    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        pyo3_log::Logger::new(m.py(), pyo3_log::Caching::LoggersAndLevels)?
            .filter(log::LevelFilter::Off)
            .filter_target("bauplan".to_owned(), log::LevelFilter::Trace)
            .install()
            .ok();

        m.add("__version__", env!("CARGO_PKG_VERSION"))?;

        let sys = m.py().import("sys")?;
        let modules = sys.getattr("modules")?;
        let name = m.name()?;

        for sub in ["exceptions", "schema", "state"] {
            let submod = m.getattr(sub)?;
            modules.set_item(format!("{name}.{sub}"), &submod)?;
        }

        Ok(())
    }
}

/// Releases the GIL and does some async work.
pub(crate) fn detach<T: Ungil>(py: Python<'_>, f: impl Future<Output = T> + Send) -> T {
    py.detach(|| rt().block_on(f))
}

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
