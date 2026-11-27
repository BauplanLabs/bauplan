//! Python exception types.

use bauplan_api::branch::MergeBranchError;
use bauplan_api::gropius::{self, client::ClientError};
use pyo3::exceptions::PyAttributeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::python::diff::PyEntity;

#[pymodule(submodule)]
pub mod exceptions {
    use super::*;

    // Re-export exception types into the module.
    #[pymodule_export]
    use super::BauplanAPIError;
    #[pymodule_export]
    use super::BauplanError;
    #[pymodule_export]
    use super::BauplanStatusError;

    // API errors
    #[pymodule_export]
    use super::BadRequestError;
    #[pymodule_export]
    use super::BranchHeadChangedError;
    #[pymodule_export]
    use super::BranchNameConflictError;
    #[pymodule_export]
    use super::BranchNotFoundError;
    #[pymodule_export]
    use super::CatalogNotFoundError;
    #[pymodule_export]
    use super::ForbiddenError;
    #[pymodule_export]
    use super::FromRefNotFoundError;
    #[pymodule_export]
    use super::FromTableNotFoundError;
    #[pymodule_export]
    use super::InternalError;
    #[pymodule_export]
    use super::MainBranchError;
    #[pymodule_export]
    use super::MergeConflictError;
    #[pymodule_export]
    use super::NameConflictError;
    #[pymodule_export]
    use super::NamespaceNotEmptyError;
    #[pymodule_export]
    use super::NamespaceNotFoundError;
    #[pymodule_export]
    use super::NotABranchError;
    #[pymodule_export]
    use super::RefNotFoundError;
    #[pymodule_export]
    use super::ServiceUnavailableError;
    #[pymodule_export]
    use super::TableNotFoundError;
    #[pymodule_export]
    use super::TagChangedError;
    #[pymodule_export]
    use super::TagNameConflictError;
    #[pymodule_export]
    use super::TagNotFoundError;
    #[pymodule_export]
    use super::UnauthorizedError;
    #[pymodule_export]
    use super::UnsupportedTableKindError;

    // Non-HTTP errors
    #[pymodule_export]
    use super::BauplanJobError;
    #[pymodule_export]
    use super::BauplanQueryError;
    #[pymodule_export]
    use super::TableCreatePlanApplyStatusError;
    #[pymodule_export]
    use super::TableCreatePlanError;
    #[pymodule_export]
    use super::TableCreatePlanStatusError;

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        // Register in sys.modules so "from bauplan.exceptions import X" works.
        let py = m.py();
        py.import("sys")?
            .getattr("modules")?
            .set_item("bauplan.exceptions", m)?;
        Ok(())
    }
}

/// Base class for all bauplan SDK exceptions.
#[pyclass(extends=pyo3::exceptions::PyException, subclass, module="bauplan.exceptions")]
pub(crate) struct BauplanError;

#[pymethods]
impl BauplanError {
    #[new]
    #[pyo3(signature = (*_args))]
    fn new(_args: &Bound<'_, pyo3::types::PyTuple>) -> Self {
        Self
    }
}

impl BauplanError {
    pub(crate) fn new_err(msg: impl std::fmt::Display) -> PyErr {
        PyErr::new::<Self, _>(msg.to_string())
    }
}

/// An HTTP error, with the status of the response.
///
/// The errors with a type we know are subclasses of `BauplanAPIError`. We raise
/// this class for the others, like a 502 from a proxy or an error added after
/// this release.
#[pyclass(extends=BauplanError, module="bauplan.exceptions", subclass)]
pub(crate) struct BauplanStatusError {
    #[pyo3(get)]
    status: u16,
    #[pyo3(get)]
    message: String,
}

#[pymethods]
impl BauplanStatusError {
    #[new]
    fn new(status: u16, message: String) -> PyClassInitializer<Self> {
        PyClassInitializer::from(BauplanError).add_subclass(Self { status, message })
    }

    fn __str__(&self) -> String {
        self.message.clone()
    }
}

/// An error from the API, with a type we know.
///
/// Each type of error has its own subclass. The fields of the error are in
/// `details`, and each one is also an attribute.
#[pyclass(extends=BauplanStatusError, module="bauplan.exceptions", subclass)]
pub(crate) struct BauplanAPIError {
    #[pyo3(get)]
    r#type: String,
    #[pyo3(get)]
    details: Py<PyDict>,
}

#[pymethods]
impl BauplanAPIError {
    #[new]
    #[pyo3(signature = (status, r#type, message, details=None))]
    fn new(
        py: Python<'_>,
        status: u16,
        r#type: String,
        message: String,
        details: Option<Bound<'_, PyDict>>,
    ) -> PyClassInitializer<Self> {
        let details = details.unwrap_or_else(|| PyDict::new(py)).unbind();
        PyClassInitializer::from(BauplanError)
            .add_subclass(BauplanStatusError { status, message })
            .add_subclass(Self { r#type, details })
    }

    fn __getattr__(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        match self.details.bind(py).get_item(name)? {
            Some(value) => Ok(value.unbind()),
            None => Err(PyAttributeError::new_err(name.to_owned())),
        }
    }
}

type ApiErrorArgs = (u16, String, String, Option<Py<PyDict>>);

/// Turns an error of `bauplan-api` into its Python exception, picked by its
/// `type`. The message and the fields come from the error envelope.
pub(crate) fn api_error<E>(err: ClientError<E>) -> PyErr
where
    E: gropius::ApiError + std::error::Error,
{
    match &err {
        ClientError::Api(api) => typed_error(envelope_args(api)),
        // Not an error of this endpoint. If the body has a type we know, we raise
        // its class, and anything else raises `BauplanStatusError`.
        ClientError::Unexpected { status, body } => match serde_json::from_slice(body) {
            Ok(serde_json::Value::Object(envelope)) => {
                typed_error(fields_args(status.as_u16(), envelope, err.to_string()))
            }
            _ => PyErr::new::<BauplanStatusError, _>((status.as_u16(), err.to_string())),
        },
        _ => {
            let mut message = err.to_string();
            let sources = std::iter::successors(std::error::Error::source(&err), |e| e.source());
            for source in sources {
                message = format!("{message}: {source}");
            }
            BauplanError::new_err(message)
        }
    }
}

/// The exception of a failed merge. We take the conflicts from the error
/// itself, so they are the same entities a diff returns.
pub(crate) fn merge_error(err: ClientError<MergeBranchError>) -> PyErr {
    let api = match err {
        ClientError::Api(api) => api,
        err => return api_error(err),
    };

    let (status, r#type, message, details) = envelope_args(&api);
    let MergeBranchError::MergeConflict { conflicts, .. } = api else {
        return typed_error((status, r#type, message, details));
    };

    let conflicts: Vec<PyEntity> = conflicts.into_iter().map(PyEntity::from).collect();
    let details = Python::attach(|py| {
        let details = details?.into_bound(py);
        details.set_item("conflicts", conflicts).ok()?;
        Some(details.unbind())
    });
    PyErr::new::<MergeConflictError, _>((status, r#type, message, details))
}

/// The arguments of the exception, from an error of the endpoint.
fn envelope_args<E>(api: &E) -> ApiErrorArgs
where
    E: gropius::ApiError + std::error::Error,
{
    let status = api.status_code().as_u16();
    let serde_json::Value::Object(envelope) = serde_json::to_value(api).unwrap_or_default() else {
        return (status, String::new(), api.to_string(), None);
    };
    fields_args(status, envelope, api.to_string())
}

/// The arguments of the exception, from an envelope as JSON.
fn fields_args(
    status: u16,
    mut envelope: serde_json::Map<String, serde_json::Value>,
    fallback: String,
) -> ApiErrorArgs {
    let r#type = take_str(&mut envelope, "type").unwrap_or_default();
    let message = take_str(&mut envelope, "message").unwrap_or(fallback);
    let details = Python::attach(|py| details(py, envelope)).ok();
    (status, r#type, message, details)
}

/// Picks the class of the error type. A type we do not know becomes
/// `BauplanStatusError`.
fn typed_error(args: ApiErrorArgs) -> PyErr {
    match args.1.as_str() {
        "BAD_REQUEST" => PyErr::new::<BadRequestError, _>(args),
        "BRANCH_HEAD_CHANGED" => PyErr::new::<BranchHeadChangedError, _>(args),
        "BRANCH_NAME_CONFLICT" => PyErr::new::<BranchNameConflictError, _>(args),
        "BRANCH_NOT_FOUND" => PyErr::new::<BranchNotFoundError, _>(args),
        "CATALOG_NOT_FOUND" => PyErr::new::<CatalogNotFoundError, _>(args),
        "FORBIDDEN" => PyErr::new::<ForbiddenError, _>(args),
        "FROM_REF_NOT_FOUND" => PyErr::new::<FromRefNotFoundError, _>(args),
        "FROM_TABLE_NOT_FOUND" => PyErr::new::<FromTableNotFoundError, _>(args),
        "INTERNAL" => PyErr::new::<InternalError, _>(args),
        "MAIN_BRANCH" => PyErr::new::<MainBranchError, _>(args),
        "MERGE_CONFLICT" => PyErr::new::<MergeConflictError, _>(args),
        "NAME_CONFLICT" => PyErr::new::<NameConflictError, _>(args),
        "NAMESPACE_NOT_EMPTY" => PyErr::new::<NamespaceNotEmptyError, _>(args),
        "NAMESPACE_NOT_FOUND" => PyErr::new::<NamespaceNotFoundError, _>(args),
        "NOT_A_BRANCH" => PyErr::new::<NotABranchError, _>(args),
        "REF_NOT_FOUND" => PyErr::new::<RefNotFoundError, _>(args),
        "SERVICE_UNAVAILABLE" => PyErr::new::<ServiceUnavailableError, _>(args),
        "TABLE_NOT_FOUND" => PyErr::new::<TableNotFoundError, _>(args),
        "TAG_CHANGED" => PyErr::new::<TagChangedError, _>(args),
        "TAG_NAME_CONFLICT" => PyErr::new::<TagNameConflictError, _>(args),
        "TAG_NOT_FOUND" => PyErr::new::<TagNotFoundError, _>(args),
        "UNAUTHORIZED" => PyErr::new::<UnauthorizedError, _>(args),
        "UNSUPPORTED_TABLE_KIND" => PyErr::new::<UnsupportedTableKindError, _>(args),
        _ => PyErr::new::<BauplanStatusError, _>((args.0, args.2)),
    }
}

/// Takes a text field out of an error envelope.
fn take_str(
    envelope: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<String> {
    match envelope.remove(key)? {
        serde_json::Value::String(text) => Some(text),
        _ => None,
    }
}

/// The fields of an error envelope as Python values.
fn details(
    py: Python<'_>,
    fields: serde_json::Map<String, serde_json::Value>,
) -> PyResult<Py<PyDict>> {
    let json = py.import("json")?;
    let details = PyDict::new(py);
    for (key, value) in fields {
        details.set_item(key, json.call_method1("loads", (value.to_string(),))?)?;
    }
    Ok(details.unbind())
}

// API errors
pyo3::create_exception!(
    bauplan.exceptions,
    BadRequestError,
    BauplanAPIError,
    "Raised when the request is not valid."
);
pyo3::create_exception!(
    bauplan.exceptions,
    BranchHeadChangedError,
    BauplanAPIError,
    "Raised when the `bauplan.schema.Branch` head hash has changed since it was last read."
);
pyo3::create_exception!(
    bauplan.exceptions,
    BranchNameConflictError,
    BauplanAPIError,
    "Raised when a `bauplan.schema.Branch` with the name already exists."
);
pyo3::create_exception!(
    bauplan.exceptions,
    BranchNotFoundError,
    BauplanAPIError,
    "Raised when the referenced `bauplan.schema.Branch` does not exist."
);
pyo3::create_exception!(
    bauplan.exceptions,
    CatalogNotFoundError,
    BauplanAPIError,
    "Raised when the catalog does not exist."
);
pyo3::create_exception!(
    bauplan.exceptions,
    ForbiddenError,
    BauplanAPIError,
    "Raised when the caller is not permitted to perform the action."
);
pyo3::create_exception!(
    bauplan.exceptions,
    FromRefNotFoundError,
    BauplanAPIError,
    "Raised when the ref to start from does not exist."
);
pyo3::create_exception!(
    bauplan.exceptions,
    FromTableNotFoundError,
    BauplanAPIError,
    "Raised when the table to start from does not exist at its ref."
);
pyo3::create_exception!(
    bauplan.exceptions,
    InternalError,
    BauplanAPIError,
    "Raised when the API fails on its side."
);
pyo3::create_exception!(
    bauplan.exceptions,
    MainBranchError,
    BauplanAPIError,
    "Raised when the operation is not allowed on the main branch."
);
pyo3::create_exception!(
    bauplan.exceptions,
    MergeConflictError,
    BauplanAPIError,
    "Raised when a merge cannot be completed due to conflicting changes."
);
pyo3::create_exception!(
    bauplan.exceptions,
    NameConflictError,
    BauplanAPIError,
    "Raised when a namespace or a table with the name already exists."
);
pyo3::create_exception!(
    bauplan.exceptions,
    NamespaceNotEmptyError,
    BauplanAPIError,
    "Raised when attempting to delete a namespace that still contains tables."
);
pyo3::create_exception!(
    bauplan.exceptions,
    NamespaceNotFoundError,
    BauplanAPIError,
    "Raised when the referenced namespace does not exist at the given ref."
);
pyo3::create_exception!(
    bauplan.exceptions,
    NotABranchError,
    BauplanAPIError,
    "Raised when the provided ref is not a branch."
);
pyo3::create_exception!(
    bauplan.exceptions,
    RefNotFoundError,
    BauplanAPIError,
    "Raised when the referenced ref does not exist."
);
pyo3::create_exception!(
    bauplan.exceptions,
    ServiceUnavailableError,
    BauplanAPIError,
    "Raised when the API is temporarily unavailable."
);
pyo3::create_exception!(
    bauplan.exceptions,
    TableNotFoundError,
    BauplanAPIError,
    "Raised when the referenced table does not exist at the given ref."
);
pyo3::create_exception!(
    bauplan.exceptions,
    TagChangedError,
    BauplanAPIError,
    "Raised when a `bauplan.schema.Tag` changed during the request."
);
pyo3::create_exception!(
    bauplan.exceptions,
    TagNameConflictError,
    BauplanAPIError,
    "Raised when a `bauplan.schema.Tag` with the name already exists."
);
pyo3::create_exception!(
    bauplan.exceptions,
    TagNotFoundError,
    BauplanAPIError,
    "Raised when the referenced `bauplan.schema.Tag` does not exist."
);
pyo3::create_exception!(
    bauplan.exceptions,
    UnauthorizedError,
    BauplanAPIError,
    "Raised when the credentials are missing or not valid."
);
pyo3::create_exception!(
    bauplan.exceptions,
    UnsupportedTableKindError,
    BauplanAPIError,
    "Raised when the kind of the table does not support the operation."
);

// Non-HTTP errors
pyo3::create_exception!(
    bauplan.exceptions,
    BauplanJobError,
    BauplanError,
    "Base class for errors raised by bauplan job execution."
);
pyo3::create_exception!(
    bauplan.exceptions,
    BauplanQueryError,
    BauplanJobError,
    "Raised when a query job fails."
);
use crate::python::run::state::{TableCreatePlanApplyState, TableCreatePlanState};

/// Base class for errors raised during a table-create plan workflow.
#[pyclass(extends=BauplanError, subclass, module="bauplan.exceptions")]
pub(crate) struct TableCreatePlanError;

#[pymethods]
impl TableCreatePlanError {
    #[new]
    fn new() -> PyClassInitializer<Self> {
        PyClassInitializer::from(BauplanError).add_subclass(Self)
    }
}

/// Raised when a table-create plan job finishes in a non-success state.
#[pyclass(extends=TableCreatePlanError, module="bauplan.exceptions")]
pub(crate) struct TableCreatePlanStatusError {
    #[pyo3(get)]
    message: String,
    #[pyo3(get)]
    state: TableCreatePlanState,
}

impl TableCreatePlanStatusError {
    pub(crate) fn new_err(message: String, state: TableCreatePlanState) -> PyErr {
        PyErr::new::<Self, _>((message, state))
    }
}

#[pymethods]
impl TableCreatePlanStatusError {
    #[new]
    fn new(message: String, state: TableCreatePlanState) -> PyClassInitializer<Self> {
        PyClassInitializer::from(BauplanError)
            .add_subclass(TableCreatePlanError)
            .add_subclass(Self { message, state })
    }
}

/// Raised when a table-create plan apply job finishes in a non-success state.
#[pyclass(extends=BauplanError, module="bauplan.exceptions")]
pub(crate) struct TableCreatePlanApplyStatusError {
    #[pyo3(get)]
    message: String,
    #[pyo3(get)]
    state: TableCreatePlanApplyState,
}

impl TableCreatePlanApplyStatusError {
    pub(crate) fn new_err(message: String, state: TableCreatePlanApplyState) -> PyErr {
        PyErr::new::<Self, _>((message, state))
    }
}

#[pymethods]
impl TableCreatePlanApplyStatusError {
    #[new]
    fn new(message: String, state: TableCreatePlanApplyState) -> PyClassInitializer<Self> {
        PyClassInitializer::from(BauplanError).add_subclass(Self { message, state })
    }
}
