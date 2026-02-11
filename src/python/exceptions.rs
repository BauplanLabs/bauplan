//! Python exception types.

use pyo3::prelude::*;

use crate::{
    api::{ApiError, ApiErrorKind},
    python::ClientError,
};

#[pymodule(submodule)]
pub mod exceptions {
    use super::*;

    #[pymodule_export]
    use crate::api::ApiErrorKind;

    // Re-export exception types into the module.
    #[pymodule_export]
    use super::BauplanError;
    #[pymodule_export]
    use super::BauplanHTTPError;

    // 400 Bad Request
    #[pymodule_export]
    use super::BadRequestError;
    #[pymodule_export]
    use super::InvalidDataError;
    #[pymodule_export]
    use super::InvalidRefError;
    #[pymodule_export]
    use super::NotABranchRefError;
    #[pymodule_export]
    use super::NotATagRefError;
    #[pymodule_export]
    use super::NotAWriteBranchRefError;
    #[pymodule_export]
    use super::SameRefError;

    // 401 Unauthorized
    #[pymodule_export]
    use super::UnauthorizedError;

    // 403 Forbidden
    #[pymodule_export]
    use super::CreateBranchForbiddenError;
    #[pymodule_export]
    use super::CreateNamespaceForbiddenError;
    #[pymodule_export]
    use super::CreateTagForbiddenError;
    #[pymodule_export]
    use super::DeleteBranchForbiddenError;
    #[pymodule_export]
    use super::DeleteNamespaceForbiddenError;
    #[pymodule_export]
    use super::DeleteTableForbiddenError;
    #[pymodule_export]
    use super::DeleteTagForbiddenError;
    #[pymodule_export]
    use super::ForbiddenError;
    #[pymodule_export]
    use super::MergeForbiddenError;
    #[pymodule_export]
    use super::RenameBranchForbiddenError;
    #[pymodule_export]
    use super::RenameTagForbiddenError;
    #[pymodule_export]
    use super::RevertTableForbiddenError;

    // 404 Not Found
    #[pymodule_export]
    use super::ApiMethodError;
    #[pymodule_export]
    use super::BranchNotFoundError;
    #[pymodule_export]
    use super::NamespaceNotFoundError;
    #[pymodule_export]
    use super::NotFoundError;
    #[pymodule_export]
    use super::RefNotFoundError;
    #[pymodule_export]
    use super::ResourceNotFoundError;
    #[pymodule_export]
    use super::TableNotFoundError;
    #[pymodule_export]
    use super::TagNotFoundError;

    // 405 Method Not Allowed
    #[pymodule_export]
    use super::ApiRouteError;
    #[pymodule_export]
    use super::MethodNotAllowedError;

    // 409 Conflict
    #[pymodule_export]
    use super::BranchExistsError;
    #[pymodule_export]
    use super::BranchHeadChangedError;
    #[pymodule_export]
    use super::ConflictError;
    #[pymodule_export]
    use super::MergeConflictError;
    #[pymodule_export]
    use super::NamespaceExistsError;
    #[pymodule_export]
    use super::NamespaceIsNotEmptyError;
    #[pymodule_export]
    use super::NamespaceUnresolvedError;
    #[pymodule_export]
    use super::RevertDestinationTableExistsError;
    #[pymodule_export]
    use super::RevertIdenticalTableError;
    #[pymodule_export]
    use super::TagExistsError;
    #[pymodule_export]
    use super::UpdateConflictError;

    // 429 Too Many Requests
    #[pymodule_export]
    use super::TooManyRequestsError;

    // 5xx Server Errors
    #[pymodule_export]
    use super::BadGatewayError;
    #[pymodule_export]
    use super::GatewayTimeoutError;
    #[pymodule_export]
    use super::InternalError;
    #[pymodule_export]
    use super::ServiceUnavailableError;

    // Non-HTTP errors
    #[pymodule_export]
    use super::BauplanJobError;
    #[pymodule_export]
    use super::BauplanQueryError;
    #[pymodule_export]
    use super::InvalidPlanError;
    #[pymodule_export]
    use super::NoResultsFoundError;
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

#[pyclass(extends=pyo3::exceptions::PyException, subclass, module="bauplan.exceptions")]
pub(crate) struct BauplanError;

impl BauplanError {
    pub(crate) fn new_err(msg: impl std::fmt::Display) -> PyErr {
        PyErr::new::<Self, _>(msg.to_string())
    }
}

/// An HTTP error from the API.
#[pyclass(extends=BauplanError, subclass, module="bauplan.exceptions")]
#[derive(Clone)]
pub(crate) struct BauplanHTTPError {
    #[pyo3(get)]
    code: u16,
    #[pyo3(get)]
    r#type: String,
    #[pyo3(get)]
    message: String,
    #[pyo3(get)]
    kind: Option<ApiErrorKind>,
}

#[pymethods]
impl BauplanHTTPError {
    #[new]
    #[pyo3(signature = (code, r#type, message, kind=None))]
    fn new(
        code: u16,
        r#type: String,
        message: String,
        kind: Option<ApiErrorKind>,
    ) -> (Self, BauplanError) {
        (
            Self {
                code,
                r#type,
                message,
                kind,
            },
            BauplanError,
        )
    }
}

impl From<ClientError> for PyErr {
    fn from(err: ClientError) -> Self {
        match err {
            ClientError::Api(api_error) => api_error.into_py_err(),
            _ => BauplanError::new_err(err.to_string()),
        }
    }
}

impl ApiError {
    pub(crate) fn into_py_err(self) -> PyErr {
        let (code, kind, message) = match self {
            ApiError::ErrorResponse {
                status,
                kind,
                message,
            } => (status.as_u16(), Some(kind), message.unwrap_or_default()),
            ApiError::Other(status) => (status.as_u16(), None, status.to_string()),
            ApiError::InvalidResponse(status) => (
                status.as_u16(),
                None,
                format!("Invalid response ({status})"),
            ),
        };

        let type_str = kind.as_ref().map(|k| k.to_string()).unwrap_or_default();
        let args = (code, type_str, message, kind);

        // Pick the exception subclass based on kind, falling back to
        // the status code for errors without a recognized type.
        if let Some(ref kind) = args.3 {
            match kind {
                // 400
                ApiErrorKind::InvalidRef { .. } => PyErr::new::<InvalidRefError, _>(args),
                ApiErrorKind::NotABranchRef { .. } => PyErr::new::<NotABranchRefError, _>(args),
                ApiErrorKind::NotATagRef { .. } => PyErr::new::<NotATagRefError, _>(args),
                ApiErrorKind::NotAWriteBranchRef { .. } => {
                    PyErr::new::<NotAWriteBranchRefError, _>(args)
                }
                ApiErrorKind::SameRef { .. } => PyErr::new::<SameRefError, _>(args),
                // 403
                ApiErrorKind::CreateBranchForbidden { .. } => {
                    PyErr::new::<CreateBranchForbiddenError, _>(args)
                }
                ApiErrorKind::CreateNamespaceForbidden { .. } => {
                    PyErr::new::<CreateNamespaceForbiddenError, _>(args)
                }
                ApiErrorKind::CreateTagForbidden { .. } => {
                    PyErr::new::<CreateTagForbiddenError, _>(args)
                }
                ApiErrorKind::DeleteBranchForbidden { .. } => {
                    PyErr::new::<DeleteBranchForbiddenError, _>(args)
                }
                ApiErrorKind::DeleteNamespaceForbidden { .. } => {
                    PyErr::new::<DeleteNamespaceForbiddenError, _>(args)
                }
                ApiErrorKind::DeleteTableForbidden { .. } => {
                    PyErr::new::<DeleteTableForbiddenError, _>(args)
                }
                ApiErrorKind::DeleteTagForbidden { .. } => {
                    PyErr::new::<DeleteTagForbiddenError, _>(args)
                }
                ApiErrorKind::MergeForbidden { .. } => PyErr::new::<MergeForbiddenError, _>(args),
                ApiErrorKind::RenameBranchForbidden { .. } => {
                    PyErr::new::<RenameBranchForbiddenError, _>(args)
                }
                ApiErrorKind::RenameTagForbidden { .. } => {
                    PyErr::new::<RenameTagForbiddenError, _>(args)
                }
                ApiErrorKind::RevertTableForbidden { .. } => {
                    PyErr::new::<RevertTableForbiddenError, _>(args)
                }
                // 404
                ApiErrorKind::BranchNotFound { .. } => PyErr::new::<BranchNotFoundError, _>(args),
                ApiErrorKind::NamespaceNotFound { .. } => {
                    PyErr::new::<NamespaceNotFoundError, _>(args)
                }
                ApiErrorKind::RefNotFound { .. } => PyErr::new::<RefNotFoundError, _>(args),
                ApiErrorKind::TableNotFound { .. } => PyErr::new::<TableNotFoundError, _>(args),
                ApiErrorKind::TagNotFound { .. } => PyErr::new::<TagNotFoundError, _>(args),
                // 409
                ApiErrorKind::BranchExists { .. } => PyErr::new::<BranchExistsError, _>(args),
                ApiErrorKind::BranchHeadChanged { .. } => {
                    PyErr::new::<BranchHeadChangedError, _>(args)
                }
                ApiErrorKind::MergeConflict { .. } => PyErr::new::<MergeConflictError, _>(args),
                ApiErrorKind::NamespaceExists { .. } => PyErr::new::<NamespaceExistsError, _>(args),
                ApiErrorKind::NamespaceIsNotEmpty { .. } => {
                    PyErr::new::<NamespaceIsNotEmptyError, _>(args)
                }
                ApiErrorKind::NamespaceUnresolved { .. } => {
                    PyErr::new::<NamespaceUnresolvedError, _>(args)
                }
                ApiErrorKind::RevertDestinationTableExists { .. } => {
                    PyErr::new::<RevertDestinationTableExistsError, _>(args)
                }
                ApiErrorKind::RevertIdenticalTable { .. } => {
                    PyErr::new::<RevertIdenticalTableError, _>(args)
                }
                ApiErrorKind::TagExists { .. } => PyErr::new::<TagExistsError, _>(args),
            }
        } else {
            match code {
                400 => PyErr::new::<BadRequestError, _>(args),
                401 => PyErr::new::<UnauthorizedError, _>(args),
                403 => PyErr::new::<ForbiddenError, _>(args),
                404 => PyErr::new::<NotFoundError, _>(args),
                405 => PyErr::new::<MethodNotAllowedError, _>(args),
                409 => PyErr::new::<ConflictError, _>(args),
                429 => PyErr::new::<TooManyRequestsError, _>(args),
                500 => PyErr::new::<InternalError, _>(args),
                502 => PyErr::new::<BadGatewayError, _>(args),
                503 => PyErr::new::<ServiceUnavailableError, _>(args),
                504 => PyErr::new::<GatewayTimeoutError, _>(args),
                _ => PyErr::new::<BauplanHTTPError, _>(args),
            }
        }
    }
}

// 400 Bad Request
pyo3::create_exception!(bauplan.exceptions, BadRequestError, BauplanHTTPError);
pyo3::create_exception!(bauplan.exceptions, InvalidDataError, BadRequestError);
pyo3::create_exception!(bauplan.exceptions, InvalidRefError, BadRequestError);
pyo3::create_exception!(bauplan.exceptions, NotABranchRefError, InvalidRefError);
pyo3::create_exception!(bauplan.exceptions, NotATagRefError, InvalidRefError);
pyo3::create_exception!(
    bauplan.exceptions,
    NotAWriteBranchRefError,
    NotABranchRefError
);
pyo3::create_exception!(bauplan.exceptions, SameRefError, InvalidRefError);

// 401 Unauthorized
pyo3::create_exception!(bauplan.exceptions, UnauthorizedError, BauplanHTTPError);

// 403 Forbidden
pyo3::create_exception!(bauplan.exceptions, ForbiddenError, BauplanHTTPError);
pyo3::create_exception!(
    bauplan.exceptions,
    CreateBranchForbiddenError,
    ForbiddenError
);
pyo3::create_exception!(
    bauplan.exceptions,
    CreateNamespaceForbiddenError,
    ForbiddenError
);
pyo3::create_exception!(bauplan.exceptions, CreateTagForbiddenError, ForbiddenError);
pyo3::create_exception!(
    bauplan.exceptions,
    DeleteBranchForbiddenError,
    ForbiddenError
);
pyo3::create_exception!(
    bauplan.exceptions,
    DeleteNamespaceForbiddenError,
    ForbiddenError
);
pyo3::create_exception!(
    bauplan.exceptions,
    DeleteTableForbiddenError,
    ForbiddenError
);
pyo3::create_exception!(bauplan.exceptions, DeleteTagForbiddenError, ForbiddenError);
pyo3::create_exception!(bauplan.exceptions, MergeForbiddenError, ForbiddenError);
pyo3::create_exception!(
    bauplan.exceptions,
    RenameBranchForbiddenError,
    ForbiddenError
);
pyo3::create_exception!(bauplan.exceptions, RenameTagForbiddenError, ForbiddenError);
pyo3::create_exception!(
    bauplan.exceptions,
    RevertTableForbiddenError,
    ForbiddenError
);

// 404 Not Found
pyo3::create_exception!(bauplan.exceptions, NotFoundError, BauplanHTTPError);
pyo3::create_exception!(bauplan.exceptions, ResourceNotFoundError, NotFoundError);
pyo3::create_exception!(
    bauplan.exceptions,
    TableNotFoundError,
    ResourceNotFoundError
);
pyo3::create_exception!(
    bauplan.exceptions,
    NamespaceNotFoundError,
    ResourceNotFoundError
);
pyo3::create_exception!(
    bauplan.exceptions,
    BranchNotFoundError,
    ResourceNotFoundError
);
pyo3::create_exception!(bauplan.exceptions, RefNotFoundError, ResourceNotFoundError);
pyo3::create_exception!(bauplan.exceptions, TagNotFoundError, ResourceNotFoundError);
pyo3::create_exception!(bauplan.exceptions, ApiMethodError, ResourceNotFoundError);

// 405 Method Not Allowed
pyo3::create_exception!(bauplan.exceptions, MethodNotAllowedError, BauplanHTTPError);
pyo3::create_exception!(bauplan.exceptions, ApiRouteError, MethodNotAllowedError);

// 409 Conflict
pyo3::create_exception!(bauplan.exceptions, ConflictError, BauplanHTTPError);
pyo3::create_exception!(bauplan.exceptions, UpdateConflictError, ConflictError);
pyo3::create_exception!(bauplan.exceptions, BranchExistsError, UpdateConflictError);
pyo3::create_exception!(bauplan.exceptions, TagExistsError, UpdateConflictError);
pyo3::create_exception!(
    bauplan.exceptions,
    NamespaceExistsError,
    UpdateConflictError
);
pyo3::create_exception!(bauplan.exceptions, NamespaceUnresolvedError, ConflictError);
pyo3::create_exception!(
    bauplan.exceptions,
    BranchHeadChangedError,
    UpdateConflictError
);
pyo3::create_exception!(bauplan.exceptions, MergeConflictError, UpdateConflictError);
pyo3::create_exception!(
    bauplan.exceptions,
    NamespaceIsNotEmptyError,
    UpdateConflictError
);
pyo3::create_exception!(
    bauplan.exceptions,
    RevertDestinationTableExistsError,
    UpdateConflictError
);
pyo3::create_exception!(
    bauplan.exceptions,
    RevertIdenticalTableError,
    UpdateConflictError
);

// 429 Too Many Requests
pyo3::create_exception!(bauplan.exceptions, TooManyRequestsError, BauplanHTTPError);

// 5xx Server Errors
pyo3::create_exception!(bauplan.exceptions, InternalError, BauplanHTTPError);
pyo3::create_exception!(bauplan.exceptions, BadGatewayError, BauplanHTTPError);
pyo3::create_exception!(
    bauplan.exceptions,
    ServiceUnavailableError,
    BauplanHTTPError
);
pyo3::create_exception!(bauplan.exceptions, GatewayTimeoutError, BauplanHTTPError);

// Non-HTTP errors
pyo3::create_exception!(bauplan.exceptions, BauplanJobError, BauplanError);
pyo3::create_exception!(bauplan.exceptions, BauplanQueryError, BauplanJobError);
pyo3::create_exception!(bauplan.exceptions, NoResultsFoundError, BauplanError);
pyo3::create_exception!(bauplan.exceptions, InvalidPlanError, BauplanError);
use crate::python::run::state::{TableCreatePlanApplyState, TableCreationPlanState};

#[pyclass(extends=BauplanError, subclass, module="bauplan.exceptions")]
pub(crate) struct TableCreatePlanError;

#[pymethods]
impl TableCreatePlanError {
    #[new]
    fn new() -> (Self, BauplanError) {
        (Self, BauplanError)
    }
}

#[pyclass(extends=TableCreatePlanError, module="bauplan.exceptions")]
pub(crate) struct TableCreatePlanStatusError {
    #[pyo3(get)]
    message: String,
    #[pyo3(get)]
    state: TableCreationPlanState,
}

impl TableCreatePlanStatusError {
    pub(crate) fn new_err(message: String, state: TableCreationPlanState) -> PyErr {
        PyErr::new::<Self, _>((message, state))
    }
}

#[pymethods]
impl TableCreatePlanStatusError {
    #[new]
    fn new(message: String, state: TableCreationPlanState) -> PyClassInitializer<Self> {
        PyClassInitializer::from(BauplanError)
            .add_subclass(TableCreatePlanError)
            .add_subclass(Self { message, state })
    }
}

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
    fn new(message: String, state: TableCreatePlanApplyState) -> (Self, BauplanError) {
        (Self { message, state }, BauplanError)
    }
}
