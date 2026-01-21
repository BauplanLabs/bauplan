//! Python exception types.

use pyo3::PyTypeInfo;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

use crate::api::{ApiError, ApiErrorKind};

#[pymodule(submodule)]
pub mod exceptions {
    use super::*;

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
    use super::AccessDeniedError;
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
    use super::BauplanQueryError;
    #[pymodule_export]
    use super::InvalidPlanError;
    #[pymodule_export]
    use super::JobError;
    #[pymodule_export]
    use super::NoResultsFoundError;

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

pyo3::create_exception!(bauplan.exceptions, BauplanError, PyException);

/// An HTTP error from the API.
#[pyclass(extends=PyException, subclass, module="bauplan.exceptions")]
#[derive(Clone)]
pub(crate) struct BauplanHTTPError {
    #[pyo3(get)]
    code: u16,
    #[pyo3(get)]
    r#type: String,
    #[pyo3(get)]
    message: String,
}

impl BauplanHTTPError {
    fn into_err<T: PyTypeInfo>(self) -> PyErr {
        PyErr::new::<T, _>((self.code, self.r#type, self.message))
    }
}

impl From<&ApiError> for BauplanHTTPError {
    fn from(err: &ApiError) -> Self {
        match err {
            ApiError::ErrorResponse {
                status,
                kind,
                message,
            } => Self {
                code: status.as_u16(),
                r#type: kind.to_string(),
                message: message.clone().unwrap_or_default(),
            },
            ApiError::Other(status) => Self {
                code: status.as_u16(),
                r#type: String::new(),
                message: status.to_string(),
            },
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
pyo3::create_exception!(bauplan.exceptions, AccessDeniedError, BauplanHTTPError);
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
    BranchNotFoundError,
    ResourceNotFoundError
);
pyo3::create_exception!(
    bauplan.exceptions,
    NamespaceNotFoundError,
    ResourceNotFoundError
);
pyo3::create_exception!(bauplan.exceptions, RefNotFoundError, ResourceNotFoundError);
pyo3::create_exception!(
    bauplan.exceptions,
    TableNotFoundError,
    ResourceNotFoundError
);
pyo3::create_exception!(bauplan.exceptions, TagNotFoundError, ResourceNotFoundError);
pyo3::create_exception!(bauplan.exceptions, ApiMethodError, ResourceNotFoundError);

// 405 Method Not Allowed
pyo3::create_exception!(bauplan.exceptions, MethodNotAllowedError, BauplanHTTPError);
pyo3::create_exception!(bauplan.exceptions, ApiRouteError, MethodNotAllowedError);

// 409 Conflict
pyo3::create_exception!(bauplan.exceptions, ConflictError, BauplanHTTPError);
pyo3::create_exception!(bauplan.exceptions, NamespaceUnresolvedError, ConflictError);
pyo3::create_exception!(bauplan.exceptions, UpdateConflictError, ConflictError);
pyo3::create_exception!(bauplan.exceptions, BranchExistsError, UpdateConflictError);
pyo3::create_exception!(
    bauplan.exceptions,
    BranchHeadChangedError,
    UpdateConflictError
);
pyo3::create_exception!(bauplan.exceptions, MergeConflictError, UpdateConflictError);
pyo3::create_exception!(
    bauplan.exceptions,
    NamespaceExistsError,
    UpdateConflictError
);
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
pyo3::create_exception!(bauplan.exceptions, TagExistsError, UpdateConflictError);

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
pyo3::create_exception!(bauplan.exceptions, JobError, BauplanError);
pyo3::create_exception!(bauplan.exceptions, BauplanQueryError, JobError);
pyo3::create_exception!(bauplan.exceptions, NoResultsFoundError, BauplanError);
pyo3::create_exception!(bauplan.exceptions, InvalidPlanError, BauplanError);

impl From<ApiError> for PyErr {
    fn from(err: ApiError) -> PyErr {
        let parent = BauplanHTTPError::from(&err);

        match &err {
            ApiError::ErrorResponse { kind, .. } => match kind {
                ApiErrorKind::BranchExists => parent.into_err::<BranchExistsError>(),
                ApiErrorKind::BranchHeadChanged => parent.into_err::<BranchHeadChangedError>(),
                ApiErrorKind::BranchNotFound => parent.into_err::<BranchNotFoundError>(),
                ApiErrorKind::CreateBranchForbidden => {
                    parent.into_err::<CreateBranchForbiddenError>()
                }
                ApiErrorKind::CreateNamespaceForbidden => {
                    parent.into_err::<CreateNamespaceForbiddenError>()
                }
                ApiErrorKind::CreateTagForbidden => parent.into_err::<CreateTagForbiddenError>(),
                ApiErrorKind::DeleteBranchForbidden => {
                    parent.into_err::<DeleteBranchForbiddenError>()
                }
                ApiErrorKind::DeleteNamespaceForbidden => {
                    parent.into_err::<DeleteNamespaceForbiddenError>()
                }
                ApiErrorKind::DeleteTableForbidden => {
                    parent.into_err::<DeleteTableForbiddenError>()
                }
                ApiErrorKind::DeleteTagForbidden => parent.into_err::<DeleteTagForbiddenError>(),
                ApiErrorKind::InvalidRef => parent.into_err::<InvalidRefError>(),
                ApiErrorKind::MergeConflict => parent.into_err::<MergeConflictError>(),
                ApiErrorKind::MergeForbidden => parent.into_err::<MergeForbiddenError>(),
                ApiErrorKind::NamespaceUnresolved => parent.into_err::<NamespaceUnresolvedError>(),
                ApiErrorKind::NamespaceExists => parent.into_err::<NamespaceExistsError>(),
                ApiErrorKind::NamespaceIsNotEmpty => parent.into_err::<NamespaceIsNotEmptyError>(),
                ApiErrorKind::NamespaceNotFound => parent.into_err::<NamespaceNotFoundError>(),
                ApiErrorKind::NotABranchRef => parent.into_err::<NotABranchRefError>(),
                ApiErrorKind::NotATagRef => parent.into_err::<NotATagRefError>(),
                ApiErrorKind::NotAWriteBranchRef => parent.into_err::<NotAWriteBranchRefError>(),
                ApiErrorKind::RefNotFound => parent.into_err::<RefNotFoundError>(),
                ApiErrorKind::RenameBranchForbidden => {
                    parent.into_err::<RenameBranchForbiddenError>()
                }
                ApiErrorKind::RenameTagForbidden => parent.into_err::<RenameTagForbiddenError>(),
                ApiErrorKind::RevertDestinationTableExists => {
                    parent.into_err::<RevertDestinationTableExistsError>()
                }
                ApiErrorKind::RevertIdenticalTable => {
                    parent.into_err::<RevertIdenticalTableError>()
                }
                ApiErrorKind::RevertTableForbidden => {
                    parent.into_err::<RevertTableForbiddenError>()
                }
                ApiErrorKind::SameRef => parent.into_err::<SameRefError>(),
                ApiErrorKind::TableNotFound => parent.into_err::<TableNotFoundError>(),
                ApiErrorKind::TagExists => parent.into_err::<TagExistsError>(),
                ApiErrorKind::TagNotFound => parent.into_err::<TagNotFoundError>(),
                ApiErrorKind::Unknown(_) => match parent.code {
                    400 => parent.into_err::<InvalidDataError>(),
                    401 => parent.into_err::<UnauthorizedError>(),
                    403 => parent.into_err::<AccessDeniedError>(),
                    404 => parent.into_err::<ResourceNotFoundError>(),
                    405 => parent.into_err::<ApiRouteError>(),
                    409 => parent.into_err::<UpdateConflictError>(),
                    429 => parent.into_err::<TooManyRequestsError>(),
                    500 => parent.into_err::<InternalError>(),
                    502 => parent.into_err::<BadGatewayError>(),
                    503 => parent.into_err::<ServiceUnavailableError>(),
                    504 => parent.into_err::<GatewayTimeoutError>(),
                    _ => parent.into_err::<BauplanHTTPError>(),
                },
            },
            ApiError::Other(_) => match parent.code {
                400 => parent.into_err::<InvalidDataError>(),
                401 => parent.into_err::<UnauthorizedError>(),
                403 => parent.into_err::<AccessDeniedError>(),
                404 => parent.into_err::<ResourceNotFoundError>(),
                405 => parent.into_err::<ApiRouteError>(),
                409 => parent.into_err::<UpdateConflictError>(),
                429 => parent.into_err::<TooManyRequestsError>(),
                500 => parent.into_err::<InternalError>(),
                502 => parent.into_err::<BadGatewayError>(),
                503 => parent.into_err::<ServiceUnavailableError>(),
                504 => parent.into_err::<GatewayTimeoutError>(),
                _ => parent.into_err::<BauplanHTTPError>(),
            },
        }
    }
}
