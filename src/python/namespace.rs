//! Namespace operations.

use bauplan_api::gropius::client::ClientError;
use bauplan_api::model::{CommitOptions, WithCommit};
use bauplan_api::namespace::{
    self, CreateNamespaceBody, CreateNamespaceError, DeleteNamespaceBody, DeleteNamespaceError,
    GetNamespaceError, NamespaceFilters,
};
use bauplan_api::pagination::ListQuery;
use pyo3::prelude::*;

use super::exceptions::api_error;
use super::paginate::PyIteratorAtCommit;
use super::{Client, commit::PyCommit};

/// A namespace, with its commit. A namespace is a container for organizing
/// tables.
#[pyclass(name = "Namespace", module = "bauplan.schema", frozen, get_all)]
pub(crate) struct PyNamespace {
    /// The name of the namespace.
    name: String,
    /// The commit the namespace was read or written at.
    commit: Py<PyCommit>,
}

#[pymethods]
impl PyNamespace {
    fn __repr__(&self) -> String {
        format!("Namespace(name={:?})", self.name)
    }
}

impl PyNamespace {
    fn new(py: Python<'_>, namespace: WithCommit<namespace::Namespace>) -> PyResult<Self> {
        Ok(Self {
            name: namespace.inner.name,
            commit: Py::new(py, PyCommit::from(namespace.commit))?,
        })
    }
}

/// A namespace in a list. A namespace is a container for organizing tables.
#[pyclass(
    name = "NamespaceIdentifier",
    module = "bauplan.schema",
    frozen,
    get_all
)]
pub(crate) struct PyNamespaceIdentifier {
    /// The name of the namespace.
    name: String,
}

#[pymethods]
impl PyNamespaceIdentifier {
    fn __repr__(&self) -> String {
        format!("NamespaceIdentifier(name={:?})", self.name)
    }
}

impl From<namespace::NamespaceIdentifier> for PyNamespaceIdentifier {
    fn from(namespace: namespace::NamespaceIdentifier) -> Self {
        Self {
            name: namespace.name,
        }
    }
}

#[pymethods]
impl Client {
    /// List the namespaces at a ref.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// for namespace in client.list_namespaces('main', name='^raw_'):
    ///     ...
    /// ```
    ///
    /// Parameters:
    ///     ref: The ref, a branch, a tag or a commit hash.
    ///     name: Optional, filter the namespaces by name. Use `^` and `$` to anchor it to the start or the end.
    ///     limit: Optional, max number of namespaces to get.
    /// Returns:
    ///     A `bauplan.schema.IteratorAtCommit` over `bauplan.schema.NamespaceIdentifier` objects. Its `commit_hash` is the hash of the commit `ref` points to, and all the pages are read at that commit.
    ///
    /// Raises:
    ///     `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
    ///     `bauplan.exceptions.BadRequestError`: if the name filter is not valid.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (
        r#ref: "str",
        *,
        name: "str | None" = None,
        limit: "int | None" = None,
    ) -> "IteratorAtCommit[NamespaceIdentifier]")]
    fn list_namespaces(
        &self,
        py: Python<'_>,
        r#ref: String,
        name: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<PyIteratorAtCommit> {
        let client = self.namespaces();
        let catalog = self.profile.catalog.clone();
        let filters = NamespaceFilters { name };
        PyIteratorAtCommit::new::<_, PyNamespaceIdentifier, _>(
            py,
            &r#ref,
            limit,
            move |py, at, pagination_token, limit| {
                let query = ListQuery {
                    pagination_token: pagination_token.map(str::to_owned),
                    limit: limit.and_then(|limit| u32::try_from(limit).ok()),
                    filters: filters.clone(),
                };
                let path = (catalog.clone(), at.to_owned());
                py.detach(|| client.list_namespaces(path, query))
                    .map_err(api_error)
            },
        )
    }

    /// Get a namespace at a ref.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// namespace = client.get_namespace('raw_data', 'main')
    /// ```
    ///
    /// Parameters:
    ///     namespace: The name of the namespace.
    ///     ref: The ref, a branch, a tag or a commit hash.
    /// Returns:
    ///     The `bauplan.schema.Namespace`, with the commit it was read at.
    ///
    /// Raises:
    ///     `bauplan.exceptions.NamespaceNotFoundError`: if the namespace does not exist.
    ///     `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (namespace: "str", r#ref: "str") -> "Namespace")]
    fn get_namespace(
        &self,
        py: Python<'_>,
        namespace: String,
        r#ref: String,
    ) -> PyResult<PyNamespace> {
        let client = self.namespaces();
        let path = (self.profile.catalog.clone(), r#ref, namespace);
        let namespace = py.detach(|| client.get_namespace(path));
        PyNamespace::new(py, namespace.map_err(api_error)?)
    }

    /// Check if a namespace exists at a ref.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// if client.has_namespace('raw_data', 'main'):
    ///     ...
    /// ```
    ///
    /// Parameters:
    ///     namespace: The name of the namespace.
    ///     ref: The ref, a branch, a tag or a commit hash.
    /// Returns:
    ///     A boolean for if the namespace exists.
    ///
    /// Raises:
    ///     `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (namespace: "str", r#ref: "str") -> "bool")]
    fn has_namespace(&self, py: Python<'_>, namespace: String, r#ref: String) -> PyResult<bool> {
        let client = self.namespaces();
        let path = (self.profile.catalog.clone(), r#ref, namespace);
        match py.detach(|| client.get_namespace(path)) {
            Ok(_) => Ok(true),
            Err(ClientError::Api(GetNamespaceError::NamespaceNotFound { .. })) => Ok(false),
            Err(err) => Err(api_error(err)),
        }
    }

    /// Create a namespace on a branch.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// namespace = client.create_namespace('raw_data', 'username.feature', if_not_exists=True)
    /// ```
    ///
    /// Parameters:
    ///     namespace: The name of the new namespace.
    ///     branch: The branch to create the namespace on.
    ///     commit_message: Optional, the commit message. Without it, the server writes one for the operation.
    ///     expected_head: Optional, create only if the branch points to this commit hash.
    ///     if_not_exists: If set to `True`, a namespace or a table with the same name is not an error.
    /// Returns:
    ///     The new `bauplan.schema.Namespace`, with the commit that creates it, or `None` when the name is in use and `if_not_exists` is set.
    ///
    /// Raises:
    ///     `bauplan.exceptions.NameConflictError`: if a namespace or a table with the same name exists.
    ///     `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
    ///     `bauplan.exceptions.NotABranchError`: if the ref is not a branch.
    ///     `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `expected_head`.
    ///     `bauplan.exceptions.ForbiddenError`: if the user can't create the namespace.
    ///     `bauplan.exceptions.BadRequestError`: if the name is not valid.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (
        namespace: "str",
        branch: "str",
        *,
        commit_message: "str | None" = None,
        expected_head: "str | None" = None,
        if_not_exists: "bool" = false,
    ) -> "Namespace | None")]
    fn create_namespace(
        &self,
        py: Python<'_>,
        namespace: String,
        branch: String,
        commit_message: Option<String>,
        expected_head: Option<String>,
        if_not_exists: bool,
    ) -> PyResult<Option<PyNamespace>> {
        let client = self.namespaces();
        let body = CreateNamespaceBody {
            name: namespace,
            commit: CommitOptions {
                message: commit_message,
                expected_head,
            },
        };
        let path = (self.profile.catalog.clone(), branch);
        match py.detach(|| client.create_namespace(path, body)) {
            Ok(created) => Ok(Some(PyNamespace::new(py, created)?)),
            Err(ClientError::Api(CreateNamespaceError::NameConflict { .. })) if if_not_exists => {
                Ok(None)
            }
            Err(err) => Err(api_error(err)),
        }
    }

    /// Delete an empty namespace from a branch.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// commit = client.delete_namespace('raw_data', 'username.feature', if_exists=True)
    /// ```
    ///
    /// Parameters:
    ///     namespace: The name of the namespace.
    ///     branch: The branch to delete the namespace from.
    ///     commit_message: Optional, the commit message. Without it, the server writes one for the operation.
    ///     expected_head: Optional, delete only if the branch points to this commit hash.
    ///     if_exists: If set to `True`, a missing namespace is not an error.
    /// Returns:
    ///     The `bauplan.schema.Commit` that deletes the namespace, or `None` when it does not exist and `if_exists` is set.
    ///
    /// Raises:
    ///     `bauplan.exceptions.NamespaceNotFoundError`: if the namespace does not exist.
    ///     `bauplan.exceptions.NamespaceNotEmptyError`: if the namespace still has tables.
    ///     `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
    ///     `bauplan.exceptions.NotABranchError`: if the ref is not a branch.
    ///     `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `expected_head`.
    ///     `bauplan.exceptions.ForbiddenError`: if the user can't delete the namespace.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (
        namespace: "str",
        branch: "str",
        *,
        commit_message: "str | None" = None,
        expected_head: "str | None" = None,
        if_exists: "bool" = false,
    ) -> "Commit | None")]
    fn delete_namespace(
        &self,
        py: Python<'_>,
        namespace: String,
        branch: String,
        commit_message: Option<String>,
        expected_head: Option<String>,
        if_exists: bool,
    ) -> PyResult<Option<PyCommit>> {
        let client = self.namespaces();
        let body = DeleteNamespaceBody {
            commit: CommitOptions {
                message: commit_message,
                expected_head,
            },
        };
        let path = (self.profile.catalog.clone(), branch, namespace);
        match py.detach(|| client.delete_namespace(path, Some(body))) {
            Ok(commit) => Ok(Some(commit.into())),
            Err(ClientError::Api(DeleteNamespaceError::NamespaceNotFound { .. })) if if_exists => {
                Ok(None)
            }
            Err(err) => Err(api_error(err)),
        }
    }
}
