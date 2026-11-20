//! Namespace operations.

use bauplan_api::gropius::client::ClientError;
use bauplan_api::model::CommitOptions;
use bauplan_api::namespace::{
    self, CreateNamespaceBody, CreateNamespaceError, DeleteNamespaceBody, DeleteNamespaceError,
    NamespaceFilters,
};
use bauplan_api::pagination::ListQuery;
use pyo3::prelude::*;

use super::exceptions::api_error;
use super::paginate::{PyPaginator, from_api_page};
use super::{Client, commit::PyCommit};

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
    /// Get the namespaces at a ref.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// for namespace in client.get_namespaces('main', name='^raw_'):
    ///     ...
    /// ```
    ///
    /// Parameters:
    ///     ref: The ref, a branch, a tag or a commit hash.
    ///     name: Filter the namespaces by name, `^` and `$` anchor it to the start or the end.
    ///     limit: Optional, max number of namespaces to get.
    /// Returns:
    ///     An iterator over `bauplan.schema.NamespaceIdentifier` objects.
    ///
    /// Raises:
    ///     `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
    ///     `bauplan.exceptions.BadRequestError`: if the name filter is not valid.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    #[pyo3(signature = (
        r#ref: "str",
        *,
        name: "str | None" = None,
        limit: "int | None" = None,
    ) -> "typing.Iterator[NamespaceIdentifier]")]
    fn get_namespaces(
        &self,
        py: Python<'_>,
        r#ref: String,
        name: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<PyPaginator> {
        let client = self.namespaces()?;
        let catalog = self.profile.catalog.clone();
        let filters = NamespaceFilters { name };
        let mut commit_hash = None;
        PyPaginator::new(py, limit, move |py, pagination_token, limit| {
            let query = ListQuery {
                pagination_token: pagination_token.map(str::to_owned),
                limit: limit.and_then(|limit| u32::try_from(limit).ok()),
                filters: filters.clone(),
            };
            // We read the next pages at the commit of the first one, so the list
            // stays the same if the ref moves meanwhile.
            let at = commit_hash.clone().unwrap_or_else(|| r#ref.clone());
            let page = py.detach(|| client.list_namespaces((catalog.clone(), at), query));
            let page = page.map_err(api_error)?;
            commit_hash.get_or_insert(page.commit_hash);
            Ok(from_api_page::<_, PyNamespaceIdentifier>(page.inner))
        })
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
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    #[pyo3(signature = (namespace: "str", r#ref: "str") -> "bool")]
    fn has_namespace(&self, py: Python<'_>, namespace: String, r#ref: String) -> PyResult<bool> {
        let client = self.namespaces()?;
        // There is no endpoint for a single namespace, so we search the list
        // for the exact name.
        let query = ListQuery {
            pagination_token: None,
            limit: Some(1),
            filters: NamespaceFilters {
                name: Some(format!("^{namespace}$")),
            },
        };
        let path = (self.profile.catalog.clone(), r#ref);
        let page = py.detach(|| client.list_namespaces(path, query));
        Ok(!page.map_err(api_error)?.inner.items.is_empty())
    }

    /// Create a namespace on a branch.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// commit = client.create_namespace('raw_data', 'username.feature', if_not_exists=True)
    /// ```
    ///
    /// Parameters:
    ///     namespace: The name of the new namespace.
    ///     branch: The branch to create the namespace on.
    ///     commit_body: Optional, the body of the commit message.
    ///     if_head: Optional, create only if the branch points to this commit hash.
    ///     if_not_exists: If set to `True`, a namespace or a table with the same name is not an error.
    /// Returns:
    ///     The `bauplan.schema.Commit` that creates the namespace, or `None` when the name is in use and `if_not_exists` is set.
    ///
    /// Raises:
    ///     `bauplan.exceptions.NameConflictError`: if a namespace or a table with the same name exists.
    ///     `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
    ///     `bauplan.exceptions.NotABranchRefError`: if the ref is not a branch.
    ///     `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `if_head`.
    ///     `bauplan.exceptions.ForbiddenError`: if the user can't create the namespace.
    ///     `bauplan.exceptions.BadRequestError`: if the name is not valid.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    #[pyo3(signature = (
        namespace: "str",
        branch: "str",
        *,
        commit_body: "str | None" = None,
        if_head: "str | None" = None,
        if_not_exists: "bool" = false,
    ) -> "Commit | None")]
    fn create_namespace(
        &self,
        py: Python<'_>,
        namespace: String,
        branch: String,
        commit_body: Option<String>,
        if_head: Option<String>,
        if_not_exists: bool,
    ) -> PyResult<Option<PyCommit>> {
        let client = self.namespaces()?;
        let body = CreateNamespaceBody {
            name: namespace,
            commit: CommitOptions {
                body: commit_body,
                expected_commit_hash: if_head,
            },
        };
        let path = (self.profile.catalog.clone(), branch);
        match py.detach(|| client.create_namespace(path, body)) {
            Ok(created) => Ok(Some(created.commit.into())),
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
    ///     commit_body: Optional, the body of the commit message.
    ///     if_head: Optional, delete only if the branch points to this commit hash.
    ///     if_exists: If set to `True`, a missing namespace is not an error.
    /// Returns:
    ///     The `bauplan.schema.Commit` that deletes the namespace, or `None` when it does not exist and `if_exists` is set.
    ///
    /// Raises:
    ///     `bauplan.exceptions.NamespaceNotFoundError`: if the namespace does not exist.
    ///     `bauplan.exceptions.NamespaceIsNotEmptyError`: if the namespace still has tables.
    ///     `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
    ///     `bauplan.exceptions.NotABranchRefError`: if the ref is not a branch.
    ///     `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `if_head`.
    ///     `bauplan.exceptions.ForbiddenError`: if the user can't delete the namespace.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    #[pyo3(signature = (
        namespace: "str",
        branch: "str",
        *,
        commit_body: "str | None" = None,
        if_head: "str | None" = None,
        if_exists: "bool" = false,
    ) -> "Commit | None")]
    fn delete_namespace(
        &self,
        py: Python<'_>,
        namespace: String,
        branch: String,
        commit_body: Option<String>,
        if_head: Option<String>,
        if_exists: bool,
    ) -> PyResult<Option<PyCommit>> {
        let client = self.namespaces()?;
        let body = DeleteNamespaceBody {
            commit: CommitOptions {
                body: commit_body,
                expected_commit_hash: if_head,
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
