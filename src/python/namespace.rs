//! Namespace operations.

use pyo3::{exceptions::PyTypeError, prelude::*};
use std::collections::BTreeMap;

use crate::{
    ApiErrorKind, ApiRequest, CatalogRef,
    commit::CommitOptions,
    namespace::{CreateNamespace, DeleteNamespace, GetNamespace, GetNamespaces, Namespace},
    python::paginate::PyPaginator,
    python::refs::{BranchArg, RefArg},
};

use super::Client;

/// Accepts a namespace name or Namespace object (from which the name is
/// extracted).
pub(crate) struct NamespaceArg(pub String);

impl<'a, 'py> FromPyObject<'a, 'py> for NamespaceArg {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(s) = ob.extract::<String>() {
            Ok(NamespaceArg(s))
        } else if let Ok(ns) = ob.extract::<Namespace>() {
            Ok(NamespaceArg(ns.name))
        } else {
            Err(PyTypeError::new_err("expected str or Namespace"))
        }
    }
}

#[pymethods]
impl Client {
    /// Get the available data namespaces in the Bauplan catalog branch.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python fixture:my_namespace
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// for namespace in client.get_namespaces('my_ref_or_branch_name'):
    ///     ...
    /// ```
    ///
    /// Parameters:
    ///     ref: The ref, branch name or tag name to retrieve the namespaces from.
    ///     filter_by_name: Optional, filter the namespaces by name.
    ///     limit: Optional, max number of namespaces to get.
    ///
    /// Raises:
    ///     `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `ValueError`: if one or more parameters are invalid.
    ///
    /// Yields:
    ///     `bauplan.schema.Namespace` objects.
    #[pyo3(signature = (
        r#ref: "str | Ref",
        *,
        filter_by_name: "str | None" = None,
        limit: "int | None" = None,
    ) -> "typing.Iterator[Namespace]")]
    fn get_namespaces(
        &self,
        py: Python<'_>,
        r#ref: RefArg,
        filter_by_name: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<PyPaginator> {
        let r#ref = r#ref.0;
        let profile = self.profile.clone();
        let agent = self.agent.clone();
        PyPaginator::new(py, limit, move |py, token, limit| {
            let req = GetNamespaces {
                at_ref: &r#ref,
                filter_by_name: filter_by_name.as_deref(),
            }
            .paginate(token, limit);

            Ok(super::roundtrip(py, req, &profile, &agent)?)
        })
    }

    /// Get a namespace.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python fixture:my_namespace
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// namespace =  client.get_namespace(
    ///     namespace='my_namespace_name',
    ///     ref='my_branch_name',
    /// )
    /// ```
    ///
    /// Parameters:
    ///     namespace: The name of the namespace to get.
    ///     ref: The ref, branch name or tag name to check the namespace on.
    /// Returns:
    ///     A `bauplan.schema.Namespace` object.
    ///
    /// Raises:
    ///     `bauplan.exceptions.NamespaceNotFoundError`: if the namespace does not exist.
    ///     `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `ValueError`: if one or more parameters are invalid.
    #[pyo3(signature = (
        namespace: "str | Namespace",
        r#ref: "str | Ref",
    ) -> "Namespace")]
    fn get_namespace(&self, py: Python<'_>, namespace: NamespaceArg, r#ref: RefArg) -> PyResult<Namespace> {
        let req = GetNamespace {
            name: &namespace.0,
            at_ref: &r#ref.0,
        };

        Ok(super::roundtrip(py, req, &self.profile, &self.agent)?)
    }

    /// Create a new namespace at a given branch.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python fixture:my_branch
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// assert client.create_namespace(
    ///     namespace='my_namespace_name',
    ///     branch='my_branch_name',
    ///     if_not_exists=True,
    /// )
    /// ```
    ///
    /// Parameters:
    ///     namespace: The name of the namespace.
    ///     branch: The name of the branch to create the namespace on.
    ///     commit_body: Optional, the commit body to attach to the operation.
    ///     commit_properties: Optional, a list of properties to attach to the commit.
    ///     if_not_exists: If set to `True`, the namespace will not be created if it already exists.
    /// Returns:
    ///     The created `bauplan.schema.Namespace` object.
    ///
    /// Raises:
    ///     `bauplan.exceptions.CreateNamespaceForbiddenError`: if the user does not have access to create the namespace.
    ///     `bauplan.exceptions.BranchNotFoundError`: if the branch does not exist.
    ///     `bauplan.exceptions.NotAWriteBranchRefError`: if the destination branch is not a writable ref.
    ///     `bauplan.exceptions.BranchHeadChangedError`: if the branch head hash has changed.
    ///     `bauplan.exceptions.NamespaceExistsError`: if the namespace already exists.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `ValueError`: if one or more parameters are invalid.
    #[pyo3(signature = (
        namespace: "str | Namespace",
        branch: "str | Branch",
        *,
        commit_body: "str | None" = None,
        commit_properties: "dict[str, str] | None" = None,
        if_not_exists: "bool" = false,
    ) -> "Namespace")]
    fn create_namespace(
        &self, py: Python<'_>,
        namespace: NamespaceArg,
        branch: BranchArg,
        commit_body: Option<&str>,
        commit_properties: Option<BTreeMap<String, String>>,
        if_not_exists: bool,
    ) -> PyResult<Namespace> {
        let namespace = &namespace.0;
        let branch = &branch.0;
        let commit_properties = commit_properties.unwrap_or_default();
        let properties = commit_properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let req = CreateNamespace {
            name: namespace,
            branch,
            commit: CommitOptions {
                body: commit_body,
                properties,
            },
        };

        match super::roundtrip(py, req, &self.profile, &self.agent) {
            Ok(ns) => Ok(ns),
            Err(e) => {
                if if_not_exists
                    && let Some(ApiErrorKind::NamespaceExists { namespace_name, .. }) = e.kind()
                {
                    Ok(Namespace {
                        name: namespace_name.clone(),
                    })
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// Delete a namespace.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python fixture:my_branch fixture:my_namespace
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// assert client.delete_namespace(
    ///     namespace='my_namespace_name',
    ///     branch='my_branch_name',
    /// )
    /// ```
    ///
    /// Parameters:
    ///     namespace: The name of the namespace to delete.
    ///     branch: The name of the branch to delete the namespace from.
    ///     commit_body: Optional, the commit body to attach to the operation.
    ///     commit_properties: Optional, a list of properties to attach to the commit.
    ///     if_exists: If set to `True`, the namespace will not raise an error if it does not exist.
    /// Returns:
    ///     A `bauplan.schema.Branch` object pointing to head.
    ///
    /// Raises:
    ///     `bauplan.exceptions.DeleteNamespaceForbiddenError`: if the user does not have access to delete the namespace.
    ///     `bauplan.exceptions.BranchNotFoundError`: if the branch does not exist.
    ///     `bauplan.exceptions.NotAWriteBranchRefError`: if the destination branch is not a writable ref.
    ///     `bauplan.exceptions.BranchHeadChangedError`: if the branch head hash has changed.
    ///     `bauplan.exceptions.NamespaceNotFoundError`: if the namespace does not exist.
    ///     `bauplan.exceptions.NamespaceIsNotEmptyError`: if the namespace is not empty.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `ValueError`: if one or more parameters are invalid.
    #[pyo3(signature = (
        namespace: "str | Namespace",
        branch: "str | Branch",
        *,
        if_exists: "bool" = false,
        commit_body: "str | None" = None,
        commit_properties: "dict[str, str] | None" = None,
    ) -> "Branch")]
    fn delete_namespace(
        &self, py: Python<'_>,
        namespace: NamespaceArg,
        branch: BranchArg,
        if_exists: bool,
        commit_body: Option<&str>,
        commit_properties: Option<BTreeMap<String, String>>,
    ) -> PyResult<CatalogRef> {
        let namespace = &namespace.0;
        let branch = &branch.0;
        let commit_properties = commit_properties.unwrap_or_default();
        let properties = commit_properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let req = DeleteNamespace {
            name: namespace,
            branch,
            commit: CommitOptions {
                body: commit_body,
                properties,
            },
        };

        match super::roundtrip(py, req, &self.profile, &self.agent) {
            Ok(r) => Ok(r),
            Err(e) => {
                if if_exists
                    && let Some(ApiErrorKind::NamespaceNotFound { catalog_ref, .. }) = e.kind()
                {
                    Ok(catalog_ref.clone())
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// Check if a namespace exists.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python fixture:my_namespace
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// assert client.has_namespace(
    ///     namespace='my_namespace_name',
    ///     ref='my_branch_name',
    /// )
    /// ```
    ///
    /// Parameters:
    ///     namespace: The name of the namespace to check.
    ///     ref: The ref, branch name or tag name to check the namespace on.
    ///
    /// Returns:
    ///     A boolean for if the namespace exists.
    ///
    /// Raises:
    ///     `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `ValueError`: if one or more parameters are invalid.
    #[pyo3(signature = (
        namespace: "str | Namespace",
        r#ref: "str | Ref",
    ) -> "bool")]
    fn has_namespace(&self, py: Python<'_>, namespace: NamespaceArg, r#ref: RefArg) -> PyResult<bool> {
        let req = GetNamespace {
            name: &namespace.0,
            at_ref: &r#ref.0,
        };

        match super::roundtrip(py, req, &self.profile, &self.agent) {
            Ok(_) => Ok(true),
            Err(e) => {
                if matches!(e.kind(), Some(ApiErrorKind::NamespaceNotFound { .. })) {
                    Ok(false)
                } else {
                    Err(e.into())
                }
            }
        }
    }
}
