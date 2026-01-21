//! Namespace operations.

#![allow(unused_imports)]

use pyo3::prelude::*;
use std::collections::HashMap;

use super::bauplan::Client;

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
    ///     RefNotFoundError: if the ref does not exist.
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    /// 
    /// Yields:
    ///     A Namespace object.
    #[pyo3(signature = (ref_, filter_by_name=None, limit=None))]
    fn get_namespaces(
        &mut self,
        ref_: &str,
        filter_by_name: Option<&str>,
        limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (ref_, filter_by_name, limit);
        todo!("get_namespaces")
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
    ///     NamespaceNotFoundError: if the namespace does not exist.
    ///     RefNotFoundError: if the ref does not exist.
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (namespace, ref_))]
    fn get_namespace(
        &mut self,
        namespace: &str,
        ref_: &str,
    ) -> PyResult<Py<PyAny>> {
        let _ = (namespace, ref_);
        todo!("get_namespace")
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
    ///     properties={'k1': 'v1', 'k2': 'v2'},
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
    ///     CreateNamespaceForbiddenError: if the user does not have access to create the namespace.
    ///     BranchNotFoundError: if the branch does not exist.
    ///     NotAWriteBranchError: if the destination branch is not a writable ref.
    ///     BranchHeadChangedError: if the branch head hash has changed.
    ///     NamespaceExistsError: if the namespace already exists.
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (namespace, branch, commit_body=None, commit_properties=None, if_not_exists=None, properties=None))]
    fn create_namespace(
        &mut self,
        namespace: &str,
        branch: &str,
        commit_body: Option<&str>,
        commit_properties: Option<std::collections::HashMap<String, String>>,
        if_not_exists: Option<bool>,
        properties: Option<std::collections::HashMap<String, String>>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (namespace, branch, commit_body, commit_properties, if_not_exists, properties);
        todo!("create_namespace")
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
    ///     if_exists: If set to `True`, the namespace will not be deleted if it does not exist.
    /// Returns:
    ///     A `bauplan.schema.Branch` object pointing to head.
    /// 
    /// Raises:
    ///     DeleteBranchForbiddenError: if the user does not have access to delete the branch.
    ///     BranchNotFoundError: if the branch does not exist.
    ///     NotAWriteBranchError: if the destination branch is not a writable ref.
    ///     BranchHeadChangedError: if the branch head hash has changed.
    ///     NamespaceNotFoundError: if the namespace does not exist.
    ///     NamespaceIsNotEmptyError: if the namespace is not empty.
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (namespace, branch, if_exists=None, commit_body=None, commit_properties=None, properties=None))]
    fn delete_namespace(
        &mut self,
        namespace: &str,
        branch: &str,
        if_exists: Option<bool>,
        commit_body: Option<&str>,
        commit_properties: Option<std::collections::HashMap<String, String>>,
        properties: Option<std::collections::HashMap<String, String>>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (namespace, branch, if_exists, commit_body, commit_properties, properties);
        todo!("delete_namespace")
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
    ///     RefNotFoundError: if the ref does not exist.
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (namespace, ref_))]
    fn has_namespace(
        &mut self,
        namespace: &str,
        ref_: &str,
    ) -> PyResult<bool> {
        let _ = (namespace, ref_);
        todo!("has_namespace")
    }
}
