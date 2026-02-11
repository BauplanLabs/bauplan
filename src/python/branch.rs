//! Branch operations.

use pyo3::prelude::*;
use std::collections::BTreeMap;

use crate::{
    ApiErrorKind, ApiRequest,
    branch::{
        Branch, CreateBranch, DeleteBranch, GetBranch, GetBranches, MergeBranch,
        MergeCommitOptions, RenameBranch,
    },
    python::{
        paginate::PyPaginator,
        refs::{BranchArg, RefArg},
    },
};

use super::Client;

#[pymethods]
impl Client {
    /// Get the available data branches in the Bauplan catalog.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// for branch in client.get_branches():
    ///     ...
    /// ```
    ///
    /// Parameters:
    ///     name: Filter the branches by name.
    ///     user: Filter the branches by user.
    ///     limit: Optional, max number of branches to get.
    /// Returns:
    ///     An iterator over `Branch` objects.
    #[pyo3(signature = (
        *,
        name: "str | None" = None,
        user: "str | None" = None,
        limit: "int | None" = None,
    ) -> "typing.Iterator[Branch]")]
    fn get_branches(
        &self,
        name: Option<String>,
        user: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<PyPaginator> {
        let profile = self.profile.clone();
        let agent = self.agent.clone();
        PyPaginator::new(limit, move |token, limit| {
            let req = GetBranches {
                filter_by_name: name.as_deref(),
                filter_by_user: user.as_deref(),
            }
            .paginate(token, limit);

            Ok(super::roundtrip(req, &profile, &agent)?)
        })
    }

    /// Get the branch.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python fixture:my_branch
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// # retrieve only the tables as tuples of (name, kind)
    /// branch = client.get_branch('my_branch_name')
    /// ```
    ///
    /// Parameters:
    ///     branch: The name of the branch to retrieve.
    /// Returns:
    ///     A `Branch` object.
    ///
    /// Raises:
    ///     BranchNotFoundError: if the branch does not exist.
    ///     NotABranchRefError: if the object is not a branch.
    ///     ForbiddenError: if the user does not have access to the branch.
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (branch: "str | Branch") -> "Branch")]
    fn get_branch(&mut self, branch: BranchArg) -> PyResult<Branch> {
        let req = GetBranch { name: &branch.0 };
        let b = super::roundtrip(req, &self.profile, &self.agent)?;
        Ok(b)
    }

    /// Check if a branch exists.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python fixture:my_branch
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// if client.has_branch('my_branch_name')
    ///     # do something
    /// ```
    ///
    /// Parameters:
    ///     branch: The name of the branch to check.
    /// Returns:
    ///     A boolean for if the branch exists.
    ///
    /// Raises:
    ///     NotABranchRefError: if the object is not a branch.
    ///     ForbiddenError: if the user does not have access to the branch.
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (branch: "str | Branch") -> "bool")]
    fn has_branch(&mut self, branch: BranchArg) -> PyResult<bool> {
        let req = GetBranch { name: &branch.0 };

        match super::roundtrip(req, &self.profile, &self.agent) {
            Ok(_) => Ok(true),
            Err(e) if e.is_api_err(ApiErrorKind::BranchNotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Create a new branch at a given ref.
    /// The branch name should follow the convention of `username.branch_name`,
    /// otherwise non-admin users won't be able to complete the operation.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python fixture:my_branch
    /// import bauplan
    ///
    /// client = bauplan.Client()
    /// username = client.info().user.username
    ///
    /// branch = client.create_branch(
    ///     branch = username+'.feature_branch',
    ///     from_ref = 'branch_name@hash',
    ///     if_not_exists = True,
    /// )
    /// ```
    ///
    /// Parameters:
    ///     branch: The name of the new branch.
    ///     from_ref: The name of the base branch; either a branch like "main" or ref like "main@[sha]".
    ///     if_not_exists: If set to `True`, the branch will not be created if it already exists.
    /// Returns:
    ///     The created branch object.
    ///
    /// Raises:
    ///     CreateBranchForbiddenError: if the user does not have access to create the branch.
    ///     BranchExistsError: if the branch already exists.
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (
        branch: "str | Branch",
        from_ref: "str | Ref",
        *,
        if_not_exists: "bool" = false,
    ) -> "Branch")]
    fn create_branch(
        &mut self,
        branch: BranchArg,
        from_ref: RefArg,
        if_not_exists: bool,
    ) -> PyResult<Branch> {
        let req = CreateBranch {
            name: &branch.0,
            from_ref: &from_ref.0,
        };

        match super::roundtrip(req, &self.profile, &self.agent) {
            Ok(b) => Ok(b),
            Err(e) if e.is_api_err(ApiErrorKind::BranchExists) && if_not_exists => {
                todo!("context_ref")
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Rename an existing branch.
    /// The branch name should follow the convention of "username.branch_name",
    /// otherwise non-admin users won't be able to complete the operation.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python notest
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// assert client.rename_branch(
    ///     branch='username.old_name',
    ///     new_branch='username.new_name',
    /// )
    /// ```
    ///
    /// Parameters:
    ///     branch: The name of the branch to rename.
    ///     new_branch: The name of the new branch.
    /// Returns:
    ///     The renamed `Branch` object.
    ///
    /// Raises:
    ///     `RenameBranchForbiddenError`: if the user does not have access to create the branch.
    ///     `UnauthorizedError`: if the user's credentials are invalid.
    ///     `ValueError`: if one or more parameters are invalid.
    #[pyo3(signature = (
        branch: "str | Branch",
        new_branch: "str | Branch",
    ) -> "Branch")]
    fn rename_branch(&mut self, branch: BranchArg, new_branch: BranchArg) -> PyResult<Branch> {
        let req = RenameBranch {
            name: &branch.0,
            new_name: &new_branch.0,
        };

        let b = super::roundtrip(req, &self.profile, &self.agent)?;
        Ok(b)
    }

    /// Merge one branch into another.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python notest
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// assert client.merge_branch(
    ///     source_ref='my_ref_or_branch_name',
    ///     into_branch='main',
    /// )
    /// ```
    ///
    /// Parameters:
    ///     source_ref: The name of the merge source; either a branch like "main" or ref like "main@[sha]".
    ///     into_branch: The name of the merge target.
    ///     commit_message: Optional, the commit message.
    ///     commit_body: Optional, the commit body.
    ///     commit_properties: Optional, a list of properties to attach to the merge.
    /// Returns:
    ///     the `Branch` where the merge was made.
    ///
    /// Raises:
    ///     MergeForbiddenError: if the user does not have access to merge the branch.
    ///     BranchNotFoundError: if the destination branch does not exist.
    ///     NotAWriteBranchError: if the destination branch is not a writable ref.
    ///     MergeConflictError: if the merge operation results in a conflict.
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (
        source_ref: "str | Ref",
        into_branch: "str | Branch",
        *,
        commit_message: "str | None" = None,
        commit_body: "str | None" = None,
        commit_properties: "dict[str, str] | None" = None,
    ) -> "Branch")]
    fn merge_branch(
        &mut self,
        source_ref: RefArg,
        into_branch: BranchArg,
        commit_message: Option<&str>,
        commit_body: Option<&str>,
        commit_properties: Option<BTreeMap<String, String>>,
    ) -> PyResult<crate::CatalogRef> {
        let commit_properties = commit_properties.unwrap_or_default();
        let properties = commit_properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let req = MergeBranch {
            source_ref: &source_ref.0,
            into_branch: &into_branch.0,
            commit: MergeCommitOptions {
                commit_message,
                commit_body,
                commit_properties: properties,
            },
        };

        Ok(super::roundtrip(req, &self.profile, &self.agent)?)
    }

    /// Delete a branch.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python fixture:my_branch
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// if client.delete_branch('my_branch_name')
    ///     #do something
    /// ```
    ///
    /// Parameters:
    ///     branch: The name of the branch to delete.
    ///     if_exists: If set to `True`, the branch will not raise an error if it does not exist.
    /// Returns:
    ///     A boolean for if the branch was deleted.
    ///
    /// Raises:
    ///     DeleteBranchForbiddenError: if the user does not have access to delete the branch.
    ///     BranchNotFoundError: if the branch does not exist.
    ///     BranchHeadChangedError: if the branch head hash has changed.
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (
        branch: "str | Branch",
        *,
        if_exists: "bool" = false,
    ) -> "bool")]
    fn delete_branch(&mut self, branch: BranchArg, if_exists: bool) -> PyResult<bool> {
        let req = DeleteBranch { name: &branch.0 };

        match super::roundtrip(req, &self.profile, &self.agent) {
            Ok(_) => Ok(true),
            Err(e) if e.is_api_err(ApiErrorKind::BranchNotFound) && if_exists => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}
