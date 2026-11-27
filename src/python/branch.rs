//! Branch operations.

use bauplan_api::branch::{
    self, BranchFilters, CreateBranchBody, CreateBranchError, DeleteBranchError, MergeBranchBody,
    MergeStrategy, RenameBranchBody, ResolveBranchError,
};
use bauplan_api::gropius::client::ClientError;
use bauplan_api::model::ExpectedHead;
use bauplan_api::pagination::ListQuery;
use chrono::{DateTime, Utc};
use pyo3::prelude::*;

use super::exceptions::{api_error, merge_error};
use super::paginate::{PyPaginator, from_api_page};
use super::{Client, commit::PyCommit};

/// How a merge applies the changes of the source. `MergeStrategy.Squash` makes
/// one commit with all the changes, and it takes an optional commit `message`.
#[derive(Clone)]
#[pyclass(
    name = "MergeStrategy",
    module = "bauplan.schema",
    frozen,
    from_py_object
)]
pub(crate) enum PyMergeStrategy {
    /// One commit with all the changes of the source.
    #[pyo3(constructor = (message = None))]
    Squash {
        /// Optional, the commit message. Without it, the commit message is
        /// `Merge {from_ref} into {into_branch}`.
        message: Option<String>,
    },
}

#[pymethods]
impl PyMergeStrategy {
    fn __repr__(&self) -> String {
        match self {
            Self::Squash {
                message: Some(message),
            } => format!("MergeStrategy.Squash(message={message:?})"),
            Self::Squash { message: None } => "MergeStrategy.Squash()".to_owned(),
        }
    }
}

impl From<PyMergeStrategy> for MergeStrategy {
    fn from(strategy: PyMergeStrategy) -> Self {
        match strategy {
            PyMergeStrategy::Squash { message } => Self::Squash { message },
        }
    }
}

/// A data branch, used to isolate data changes before merging into main.
#[pyclass(name = "Branch", module = "bauplan.schema", frozen, get_all)]
pub(crate) struct PyBranch {
    /// The name of the branch.
    name: String,
    /// The hash of the commit the branch points to.
    commit_hash: String,
    /// When the branch was created.
    created_at: DateTime<Utc>,
    /// When the branch last moved.
    updated_at: DateTime<Utc>,
}

#[pymethods]
impl PyBranch {
    fn __repr__(&self) -> String {
        format!(
            "Branch(name={:?}, commit_hash={:?})",
            self.name, self.commit_hash
        )
    }
}

impl From<branch::Branch> for PyBranch {
    fn from(branch: branch::Branch) -> Self {
        Self {
            name: branch.name,
            commit_hash: branch.commit_hash,
            created_at: branch.created_at,
            updated_at: branch.updated_at,
        }
    }
}

#[pymethods]
impl Client {
    /// List the branches in the Bauplan catalog.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// for branch in client.list_branches(name='^username.', owned=True):
    ///     ...
    /// ```
    ///
    /// Parameters:
    ///     name: Optional, filter the branches by name. Use `^` and `$` to anchor it to the start or the end.
    ///     owned: If set to `True`, only the branches of the caller.
    ///     limit: Optional, max number of branches to get.
    /// Returns:
    ///     An iterator over `bauplan.schema.Branch` objects.
    ///
    /// Raises:
    ///     `bauplan.exceptions.BadRequestError`: if the name filter is not valid.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (
        *,
        name: "str | None" = None,
        owned: "bool" = false,
        limit: "int | None" = None,
    ) -> "typing.Iterator[Branch]")]
    fn list_branches(
        &self,
        py: Python<'_>,
        name: Option<String>,
        owned: bool,
        limit: Option<usize>,
    ) -> PyResult<PyPaginator> {
        let client = self.branches();
        let catalog = self.profile.catalog.clone();
        let filters = BranchFilters { owned, name };
        PyPaginator::new(py, limit, move |py, pagination_token, limit| {
            let query = ListQuery {
                pagination_token: pagination_token.map(str::to_owned),
                limit: limit.and_then(|limit| u32::try_from(limit).ok()),
                filters: filters.clone(),
            };
            let page = py.detach(|| client.list_branches(catalog.clone(), query));
            Ok(from_api_page::<_, PyBranch>(page.map_err(api_error)?))
        })
    }

    /// Get a branch.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// branch = client.get_branch('main')
    /// ```
    ///
    /// Parameters:
    ///     branch: The name of the branch.
    /// Returns:
    ///     A `bauplan.schema.Branch` object.
    ///
    /// Raises:
    ///     `bauplan.exceptions.BranchNotFoundError`: if the branch does not exist.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (branch: "str") -> "Branch")]
    fn get_branch(&self, py: Python<'_>, branch: String) -> PyResult<PyBranch> {
        let client = self.branches();
        let path = (self.profile.catalog.clone(), branch);
        let branch = py.detach(|| client.resolve_branch(path));
        Ok(branch.map_err(api_error)?.into())
    }

    /// Check if a branch exists.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// if client.has_branch('main'):
    ///     ...
    /// ```
    ///
    /// Parameters:
    ///     branch: The name of the branch.
    /// Returns:
    ///     A boolean for if the branch exists.
    ///
    /// Raises:
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (branch: "str") -> "bool")]
    fn has_branch(&self, py: Python<'_>, branch: String) -> PyResult<bool> {
        let client = self.branches();
        let path = (self.profile.catalog.clone(), branch);
        match py.detach(|| client.resolve_branch(path)) {
            Ok(_) => Ok(true),
            Err(ClientError::Api(ResolveBranchError::BranchNotFound { .. })) => Ok(false),
            Err(err) => Err(api_error(err)),
        }
    }

    /// Create a branch at a ref. The name of the branch should start with
    /// the username, `username.branch_name`, otherwise non-admin users can't
    /// create it.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// branch = client.create_branch('username.feature', 'main', if_not_exists=True)
    /// ```
    ///
    /// Parameters:
    ///     branch: The name of the new branch.
    ///     from_ref: The ref to start from, a branch, a tag or a commit hash.
    ///     if_not_exists: If set to `True`, a branch with the same name is not an error.
    /// Returns:
    ///     The created `bauplan.schema.Branch`, or `None` when a branch with the same name exists and `if_not_exists` is set.
    ///
    /// Raises:
    ///     `bauplan.exceptions.BranchNameConflictError`: if a branch with the same name exists.
    ///     `bauplan.exceptions.TagNameConflictError`: if a tag with the same name exists.
    ///     `bauplan.exceptions.FromRefNotFoundError`: if the ref to start from does not exist.
    ///     `bauplan.exceptions.ForbiddenError`: if the user can't create the branch.
    ///     `bauplan.exceptions.BadRequestError`: if the name is not valid.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (
        branch: "str",
        from_ref: "str",
        *,
        if_not_exists: "bool" = false,
    ) -> "Branch | None")]
    fn create_branch(
        &self,
        py: Python<'_>,
        branch: String,
        from_ref: String,
        if_not_exists: bool,
    ) -> PyResult<Option<PyBranch>> {
        let client = self.branches();
        let body = CreateBranchBody {
            name: branch,
            from_ref,
            ttl_seconds: None,
        };
        let catalog = self.profile.catalog.clone();
        match py.detach(|| client.create_branch(catalog, body)) {
            Ok(branch) => Ok(Some(branch.into())),
            Err(ClientError::Api(CreateBranchError::BranchNameConflict { .. }))
                if if_not_exists =>
            {
                Ok(None)
            }
            Err(err) => Err(api_error(err)),
        }
    }

    /// Rename a branch. The new name should start with the username,
    /// `username.branch_name`, otherwise non-admin users can't rename it.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// branch = client.rename_branch('username.old_name', 'username.new_name')
    /// ```
    ///
    /// Parameters:
    ///     branch: The name of the branch.
    ///     new_branch: The new name of the branch.
    /// Returns:
    ///     The renamed `bauplan.schema.Branch`.
    ///
    /// Raises:
    ///     `bauplan.exceptions.BranchNotFoundError`: if the branch does not exist.
    ///     `bauplan.exceptions.BranchNameConflictError`: if a branch with the new name exists.
    ///     `bauplan.exceptions.TagNameConflictError`: if a tag with the new name exists.
    ///     `bauplan.exceptions.MainBranchError`: if the branch is main.
    ///     `bauplan.exceptions.BranchHeadChangedError`: if the branch moves during the rename.
    ///     `bauplan.exceptions.ForbiddenError`: if the user can't rename the branch.
    ///     `bauplan.exceptions.BadRequestError`: if the new name is not valid.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (branch: "str", new_branch: "str") -> "Branch")]
    fn rename_branch(
        &self,
        py: Python<'_>,
        branch: String,
        new_branch: String,
    ) -> PyResult<PyBranch> {
        let client = self.branches();
        let body = RenameBranchBody { name: new_branch };
        let path = (self.profile.catalog.clone(), branch);
        let branch = py.detach(|| client.rename_branch(path, body));
        Ok(branch.map_err(api_error)?.into())
    }

    /// Merge a ref into a branch.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// commit = client.merge_branch('username.feature', 'main')
    /// ```
    ///
    /// To set the message of the squash commit:
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// commit = client.merge_branch(
    ///     'username.feature',
    ///     'main',
    ///     strategy=bauplan.MergeStrategy.Squash(message='Merge the feature'),
    /// )
    /// ```
    ///
    /// Parameters:
    ///     from_ref: The ref to merge, a branch, a tag or a commit hash.
    ///     into_branch: The branch to merge into.
    ///     strategy: Optional, how to merge, a `bauplan.schema.MergeStrategy`. It is `MergeStrategy.Squash()` by default.
    ///     expected_head: Optional, merge only if the branch points to this commit hash.
    /// Returns:
    ///     The `bauplan.schema.Commit` of the merge, or `None` when there is nothing to merge.
    ///
    /// Raises:
    ///     `bauplan.exceptions.BranchNotFoundError`: if the branch does not exist.
    ///     `bauplan.exceptions.FromRefNotFoundError`: if the ref to merge does not exist.
    ///     `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `expected_head`.
    ///     `bauplan.exceptions.MergeConflictError`: if the merge has conflicts.
    ///     `bauplan.exceptions.ForbiddenError`: if the user can't merge into the branch.
    ///     `bauplan.exceptions.BadRequestError`: if `from_ref` is not a valid ref.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (
        from_ref: "str",
        into_branch: "str",
        *,
        strategy: "MergeStrategy | None" = None,
        expected_head: "str | None" = None,
    ) -> "Commit | None")]
    fn merge_branch(
        &self,
        py: Python<'_>,
        from_ref: String,
        into_branch: String,
        strategy: Option<PyMergeStrategy>,
        expected_head: Option<String>,
    ) -> PyResult<Option<PyCommit>> {
        let client = self.branches();
        let body = MergeBranchBody {
            from_ref,
            strategy: strategy.map(Into::into).unwrap_or_default(),
            expected_head,
        };
        let path = (self.profile.catalog.clone(), into_branch);
        let commit = py.detach(|| client.merge_branch(path, body));
        Ok(commit.map_err(merge_error)?.map(PyCommit::from))
    }

    /// Delete a branch.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// client.delete_branch('username.feature', if_exists=True)
    /// ```
    ///
    /// Parameters:
    ///     branch: The name of the branch.
    ///     expected_head: Optional, delete only if the branch points to this commit hash.
    ///     if_exists: If set to `True`, a missing branch is not an error.
    ///
    /// Raises:
    ///     `bauplan.exceptions.BranchNotFoundError`: if the branch does not exist.
    ///     `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `expected_head`.
    ///     `bauplan.exceptions.MainBranchError`: if the branch is main.
    ///     `bauplan.exceptions.ForbiddenError`: if the user can't delete the branch.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (
        branch: "str",
        *,
        expected_head: "str | None" = None,
        if_exists: "bool" = false,
    ) -> "None")]
    fn delete_branch(
        &self,
        py: Python<'_>,
        branch: String,
        expected_head: Option<String>,
        if_exists: bool,
    ) -> PyResult<()> {
        let client = self.branches();
        let expected_head = expected_head.map(|hash| ExpectedHead {
            expected_head: Some(hash),
        });
        let path = (self.profile.catalog.clone(), branch);
        match py.detach(|| client.delete_branch(path, expected_head)) {
            Ok(_) => Ok(()),
            Err(ClientError::Api(DeleteBranchError::BranchNotFound { .. })) if if_exists => Ok(()),
            Err(err) => Err(api_error(err)),
        }
    }
}
