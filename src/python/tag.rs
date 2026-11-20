//! Tag operations.

use bauplan_api::gropius::client::ClientError;
use bauplan_api::pagination::ListQuery;
use bauplan_api::tag::{
    self, CreateTagBody, CreateTagError, DeleteTagError, RenameTagBody, ResolveTagError, TagFilters,
};
use chrono::{DateTime, Utc};
use pyo3::prelude::*;

use super::Client;
use super::exceptions::api_error;
use super::paginate::{PyPaginator, from_api_page};

/// A human-readable name that points to a specific commit in the data lake.
/// Tags are often used to mark important milestones in the data lake
/// history, such as releases or experiments.
#[pyclass(name = "Tag", module = "bauplan.schema", frozen, get_all)]
pub(crate) struct PyTag {
    /// The name of the tag.
    name: String,
    /// The hash of the commit the tag points to.
    commit_hash: String,
    /// When the tag was created.
    created_at: DateTime<Utc>,
    /// When the tag was last updated.
    updated_at: DateTime<Utc>,
}

#[pymethods]
impl PyTag {
    fn __repr__(&self) -> String {
        format!(
            "Tag(name={:?}, commit_hash={:?})",
            self.name, self.commit_hash
        )
    }
}

impl From<tag::Tag> for PyTag {
    fn from(tag: tag::Tag) -> Self {
        Self {
            name: tag.name,
            commit_hash: tag.commit_hash,
            created_at: tag.created_at,
            updated_at: tag.updated_at,
        }
    }
}

#[pymethods]
impl Client {
    /// Get the tags in the Bauplan catalog.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// for tag in client.get_tags(name='^v1.'):
    ///     ...
    /// ```
    ///
    /// Parameters:
    ///     name: Filter the tags by name, `^` and `$` anchor it to the start or the end.
    ///     limit: Optional, max number of tags to get.
    /// Returns:
    ///     An iterator over `bauplan.schema.Tag` objects.
    ///
    /// Raises:
    ///     `bauplan.exceptions.BadRequestError`: if the name filter is not valid.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    #[pyo3(signature = (
        *,
        name: "str | None" = None,
        limit: "int | None" = None,
    ) -> "typing.Iterator[Tag]")]
    fn get_tags(
        &self,
        py: Python<'_>,
        name: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<PyPaginator> {
        let client = self.tags()?;
        let catalog = self.profile.catalog.clone();
        let filters = TagFilters { name };
        PyPaginator::new(py, limit, move |py, pagination_token, limit| {
            let query = ListQuery {
                pagination_token: pagination_token.map(str::to_owned),
                limit: limit.and_then(|limit| u32::try_from(limit).ok()),
                filters: filters.clone(),
            };
            let page = py.detach(|| client.list_tags(catalog.clone(), query));
            Ok(from_api_page::<_, PyTag>(page.map_err(api_error)?))
        })
    }

    /// Get a tag.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// tag = client.get_tag('v1.0')
    /// ```
    ///
    /// Parameters:
    ///     tag: The name of the tag.
    /// Returns:
    ///     A `bauplan.schema.Tag` object.
    ///
    /// Raises:
    ///     `bauplan.exceptions.TagNotFoundError`: if the tag does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    #[pyo3(signature = (tag: "str") -> "Tag")]
    fn get_tag(&self, py: Python<'_>, tag: String) -> PyResult<PyTag> {
        let client = self.tags()?;
        let path = (self.profile.catalog.clone(), tag);
        let tag = py.detach(|| client.resolve_tag(path));
        Ok(tag.map_err(api_error)?.into())
    }

    /// Check if a tag exists.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// if client.has_tag('v1.0'):
    ///     ...
    /// ```
    ///
    /// Parameters:
    ///     tag: The name of the tag.
    /// Returns:
    ///     A boolean for if the tag exists.
    ///
    /// Raises:
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    #[pyo3(signature = (tag: "str") -> "bool")]
    fn has_tag(&self, py: Python<'_>, tag: String) -> PyResult<bool> {
        let client = self.tags()?;
        let path = (self.profile.catalog.clone(), tag);
        match py.detach(|| client.resolve_tag(path)) {
            Ok(_) => Ok(true),
            Err(ClientError::Api(ResolveTagError::TagNotFound { .. })) => Ok(false),
            Err(err) => Err(api_error(err)),
        }
    }

    /// Create a tag at a ref.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// tag = client.create_tag('v1.0', 'main', if_not_exists=True)
    /// ```
    ///
    /// Parameters:
    ///     tag: The name of the new tag.
    ///     from_ref: The ref to tag, a branch, a tag or a commit hash.
    ///     if_not_exists: If set to `True`, a tag with the same name is not an error.
    /// Returns:
    ///     The created `bauplan.schema.Tag`, or `None` when a tag with the same name exists and `if_not_exists` is set.
    ///
    /// Raises:
    ///     `bauplan.exceptions.TagNameConflictError`: if a tag with the same name exists.
    ///     `bauplan.exceptions.BranchNameConflictError`: if a branch with the same name exists.
    ///     `bauplan.exceptions.FromRefNotFoundError`: if the ref to tag does not exist.
    ///     `bauplan.exceptions.ForbiddenError`: if the user can't create the tag.
    ///     `bauplan.exceptions.BadRequestError`: if the name is not valid.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    #[pyo3(signature = (
        tag: "str",
        from_ref: "str",
        *,
        if_not_exists: "bool" = false,
    ) -> "Tag | None")]
    fn create_tag(
        &self,
        py: Python<'_>,
        tag: String,
        from_ref: String,
        if_not_exists: bool,
    ) -> PyResult<Option<PyTag>> {
        let client = self.tags()?;
        let body = CreateTagBody {
            name: tag,
            from_ref,
        };
        let catalog = self.profile.catalog.clone();
        match py.detach(|| client.create_tag(catalog, body)) {
            Ok(tag) => Ok(Some(tag.into())),
            Err(ClientError::Api(CreateTagError::TagNameConflict { .. })) if if_not_exists => {
                Ok(None)
            }
            Err(err) => Err(api_error(err)),
        }
    }

    /// Rename a tag.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// tag = client.rename_tag('v1.0', 'v1.0.0')
    /// ```
    ///
    /// Parameters:
    ///     tag: The name of the tag.
    ///     new_tag: The new name of the tag.
    /// Returns:
    ///     The renamed `bauplan.schema.Tag`.
    ///
    /// Raises:
    ///     `bauplan.exceptions.TagNotFoundError`: if the tag does not exist.
    ///     `bauplan.exceptions.TagNameConflictError`: if a tag with the new name exists.
    ///     `bauplan.exceptions.BranchNameConflictError`: if a branch with the new name exists.
    ///     `bauplan.exceptions.TagChangedError`: if the tag changed during the rename.
    ///     `bauplan.exceptions.ForbiddenError`: if the user can't rename the tag.
    ///     `bauplan.exceptions.BadRequestError`: if the new name is not valid.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    #[pyo3(signature = (tag: "str", new_tag: "str") -> "Tag")]
    fn rename_tag(&self, py: Python<'_>, tag: String, new_tag: String) -> PyResult<PyTag> {
        let client = self.tags()?;
        let body = RenameTagBody { name: new_tag };
        let path = (self.profile.catalog.clone(), tag);
        let tag = py.detach(|| client.rename_tag(path, body));
        Ok(tag.map_err(api_error)?.into())
    }

    /// Delete a tag.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// client.delete_tag('v1.0', if_exists=True)
    /// ```
    ///
    /// Parameters:
    ///     tag: The name of the tag.
    ///     if_exists: If set to `True`, a missing tag is not an error.
    ///
    /// Raises:
    ///     `bauplan.exceptions.TagNotFoundError`: if the tag does not exist.
    ///     `bauplan.exceptions.TagChangedError`: if the tag changed during the delete.
    ///     `bauplan.exceptions.ForbiddenError`: if the user can't delete the tag.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    #[pyo3(signature = (
        tag: "str",
        *,
        if_exists: "bool" = false,
    ) -> "None")]
    fn delete_tag(&self, py: Python<'_>, tag: String, if_exists: bool) -> PyResult<()> {
        let client = self.tags()?;
        let path = (self.profile.catalog.clone(), tag);
        match py.detach(|| client.delete_tag(path)) {
            Ok(_) => Ok(()),
            Err(ClientError::Api(DeleteTagError::TagNotFound { .. })) if if_exists => Ok(()),
            Err(err) => Err(api_error(err)),
        }
    }
}
