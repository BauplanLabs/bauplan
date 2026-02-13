//! Tag operations.

use pyo3::prelude::*;

use crate::{
    ApiErrorKind, ApiRequest, CatalogRef,
    python::{
        paginate::PyPaginator,
        refs::{RefArg, TagArg},
    },
    tag::{CreateTag, DeleteTag, GetTag, GetTags, RenameTag, Tag},
};

use super::Client;

#[pymethods]
impl Client {
    /// Get all the tags.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// Parameters:
    ///     filter_by_name: Optional, filter the tags by name.
    ///     limit: Optional, max number of tags to get.
    /// Returns:
    ///     An iterator over `Tag` objects.
    ///
    /// Raises:
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (
        *,
        filter_by_name: "str | None" = None,
        limit: "int | None" = None,
    ) -> "typing.Iterator[Tag]")]
    fn get_tags(
        &self,
        filter_by_name: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<PyPaginator> {
        let profile = self.profile.clone();
        let agent = self.agent.clone();
        PyPaginator::new(limit, move |token, limit| {
            let req = GetTags {
                filter_by_name: filter_by_name.as_deref(),
            }
            .paginate(token, limit);

            Ok(super::roundtrip(req, &profile, &agent)?)
        })
    }

    /// Get the tag.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python fixture:my_tag
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// # retrieve only the tables as tuples of (name, kind)
    /// tag = client.get_tag('my_tag_name')
    /// ```
    ///
    /// Parameters:
    ///     tag: The name of the tag to retrieve.
    /// Returns:
    ///     A `bauplan.schema.Tag` object.
    ///
    /// Raises:
    ///     TagNotFoundError: if the tag does not exist.
    ///     NotATagRefError: if the object is not a tag.
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (tag: "str | Tag") -> "Tag")]
    fn get_tag(&mut self, tag: TagArg) -> PyResult<Tag> {
        let req = GetTag { name: &tag.0 };
        let t = super::roundtrip(req, &self.profile, &self.agent)?;
        Ok(t)
    }

    /// Check if a tag exists.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python fixture:my_tag
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// assert client.has_tag(
    ///     tag='my_tag_name',
    /// )
    /// ```
    ///
    /// Parameters:
    ///     tag: The tag to retrieve.
    /// Returns:
    ///     A boolean for if the tag exists.
    ///
    /// Raises:
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (tag: "str | Tag") -> "bool")]
    fn has_tag(&mut self, tag: TagArg) -> PyResult<bool> {
        let req = GetTag { name: &tag.0 };

        match super::roundtrip(req, &self.profile, &self.agent) {
            Ok(_) => Ok(true),
            Err(e)
                if matches!(
                    e.kind(),
                    Some(ApiErrorKind::TagNotFound { .. } | ApiErrorKind::NotATagRef { .. })
                ) =>
            {
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Create a new tag at a given ref.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python notest
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// assert client.create_tag(
    ///     tag='my_tag',
    ///     from_ref='my_ref_or_branch_name',
    /// )
    /// ```
    ///
    /// Parameters:
    ///     tag: The name of the new tag.
    ///     from_ref: The name of the base branch; either a branch like "main" or ref like "main@[sha]".
    ///     if_not_exists: If set to `True`, the tag will not be created if it already exists.
    /// Returns:
    ///     The created `bauplan.schema.Tag` object.
    ///
    /// Raises:
    ///     CreateTagForbiddenError: if the user does not have access to create the tag.
    ///     RefNotFoundError: if the ref does not exist.
    ///     TagExistsError: if the tag already exists.
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (
        tag: "str | Tag",
        from_ref: "str | Ref",
        *,
        if_not_exists: "bool" = false,
    ) -> "Tag")]
    fn create_tag(&mut self, tag: TagArg, from_ref: RefArg, if_not_exists: bool) -> PyResult<Tag> {
        let req = CreateTag {
            name: &tag.0,
            from_ref: &from_ref.0,
        };

        match super::roundtrip(req, &self.profile, &self.agent) {
            Ok(t) => Ok(t),

            Err(e) => {
                if if_not_exists
                    && let Some(ApiErrorKind::TagExists {
                        catalog_ref: CatalogRef::Tag { name, hash },
                        ..
                    }) = e.kind()
                {
                    Ok(Tag {
                        name: name.clone(),
                        hash: hash.clone(),
                    })
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// Rename an existing tag.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python notest
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// assert client.rename_tag(
    ///     tag='old_tag_name',
    ///     new_tag='new_tag_name',
    /// )
    /// ```
    ///
    /// Parameters:
    ///     tag: The name of the tag to rename.
    ///     new_tag: The name of the new tag.
    /// Returns:
    ///     The renamed tag object.
    ///
    /// Raises:
    ///     RenameTagForbiddenError: if the user does not have access to create the tag.
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (
        tag: "str | Tag",
        new_tag: "str | Tag",
    ) -> "Tag")]
    fn rename_tag(&mut self, tag: TagArg, new_tag: TagArg) -> PyResult<Tag> {
        let req = RenameTag {
            name: &tag.0,
            new_name: &new_tag.0,
        };

        let t = super::roundtrip(req, &self.profile, &self.agent)?;
        Ok(t)
    }

    /// Delete a tag.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python fixture:my_tag
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// assert client.delete_tag('my_tag_name')
    /// ```
    ///
    /// Parameters:
    ///     tag: The name of the tag to delete.
    ///     if_exists: If set to `True`, the tag will not raise an error if it does not exist.
    /// Returns:
    ///     A boolean for if the tag was deleted.
    ///
    /// Raises:
    ///     DeleteTagForbiddenError: if the user does not have access to delete the tag.
    ///     TagNotFoundError: if the tag does not exist.
    ///     NotATagRefError: if the object is not a tag.
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (
        tag: "str | Tag",
        *,
        if_exists: "bool" = false,
    ) -> "bool")]
    fn delete_tag(&mut self, tag: TagArg, if_exists: bool) -> PyResult<bool> {
        let req = DeleteTag { name: &tag.0 };

        match super::roundtrip(req, &self.profile, &self.agent) {
            Ok(_) => Ok(true),
            Err(e) if matches!(e.kind(), Some(ApiErrorKind::TagNotFound { .. })) && if_exists => {
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }
}
