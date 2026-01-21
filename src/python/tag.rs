//! Tag operations.

#![allow(unused_imports)]

use pyo3::prelude::*;
use std::collections::HashMap;

use super::bauplan::Client;

#[pymethods]
impl Client {
    /// Get all the tags.
    /// 
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    /// 
    /// Parameters:
    ///     filter_by_name: Optional, filter the commits by message.
    ///     limit: Optional, max number of commits to get.
    /// Returns:
    ///     A `bauplan.schema.GetTagsResponse` object.
    /// 
    /// Raises:
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (filter_by_name=None, limit=None))]
    fn get_tags(
        &mut self,
        filter_by_name: Option<&str>,
        limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (filter_by_name, limit);
        todo!("get_tags")
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
    #[pyo3(signature = (tag))]
    fn get_tag(
        &mut self,
        tag: &str,
    ) -> PyResult<Py<PyAny>> {
        let _ = tag;
        todo!("get_tag")
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
    ///     NotATagRefError: if the object is not a tag.
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (tag))]
    fn has_tag(
        &mut self,
        tag: &str,
    ) -> PyResult<bool> {
        let _ = tag;
        todo!("has_tag")
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
    #[pyo3(signature = (tag, from_ref, if_not_exists=None))]
    fn create_tag(
        &mut self,
        tag: &str,
        from_ref: &str,
        if_not_exists: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (tag, from_ref, if_not_exists);
        todo!("create_tag")
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
    #[pyo3(signature = (tag, new_tag))]
    fn rename_tag(
        &mut self,
        tag: &str,
        new_tag: &str,
    ) -> PyResult<Py<PyAny>> {
        let _ = (tag, new_tag);
        todo!("rename_tag")
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
    #[pyo3(signature = (tag, if_exists=None))]
    fn delete_tag(
        &mut self,
        tag: &str,
        if_exists: Option<bool>,
    ) -> PyResult<bool> {
        let _ = (tag, if_exists);
        todo!("delete_tag")
    }
}
