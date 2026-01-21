//! Commit operations.

#![allow(unused_imports)]

use pyo3::prelude::*;
use std::collections::HashMap;

use super::bauplan::Client;

#[pymethods]
impl Client {
    /// Get the commits for the target branch or ref.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// Parameters:
    ///     ref: The ref or branch to get the commits from.
    ///     filter_by_message: Optional, filter the commits by message (can be a string or a regex like '^abc.*$')
    ///     filter_by_author_username: Optional, filter the commits by author username (can be a string or a regex like '^abc.*$')
    ///     filter_by_author_name: Optional, filter the commits by author name (can be a string or a regex like '^abc.*$')
    ///     filter_by_author_email: Optional, filter the commits by author email (can be a string or a regex like '^abc.*$')
    ///     filter_by_authored_date: Optional, filter the commits by the exact authored date.
    ///     filter_by_authored_date_start_at: Optional, filter the commits by authored date start at.
    ///     filter_by_authored_date_end_at: Optional, filter the commits by authored date end at.
    ///     filter_by_parent_hash: Optional, filter the commits by parent hash.
    ///     filter_by_properties: Optional, filter the commits by commit properties.
    ///     filter: Optional, a CEL filter expression to filter the commits.
    ///     limit: Optional, max number of commits to get.
    /// Returns:
    ///     A `bauplan.schema.GetCommitsResponse` object.
    ///
    /// Raises:
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (ref_, filter_by_message=None, filter_by_author_username=None, filter_by_author_name=None, filter_by_author_email=None, filter_by_authored_date=None, filter_by_authored_date_start_at=None, filter_by_authored_date_end_at=None, filter_by_parent_hash=None, filter_by_properties=None, filter_=None, limit=None))]
    #[allow(clippy::too_many_arguments)]
    fn get_commits(
        &mut self,
        ref_: &str,
        filter_by_message: Option<&str>,
        filter_by_author_username: Option<&str>,
        filter_by_author_name: Option<&str>,
        filter_by_author_email: Option<&str>,
        filter_by_authored_date: Option<&str>,
        filter_by_authored_date_start_at: Option<&str>,
        filter_by_authored_date_end_at: Option<&str>,
        filter_by_parent_hash: Option<&str>,
        filter_by_properties: Option<std::collections::HashMap<String, String>>,
        filter_: Option<&str>,
        limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (
            ref_,
            filter_by_message,
            filter_by_author_username,
            filter_by_author_name,
            filter_by_author_email,
            filter_by_authored_date,
            filter_by_authored_date_start_at,
            filter_by_authored_date_end_at,
            filter_by_parent_hash,
            filter_by_properties,
            filter_,
            limit,
        );
        todo!("get_commits")
    }
}
