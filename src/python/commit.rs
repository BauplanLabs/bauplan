//! Commit operations.

use std::collections::BTreeMap;

use pyo3::{exceptions::PyTypeError, prelude::*};

use crate::{ApiRequest, commit::GetCommits};

use super::{Client, paginate::PyPaginator, refs::RefArg};

struct DatetimeArg(String);

impl<'a, 'py> FromPyObject<'a, 'py> for DatetimeArg {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(s) = ob.extract::<String>() {
            Ok(DatetimeArg(s))
        } else if let Ok(s) = ob
            .call_method0("isoformat")
            .and_then(|v| v.extract::<String>())
        {
            Ok(DatetimeArg(s))
        } else {
            Err(PyTypeError::new_err("expected str or datetime"))
        }
    }
}

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
    ///     An iterator over `Commit` objects.
    ///
    /// Raises:
    ///     UnauthorizedError: if the user's credentials are invalid.
    ///     ValueError: if one or more parameters are invalid.
    #[pyo3(signature = (
        r#ref: "str | Ref",
        *,
        filter_by_message: "str | None" = None,
        filter_by_author_username: "str | None" = None,
        filter_by_author_name: "str | None" = None,
        filter_by_author_email: "str | None" = None,
        filter_by_authored_date: "str | datetime | None" = None,
        filter_by_authored_date_start_at: "str | datetime | None" = None,
        filter_by_authored_date_end_at: "str | datetime | None" = None,
        filter_by_parent_hash: "str | None" = None,
        filter_by_properties: "dict[str, str] | None" = None,
        filter: "str | None" = None,
        limit: "int | None" = None,
    ) -> "typing.Iterator[Commit]")]
    #[allow(clippy::too_many_arguments)]
    fn get_commits(
        &self,
        r#ref: RefArg,
        filter_by_message: Option<String>,
        filter_by_author_username: Option<String>,
        filter_by_author_name: Option<String>,
        filter_by_author_email: Option<String>,
        filter_by_authored_date: Option<DatetimeArg>,
        filter_by_authored_date_start_at: Option<DatetimeArg>,
        filter_by_authored_date_end_at: Option<DatetimeArg>,
        filter_by_parent_hash: Option<String>,
        filter_by_properties: Option<BTreeMap<String, String>>,
        filter: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<PyPaginator> {
        let profile = self.profile.clone();
        let agent = self.agent.clone();
        let r#ref = r#ref.0;
        let filter_by_authored_date = filter_by_authored_date.map(|a| a.0);
        let filter_by_authored_date_start_at = filter_by_authored_date_start_at.map(|a| a.0);
        let filter_by_authored_date_end_at = filter_by_authored_date_end_at.map(|a| a.0);

        PyPaginator::new(limit, move |token, limit| {
            let req = GetCommits {
                at_ref: &r#ref,
                filter_by_message: filter_by_message.as_deref(),
                filter_by_author_username: filter_by_author_username.as_deref(),
                filter_by_author_name: filter_by_author_name.as_deref(),
                filter_by_author_email: filter_by_author_email.as_deref(),
                filter_by_authored_date: filter_by_authored_date.as_deref(),
                filter_by_authored_date_start_at: filter_by_authored_date_start_at.as_deref(),
                filter_by_authored_date_end_at: filter_by_authored_date_end_at.as_deref(),
                filter_by_parent_hash: filter_by_parent_hash.as_deref(),
                filter_by_properties: filter_by_properties.as_ref(),
                filter: filter.as_deref(),
            }
            .paginate(token, limit);

            Ok(super::roundtrip(req, &profile, &agent)?)
        })
    }
}
