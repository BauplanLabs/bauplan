//! Commit operations.

use std::collections::BTreeMap;

use bauplan_api::model::Commit;
use bauplan_api::pagination::ListQuery;
use chrono::{DateTime, Utc};
use pyo3::prelude::*;

use super::Client;
use super::exceptions::api_error;
use super::paginate::PyIteratorAtCommit;

/// A commit in the catalog history.
#[pyclass(name = "Commit", module = "bauplan.schema", frozen, get_all)]
pub(crate) struct PyCommit {
    /// The commit hash.
    hash: String,
    /// The commit title, the first line of the message.
    title: String,
    /// The full commit message.
    message: String,
    /// The author's display name.
    author_name: String,
    /// The author's email, if known.
    author_email: Option<String>,
    /// When the commit was authored.
    authored_at: DateTime<Utc>,
    /// The committer's display name.
    committer_name: String,
    /// The committer's email, if known.
    committer_email: Option<String>,
    /// When the commit was committed.
    committed_at: DateTime<Utc>,
    /// The parent commit hash, `None` only on the root commit.
    parent_hash: Option<String>,
    /// The hash of the commit merged in, only on merge commits.
    merge_parent_hash: Option<String>,
    /// The properties of the commit.
    properties: BTreeMap<String, String>,
}

#[pymethods]
impl PyCommit {
    fn __repr__(&self) -> String {
        format!(
            "Commit(hash={:?}, author_name={:?}, title={:?})",
            self.hash, self.author_name, self.title
        )
    }
}

impl From<Commit> for PyCommit {
    fn from(commit: Commit) -> Self {
        Self {
            hash: commit.hash,
            title: commit.title,
            message: commit.message,
            author_name: commit.author_name,
            author_email: commit.author_email,
            authored_at: commit.authored_at,
            committer_name: commit.committer_name,
            committer_email: commit.committer_email,
            committed_at: commit.committed_at,
            parent_hash: commit.parent_hash,
            merge_parent_hash: commit.merge_parent_hash,
            properties: commit.properties,
        }
    }
}

#[pymethods]
impl Client {
    /// List the commit history at a ref, newest first.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// for commit in client.list_commits('main', limit=50):
    ///     ...
    /// ```
    ///
    /// Parameters:
    ///     ref: The ref, a branch, a tag or a commit hash.
    ///     limit: Optional, max number of commits to get.
    /// Returns:
    ///     A `bauplan.schema.IteratorAtCommit` over `bauplan.schema.Commit` objects. Its `commit_hash` is the hash of the commit `ref` points to, and all the pages are read at that commit.
    ///
    /// Raises:
    ///     `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (
        r#ref: "str",
        *,
        limit: "int | None" = None,
    ) -> "IteratorAtCommit[Commit]")]
    fn list_commits(
        &self,
        py: Python<'_>,
        r#ref: String,
        limit: Option<usize>,
    ) -> PyResult<PyIteratorAtCommit> {
        let client = self.refs();
        let catalog = self.profile.catalog.clone();
        PyIteratorAtCommit::new::<_, PyCommit, _>(
            py,
            &r#ref,
            limit,
            move |py, at, pagination_token, limit| {
                let query = ListQuery {
                    pagination_token: pagination_token.map(str::to_owned),
                    limit: limit.and_then(|limit| u32::try_from(limit).ok()),
                    filters: (),
                };
                let path = (catalog.clone(), at.to_owned());
                py.detach(|| client.list_history(path, query))
                    .map_err(api_error)
            },
        )
    }
}
