use std::collections::VecDeque;
use std::sync::Mutex;

use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;

use bauplan_api::catalog_ref::BetweenCommits;
use bauplan_api::model::AtCommit;
use bauplan_api::pagination::Page;

/// One page of results, with the token of the next page.
pub(crate) struct PaginatedResponse<T> {
    pub(crate) page: Vec<T>,
    pub(crate) pagination_token: Option<String>,
}

type FetchFn = dyn FnMut(
        Python<'_>,
        Option<&str>,  // Pagination token.
        Option<usize>, // Limit.
    ) -> PyResult<(VecDeque<Py<PyAny>>, Option<String>)>
    + Send;

/// A Python iterator that makes repeated paginated requests.
#[pyclass]
pub(crate) struct PyPaginator {
    inner: Mutex<PaginatorState>,
}

struct PaginatorState {
    batch: VecDeque<Py<PyAny>>,
    pagination_token: Option<String>,
    limit: Option<usize>,
    off: usize,
    fetch: Box<FetchFn>,
}

// The passed function takes a pagination token (None for the first request)
// and a limit, and should return N <= limit items.
impl PyPaginator {
    pub(crate) fn new<T, F>(
        py: Python<'_>,
        limit: Option<usize>,
        mut fetch_batch: F,
    ) -> PyResult<PyPaginator>
    where
        T: for<'py> IntoPyObject<'py> + Send + 'static,
        F: FnMut(Python<'_>, Option<&str>, Option<usize>) -> PyResult<PaginatedResponse<T>>
            + Send
            + 'static,
    {
        // The api wants a limit of at least 1, so we do not ask for zero items.
        let first = match limit {
            Some(0) => PaginatedResponse {
                page: Vec::new(),
                pagination_token: None,
            },
            _ => fetch_batch(py, None, limit)?,
        };
        Self::with_first_page(py, limit, first, fetch_batch)
    }

    fn with_first_page<T, F>(
        py: Python<'_>,
        limit: Option<usize>,
        first: PaginatedResponse<T>,
        mut fetch_batch: F,
    ) -> PyResult<PyPaginator>
    where
        T: for<'py> IntoPyObject<'py> + Send + 'static,
        F: FnMut(Python<'_>, Option<&str>, Option<usize>) -> PyResult<PaginatedResponse<T>>
            + Send
            + 'static,
    {
        let buf = convert_page(py, first.page)?;

        let fetch: Box<FetchFn> = Box::new(move |py, token, limit| {
            let resp = fetch_batch(py, token, limit)?;
            Ok((convert_page(py, resp.page)?, resp.pagination_token))
        });

        Ok(PyPaginator {
            inner: Mutex::new(PaginatorState {
                batch: buf,
                pagination_token: first.pagination_token,
                limit,
                off: 0,
                fetch,
            }),
        })
    }
}

/// An iterator that reads all its pages at one commit, so the list does not
/// change if the ref moves.
#[pyclass(name = "IteratorAtCommit", module = "bauplan.schema", generic)]
pub(crate) struct PyIteratorAtCommit {
    /// The hash of the commit the list is read at.
    #[pyo3(get)]
    commit_hash: String,
    pages: PyPaginator,
}

impl PyIteratorAtCommit {
    /// `fetch_batch` takes the ref or the commit to read at, a pagination
    /// token and a limit. We read the first page at `ref` and the next ones
    /// at its commit.
    pub(crate) fn new<T, U, F>(
        py: Python<'_>,
        r#ref: &str,
        limit: Option<usize>,
        mut fetch_batch: F,
    ) -> PyResult<Self>
    where
        U: From<T> + for<'py> IntoPyObject<'py> + Send + 'static,
        F: FnMut(Python<'_>, &str, Option<&str>, Option<usize>) -> PyResult<AtCommit<Page<T>>>
            + Send
            + 'static,
    {
        // With a limit of 0 we still ask for one item to learn the commit, and
        // the iterator then yields nothing.
        let first = fetch_batch(py, r#ref, None, limit.map(|limit| limit.max(1)))?;
        let commit_hash = first.commit_hash;
        let at = commit_hash.clone();
        let pages = PyPaginator::with_first_page(
            py,
            limit,
            from_api_page::<T, U>(first.inner),
            move |py, pagination_token, limit| {
                let page = fetch_batch(py, &at, pagination_token, limit)?;
                Ok(from_api_page(page.inner))
            },
        )?;

        Ok(Self { commit_hash, pages })
    }
}

#[pymethods]
impl PyIteratorAtCommit {
    fn __repr__(&self) -> String {
        format!("IteratorAtCommit(commit_hash={:?})", self.commit_hash)
    }

    fn __iter__(this: PyRef<'_, Self>) -> PyRef<'_, Self> {
        this
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.pages.__next__(py)
    }
}

/// An iterator that reads all its pages at the same two commits, so the
/// diff does not change if the refs move.
#[pyclass(name = "IteratorBetweenCommits", module = "bauplan.schema", generic)]
pub(crate) struct PyIteratorBetweenCommits {
    /// The hash of the commit at `from_ref`.
    #[pyo3(get)]
    from_commit_hash: String,
    /// The hash of the commit at `to_ref`.
    #[pyo3(get)]
    to_commit_hash: String,
    pages: PyPaginator,
}

impl PyIteratorBetweenCommits {
    /// `fetch_batch` takes the two refs or commits to read at, a pagination
    /// token and a limit. We read the first page at the refs and the next
    /// ones at their commits.
    pub(crate) fn new<T, U, F>(
        py: Python<'_>,
        from_ref: &str,
        to_ref: &str,
        limit: Option<usize>,
        mut fetch_batch: F,
    ) -> PyResult<Self>
    where
        U: From<T> + for<'py> IntoPyObject<'py> + Send + 'static,
        F: FnMut(
                Python<'_>,
                &str,
                &str,
                Option<&str>,
                Option<usize>,
            ) -> PyResult<BetweenCommits<Page<T>>>
            + Send
            + 'static,
    {
        // With a limit of 0 we still ask for one item to learn the commits,
        // and the iterator then yields nothing.
        let first = fetch_batch(py, from_ref, to_ref, None, limit.map(|limit| limit.max(1)))?;
        let from_commit_hash = first.from_commit_hash;
        let to_commit_hash = first.to_commit_hash;
        let (from_at, to_at) = (from_commit_hash.clone(), to_commit_hash.clone());
        let pages = PyPaginator::with_first_page(
            py,
            limit,
            from_api_page::<T, U>(first.inner),
            move |py, pagination_token, limit| {
                let page = fetch_batch(py, &from_at, &to_at, pagination_token, limit)?;
                Ok(from_api_page(page.inner))
            },
        )?;

        Ok(Self {
            from_commit_hash,
            to_commit_hash,
            pages,
        })
    }
}

#[pymethods]
impl PyIteratorBetweenCommits {
    fn __repr__(&self) -> String {
        format!(
            "IteratorBetweenCommits(from_commit_hash={:?}, to_commit_hash={:?})",
            self.from_commit_hash, self.to_commit_hash
        )
    }

    fn __iter__(this: PyRef<'_, Self>) -> PyRef<'_, Self> {
        this
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.pages.__next__(py)
    }
}

/// A page of `bauplan-api`, with its items converted for Python.
pub(crate) fn from_api_page<T, U: From<T>>(page: Page<T>) -> PaginatedResponse<U> {
    PaginatedResponse {
        page: page.items.into_iter().map(U::from).collect(),
        pagination_token: page.pagination_token,
    }
}

fn convert_page<T: for<'py> IntoPyObject<'py>>(
    py: Python<'_>,
    page: Vec<T>,
) -> PyResult<VecDeque<Py<PyAny>>> {
    page.into_iter()
        .map(|v| v.into_py_any(py))
        .collect::<PyResult<_>>()
}

#[pymethods]
impl PyPaginator {
    fn __iter__(this: PyRef<'_, Self>) -> PyRef<'_, Self> {
        this
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let state = &mut *self.inner.lock().unwrap();

        if state.limit.is_some_and(|l| state.off >= l) {
            return Ok(None);
        }

        // A page can come back empty with a token, when the server drops what
        // it can't show, so we read on until we find an item or the end.
        loop {
            if let Some(item) = state.batch.pop_front() {
                state.off += 1;
                return Ok(Some(item));
            }

            let Some(token) = state.pagination_token.take() else {
                return Ok(None);
            };

            let remaining = state.limit.map(|l| l - state.off);
            let (batch, token) = (state.fetch)(py, Some(&token), remaining)?;
            state.batch = batch;
            state.pagination_token = token;
        }
    }
}
