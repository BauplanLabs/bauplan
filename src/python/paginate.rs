use std::collections::VecDeque;
use std::sync::Mutex;

use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;

use crate::PaginatedResponse;

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

// Note: we don't use crate::paginate here; we need the passed closure to own
// the original python function arguments, so that they can be 'static.
//
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
        let first = fetch_batch(py, None, limit)?;
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

        if let Some(item) = state.batch.pop_front() {
            state.off += 1;
            return Ok(Some(item));
        }

        let token = match state.pagination_token.take() {
            Some(t) => t,
            None => return Ok(None),
        };

        let remaining = state.limit.map(|l| l - state.off);
        let (batch, token) = (state.fetch)(py, Some(&token), remaining)?;
        state.batch = batch;
        state.pagination_token = token;

        if let Some(item) = state.batch.pop_front() {
            state.off += 1;
            Ok(Some(item))
        } else {
            Ok(None)
        }
    }
}
