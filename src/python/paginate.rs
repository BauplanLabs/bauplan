use std::sync::Mutex;

use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;

use crate::PaginatedResponse;

/// A Python iterator that wraps a Rust paginating iterator.
#[pyclass]
pub(crate) struct PyPaginator {
    inner: Mutex<Box<dyn Iterator<Item = PyResult<Py<PyAny>>> + Send>>,
}

// Note: we don't use crate::paginate here; we need the passed closure to own
// the original python function arguments, so that they can be 'static.
impl PyPaginator {
    pub(crate) fn new<T, F>(limit: Option<usize>, mut fetch_batch: F) -> PyResult<PyPaginator>
    where
        T: for<'py> IntoPyObject<'py> + Send + 'static,
        F: FnMut(Option<&str>, Option<usize>) -> PyResult<PaginatedResponse<T>> + Send + 'static,
    {
        let first_batch = fetch_batch(None, limit)?;
        let mut pagination_token = first_batch.pagination_token;
        let mut batch = first_batch.page.into_iter();
        let mut off = 0;
        let iter = std::iter::from_fn(move || {
            // Respect the overall limit.
            if limit.is_some_and(|l| off >= l) {
                return None;
            }

            // Return a value from the last batch, if we still have any.
            if let Some(v) = batch.next() {
                off += 1;
                return Some(Ok(v));
            }

            let token = pagination_token.take()?;
            let limit = limit.map(|l| l - off);
            let resp = match fetch_batch(Some(&token), limit) {
                Ok(v) => v,
                Err(e) => return Some(Err(e)),
            };

            batch = resp.page.into_iter();
            pagination_token = resp.pagination_token;

            if let Some(v) = batch.next() {
                off += 1;
                Some(Ok(v))
            } else {
                None
            }
        })
        .map(|result: PyResult<T>| -> PyResult<Py<PyAny>> {
            Python::attach(|py| result?.into_py_any(py))
        });

        Ok(PyPaginator {
            inner: Mutex::new(Box::new(iter)),
        })
    }
}

#[pymethods]
impl PyPaginator {
    fn __iter__(this: PyRef<'_, Self>) -> PyRef<'_, Self> {
        this
    }

    fn __next__(this: PyRefMut<'_, Self>) -> PyResult<Option<Py<PyAny>>> {
        match this.inner.lock().unwrap().next() {
            Some(Ok(item)) => Ok(Some(item)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}
