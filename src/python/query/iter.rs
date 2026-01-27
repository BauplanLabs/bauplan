use std::pin::Pin;
use std::sync::Mutex;

use arrow::array::RecordBatch;
use futures::{Stream, TryStreamExt};
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::python::rt;

type BatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, PyErr>> + Send>>;

/// A Python iterator that yields object rows from a stream of record batches.
///
/// Because the object conversion is annoying to do ourselves, we use
/// `pa.Table.to_pylist()` and then yield rows from it.
#[pyclass]
pub(crate) struct BatchStreamRowIterator {
    inner: Mutex<RowIterInner>,
}

struct RowIterInner {
    stream: BatchStream,
    pylist: Option<Py<PyList>>,
    pos: usize,
    len: usize,
}

impl BatchStreamRowIterator {
    pub(crate) fn new(stream: BatchStream) -> Self {
        Self {
            inner: Mutex::new(RowIterInner {
                stream,
                pylist: None,
                pos: 0,
                len: 0,
            }),
        }
    }
}

fn batch_to_pylist(py: Python<'_>, batch: RecordBatch) -> PyResult<Py<PyList>> {
    let py_batch = pyo3_arrow::PyRecordBatch::new(batch);
    let pa_batch = py_batch.into_pyarrow(py)?;
    Ok(pa_batch
        .call_method0("to_pylist")?
        .cast_into::<PyList>()?
        .unbind())
}

#[pymethods]
impl BatchStreamRowIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let mut inner = self.inner.lock().unwrap();

        if inner.pos < inner.len {
            let item = inner
                .pylist
                .as_ref()
                .unwrap()
                .bind(py)
                .get_item(inner.pos)?;
            inner.pos += 1;
            return Ok(Some(item));
        }

        let batch = match rt().block_on(inner.stream.try_next())? {
            Some(b) => b,
            None => return Ok(None),
        };

        inner.len = batch.num_rows();
        inner.pylist = Some(batch_to_pylist(py, batch)?);
        inner.pos = 1;

        Ok(inner.pylist.as_ref().unwrap().bind(py).get_item(0).ok())
    }
}
