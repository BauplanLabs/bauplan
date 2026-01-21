//! Ref types for the Python SDK.

use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;

use crate::api::CatalogRef;

#[pyclass(name = "Branch", module = "bauplan")]
#[derive(Debug, Clone)]
pub(crate) struct PyBranch {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    hash: String,
}

#[pymethods]
impl PyBranch {
    #[new]
    fn new(name: String, hash: String) -> Self {
        Self { name, hash }
    }

    #[getter]
    fn r#type(&self) -> &'static str {
        "BRANCH"
    }

    fn __str__(&self) -> String {
        format!("{}@{}", self.name, self.hash)
    }
}

#[pyclass(name = "Tag", module = "bauplan")]
#[derive(Debug, Clone)]
pub(crate) struct PyTag {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    hash: String,
}

#[pymethods]
impl PyTag {
    #[new]
    fn new(name: String, hash: String) -> Self {
        Self { name, hash }
    }

    #[getter]
    fn r#type(&self) -> &'static str {
        "TAG"
    }

    fn __str__(&self) -> String {
        format!("{}@{}", self.name, self.hash)
    }
}

#[pyclass(name = "DetachedRef", module = "bauplan")]
#[derive(Debug, Clone)]
pub(crate) struct PyDetachedRef {
    #[pyo3(get)]
    hash: String,
}

#[pymethods]
impl PyDetachedRef {
    #[new]
    fn new(hash: String) -> Self {
        Self { hash }
    }

    #[getter]
    fn r#type(&self) -> &'static str {
        "DETACHED"
    }

    fn __str__(&self) -> String {
        format!("@{}", self.hash)
    }
}

impl<'py> IntoPyObject<'py> for CatalogRef {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            CatalogRef::Branch { name, hash } => PyBranch { name, hash }.into_bound_py_any(py),
            CatalogRef::Tag { name, hash } => PyTag { name, hash }.into_bound_py_any(py),
            CatalogRef::Detached { hash } => PyDetachedRef { hash }.into_bound_py_any(py),
        }
    }
}
