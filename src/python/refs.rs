//! Ref types for the Python SDK.

use pyo3::prelude::*;
use pyo3::{Borrowed, IntoPyObjectExt};

use crate::api::CatalogRef;

#[pyclass(name = "Branch", module = "bauplan")]
#[derive(Debug, Clone)]
pub(crate) struct PyBranch {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    hash: String,
}

/// Accepts either a branch name (str) or a Branch object.
pub(crate) struct BranchArg(pub String);

impl<'a, 'py> FromPyObject<'a, 'py> for BranchArg {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(s) = ob.extract::<String>() {
            Ok(BranchArg(s))
        } else if let Ok(branch) = ob.extract::<PyRef<'_, PyBranch>>() {
            Ok(BranchArg(branch.name.clone()))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "expected str or Branch",
            ))
        }
    }
}

/// Accepts a ref name (str), Branch, Tag, or DetachedRef object.
pub(crate) struct RefArg(pub String);

impl<'a, 'py> FromPyObject<'a, 'py> for RefArg {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(s) = ob.extract::<String>() {
            Ok(RefArg(s))
        } else if let Ok(branch) = ob.extract::<PyRef<'_, PyBranch>>() {
            Ok(RefArg(branch.name.clone()))
        } else if let Ok(tag) = ob.extract::<PyRef<'_, PyTag>>() {
            Ok(RefArg(tag.name.clone()))
        } else if let Ok(detached) = ob.extract::<PyRef<'_, PyDetachedRef>>() {
            Ok(RefArg(detached.hash.clone()))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "expected str, Branch, Tag, or DetachedRef",
            ))
        }
    }
}

/// Accepts a namespace name (str) or Namespace object.
pub(crate) struct NamespaceArg(pub String);

impl<'a, 'py> FromPyObject<'a, 'py> for NamespaceArg {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(s) = ob.extract::<String>() {
            Ok(NamespaceArg(s))
        } else if let Ok(ns) = ob.extract::<PyRef<'_, crate::namespace::Namespace>>() {
            Ok(NamespaceArg(ns.name.clone()))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "expected str or Namespace",
            ))
        }
    }
}

/// Accepts a table name (str) or TableWithMetadata object.
pub(crate) struct TableArg(pub String);

impl<'a, 'py> FromPyObject<'a, 'py> for TableArg {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(s) = ob.extract::<String>() {
            Ok(TableArg(s))
        } else if let Ok(table) = ob.extract::<PyRef<'_, crate::table::TableWithMetadata>>() {
            Ok(TableArg(table.name.clone()))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "expected str or Table",
            ))
        }
    }
}

/// Accepts a tag name (str) or Tag object.
pub(crate) struct TagArg(pub String);

impl<'a, 'py> FromPyObject<'a, 'py> for TagArg {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(s) = ob.extract::<String>() {
            Ok(TagArg(s))
        } else if let Ok(tag) = ob.extract::<PyRef<'_, PyTag>>() {
            Ok(TagArg(tag.name.clone()))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "expected str or Tag",
            ))
        }
    }
}

#[pymethods]
impl PyBranch {
    #[new]
    pub(crate) fn new(name: String, hash: String) -> Self {
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
    pub(crate) name: String,
    #[pyo3(get)]
    hash: String,
}

#[pymethods]
impl PyTag {
    #[new]
    pub(crate) fn new(name: String, hash: String) -> Self {
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

impl<'py> IntoPyObject<'py> for crate::branch::Branch {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyBranch {
            name: self.name,
            hash: self.hash,
        }
        .into_bound_py_any(py)
    }
}

impl<'py> IntoPyObject<'py> for crate::tag::Tag {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyTag {
            name: self.name,
            hash: self.hash,
        }
        .into_bound_py_any(py)
    }
}
