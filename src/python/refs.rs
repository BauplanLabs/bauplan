//! Ref types for the Python SDK.

use pyo3::Borrowed;
use pyo3::prelude::*;

use crate::CatalogRef;

/// The type of a ref.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[pyclass(
    name = "RefType",
    module = "bauplan.schema",
    rename_all = "SCREAMING_SNAKE_CASE",
    from_py_object,
    eq,
    str
)]
pub enum PyRefType {
    Branch,
    Tag,
    // Note: this doesn't really seem to be used.
    Detached,
}

impl std::fmt::Display for PyRefType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PyRefType::Branch => write!(f, "BRANCH"),
            PyRefType::Tag => write!(f, "TAG"),
            PyRefType::Detached => write!(f, "DETACHED"),
        }
    }
}

/// A reference to a branch, tag, or commit, as returned by API operations.
#[derive(Debug, Clone)]
#[pyclass(
    name = "Ref",
    module = "bauplan.schema",
    subclass,
    from_py_object,
    str,
    get_all
)]
pub struct PyRef {
    /// The name of the branch or tag.
    pub name: String,
    /// The hash of the branch or tag.
    pub hash: String,
    /// The type of the ref, either 'BRANCH', 'TAG', or 'DETACHED'.
    pub r#type: PyRefType,
}

impl PyRef {
    fn detached(hash: String) -> (PyDetachedRef, Self) {
        (
            PyDetachedRef,
            PyRef {
                name: "".to_string(),
                hash,
                r#type: PyRefType::Detached,
            },
        )
    }
}

// todo: does this work for subclasses?
impl std::fmt::Display for PyRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.name, self.hash)
    }
}

#[pymethods]
impl PyRef {
    fn __repr__(&self) -> String {
        format!(
            "{}(name={:?}, hash={:?})",
            self.r#type.class_name(),
            self.name,
            self.hash,
        )
    }
}

impl PyRefType {
    fn class_name(self) -> &'static str {
        match self {
            PyRefType::Branch | PyRefType::Tag => "Ref",
            PyRefType::Detached => "DetachedRef",
        }
    }
}

/// A ref not attached to a branch or tag, pointing directly to a commit hash.
#[derive(Debug, Clone, Copy)]
#[pyclass(name = "DetachedRef", module = "bauplan.schema", extends = PyRef, from_py_object)]
pub(crate) struct PyDetachedRef;

impl<'py> IntoPyObject<'py> for CatalogRef {
    type Target = PyRef;
    type Output = Bound<'py, PyRef>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            CatalogRef::Branch { name, hash } => {
                let r#ref = PyRef {
                    name,
                    hash,
                    r#type: PyRefType::Branch,
                };
                Ok(Bound::new(py, r#ref)?)
            }
            CatalogRef::Tag { name, hash } => {
                let r#ref = PyRef {
                    name,
                    hash,
                    r#type: PyRefType::Tag,
                };
                Ok(Bound::new(py, r#ref)?)
            }
            CatalogRef::Detached { hash } => Ok(Py::new(py, PyRef::detached(hash))?
                .into_bound(py)
                .into_super()),
        }
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for CatalogRef {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let r = ob.extract::<pyo3::PyRef<'_, PyRef>>()?;
        Ok(match r.r#type {
            PyRefType::Branch => CatalogRef::Branch {
                name: r.name.clone(),
                hash: r.hash.clone(),
            },
            PyRefType::Tag => CatalogRef::Tag {
                name: r.name.clone(),
                hash: r.hash.clone(),
            },
            PyRefType::Detached => CatalogRef::Detached {
                hash: r.hash.clone(),
            },
        })
    }
}
