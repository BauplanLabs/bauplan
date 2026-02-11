//! Ref types for the Python SDK.

use pyo3::Borrowed;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use crate::CatalogRef;
use crate::branch::Branch;
use crate::tag::Tag;

/// The type of a ref.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[pyclass(
    name = "RefType",
    module = "bauplan",
    eq,
    str,
    rename_all = "SCREAMING_SNAKE_CASE"
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
#[pyclass(name = "Ref", module = "bauplan", str, get_all, subclass)]
pub struct PyRef {
    pub name: String,
    pub hash: String,
    pub r#type: PyRefType,
}

impl PyRef {
    fn branch(name: String, hash: String) -> (PyBranch, Self) {
        (
            PyBranch,
            PyRef {
                name,
                hash,
                r#type: PyRefType::Branch,
            },
        )
    }

    fn tag(name: String, hash: String) -> (PyTag, Self) {
        (
            PyTag,
            PyRef {
                name,
                hash,
                r#type: PyRefType::Tag,
            },
        )
    }

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
            PyRefType::Branch => "Branch",
            PyRefType::Tag => "Tag",
            PyRefType::Detached => "DetachedRef",
        }
    }
}

/// A branch reference returned by the API.
#[derive(Debug, Clone, Copy)]
#[pyclass(name = "Branch", module = "bauplan", extends = PyRef)]
pub struct PyBranch;

/// A tag reference returned by the API.
#[derive(Debug, Clone, Copy)]
#[pyclass(name = "Tag", module = "bauplan", extends = PyRef)]
pub struct PyTag;

/// A detached reference (a specific commit, not on any branch) returned by the API.
#[derive(Debug, Clone, Copy)]
#[pyclass(name = "DetachedRef", module = "bauplan", extends = PyRef)]
pub(crate) struct PyDetachedRef;

/// Accepts a ref hash, a tag/branch name, or any ref object (Ref, Branch,
/// Tag, DetachedRef), from which a ref string that the API understands is
/// extracted.
///
/// This is used by API methods which operate on some ref (eg `query`).
///
/// For example:
///  - For `Branch(name='foo', hash='abcd...')`, the result
///    is `foo@abcd...`.
///  - For `Tag(name='bar', hash=None)`, the result is `bar`.
pub(crate) struct RefArg(pub String);

impl<'a, 'py> FromPyObject<'a, 'py> for RefArg {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(s) = ob.extract::<String>() {
            Ok(RefArg(s))
        } else if let Ok(r) = ob.extract::<pyo3::PyRef<'_, PyRef>>() {
            Ok(RefArg(r.to_string()))
        } else {
            Err(PyTypeError::new_err("expected str or Ref"))
        }
    }
}

/// Accepts either a branch name or a Branch object (from which the name is extracted).
///
/// This is used by methods like `rename_branch`, which operate on the branch
/// name and not a specific hash.
pub(crate) struct BranchArg(pub(crate) String);

impl<'a, 'py> FromPyObject<'a, 'py> for BranchArg {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(s) = ob.extract::<String>() {
            Ok(BranchArg(s))
        } else if let Ok(branch) = ob.extract::<pyo3::PyRef<'_, PyBranch>>() {
            Ok(BranchArg(branch.as_super().name.clone()))
        } else {
            Err(PyTypeError::new_err("expected str or Branch"))
        }
    }
}

/// Accepts a tag name or Tag object (from which the name is extracted).
///
/// This is used by methods like `delete_tag`, which operate on the tag name and
/// not a specific tag.
pub(crate) struct TagArg(pub String);

impl<'a, 'py> FromPyObject<'a, 'py> for TagArg {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(s) = ob.extract::<String>() {
            Ok(TagArg(s))
        } else if let Ok(tag) = ob.extract::<pyo3::PyRef<'_, PyTag>>() {
            Ok(TagArg(tag.as_super().name.clone()))
        } else {
            Err(PyTypeError::new_err("expected str or Tag"))
        }
    }
}

impl<'py> IntoPyObject<'py> for CatalogRef {
    type Target = PyRef;
    type Output = Bound<'py, PyRef>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            CatalogRef::Branch { name, hash } => Ok(Py::new(py, PyRef::branch(name, hash))?
                .into_bound(py)
                .into_super()),
            CatalogRef::Tag { name, hash } => Ok(Py::new(py, PyRef::tag(name, hash))?
                .into_bound(py)
                .into_super()),
            CatalogRef::Detached { hash } => Ok(Py::new(py, PyRef::detached(hash))?
                .into_bound(py)
                .into_super()),
        }
    }
}

impl<'py> IntoPyObject<'py> for Branch {
    type Target = PyBranch;
    type Output = Bound<'py, PyBranch>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(Py::new(py, PyRef::branch(self.name, self.hash))?.into_bound(py))
    }
}

impl<'py> IntoPyObject<'py> for Tag {
    type Target = PyTag;
    type Output = Bound<'py, PyTag>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(Py::new(py, PyRef::tag(self.name, self.hash))?.into_bound(py))
    }
}
