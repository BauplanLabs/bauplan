//! The diff between two refs.

use bauplan_api::catalog_ref;
use bauplan_api::model::Entity;
use bauplan_api::pagination::ListQuery;
use pyo3::prelude::*;

use super::Client;
use super::exceptions::api_error;
use super::paginate::PyIteratorBetweenCommits;

/// An entity of the catalog, like a table or a namespace.
#[derive(Clone, PartialEq)]
#[pyclass(name = "Entity", module = "bauplan.schema", frozen, eq)]
pub(crate) enum PyEntity {
    /// A table.
    Table {
        /// The full name of the table, `namespace.table`.
        name: String,
    },
    /// A namespace.
    Namespace {
        /// The name of the namespace.
        name: String,
    },
    /// A kind of entity this client does not know yet.
    Unknown {},
}

#[pymethods]
impl PyEntity {
    fn __repr__(&self) -> String {
        match self {
            Self::Table { name } => format!("Entity.Table(name={name:?})"),
            Self::Namespace { name } => format!("Entity.Namespace(name={name:?})"),
            Self::Unknown {} => "Entity.Unknown()".to_owned(),
        }
    }
}

impl From<Entity> for PyEntity {
    fn from(entity: Entity) -> Self {
        match entity {
            Entity::Table(name) => Self::Table { name },
            Entity::Namespace(name) => Self::Namespace { name },
            Entity::Unknown(_) => Self::Unknown {},
        }
    }
}

/// How an entity changes from `from_ref` to `to_ref`. It is one of
/// `DiffChange.Added`, `DiffChange.Removed`, `DiffChange.Changed`,
/// `DiffChange.Renamed` or `DiffChange.Unknown`.
#[derive(Clone, PartialEq)]
#[pyclass(name = "DiffChange", module = "bauplan.schema", frozen, eq)]
pub(crate) enum PyDiffChange {
    /// Only at `to_ref`.
    Added {},
    /// Only at `from_ref`.
    Removed {},
    /// At both refs, but it changed.
    Changed {},
    /// At `to_ref` with a new name.
    Renamed {
        /// The name at `from_ref`.
        from_name: String,
    },
    /// A kind of change this client does not know yet.
    Unknown {},
}

#[pymethods]
impl PyDiffChange {
    fn __repr__(&self) -> String {
        match self {
            Self::Added {} => "DiffChange.Added()".to_owned(),
            Self::Removed {} => "DiffChange.Removed()".to_owned(),
            Self::Changed {} => "DiffChange.Changed()".to_owned(),
            Self::Renamed { from_name } => format!("DiffChange.Renamed(from_name={from_name:?})"),
            Self::Unknown {} => "DiffChange.Unknown()".to_owned(),
        }
    }
}

impl From<catalog_ref::DiffChange> for PyDiffChange {
    fn from(change: catalog_ref::DiffChange) -> Self {
        match change {
            catalog_ref::DiffChange::Added => Self::Added {},
            catalog_ref::DiffChange::Removed => Self::Removed {},
            catalog_ref::DiffChange::Changed => Self::Changed {},
            catalog_ref::DiffChange::Renamed { from } => Self::Renamed { from_name: from },
            catalog_ref::DiffChange::Unknown(_) => Self::Unknown {},
        }
    }
}

/// A table or a namespace that differs between the two refs, and how.
#[pyclass(name = "DiffEntry", module = "bauplan.schema", frozen, get_all)]
pub(crate) struct PyDiffEntry {
    /// The table or the namespace.
    entity: PyEntity,
    /// How it changes from `from_ref` to `to_ref`.
    change: PyDiffChange,
}

#[pymethods]
impl PyDiffEntry {
    fn __repr__(&self) -> String {
        format!(
            "DiffEntry(entity={}, change={})",
            self.entity.__repr__(),
            self.change.__repr__()
        )
    }
}

impl From<catalog_ref::DiffEntry> for PyDiffEntry {
    fn from(entry: catalog_ref::DiffEntry) -> Self {
        Self {
            entity: entry.entity.into(),
            change: entry.change.into(),
        }
    }
}

#[pymethods]
impl Client {
    /// List the tables and namespaces that differ between two refs, in name
    /// order.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// for entry in client.diff_refs('main', 'username.feature'):
    ///     print(entry.change, entry.entity)
    /// ```
    ///
    /// Parameters:
    ///     from_ref: The ref to compare from, a branch, a tag or a commit hash.
    ///     to_ref: The ref to compare to, a branch, a tag or a commit hash.
    ///     limit: Optional, max number of entries to get.
    /// Returns:
    ///     A `bauplan.schema.IteratorBetweenCommits` over `bauplan.schema.DiffEntry` objects. Its `from_commit_hash` and `to_commit_hash` are the hashes of the commits the refs point to, and all the pages are read at those commits.
    ///
    /// Raises:
    ///     `bauplan.exceptions.RefNotFoundError`: if a ref does not exist.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (
        from_ref: "str",
        to_ref: "str",
        *,
        limit: "int | None" = None,
    ) -> "IteratorBetweenCommits[DiffEntry]")]
    fn diff_refs(
        &self,
        py: Python<'_>,
        from_ref: String,
        to_ref: String,
        limit: Option<usize>,
    ) -> PyResult<PyIteratorBetweenCommits> {
        let client = self.refs();
        let catalog = self.profile.catalog.clone();
        PyIteratorBetweenCommits::new::<_, PyDiffEntry, _>(
            py,
            &from_ref,
            &to_ref,
            limit,
            move |py, from_at, to_at, pagination_token, limit| {
                let query = ListQuery {
                    pagination_token: pagination_token.map(str::to_owned),
                    limit: limit.and_then(|limit| u32::try_from(limit).ok()),
                    filters: (),
                };
                let path = (catalog.clone(), from_at.to_owned(), to_at.to_owned());
                py.detach(|| client.diff_refs(path, query))
                    .map_err(api_error)
            },
        )
    }
}
