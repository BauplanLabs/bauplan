use pyo3::prelude::*;

#[pymodule(submodule)]
pub mod schema {
    // Refs
    #[pymodule_export]
    use crate::python::branch::PyBranch as Branch;
    #[pymodule_export]
    use crate::python::refs::PyDetachedRef as DetachedRef;
    #[pymodule_export]
    use crate::python::refs::PyRef as Ref;
    #[pymodule_export]
    use crate::python::refs::PyRefType;
    #[pymodule_export]
    use crate::python::tag::PyTag as Tag;

    // Commits
    #[pymodule_export]
    use crate::python::commit::PyCommit as Commit;

    // Catalog
    #[pymodule_export]
    use crate::python::namespace::PyNamespaceIdentifier as NamespaceIdentifier;
    #[pymodule_export]
    use crate::python::table::PyPartitionField as PartitionField;
    #[pymodule_export]
    use crate::python::table::PyTable as Table;
    #[pymodule_export]
    use crate::python::table::PyTableIdentifier as TableIdentifier;

    // Jobs
    #[pymodule_export]
    use crate::grpc::job::Job;
    #[pymodule_export]
    use crate::grpc::job::JobKind;
    #[pymodule_export]
    use crate::grpc::job::JobState;
    #[pymodule_export]
    use crate::python::job::DAGEdge;
    #[pymodule_export]
    use crate::python::job::DAGNode;
    #[pymodule_export]
    use crate::python::job::JobContext;
    #[pymodule_export]
    use crate::python::job::JobLogEvent;
    #[pymodule_export]
    use crate::python::job::JobLogLevel;
    #[pymodule_export]
    use crate::python::job::JobLogStream;
}
