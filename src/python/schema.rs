use pyo3::prelude::*;

#[pymodule(submodule)]
pub mod schema {
    // Refs
    #[pymodule_export]
    use crate::python::refs::PyBranch as Branch;
    #[pymodule_export]
    use crate::python::refs::PyDetachedRef as DetachedRef;
    #[pymodule_export]
    use crate::python::refs::PyRef as Ref;
    #[pymodule_export]
    use crate::python::refs::PyRefType;
    #[pymodule_export]
    use crate::python::refs::PyTag as Tag;

    // Commits
    #[pymodule_export]
    use crate::commit::Actor;
    #[pymodule_export]
    use crate::commit::Commit;

    // Catalog
    #[pymodule_export]
    use crate::namespace::Namespace;
    #[pymodule_export]
    use crate::table::PartitionField;
    #[pymodule_export]
    use crate::table::Table;
    #[pymodule_export]
    use crate::table::TableField;
    #[pymodule_export]
    use crate::table::TableKind;

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
    use crate::python::job::JobLogList;
    #[pymodule_export]
    use crate::python::job::JobLogStream;
}
