//! Table operations.

use std::sync::Arc;

use bauplan_api::gropius::client::ClientError;
use bauplan_api::model::{CommitOptions, WithCommit};
use bauplan_api::pagination::ListQuery;
use bauplan_api::table::{
    self, CloneTableBody, CreateTableBody, CreateTableKind, DeleteTableBody, DeleteTableError,
    GetTableError, PartitionTransform, RenameTableBody, TableFilters, TableKind,
};
use chrono::{DateTime, Utc};
use commanderpb::runner_event::Event as RunnerEvent;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;

use crate::{grpc::generated as commanderpb, python::job_err};

use super::arrow_schema::{from_arrow_schema, to_arrow_schema};
use super::exceptions::{
    BauplanError, TableCreatePlanApplyStatusError, TableCreatePlanStatusError, api_error,
};
use super::paginate::PyIteratorAtCommit;
use super::run::job_status_strings;
use super::{Client, commit::PyCommit};
use crate::python::run::state::{
    ExternalTableCreateContext, ExternalTableCreateState, TableCreatePlanApplyState,
    TableCreatePlanContext, TableCreatePlanState, TableDataImportContext, TableDataImportState,
};

/// A table in a list.
#[pyclass(name = "TableIdentifier", module = "bauplan.schema", frozen, get_all)]
pub(crate) struct PyTableIdentifier {
    /// The full name of the table, `namespace.table`.
    name: String,
}

#[pymethods]
impl PyTableIdentifier {
    fn __repr__(&self) -> String {
        format!("TableIdentifier(name={:?})", self.name)
    }
}

impl From<table::TableIdentifier> for PyTableIdentifier {
    fn from(table: table::TableIdentifier) -> Self {
        Self { name: table.name }
    }
}

/// A table in the lake.
#[pyclass(name = "Table", module = "bauplan.schema", frozen, get_all)]
pub(crate) struct PyTable {
    /// The full name of the table, `namespace.table`.
    name: String,
    /// The kind of table and, when known, the location of its metadata file.
    kind: PyTableKind,
    /// The documentation of the table.
    doc: Option<String>,
    /// The number of records, if the current snapshot has the totals.
    records: Option<u64>,
    /// The size of the data files in bytes, if the current snapshot has the
    /// totals.
    size: Option<u64>,
    /// The schema of the table, a `pyarrow.Schema`.
    schema: Py<PyAny>,
    /// The partition fields.
    partitions: Vec<PyPartitionField>,
    /// When the table was created, only for the tables that record it.
    created_at: Option<DateTime<Utc>>,
    /// When the table was last written.
    updated_at: DateTime<Utc>,
    /// The commit the table was read or written at.
    commit: Py<PyCommit>,
}

#[pymethods]
impl PyTable {
    fn __repr__(&self) -> String {
        format!("Table(name={:?})", self.name)
    }
}

impl PyTable {
    fn new(py: Python<'_>, table: WithCommit<table::Table>) -> PyResult<Self> {
        let WithCommit { inner, commit } = table;
        let schema = to_arrow_schema(inner.columns)
            .map_err(|err| BauplanError::new_err(format!("invalid table schema: {err}")))?;
        let schema = pyo3_arrow::PySchema::new(Arc::new(schema)).into_pyarrow(py)?;
        Ok(Self {
            name: inner.name,
            kind: inner.kind.into(),
            doc: inner.doc,
            records: inner.records,
            size: inner.size,
            schema: schema.unbind(),
            partitions: inner.partitions.into_iter().map(Into::into).collect(),
            created_at: inner.created_at,
            updated_at: inner.updated_at,
            commit: Py::new(py, PyCommit::from(commit))?,
        })
    }
}

/// The kind of a table. It is one of `TableKind.IcebergManaged`,
/// `TableKind.IcebergExternal`, `TableKind.IcebergExternalParquet` or
/// `TableKind.Unknown`. All of them except `TableKind.Unknown` have the
/// `metadata_location` of the table.
#[derive(Clone)]
#[pyclass(name = "TableKind", module = "bauplan.schema", frozen)]
pub(crate) enum PyTableKind {
    /// An iceberg table whose files bauplan writes and owns.
    IcebergManaged {
        /// The location of the current metadata file.
        metadata_location: String,
    },
    /// A read-only iceberg table whose metadata and data files live outside
    /// bauplan.
    IcebergExternal {
        /// The location of the current metadata file.
        metadata_location: String,
    },
    /// A read-only iceberg table whose metadata bauplan writes, over parquet
    /// files that live outside bauplan.
    IcebergExternalParquet {
        /// The location of the current metadata file.
        metadata_location: String,
    },
    /// A kind this client does not know yet.
    Unknown {},
}

#[pymethods]
impl PyTableKind {
    fn __repr__(&self) -> String {
        match self {
            Self::IcebergManaged { metadata_location } => {
                format!("TableKind.IcebergManaged(metadata_location={metadata_location:?})")
            }
            Self::IcebergExternal { metadata_location } => {
                format!("TableKind.IcebergExternal(metadata_location={metadata_location:?})")
            }
            Self::IcebergExternalParquet { metadata_location } => {
                format!("TableKind.IcebergExternalParquet(metadata_location={metadata_location:?})")
            }
            Self::Unknown {} => "TableKind.Unknown()".to_owned(),
        }
    }
}

impl From<TableKind> for PyTableKind {
    fn from(kind: TableKind) -> Self {
        match kind {
            TableKind::IcebergManaged { metadata_location } => {
                Self::IcebergManaged { metadata_location }
            }
            TableKind::IcebergExternal { metadata_location } => {
                Self::IcebergExternal { metadata_location }
            }
            TableKind::IcebergExternalParquet { metadata_location } => {
                Self::IcebergExternalParquet { metadata_location }
            }
            TableKind::Unknown(_) => Self::Unknown {},
        }
    }
}

/// A partition field of a table.
#[derive(Clone)]
#[pyclass(
    name = "PartitionField",
    module = "bauplan.schema",
    frozen,
    get_all,
    from_py_object
)]
pub(crate) struct PyPartitionField {
    /// The field the table is partitioned by.
    name: String,
    /// The partition transform.
    transform: PyPartitionTransform,
}

#[pymethods]
impl PyPartitionField {
    #[new]
    #[pyo3(signature = (name: "str", transform: "PartitionTransform"))]
    fn new(name: String, transform: PyPartitionTransform) -> Self {
        Self { name, transform }
    }

    fn __repr__(&self) -> String {
        format!(
            "PartitionField(name={:?}, transform={})",
            self.name,
            self.transform.__repr__()
        )
    }
}

impl TryFrom<PyPartitionField> for table::PartitionField {
    type Error = PyErr;

    fn try_from(field: PyPartitionField) -> PyResult<Self> {
        let transform = match field.transform {
            PyPartitionTransform::Identity {} => PartitionTransform::Identity,
            PyPartitionTransform::Year {} => PartitionTransform::Year,
            PyPartitionTransform::Month {} => PartitionTransform::Month,
            PyPartitionTransform::Day {} => PartitionTransform::Day,
            PyPartitionTransform::Hour {} => PartitionTransform::Hour,
            PyPartitionTransform::Bucket { num_buckets } => PartitionTransform::Bucket(num_buckets),
            PyPartitionTransform::Truncate { width } => PartitionTransform::Truncate(width),
            PyPartitionTransform::Void {} => PartitionTransform::Void,
            PyPartitionTransform::Unknown {} => {
                return Err(PyValueError::new_err(format!(
                    "unknown partition transform of {:?}",
                    field.name
                )));
            }
        };
        Ok(Self {
            name: field.name,
            transform,
        })
    }
}

impl From<table::PartitionField> for PyPartitionField {
    fn from(field: table::PartitionField) -> Self {
        Self {
            name: field.name,
            transform: field.transform.into(),
        }
    }
}

/// How a partition field derives its value from the column. It is one of
/// `PartitionTransform.Identity`, `PartitionTransform.Year`,
/// `PartitionTransform.Month`, `PartitionTransform.Day`,
/// `PartitionTransform.Hour`, `PartitionTransform.Bucket`,
/// `PartitionTransform.Truncate`, `PartitionTransform.Void` or
/// `PartitionTransform.Unknown`.
#[derive(Clone)]
#[pyclass(
    name = "PartitionTransform",
    module = "bauplan.schema",
    frozen,
    from_py_object
)]
pub(crate) enum PyPartitionTransform {
    /// The value of the column.
    Identity {},
    /// The year of a date or a timestamp.
    Year {},
    /// The month of a date or a timestamp.
    Month {},
    /// The day of a date or a timestamp.
    Day {},
    /// The hour of a timestamp.
    Hour {},
    /// A hash of the value, in `num_buckets` buckets.
    Bucket {
        /// The number of buckets.
        num_buckets: u32,
    },
    /// The value truncated to `width`.
    Truncate {
        /// The width to truncate to.
        width: u32,
    },
    /// Always null, for a partition field that is no longer used.
    Void {},
    /// A transform this client does not know yet.
    Unknown {},
}

#[pymethods]
impl PyPartitionTransform {
    fn __repr__(&self) -> String {
        match self {
            Self::Identity {} => "PartitionTransform.Identity()".to_owned(),
            Self::Year {} => "PartitionTransform.Year()".to_owned(),
            Self::Month {} => "PartitionTransform.Month()".to_owned(),
            Self::Day {} => "PartitionTransform.Day()".to_owned(),
            Self::Hour {} => "PartitionTransform.Hour()".to_owned(),
            Self::Bucket { num_buckets } => {
                format!("PartitionTransform.Bucket(num_buckets={num_buckets})")
            }
            Self::Truncate { width } => format!("PartitionTransform.Truncate(width={width})"),
            Self::Void {} => "PartitionTransform.Void()".to_owned(),
            Self::Unknown {} => "PartitionTransform.Unknown()".to_owned(),
        }
    }
}

impl From<PartitionTransform> for PyPartitionTransform {
    fn from(transform: PartitionTransform) -> Self {
        match transform {
            PartitionTransform::Identity => Self::Identity {},
            PartitionTransform::Year => Self::Year {},
            PartitionTransform::Month => Self::Month {},
            PartitionTransform::Day => Self::Day {},
            PartitionTransform::Hour => Self::Hour {},
            PartitionTransform::Bucket(num_buckets) => Self::Bucket { num_buckets },
            PartitionTransform::Truncate(width) => Self::Truncate { width },
            PartitionTransform::Void => Self::Void {},
            PartitionTransform::Unknown(_) => Self::Unknown {},
        }
    }
}

/// The kind of a new table. It is one of `CreateTableKind.IcebergManaged` or
/// `CreateTableKind.IcebergExternal`.
#[pyclass(name = "CreateTableKind", module = "bauplan.schema", frozen)]
pub(crate) enum PyCreateTableKind {
    /// An empty iceberg table that bauplan manages.
    #[pyo3(constructor = (schema, partitions = Vec::new(), doc = None))]
    IcebergManaged {
        /// The columns of the table, a `pyarrow.Schema`.
        schema: Py<PyAny>,
        /// The partition fields.
        partitions: Vec<PyPartitionField>,
        /// The documentation of the table.
        doc: Option<String>,
    },
    /// A read-only iceberg table whose metadata and data files live outside
    /// bauplan.
    IcebergExternal {
        /// The location of the metadata file, in object storage and outside
        /// the warehouse.
        metadata_location: String,
    },
}

#[pymethods]
impl PyCreateTableKind {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(match self {
            Self::IcebergManaged {
                schema,
                partitions,
                doc,
            } => {
                let names: Vec<String> = schema.bind(py).getattr("names")?.extract()?;
                let partitions: Vec<String> =
                    partitions.iter().map(PyPartitionField::__repr__).collect();
                let doc = match doc {
                    Some(doc) => format!(", doc={doc:?}"),
                    None => String::new(),
                };
                format!(
                    "CreateTableKind.IcebergManaged(schema={names:?}, partitions=[{}]{doc})",
                    partitions.join(", ")
                )
            }
            Self::IcebergExternal { metadata_location } => {
                format!("CreateTableKind.IcebergExternal(metadata_location={metadata_location:?})")
            }
        })
    }
}

impl PyCreateTableKind {
    fn to_api(&self, py: Python<'_>) -> PyResult<CreateTableKind> {
        Ok(match self {
            Self::IcebergManaged {
                schema,
                partitions,
                doc,
            } => CreateTableKind::IcebergManaged {
                doc: doc.clone(),
                columns: from_arrow_schema(
                    &schema
                        .extract::<pyo3_arrow::PySchema>(py)
                        .map_err(|_| PyValueError::new_err("schema: not an arrow schema"))?
                        .into_inner(),
                ),
                partitions: partitions
                    .iter()
                    .cloned()
                    .map(TryInto::try_into)
                    .collect::<PyResult<_>>()?,
            },
            Self::IcebergExternal { metadata_location } => CreateTableKind::IcebergExternal {
                metadata_location: metadata_location.clone(),
            },
        })
    }
}

#[pymethods]
impl Client {
    /// Create a table from an object storage location.
    ///
    /// This operation will attempt to create a table based on schemas of N
    /// parquet files found by a given search uri. This is a two step operation using
    /// `plan_table_creation` and `apply_table_creation_plan`.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// table = client.create_table_from_search_uri(
    ///     table='my_table_name',
    ///     search_uri='s3://path/to/my/files/*.parquet',
    ///     branch='my_branch_name',
    /// )
    /// ```
    ///
    /// To specify partitioning, use the `partitioned_by` parameter:
    ///
    /// ```python
    /// import bauplan
    ///
    /// client = bauplan.Client()
    ///
    /// # Create a partitioned table
    /// table = client.create_table_from_search_uri(
    ///     table='my_partitioned_table',
    ///     search_uri='s3://your-bucket/data/*.parquet',
    ///     partitioned_by="hour(tpep_pickup_datetime), PULocationID",
    ///     branch='my_branch'
    /// )
    /// ```
    ///
    /// Parameters:
    ///     table: The table which will be created.
    ///     search_uri: The location of the files to scan for schema.
    ///     branch: The branch name in which to create the table.
    ///     namespace: Optional argument specifying the namespace. If not specified, it will be inferred based on table location or the default.
    ///     partitioned_by: Optional argument specifying the table partitioning.
    ///     replace: Replace the table if it already exists.
    ///     args: dict of arbitrary args to pass to the backend.
    ///     priority: Optional job priority (1-10, where 10 is highest priority).
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.
    /// Returns:
    ///     A `bauplan.schema.Table` object.
    ///
    /// Raises:
    ///     `bauplan.exceptions.TableCreatePlanStatusError`: if the table creation plan fails.
    ///     `bauplan.exceptions.TableCreatePlanApplyStatusError`: if the table creation plan apply fails.
    #[pyo3(signature = (
        table: "str",
        search_uri: "str",
        *,
        branch: "str | None" = None,
        namespace: "str | None" = None,
        partitioned_by: "str | None" = None,
        replace: "bool | None" = None,
        args: "dict[str, str] | None" = None,
        priority: "int | None" = None,
        client_timeout: "int | None" = None,
    ) -> "Table")]
    #[allow(clippy::too_many_arguments)]
    fn create_table_from_search_uri(
        &self,
        py: Python<'_>,
        table: &str,
        search_uri: &str,
        branch: Option<&str>,
        namespace: Option<&str>,
        partitioned_by: Option<&str>,
        replace: Option<bool>,
        args: Option<std::collections::HashMap<String, String>>,
        priority: Option<i64>,
        client_timeout: Option<i64>,
    ) -> PyResult<PyTable> {
        // Create the plan.
        let plan_state = self.plan_table_creation(
            py,
            table,
            search_uri,
            branch,
            namespace,
            partitioned_by,
            replace,
            args.clone(),
            priority,
            client_timeout,
        )?;

        if plan_state.error.is_some() {
            let msg = plan_state
                .error
                .clone()
                .unwrap_or_else(|| "table create plan failed".into());
            return Err(TableCreatePlanStatusError::new_err(msg, plan_state));
        }

        let Some(ref plan_yaml) = plan_state.plan else {
            return Err(TableCreatePlanStatusError::new_err(
                "plan completed without producing a plan".to_string(),
                plan_state,
            ));
        };

        if !plan_state.can_auto_apply {
            return Err(TableCreatePlanStatusError::new_err(
                "plan has schema conflicts and cannot be auto-applied; use plan_table_creation and apply_table_creation_plan instead"
                    .to_string(),
                plan_state,
            ));
        }

        // Apply the plan.
        let timeout = self.job_timeout(client_timeout.map(|v| v as u64));
        let common =
            self.job_request_common(priority.map(|p| p as u32), args.unwrap_or_default())?;

        let req = commanderpb::TableCreatePlanApplyRequest {
            job_request_common: Some(common),
            plan_yaml: plan_yaml.clone(),
        };

        let mut client = self.grpc.clone();
        super::detach(py, async {
            let resp = client
                .table_create_plan_apply(req)
                .await
                .map_err(job_err)?
                .into_inner();

            let Some(commanderpb::JobResponseCommon { job_id, .. }) = resp.job_response_common
            else {
                return Err(job_err("response missing job ID"));
            };

            let res = self.monitor_job(&job_id, timeout, |_| {}).await?;
            let (job_status, error) = job_status_strings(res);

            if let Some(err_msg) = error.clone() {
                let state = TableCreatePlanApplyState {
                    job_id: Some(job_id),
                    job_status: Some(job_status),
                    error,
                };

                return Err(TableCreatePlanApplyStatusError::new_err(err_msg, state));
            }

            Ok(())
        })?;

        // Fetch the created table from the catalog.
        let ctx = &plan_state.ctx;
        let table = format!("{}.{}", ctx.namespace, ctx.table_name);
        self.get_table(py, table, ctx.branch_name.clone())
    }

    /// Create a table import plan from an object storage location.
    ///
    /// This operation will attempt to create a table based on schemas of N
    /// parquet files found by a given search uri. A YAML file containing the
    /// schema and plan is returned and if there are no conflicts, it is
    /// automatically applied.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// plan_state = client.plan_table_creation(
    ///     table='my_table_name',
    ///     search_uri='s3://path/to/my/files/*.parquet',
    ///     branch='my_branch_name',
    /// )
    /// if plan_state.error:
    ///     print(f"Plan failed: {plan_state.error}")
    /// else:
    ///     print(plan_state.plan)
    /// ```
    ///
    /// To manually add partitioning to a plan before applying it:
    ///
    /// ```python
    /// import yaml
    /// import bauplan
    ///
    /// client = bauplan.Client()
    ///
    /// plan_state = client.plan_table_creation(
    ///     table='my_partitioned_table',
    ///     search_uri='s3://your-data/*.parquet',
    ///     branch='my_branch',
    /// )
    ///
    /// # Modify plan to add partitioning
    /// plan = yaml.safe_load(plan_state.plan)
    /// plan['schema_info']['partitions'] = [
    ///     {
    ///         'from_column_name': 'datetime_column',
    ///         'transform': {'name': 'year'},
    ///     }
    /// ]
    ///
    /// # Apply the modified plan
    /// client.apply_table_creation_plan(yaml.dump(plan))
    /// ```
    ///
    /// Parameters:
    ///     table: The table which will be created.
    ///     search_uri: The location of the files to scan for schema.
    ///     branch: The branch name in which to create the table.
    ///     namespace: Optional argument specifying the namespace. If not specified, it will be inferred based on table location or the default.
    ///     partitioned_by: Optional argument specifying the table partitioning.
    ///     replace: Replace the table if it already exists.
    ///     args: dict of arbitrary args to pass to the backend.
    ///     priority: Optional job priority (1-10, where 10 is highest priority).
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.
    ///
    /// Returns:
    ///     A `bauplan.state.TableCreatePlanState` object.
    ///
    /// Raises:
    ///     `bauplan.exceptions.TableCreatePlanStatusError`: if the table creation plan fails.
    #[pyo3(signature = (
        table: "str",
        search_uri: "str",
        *,
        branch: "str | None" = None,
        namespace: "str | None" = None,
        partitioned_by: "str | None" = None,
        replace: "bool | None" = None,
        args: "dict[str, str] | None" = None,
        priority: "int | None" = None,
        client_timeout: "int | None" = None,
    ) -> "TableCreatePlanState")]
    #[allow(clippy::too_many_arguments)]
    fn plan_table_creation(
        &self,
        py: Python<'_>,
        table: &str,
        search_uri: &str,
        branch: Option<&str>,
        namespace: Option<&str>,
        partitioned_by: Option<&str>,
        replace: Option<bool>,
        args: Option<std::collections::HashMap<String, String>>,
        priority: Option<i64>,
        client_timeout: Option<i64>,
    ) -> PyResult<TableCreatePlanState> {
        let timeout = self.job_timeout(client_timeout.map(|v| v as u64));
        let common =
            self.job_request_common(priority.map(|p| p as u32), args.unwrap_or_default())?;

        let req = commanderpb::TableCreatePlanRequest {
            job_request_common: Some(common),
            branch_name: branch.map(str::to_owned),
            table_name: table.to_owned(),
            namespace: namespace.map(str::to_owned),
            search_string: search_uri.to_owned(),
            table_replace: replace.unwrap_or(false),
            table_partitioned_by: partitioned_by.map(str::to_owned),
        };

        let mut client = self.grpc.clone();
        super::detach(py, async {
            let resp = client
                .table_create_plan(req)
                .await
                .map_err(job_err)?
                .into_inner();

            let Some(commanderpb::JobResponseCommon { job_id, .. }) = resp.job_response_common
            else {
                return Err(job_err("response missing job ID"));
            };

            let ctx = TableCreatePlanContext {
                branch_name: resp.branch_name,
                table_name: resp.table_name,
                table_replace: resp.table_replace,
                table_partitioned_by: resp.table_partitioned_by,
                namespace: resp.namespace,
                search_string: resp.search_string,
            };

            let mut state = TableCreatePlanState {
                job_id: Some(job_id.clone()),
                ctx,
                job_status: None,
                error: None,
                plan: None,
                can_auto_apply: false,
                files_to_be_imported: Vec::new(),
            };

            let res = self
                .monitor_job(&job_id, timeout, |event| {
                    if let RunnerEvent::TableCreatePlanDoneEvent(ev) = event {
                        if !ev.error_message.is_empty() {
                            state.error = Some(ev.error_message);
                        }

                        state.plan = Some(ev.plan_as_yaml);
                        state.can_auto_apply = ev.can_auto_apply;
                        state.files_to_be_imported = ev.files_to_be_imported;
                    }
                })
                .await?;

            let (job_status, error) = job_status_strings(res);
            state.job_status = Some(job_status);
            if let Some(e) = error
                && state.error.is_none()
            {
                state.error = Some(e);
            }

            // There's a conflict in the plan, and it can't be autoapplied.
            if state.error.is_none() && !state.can_auto_apply && state.plan.is_some() {
                state.error = Some("table plan created but has conflicts".to_owned());
            }

            Ok(state)
        })
    }

    /// Apply a plan for creating a table. It is done automatically during the
    /// table plan creation if no schema conflicts exist. Otherwise, if schema
    /// conflicts exist, then this function is used to apply them after the
    /// schema conflicts are resolved. The most common schema conflict is two
    /// parquet files with the same column name but different datatypes.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// plan_state = client.plan_table_creation(
    ///     table='my_table',
    ///     search_uri='s3://my-bucket/data/*.parquet',
    ///     branch='main',
    ///     namespace='my_namespace',
    /// )
    /// if plan_state.error:
    ///     raise RuntimeError(f"Planning failed: {plan_state.error}")
    ///
    /// if plan_state.can_auto_apply:
    ///     # No schema conflicts - table was already created automatically.
    ///     print("Table created automatically (no conflicts)")
    /// else:
    ///     # Schema conflicts detected (e.g. same column name, different types across files).
    ///     # Inspect and resolve the plan YAML, then apply manually.
    ///     print(plan_state.plan)  # review/edit the schema plan
    ///     apply_state = client.apply_table_creation_plan(
    ///         plan=plan_state,
    ///         priority=5,
    ///         client_timeout=30,
    ///     )
    ///     if apply_state.error:
    ///         raise RuntimeError(f"Apply failed: {apply_state.error}")
    ///     print(f"Table created after conflict resolution: {apply_state.job_status}")
    /// ```
    ///
    /// Parameters:
    ///     plan: The plan to apply.
    ///     args: dict of arbitrary args to pass to the backend.
    ///     priority: Optional job priority (1-10, where 10 is highest priority).
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.
    /// Returns:
    ///     A `bauplan.state.TableCreatePlanApplyState` object.
    ///
    /// Raises:
    ///     `bauplan.exceptions.TableCreatePlanApplyStatusError`: if the table creation plan apply fails.
    #[pyo3(signature = (
        plan: "TableCreatePlanState | str",
        *,
        args: "dict[str, str] | None" = None,
        priority: "int | None" = None,
        client_timeout: "int | None" = None,
    ) -> "TableCreatePlanApplyState")]
    fn apply_table_creation_plan(
        &self,
        py: Python<'_>,
        plan: Py<PyAny>,
        args: Option<std::collections::HashMap<String, String>>,
        priority: Option<i64>,
        client_timeout: Option<i64>,
    ) -> PyResult<TableCreatePlanApplyState> {
        // Accept either a TableCreatePlanState or a string YAML.
        let plan_yaml = if let Ok(state) = plan.extract::<TableCreatePlanState>(py) {
            state
                .plan
                .ok_or_else(|| job_err("plan state has no plan YAML"))?
        } else if let Ok(s) = plan.extract::<String>(py) {
            s
        } else {
            return Err(PyTypeError::new_err("expected str or TableCreatePlanState"));
        };

        let timeout = self.job_timeout(client_timeout.map(|v| v as u64));
        let common =
            self.job_request_common(priority.map(|p| p as u32), args.unwrap_or_default())?;

        let req = commanderpb::TableCreatePlanApplyRequest {
            job_request_common: Some(common),
            plan_yaml,
        };

        let mut client = self.grpc.clone();
        super::detach(py, async {
            let resp = client
                .table_create_plan_apply(req)
                .await
                .map_err(job_err)?
                .into_inner();

            let job_id = resp
                .job_response_common
                .as_ref()
                .map(|c| c.job_id.clone())
                .ok_or_else(|| job_err("response missing job ID"))?;

            let res = self.monitor_job(&job_id, timeout, |_| {}).await?;
            let (job_status, error) = job_status_strings(res);

            if let Some(msg) = error.clone() {
                let state = TableCreatePlanApplyState {
                    job_id: Some(job_id),
                    job_status: Some(job_status),
                    error,
                };
                return Err(TableCreatePlanApplyStatusError::new_err(msg, state));
            }

            Ok(TableCreatePlanApplyState {
                job_id: Some(job_id),
                job_status: Some(job_status),
                error,
            })
        })
    }

    /// Imports data into an already existing table.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// state = client.import_data(
    ///     table='my_table_name',
    ///     search_uri='s3://path/to/my/files/*.parquet',
    ///     branch='my_branch_name',
    /// )
    /// if state.error:
    ///     print(f"Import failed: {state.error}")
    /// else:
    ///     print(f"Import succeeded: {state.job_status}")
    /// ```
    ///
    /// Parameters:
    ///     table: Previously created table into which data will be imported.
    ///     search_uri: URI to scan for files to import.
    ///     branch: Branch in which to import the table.
    ///     namespace: Namespace of the table. If not specified, namespace will be inferred from table name or default settings.
    ///     continue_on_error: Do not fail the import even if 1 data import fails.
    ///     import_duplicate_files: Ignore prevention of importing files that were already imported.
    ///     best_effort: Ignore source columns that are not in the table schema instead of failing the import.
    ///     preview: Whether to enable or disable preview mode for the import.
    ///     args: dict of arbitrary args to pass to the backend.
    ///     priority: Optional job priority (1-10, where 10 is highest priority).
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.
    ///     detach: Whether to detach the job and return immediately without waiting for the job to finish.
    /// Returns:
    ///     A `bauplan.state.TableDataImportState` object.
    #[pyo3(signature = (
        table: "str",
        search_uri: "str",
        *,
        branch: "str | None" = None,
        namespace: "str | None" = None,
        continue_on_error: "bool" = false,
        import_duplicate_files: "bool" = false,
        best_effort: "bool" = false,
        preview: "str | None" = None,
        args: "dict[str, str] | None" = None,
        priority: "int | None" = None,
        client_timeout: "int | None" = None,
        detach: "bool" = false,
    ) -> "TableDataImportState")]
    #[allow(clippy::too_many_arguments)]
    fn import_data(
        &self,
        py: Python<'_>,
        table: &str,
        search_uri: &str,
        branch: Option<&str>,
        namespace: Option<&str>,
        continue_on_error: bool,
        import_duplicate_files: bool,
        best_effort: bool,
        preview: Option<&str>,
        args: Option<std::collections::HashMap<String, String>>,
        priority: Option<i64>,
        client_timeout: Option<i64>,
        detach: bool,
    ) -> PyResult<TableDataImportState> {
        let timeout = self.job_timeout(client_timeout.map(|v| v as u64));
        let common =
            self.job_request_common(priority.map(|p| p as u32), args.unwrap_or_default())?;

        let req = commanderpb::TableDataImportRequest {
            job_request_common: Some(common),
            branch_name: branch.map(str::to_owned),
            table_name: table.to_owned(),
            namespace: namespace.map(str::to_owned),
            search_string: search_uri.to_owned(),
            import_duplicate_files,
            best_effort,
            continue_on_error,
            transformation_query: None,
            preview: preview.unwrap_or_default().to_owned(),
        };

        let mut client = self.grpc.clone();
        super::detach(py, async {
            let resp = client
                .table_data_import(req)
                .await
                .map_err(job_err)?
                .into_inner();

            let job_id = resp
                .job_response_common
                .as_ref()
                .map(|c| c.job_id.clone())
                .ok_or_else(|| job_err("response missing job ID"))?;

            let ctx = TableDataImportContext {
                branch_name: resp.branch_name,
                table_name: resp.table_name,
                namespace: resp.namespace,
                search_string: resp.search_string,
                import_duplicate_files: resp.import_duplicate_files,
                best_effort: resp.best_effort,
                continue_on_error: resp.continue_on_error,
                transformation_query: resp.transformation_query,
                preview: resp.preview,
            };

            if detach {
                return Ok(TableDataImportState {
                    job_id: Some(job_id),
                    ctx,
                    job_status: None,
                    error: None,
                });
            }

            let res = self.monitor_job(&job_id, timeout, |_| {}).await?;
            let (job_status, error) = job_status_strings(res);

            Ok(TableDataImportState {
                job_id: Some(job_id),
                ctx,
                job_status: Some(job_status),
                error,
            })
        })
    }

    /// Creates an external table from files in your object storage.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// # Create from S3 files
    /// state = client.create_external_table_from_parquet(
    ///     table='my_external_table',
    ///     search_patterns=['s3://path1/to/my/files/*.parquet', 's3://path2/to/my/file/f1.parquet'],
    ///     branch='my_branch_name',
    /// )
    ///
    /// if state.error:
    ///     print(f"Error: {state.error}")
    /// else:
    ///     print(f"External table created: {state.ctx.table_name}")
    /// ```
    ///
    /// Parameters:
    ///     table: The name of the external table to create.
    ///     search_patterns: List of search_patterns for files to create the external table from. Must resolve to parquet files
    ///     branch: Branch in which to create the table.
    ///     namespace: Namespace of the table. If not specified, namespace will be inferred from table name or default settings.
    ///     overwrite: Whether to delete and recreate the table if it already exists.
    ///     args: dict of arbitrary args to pass to the backend.
    ///     priority: Optional job priority (1-10, where 10 is highest priority).
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.
    ///     detach: Whether to detach the job and return immediately without waiting for the job to finish.
    ///
    /// Returns:
    ///     A `bauplan.state.ExternalTableCreateState` object.
    #[pyo3(signature = (
        table: "str",
        search_patterns: "list[str]",
        *,
        branch: "str | None" = None,
        namespace: "str | None" = None,
        overwrite: "bool" = false,
        args: "dict[str, str] | None" = None,
        priority: "int | None" = None,
        client_timeout: "int | None" = None,
        detach: "bool" = false,
    ) -> "ExternalTableCreateState")]
    #[allow(clippy::too_many_arguments)]
    fn create_external_table_from_parquet(
        &self,
        py: Python<'_>,
        table: &str,
        search_patterns: Vec<String>,
        branch: Option<&str>,
        namespace: Option<&str>,
        overwrite: bool,
        args: Option<std::collections::HashMap<String, String>>,
        priority: Option<i64>,
        client_timeout: Option<i64>,
        detach: bool,
    ) -> PyResult<ExternalTableCreateState> {
        let timeout = self.job_timeout(client_timeout.map(|v| v as u64));
        let common =
            self.job_request_common(priority.map(|p| p as u32), args.unwrap_or_default())?;

        let req = commanderpb::ExternalTableCreateRequest {
            job_request_common: Some(common),
            branch_name: branch.map(str::to_owned),
            table_name: table.to_owned(),
            namespace: namespace.map(str::to_owned),
            input_source: Some(
                commanderpb::external_table_create_request::InputSource::InputFiles(
                    commanderpb::SearchUris {
                        uris: search_patterns,
                    },
                ),
            ),
            overwrite,
        };

        let mut client = self.grpc.clone();
        super::detach(py, async {
            let resp = client
                .external_table_create(req)
                .await
                .map_err(job_err)?
                .into_inner();

            let job_id = resp
                .job_response_common
                .as_ref()
                .map(|c| c.job_id.clone())
                .ok_or_else(|| job_err("response missing job ID"))?;

            let ctx = ExternalTableCreateContext {
                branch_name: resp.branch_name,
                table_name: resp.table_name,
                namespace: resp.namespace,
            };

            if detach {
                return Ok(ExternalTableCreateState {
                    job_id: Some(job_id),
                    ctx,
                    job_status: None,
                    error: None,
                });
            }

            let res = self.monitor_job(&job_id, timeout, |_| {}).await?;
            let (job_status, error) = job_status_strings(res);

            Ok(ExternalTableCreateState {
                job_id: Some(job_id),
                ctx,
                job_status: Some(job_status),
                error,
            })
        })
    }

    /// List the tables at a ref.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// for table in client.list_tables('main', namespace='bauplan'):
    ///     ...
    /// ```
    ///
    /// Parameters:
    ///     ref: The ref, a branch, a tag or a commit hash.
    ///     name: Optional, filter the tables by their full name, `namespace.table`. Use `^` and `$` to anchor it to the start or the end.
    ///     namespace: Optional, filter the tables by namespace, together with `name`. Use `^` and `$` to anchor it to the start or the end.
    ///     limit: Optional, max number of tables to get.
    /// Returns:
    ///     A `bauplan.schema.IteratorAtCommit` over `bauplan.schema.TableIdentifier` objects. Its `commit_hash` is the hash of the commit `ref` points to, and all the pages are read at that commit.
    ///
    /// Raises:
    ///     `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
    ///     `bauplan.exceptions.BadRequestError`: if a filter is not valid.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (
        r#ref: "str",
        *,
        name: "str | None" = None,
        namespace: "str | None" = None,
        limit: "int | None" = None,
    ) -> "IteratorAtCommit[TableIdentifier]")]
    fn list_tables(
        &self,
        py: Python<'_>,
        r#ref: String,
        name: Option<String>,
        namespace: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<PyIteratorAtCommit> {
        let client = self.tables();
        let catalog = self.profile.catalog.clone();
        let filters = TableFilters { name, namespace };
        PyIteratorAtCommit::new::<_, PyTableIdentifier, _>(
            py,
            &r#ref,
            limit,
            move |py, at, pagination_token, limit| {
                let query = ListQuery {
                    pagination_token: pagination_token.map(str::to_owned),
                    limit: limit.and_then(|limit| u32::try_from(limit).ok()),
                    filters: filters.clone(),
                };
                let path = (catalog.clone(), at.to_owned());
                py.detach(|| client.list_tables(path, query))
                    .map_err(api_error)
            },
        )
    }

    /// Get a table at a ref.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// table = client.get_table('bauplan.titanic', 'main')
    /// print(table.records, table.schema.names)
    /// ```
    ///
    /// Parameters:
    ///     table: The full name of the table, `namespace.table`.
    ///     ref: The ref, a branch, a tag or a commit hash.
    /// Returns:
    ///     A `bauplan.schema.Table` object.
    ///
    /// Raises:
    ///     `bauplan.exceptions.TableNotFoundError`: if the table does not exist.
    ///     `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (table: "str", r#ref: "str") -> "Table")]
    fn get_table(&self, py: Python<'_>, table: String, r#ref: String) -> PyResult<PyTable> {
        let client = self.tables();
        let path = (self.profile.catalog.clone(), r#ref, table);
        let table = py.detach(|| client.get_table(path));
        PyTable::new(py, table.map_err(api_error)?)
    }

    /// Check if a table exists at a ref.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// if client.has_table('bauplan.titanic', 'main'):
    ///     ...
    /// ```
    ///
    /// Parameters:
    ///     table: The full name of the table, `namespace.table`.
    ///     ref: The ref, a branch, a tag or a commit hash.
    /// Returns:
    ///     A boolean for if the table exists.
    ///
    /// Raises:
    ///     `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (table: "str", r#ref: "str") -> "bool")]
    fn has_table(&self, py: Python<'_>, table: String, r#ref: String) -> PyResult<bool> {
        let client = self.tables();
        let path = (self.profile.catalog.clone(), r#ref, table);
        match py.detach(|| client.get_table(path)) {
            Ok(_) => Ok(true),
            Err(ClientError::Api(GetTableError::TableNotFound { .. })) => Ok(false),
            Err(err) => Err(api_error(err)),
        }
    }

    /// Rename a table on a branch, or move it to another namespace.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// table = client.rename_table('raw_data.orders', 'raw_data.orders_v1', 'username.feature')
    /// ```
    ///
    /// Parameters:
    ///     table: The full name of the table, `namespace.table`.
    ///     new_table: The new full name of the table, `namespace.table`. The namespace must exist.
    ///     branch: The branch to rename the table on.
    ///     commit_message: Optional, the commit message. Without it, the server writes one for the operation.
    ///     expected_head: Optional, rename only if the branch points to this commit hash.
    /// Returns:
    ///     The renamed `bauplan.schema.Table`. Its `commit` is the commit that renames it, or the head of the branch when the name does not change.
    ///
    /// Raises:
    ///     `bauplan.exceptions.TableNotFoundError`: if the table does not exist.
    ///     `bauplan.exceptions.NameConflictError`: if a namespace or a table with the new name exists.
    ///     `bauplan.exceptions.NamespaceNotFoundError`: if the namespace of the new name does not exist.
    ///     `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
    ///     `bauplan.exceptions.NotABranchError`: if the ref is not a branch.
    ///     `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `expected_head`.
    ///     `bauplan.exceptions.ForbiddenError`: if the user can't rename the table.
    ///     `bauplan.exceptions.BadRequestError`: if the new name is not valid.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (
        table: "str",
        new_table: "str",
        branch: "str",
        *,
        commit_message: "str | None" = None,
        expected_head: "str | None" = None,
    ) -> "Table")]
    fn rename_table(
        &self,
        py: Python<'_>,
        table: String,
        new_table: String,
        branch: String,
        commit_message: Option<String>,
        expected_head: Option<String>,
    ) -> PyResult<PyTable> {
        let client = self.tables();
        let body = RenameTableBody {
            name: new_table,
            commit: CommitOptions {
                message: commit_message,
                expected_head,
            },
        };
        let path = (self.profile.catalog.clone(), branch, table);
        let renamed = py.detach(|| client.rename_table(path, body));
        PyTable::new(py, renamed.map_err(api_error)?)
    }

    /// Delete a table from a branch. The data files stay in the storage.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// commit = client.delete_table('raw_data.orders', 'username.feature', if_exists=True)
    /// ```
    ///
    /// Parameters:
    ///     table: The full name of the table, `namespace.table`.
    ///     branch: The branch to delete the table from.
    ///     commit_message: Optional, the commit message. Without it, the server writes one for the operation.
    ///     expected_head: Optional, delete only if the branch points to this commit hash.
    ///     if_exists: If set to `True`, a missing table is not an error.
    /// Returns:
    ///     The `bauplan.schema.Commit` that deletes the table, or `None` when it does not exist and `if_exists` is set.
    ///
    /// Raises:
    ///     `bauplan.exceptions.TableNotFoundError`: if the table does not exist.
    ///     `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
    ///     `bauplan.exceptions.NotABranchError`: if the ref is not a branch.
    ///     `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `expected_head`.
    ///     `bauplan.exceptions.ForbiddenError`: if the user can't delete the table.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (
        table: "str",
        branch: "str",
        *,
        commit_message: "str | None" = None,
        expected_head: "str | None" = None,
        if_exists: "bool" = false,
    ) -> "Commit | None")]
    fn delete_table(
        &self,
        py: Python<'_>,
        table: String,
        branch: String,
        commit_message: Option<String>,
        expected_head: Option<String>,
        if_exists: bool,
    ) -> PyResult<Option<PyCommit>> {
        let client = self.tables();
        let body = DeleteTableBody {
            commit: CommitOptions {
                message: commit_message,
                expected_head,
            },
        };
        let path = (self.profile.catalog.clone(), branch, table);
        match py.detach(|| client.delete_table(path, Some(body))) {
            Ok(commit) => Ok(Some(commit.into())),
            Err(ClientError::Api(DeleteTableError::TableNotFound { .. })) if if_exists => Ok(None),
            Err(err) => Err(api_error(err)),
        }
    }

    /// Create a table.
    ///
    /// ```python
    /// import bauplan
    /// import pyarrow as pa
    /// client = bauplan.Client()
    ///
    /// table = client.create_table(
    ///     'raw_data.orders',
    ///     bauplan.CreateTableKind.IcebergManaged(
    ///         pa.schema([('id', pa.int64()), ('created_at', pa.timestamp('us'))]),
    ///         partitions=[bauplan.PartitionField('created_at', bauplan.PartitionTransform.Day())],
    ///     ),
    ///     'username.feature',
    /// )
    ///
    /// table = client.create_table(
    ///     'raw_data.events',
    ///     bauplan.CreateTableKind.IcebergExternal('s3://my-bucket/events/metadata/00001-abc123.metadata.json'),
    ///     'username.feature',
    /// )
    /// ```
    ///
    /// Parameters:
    ///     table: The full name of the table, `namespace.table`. The namespace must exist.
    ///     kind: The kind of the table to create, a `bauplan.schema.CreateTableKind`.
    ///     branch: The branch to create the table on.
    ///     expected_head: Optional, create only if the branch points to this commit hash.
    /// Returns:
    ///     The new `bauplan.schema.Table`. Its `commit` is the commit that creates it.
    ///
    /// Raises:
    ///     `bauplan.exceptions.NameConflictError`: if a namespace or a table with the same name exists.
    ///     `bauplan.exceptions.NamespaceNotFoundError`: if the namespace does not exist.
    ///     `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
    ///     `bauplan.exceptions.NotABranchError`: if the ref is not a branch.
    ///     `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `expected_head`.
    ///     `bauplan.exceptions.ForbiddenError`: if the user can't write on the branch.
    ///     `bauplan.exceptions.BadRequestError`: if the name or the kind are not valid.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    ///     `ValueError`: if the schema is not an arrow schema, or a partition transform is `PartitionTransform.Unknown`.
    #[pyo3(signature = (
        table: "str",
        kind: "CreateTableKind",
        branch: "str",
        *,
        expected_head: "str | None" = None,
    ) -> "Table")]
    fn create_table(
        &self,
        py: Python<'_>,
        table: String,
        kind: Bound<'_, PyCreateTableKind>,
        branch: String,
        expected_head: Option<String>,
    ) -> PyResult<PyTable> {
        let client = self.tables();
        let body = CreateTableBody {
            name: table,
            kind: kind.get().to_api(py)?,
            commit: CommitOptions {
                message: None,
                expected_head,
            },
        };
        let path = (self.profile.catalog.clone(), branch);
        let created = py.detach(|| client.create_table(path, body));
        PyTable::new(py, created.map_err(api_error)?)
    }

    /// Copy the state of a table from a ref to a branch.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// table = client.revert_table('raw_data.orders', 'main', 'username.feature', replace=True)
    /// ```
    ///
    /// Parameters:
    ///     table: The full name of the table, `namespace.table`.
    ///     from_ref: The ref to take the table from, a branch, a tag or a commit hash.
    ///     into_branch: The branch to put the table on.
    ///     replace: If set to `True`, the table replaces the one with the same name on the branch.
    ///     commit_message: Optional, the commit message. Without it, the server writes one for the operation.
    ///     expected_head: Optional, revert only if the branch points to this commit hash.
    /// Returns:
    ///     The reverted `bauplan.schema.Table`. Its `commit` is the commit that reverts it, or the head of the branch when the table is already as it is at `from_ref`.
    ///
    /// Raises:
    ///     `bauplan.exceptions.FromTableNotFoundError`: if the table does not exist at `from_ref`.
    ///     `bauplan.exceptions.FromRefNotFoundError`: if `from_ref` does not exist.
    ///     `bauplan.exceptions.NameConflictError`: if the table exists on the branch and `replace` is not set.
    ///     `bauplan.exceptions.NamespaceNotFoundError`: if the namespace of the table does not exist on the branch.
    ///     `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
    ///     `bauplan.exceptions.NotABranchError`: if the ref is not a branch.
    ///     `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `expected_head`.
    ///     `bauplan.exceptions.ForbiddenError`: if the user can't write on the branch.
    ///     `bauplan.exceptions.BadRequestError`: if the table name is not valid.
    ///     `bauplan.exceptions.UnsupportedTableKindError`: if the table is not an Iceberg table.
    ///     `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
    #[pyo3(signature = (
        table: "str",
        from_ref: "str",
        into_branch: "str",
        *,
        replace: "bool" = false,
        commit_message: "str | None" = None,
        expected_head: "str | None" = None,
    ) -> "Table")]
    #[allow(clippy::too_many_arguments)]
    fn revert_table(
        &self,
        py: Python<'_>,
        table: String,
        from_ref: String,
        into_branch: String,
        replace: bool,
        commit_message: Option<String>,
        expected_head: Option<String>,
    ) -> PyResult<PyTable> {
        let client = self.tables();
        let body = CloneTableBody {
            name: table.clone(),
            from_ref,
            from_table: table,
            replace,
            commit: CommitOptions {
                message: commit_message,
                expected_head,
            },
        };
        let path = (self.profile.catalog.clone(), into_branch);
        let reverted = py.detach(|| client.clone_table(path, body));
        PyTable::new(py, reverted.map_err(api_error)?)
    }
}
