//! Table operations.

use std::collections::BTreeMap;
use std::sync::Arc;

use bauplan_api::gropius::client::ClientError;
use bauplan_api::model::{CommitOptions as ApiCommitOptions, WithCommit};
use bauplan_api::pagination::ListQuery;
use bauplan_api::table::{
    self, DeleteTableBody, DeleteTableError, PartitionTransform, TableFilters, TableKind,
};
use chrono::{DateTime, Utc};
use commanderpb::runner_event::Event as RunnerEvent;
use pyo3::{exceptions::PyTypeError, prelude::*};

use crate::{
    CatalogRef, commit::CommitOptions, grpc::generated as commanderpb, iceberg::RegisterTable,
    python::job_err, table::RevertTable,
};

use super::arrow_schema::to_arrow_schema;
use super::exceptions::{
    BauplanError, TableCreatePlanApplyStatusError, TableCreatePlanStatusError, api_error,
};
use super::paginate::{PyPaginator, from_api_page};
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
    /// The kind of table: `iceberg_managed`, `iceberg_external` or
    /// `iceberg_external_parquet`.
    kind: String,
    /// The location of the current iceberg metadata file.
    metadata_location: Option<String>,
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
    /// The hash of the commit the table was read at.
    commit_hash: String,
}

#[pymethods]
impl PyTable {
    fn __repr__(&self) -> String {
        format!("Table(name={:?}, kind={:?})", self.name, self.kind)
    }
}

impl PyTable {
    fn new(py: Python<'_>, table: WithCommit<table::Table>) -> PyResult<Self> {
        let WithCommit { inner, commit } = table;
        let (kind, metadata_location) = match inner.kind {
            TableKind::IcebergManaged { metadata_location } => {
                ("iceberg_managed".to_owned(), Some(metadata_location))
            }
            TableKind::IcebergExternal { metadata_location } => {
                ("iceberg_external".to_owned(), Some(metadata_location))
            }
            TableKind::IcebergExternalParquet { metadata_location } => (
                "iceberg_external_parquet".to_owned(),
                Some(metadata_location),
            ),
            // A kind we do not know yet is an object with one key, its name.
            TableKind::Unknown(value) => {
                let name = value.as_object().and_then(|kind| kind.keys().next());
                (name.cloned().unwrap_or_default(), None)
            }
        };
        let schema = to_arrow_schema(inner.columns)
            .map_err(|err| BauplanError::new_err(format!("invalid table schema: {err}")))?;
        let schema = pyo3_arrow::PySchema::new(Arc::new(schema)).into_pyarrow(py)?;
        Ok(Self {
            name: inner.name,
            kind,
            metadata_location,
            doc: inner.doc,
            records: inner.records,
            size: inner.size,
            schema: schema.unbind(),
            partitions: inner.partitions.into_iter().map(Into::into).collect(),
            created_at: inner.created_at,
            updated_at: inner.updated_at,
            commit_hash: commit.hash,
        })
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
    /// The partition transform, like `day` or `bucket[16]`.
    transform: String,
}

#[pymethods]
impl PyPartitionField {
    fn __repr__(&self) -> String {
        format!(
            "PartitionField(name={:?}, transform={:?})",
            self.name, self.transform
        )
    }
}

impl From<table::PartitionField> for PyPartitionField {
    fn from(field: table::PartitionField) -> Self {
        let transform = match field.transform {
            PartitionTransform::Identity => "identity".to_owned(),
            PartitionTransform::Year => "year".to_owned(),
            PartitionTransform::Month => "month".to_owned(),
            PartitionTransform::Day => "day".to_owned(),
            PartitionTransform::Hour => "hour".to_owned(),
            PartitionTransform::Bucket(n) => format!("bucket[{n}]"),
            PartitionTransform::Truncate(n) => format!("truncate[{n}]"),
        };
        Self {
            name: field.name,
            transform,
        }
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
    /// table = client.create_table(
    ///     table='my_table_name',
    ///     search_uri='s3://path/to/my/files/*.parquet',
    ///     branch='my_branch_name',
    /// )
    /// ```
    ///
    /// To specify partitioning, use the `create_table` method with the `partitioned_by` parameter:
    ///
    /// ```python
    /// import bauplan
    ///
    /// client = bauplan.Client()
    ///
    /// # Create a partitioned table
    /// table = client.create_table(
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
    fn create_table(
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

    /// Get the tables at a ref.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// for table in client.get_tables('main', namespace='bauplan'):
    ///     ...
    /// ```
    ///
    /// Parameters:
    ///     ref: The ref, a branch, a tag or a commit hash.
    ///     name: Filter the tables by their full name, `^` and `$` anchor it to the start or the end.
    ///     namespace: Filter the tables by namespace, `^` and `$` anchor it to the start or the end.
    ///     limit: Optional, max number of tables to get.
    /// Returns:
    ///     An iterator over `bauplan.schema.TableIdentifier` objects.
    ///
    /// Raises:
    ///     `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
    ///     `bauplan.exceptions.BadRequestError`: if a filter is not valid.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    #[pyo3(signature = (
        r#ref: "str",
        *,
        name: "str | None" = None,
        namespace: "str | None" = None,
        limit: "int | None" = None,
    ) -> "typing.Iterator[TableIdentifier]")]
    fn get_tables(
        &self,
        py: Python<'_>,
        r#ref: String,
        name: Option<String>,
        namespace: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<PyPaginator> {
        let client = self.tables()?;
        let catalog = self.profile.catalog.clone();
        let filters = TableFilters { name, namespace };
        let mut commit_hash = None;
        PyPaginator::new(py, limit, move |py, pagination_token, limit| {
            let query = ListQuery {
                pagination_token: pagination_token.map(str::to_owned),
                limit: limit.and_then(|limit| u32::try_from(limit).ok()),
                filters: filters.clone(),
            };
            // We read the next pages at the commit of the first one, so the list
            // stays the same if the ref moves meanwhile.
            let at = commit_hash.clone().unwrap_or_else(|| r#ref.clone());
            let page = py.detach(|| client.list_tables((catalog.clone(), at), query));
            let page = page.map_err(api_error)?;
            commit_hash.get_or_insert(page.commit_hash);
            Ok(from_api_page::<_, PyTableIdentifier>(page.inner))
        })
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
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    #[pyo3(signature = (table: "str", r#ref: "str") -> "Table")]
    fn get_table(&self, py: Python<'_>, table: String, r#ref: String) -> PyResult<PyTable> {
        let client = self.tables()?;
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
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    #[pyo3(signature = (table: "str", r#ref: "str") -> "bool")]
    fn has_table(&self, py: Python<'_>, table: String, r#ref: String) -> PyResult<bool> {
        let client = self.tables()?;
        // We search the list for the exact name, which is cheaper than
        // reading the table.
        let query = ListQuery {
            pagination_token: None,
            limit: Some(1),
            filters: TableFilters {
                name: Some(format!("^{table}$")),
                namespace: None,
            },
        };
        let path = (self.profile.catalog.clone(), r#ref);
        let page = py.detach(|| client.list_tables(path, query));
        Ok(!page.map_err(api_error)?.inner.items.is_empty())
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
    ///     commit_body: Optional, the body of the commit message.
    ///     if_head: Optional, delete only if the branch points to this commit hash.
    ///     if_exists: If set to `True`, a missing table is not an error.
    /// Returns:
    ///     The `bauplan.schema.Commit` that deletes the table, or `None` when it does not exist and `if_exists` is set.
    ///
    /// Raises:
    ///     `bauplan.exceptions.TableNotFoundError`: if the table does not exist.
    ///     `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
    ///     `bauplan.exceptions.NotABranchRefError`: if the ref is not a branch.
    ///     `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `if_head`.
    ///     `bauplan.exceptions.ForbiddenError`: if the user can't delete the table.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    #[pyo3(signature = (
        table: "str",
        branch: "str",
        *,
        commit_body: "str | None" = None,
        if_head: "str | None" = None,
        if_exists: "bool" = false,
    ) -> "Commit | None")]
    fn delete_table(
        &self,
        py: Python<'_>,
        table: String,
        branch: String,
        commit_body: Option<String>,
        if_head: Option<String>,
        if_exists: bool,
    ) -> PyResult<Option<PyCommit>> {
        let client = self.tables()?;
        let body = DeleteTableBody {
            commit: ApiCommitOptions {
                body: commit_body,
                expected_commit_hash: if_head,
            },
        };
        let path = (self.profile.catalog.clone(), branch, table);
        match py.detach(|| client.delete_table(path, Some(body))) {
            Ok(commit) => Ok(Some(commit.into())),
            Err(ClientError::Api(DeleteTableError::TableNotFound { .. })) if if_exists => Ok(None),
            Err(err) => Err(api_error(err)),
        }
    }

    /// Create an external table from an Iceberg metadata.json file.
    ///
    /// This operation creates an external table by pointing to an existing Iceberg table's
    /// metadata.json file. This is useful for importing external Iceberg tables into Bauplan
    /// without copying the data.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// # Create an external table from metadata
    /// result = client.create_external_table_from_metadata(
    ///     table='my_external_table',
    ///     metadata_json_uri='s3://my-bucket/path/to/metadata/00001-abc123.metadata.json',
    ///     namespace='my_namespace',
    ///     branch='my_branch_name',
    /// )
    /// ```
    ///
    /// Parameters:
    ///     table: The name of the table to create.
    ///     metadata_json_uri: The URI pointing to the Iceberg table's metadata.json file.
    ///     namespace: The namespace for the table (required).
    ///     branch: The branch name in which to create the table. Defaults to the active branch, or 'main'.
    ///     overwrite: Whether to overwrite an existing table with the same name (default: False).
    ///
    /// Returns:
    ///     A `bauplan.schema.Table` object.
    ///
    /// Raises:
    ///     `ValueError`: if metadata_json_uri is empty or invalid, or if table parameter is invalid.
    ///     `bauplan.exceptions.BranchNotFoundError`: if the branch does not exist.
    ///     `bauplan.exceptions.NamespaceNotFoundError`: if the namespace does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `bauplan.exceptions.InvalidDataError`: if the metadata location is within the warehouse directory.
    ///     `bauplan.exceptions.UpdateConflictError`: if a table with the same name already exists and overwrite=False.
    ///     `bauplan.exceptions.BauplanError`: for other API errors during registration or retrieval.
    #[pyo3(signature = (
        table: "str",
        metadata_json_uri: "str",
        *,
        namespace: "str",
        branch: "str | None" = None,
        overwrite: "bool" = false,
    ) -> "Table")]
    fn create_external_table_from_metadata(
        &self,
        py: Python<'_>,
        table: &str,
        metadata_json_uri: &str,
        namespace: &str,
        branch: Option<&str>,
        overwrite: bool,
    ) -> PyResult<PyTable> {
        let branch = branch
            .or(self.profile.active_branch.as_deref())
            .unwrap_or("-");

        let req = RegisterTable {
            name: table,
            metadata_location: metadata_json_uri,
            overwrite,
            branch,
            namespace,
        };

        super::roundtrip(py, req, &self.profile, &self.agent)?;

        self.get_table(py, format!("{namespace}.{table}"), branch.to_owned())
    }

    /// Revert a table to a previous state.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// assert client.revert_table(
    ///     table='my_table_name',
    ///     namespace='my_namespace',
    ///     source_ref='my_ref_or_branch_name',
    ///     into_branch='main',
    /// )
    /// ```
    ///
    /// Parameters:
    ///     table: The table to revert.
    ///     namespace: The namespace of the table to revert.
    ///     source_ref: The name of the source ref; either a branch like "main" or ref like "main@[sha]".
    ///     into_branch: The name of the target branch where the table will be reverted.
    ///     replace: Optional, whether to replace the table if it already exists.
    ///     commit_body: Optional, the commit body message to attach to the operation.
    ///     commit_properties: Optional, a list of properties to attach to the operation.
    /// Returns:
    ///     The `bauplan.schema.Ref` of the branch where the revert was made.
    ///
    /// Raises:
    ///     `ValueError`: if the table identifier is invalid, source_ref or branch_name is blank, the source entry is not Iceberg, a snapshot_id is missing, or a path validation error occurs.
    ///     `bauplan.exceptions.InvalidRefError`: if the ref format from Nessie is invalid.
    ///     `bauplan.exceptions.NotAWriteBranchRefError`: if the destination ref is not a branch.
    ///     `bauplan.exceptions.SameRefError`: if the source and destination have the same hash.
    ///     `bauplan.exceptions.UnauthorizedError`: if the JWT is invalid or the auth session is missing.
    ///     `bauplan.exceptions.ForbiddenError`: if the user lacks writer role.
    ///     `bauplan.exceptions.RevertTableForbiddenError`: if there is a zone/workspace restriction on the destination branch.
    ///     `bauplan.exceptions.RefNotFoundError`: if the source ref doesn't exist.
    ///     `bauplan.exceptions.TableNotFoundError`: if the source table is not found on the source ref.
    ///     `bauplan.exceptions.BranchNotFoundError`: if the destination branch doesn't exist.
    ///     `bauplan.exceptions.NamespaceUnresolvedError`: if the namespace cannot be resolved.
    ///     `bauplan.exceptions.BranchHeadChangedError`: if a concurrent modification changed the branch head.
    ///     `bauplan.exceptions.RevertDestinationTableExistsError`: if the destination table exists and the replace flag is not set.
    ///     `bauplan.exceptions.RevertIdenticalTableError`: if the source and destination have the same snapshot.
    ///     `bauplan.exceptions.MergeConflictError`: if there is a merge conflict during the transactional revert.
    #[pyo3(signature = (
        table: "str",
        *,
        namespace: "str | None" = None,
        source_ref: "str",
        into_branch: "str",
        replace: "bool | None" = None,
        commit_body: "str | None" = None,
        commit_properties: "dict[str, str] | None" = None,
    ) -> "Ref")]
    #[allow(clippy::too_many_arguments)]
    fn revert_table(
        &self,
        py: Python<'_>,
        table: String,
        namespace: Option<String>,
        source_ref: String,
        into_branch: String,
        replace: Option<bool>,
        commit_body: Option<&str>,
        commit_properties: Option<BTreeMap<String, String>>,
    ) -> PyResult<CatalogRef> {
        let commit_properties = commit_properties.unwrap_or_default();
        let properties = commit_properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let req = RevertTable {
            name: &table,
            source_ref: &source_ref,
            into_branch: &into_branch,
            namespace: namespace.as_deref(),
            replace: replace.unwrap_or_default(),
            commit: CommitOptions {
                body: commit_body,
                properties,
            },
        };

        let resp = super::roundtrip(py, req, &self.profile, &self.agent)?;
        Ok(resp)
    }
}
