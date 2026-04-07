//! Table operations.

use std::collections::BTreeMap;

use commanderpb::runner_event::Event as RunnerEvent;
use pyo3::{exceptions::PyTypeError, prelude::*};

use crate::{
    ApiErrorKind, ApiRequest, CatalogRef,
    api::table::Table,
    commit::CommitOptions,
    grpc::generated as commanderpb,
    iceberg::RegisterTable,
    python::{
        job_err,
        namespace::NamespaceArg,
        paginate::PyPaginator,
        refs::{BranchArg, RefArg},
        run::monitor_job,
    },
    table::{DeleteTable, GetTable, GetTables, RevertTable},
};

use super::Client;
use super::exceptions::{TableCreatePlanApplyStatusError, TableCreatePlanStatusError};
use super::run::job_status_strings;
use crate::python::run::state::{
    ExternalTableCreateContext, ExternalTableCreateState, TableCreatePlanApplyState,
    TableCreatePlanContext, TableCreatePlanState, TableDataImportContext, TableDataImportState,
};

/// Accepts a table name or Table object (from which the name is extracted).
pub(crate) struct TableArg(pub String);

impl<'a, 'py> FromPyObject<'a, 'py> for TableArg {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(s) = ob.extract::<String>() {
            Ok(TableArg(s))
        } else if let Ok(table) = ob.extract::<Table>() {
            Ok(TableArg(table.name))
        } else {
            Err(PyTypeError::new_err("expected str or Table"))
        }
    }
}

#[pymethods]
impl Client {
    /// Create a table from an S3 location.
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
        table: "str | Table",
        search_uri: "str",
        *,
        branch: "str | Branch | None" = None,
        namespace: "str | Namespace | None" = None,
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
    ) -> PyResult<Table> {
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

            let res = monitor_job(&mut client, &job_id, timeout, |_| {}).await?;
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
        let req = GetTable {
            name: &plan_state.ctx.table_name,
            at_ref: &plan_state.ctx.branch_name,
            namespace: Some(&plan_state.ctx.namespace),
        };

        Ok(super::roundtrip(py, req, &self.profile, &self.agent)?)
    }

    /// Create a table import plan from an S3 location.
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
        table: "str | Table",
        search_uri: "str",
        *,
        branch: "str | Branch | None" = None,
        namespace: "str | Namespace | None" = None,
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

            let res = monitor_job(&mut client, &job_id, timeout, |event| {
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
    ///     raise Exception(f"Planning failed: {plan_state.error}")
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
    ///         raise Exception(f"Apply failed: {apply_state.error}")
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

            let res = monitor_job(&mut client, &job_id, timeout, |_| {}).await?;
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
    ///     import_duplicate_files: Ignore prevention of importing s3 files that were already imported.
    ///     best_effort: Don't fail if schema of table does not match.
    ///     preview: Whether to enable or disable preview mode for the import.
    ///     args: dict of arbitrary args to pass to the backend.
    ///     priority: Optional job priority (1-10, where 10 is highest priority).
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.
    ///     detach: Whether to detach the job and return immediately without waiting for the job to finish.
    /// Returns:
    ///     A `bauplan.state.TableDataImportState` object.
    #[pyo3(signature = (
        table: "str | Table",
        search_uri: "str",
        *,
        branch: "str | Branch | None" = None,
        namespace: "str | Namespace | None" = None,
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

            let res = monitor_job(&mut client, &job_id, timeout, |_| {}).await?;
            let (job_status, error) = job_status_strings(res);

            Ok(TableDataImportState {
                job_id: Some(job_id),
                ctx,
                job_status: Some(job_status),
                error,
            })
        })
    }

    /// Creates an external table from S3 files.
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
        table: "str | Table",
        search_patterns: "list[str]",
        *,
        branch: "str | Branch | None" = None,
        namespace: "str | Namespace | None" = None,
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

            let res = monitor_job(&mut client, &job_id, timeout, |_| {}).await?;
            let (job_status, error) = job_status_strings(res);

            Ok(ExternalTableCreateState {
                job_id: Some(job_id),
                ctx,
                job_status: Some(job_status),
                error,
            })
        })
    }

    /// Get the tables and views in the target branch.
    ///
    /// On failure, raises `bauplan.exceptions.BauplanError`.
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// for table in client.get_tables('my_branch_name'):
    ///     ...
    /// ```
    ///
    /// Parameters:
    ///     ref: The ref or branch to get the tables from.
    ///     filter_by_name: Optional, the table name to filter by.
    ///     filter_by_namespace: Optional, the namespace to get filtered tables from.
    ///     limit: Optional, max number of tables to get.
    /// Returns:
    ///     An iterator over `bauplan.schema.Table` objects.
    ///
    /// Raises:
    ///     `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
    ///     `bauplan.exceptions.InvalidRefError`: if the ref format is invalid.
    ///     `bauplan.exceptions.NamespaceNotFoundError`: if the namespace does not exist.
    ///     `bauplan.exceptions.TableNotFoundError`: if the table does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `ValueError`: if one or more parameters are invalid.
    #[pyo3(signature = (
        r#ref: "str | Ref",
        *,
        filter_by_name: "str | None" = None,
        filter_by_namespace: "str | Namespace | None" = None,
        limit: "int | None" = None,
    ) -> "typing.Iterator[Table]")]
    fn get_tables(
        &self,
        py: Python<'_>,
        r#ref: RefArg,
        filter_by_name: Option<String>,
        filter_by_namespace: Option<NamespaceArg>,
        limit: Option<usize>,
    ) -> PyResult<PyPaginator> {
        let r#ref = r#ref.0;
        let filter_by_namespace = filter_by_namespace.map(|a| a.0);
        let profile = self.profile.clone();
        let agent = self.agent.clone();
        PyPaginator::new(py, limit, move |py, token, limit| {
            let req = GetTables {
                at_ref: &r#ref,
                filter_by_name: filter_by_name.as_deref(),
                filter_by_namespace: filter_by_namespace.as_deref(),
            }
            .paginate(token, limit);

            Ok(super::roundtrip(py, req, &profile, &agent)?)
        })
    }

    /// Get the table data and metadata for a table in the target branch.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// # get the fields and metadata for a table
    /// table = client.get_table(
    ///     table='titanic',
    ///     ref='my_ref_or_branch_name',
    ///     namespace='bauplan',
    /// )
    ///
    /// # You can get the total number of rows this way.
    /// num_records = table.records
    ///
    /// # Or access the schema.
    /// for c in table.fields:
    ///     ...
    /// ```
    ///
    /// Parameters:
    ///     ref: The ref, branch name or tag name to get the table from.
    ///     table: The table to retrieve.
    ///     namespace: The namespace of the table to retrieve.
    /// Returns:
    ///     a `bauplan.schema.Table` object
    ///
    /// Raises:
    ///     `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
    ///     `bauplan.exceptions.InvalidRefError`: if the ref format is invalid.
    ///     `bauplan.exceptions.NamespaceNotFoundError`: if the namespace does not exist.
    ///     `bauplan.exceptions.NamespaceUnresolvedError`: if conflicting namespaces names are specified.
    ///     `bauplan.exceptions.TableNotFoundError`: if the table does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `ValueError`: if one or more parameters are invalid.
    #[pyo3(signature = (
        table: "str | Table",
        r#ref: "str | Ref",
        *,
        namespace: "str | Namespace | None" = None,
    ) -> "Table")]
    fn get_table(
        &self,
        py: Python<'_>,
        table: TableArg,
        r#ref: RefArg,
        namespace: Option<NamespaceArg>,
    ) -> PyResult<Table> {
        let namespace = namespace.map(|a| a.0);
        let req = GetTable {
            name: &table.0,
            at_ref: &r#ref.0,
            namespace: namespace.as_deref(),
        };

        Ok(super::roundtrip(py, req, &self.profile, &self.agent)?)
    }

    /// Check if a table exists.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// assert client.has_table(
    ///     table='titanic',
    ///     ref='my_ref_or_branch_name',
    ///     namespace='bauplan',
    /// )
    /// ```
    ///
    /// Parameters:
    ///     ref: The ref, branch name or tag name to get the table from.
    ///     table: The table to retrieve.
    ///     namespace: The namespace of the table to check.
    /// Returns:
    ///     A boolean for if the table exists.
    ///
    /// Raises:
    ///     `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
    ///     `bauplan.exceptions.InvalidRefError`: if the ref format is invalid.
    ///     `bauplan.exceptions.NamespaceNotFoundError`: if the namespace does not exist.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `ValueError`: if one or more parameters are invalid.
    #[pyo3(signature = (
        table: "str | Table",
        r#ref: "str | Ref",
        *,
        namespace: "str | Namespace | None" = None,
    ) -> "bool")]
    fn has_table(
        &self,
        py: Python<'_>,
        table: TableArg,
        r#ref: RefArg,
        namespace: Option<NamespaceArg>,
    ) -> PyResult<bool> {
        let namespace = namespace.map(|a| a.0);
        let req = GetTable {
            name: &table.0,
            at_ref: &r#ref.0,
            namespace: namespace.as_deref(),
        };

        if let Err(e) = super::roundtrip(py, req, &self.profile, &self.agent) {
            if matches!(e.kind(), Some(ApiErrorKind::TableNotFound { .. })) {
                return Ok(false);
            } else {
                return Err(e.into());
            }
        }

        Ok(true)
    }

    /// Drop a table.
    ///
    /// Upon failure, raises `bauplan.exceptions.BauplanError`
    ///
    /// ```python
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// assert client.delete_table(
    ///     table='my_table_name',
    ///     branch='my_branch_name',
    ///     namespace='my_namespace',
    /// )
    /// ```
    ///
    /// Parameters:
    ///     table: The table to delete.
    ///     branch: The branch on which the table is stored.
    ///     namespace: The namespace of the table to delete.
    ///     commit_body: Optional, the commit body message to attach to the commit.
    ///     commit_properties: Optional, a list of properties to attach to the commit.
    ///     if_exists: If set to `True`, the table will not raise an error if it does not exist.
    /// Returns:
    ///     A `bauplan.schema.Branch` object pointing to the new head.
    ///
    /// Raises:
    ///     `bauplan.exceptions.DeleteTableForbiddenError`: if the user does not have access to delete the table.
    ///     `bauplan.exceptions.BranchNotFoundError`: if the branch does not exist.
    ///     `bauplan.exceptions.BranchHeadChangedError`: if the branch head hash has changed.
    ///     `bauplan.exceptions.NotAWriteBranchRefError`: if the destination branch is not a writable ref.
    ///     `bauplan.exceptions.MergeConflictError`: if the delete operation results in a conflict.
    ///     `bauplan.exceptions.TableNotFoundError`: if the table does not exist.
    ///     `bauplan.exceptions.NamespaceNotFoundError`: if the namespace does not exist.
    ///     `bauplan.exceptions.NamespaceUnresolvedError`: if conflicting namespaces names are specified.
    ///     `bauplan.exceptions.InvalidRefError`: if the ref format is invalid.
    ///     `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
    ///     `ValueError`: if one or more parameters are invalid.
    #[pyo3(signature = (
        table: "str | Table",
        branch: "str | Branch",
        *,
        namespace: "str | Namespace | None" = None,
        if_exists: "bool" = false,
        commit_body: "str | None" = None,
        commit_properties: "dict[str, str] | None" = None,
    ) -> "Branch")]
    #[allow(clippy::too_many_arguments)]
    fn delete_table(
        &self,
        py: Python<'_>,
        table: TableArg,
        branch: BranchArg,
        namespace: Option<NamespaceArg>,
        if_exists: bool,
        commit_body: Option<&str>,
        commit_properties: Option<BTreeMap<String, String>>,
    ) -> PyResult<CatalogRef> {
        let namespace = namespace.map(|a| a.0);
        let commit_properties = commit_properties.unwrap_or_default();
        let properties = commit_properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let req = DeleteTable {
            name: &table.0,
            branch: &branch.0,
            namespace: namespace.as_deref(),
            commit: CommitOptions {
                body: commit_body,
                properties,
            },
        };

        match super::roundtrip(py, req, &self.profile, &self.agent) {
            Ok(r) => Ok(r),
            Err(e) => {
                if if_exists && let Some(ApiErrorKind::TableNotFound { catalog_ref, .. }) = e.kind()
                {
                    Ok(catalog_ref.clone())
                } else {
                    Err(e.into())
                }
            }
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
    ///     metadata_json_uri: The S3 URI pointing to the Iceberg table's metadata.json file.
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
        table: "str | Table",
        metadata_json_uri: "str",
        *,
        namespace: "str | Namespace",
        branch: "str | Branch | None" = None,
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
    ) -> PyResult<Table> {
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

        let req = GetTable {
            name: table,
            at_ref: branch,
            namespace: Some(namespace),
        };

        Ok(super::roundtrip(py, req, &self.profile, &self.agent)?)
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
    ///     The `bauplan.schema.Branch` where the revert was made.
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
        table: "str | Table",
        *,
        namespace: "str | Namespace | None" = None,
        source_ref: "str | Ref",
        into_branch: "str | Branch",
        replace: "bool | None" = None,
        commit_body: "str | None" = None,
        commit_properties: "dict[str, str] | None" = None,
    ) -> "Branch")]
    #[allow(clippy::too_many_arguments)]
    fn revert_table(
        &self,
        py: Python<'_>,
        table: TableArg,
        namespace: Option<NamespaceArg>,
        source_ref: RefArg,
        into_branch: BranchArg,
        replace: Option<bool>,
        commit_body: Option<&str>,
        commit_properties: Option<BTreeMap<String, String>>,
    ) -> PyResult<CatalogRef> {
        let namespace = namespace.map(|a| a.0);
        let commit_properties = commit_properties.unwrap_or_default();
        let properties = commit_properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let req = RevertTable {
            name: &table.0,
            source_ref: &source_ref.0,
            into_branch: &into_branch.0,
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
