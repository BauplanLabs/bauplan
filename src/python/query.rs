//! Query operations.

mod iter;

use std::{collections::HashMap, fs::File, path::PathBuf, sync::Arc};

use arrow::{
    array::{RecordBatch, RecordBatchWriter},
    datatypes::Schema,
};
use commanderpb::runner_event::Event as RunnerEvent;
use futures::{Stream, TryStreamExt};
use pyo3::{IntoPyObjectExt, exceptions::PyValueError, prelude::*};
use tracing::{error, info};

use crate::{
    flight,
    grpc::{self, generated as commanderpb},
    python::{
        exceptions::{BauplanError, BauplanQueryError},
        optional_on_off,
        refs::RefArg,
        rt,
    },
};

pub(crate) use iter::BatchStreamRowIterator;

use super::Client;

fn query_err(e: impl std::fmt::Display) -> PyErr {
    BauplanQueryError::new_err(e.to_string())
}

impl Client {
    /// Submits a query and runs it to completion, canceling on timeout.
    #[allow(clippy::too_many_arguments)]
    async fn run_query(
        &mut self,
        query: &str,
        r#ref: Option<RefArg>,
        max_rows: Option<u64>,
        cache: Option<&str>,
        namespace: Option<&str>,
        args: HashMap<String, String>,
        priority: Option<u32>,
        client_timeout: Option<u64>,
    ) -> PyResult<(Schema, impl Stream<Item = PyResult<RecordBatch>> + use<>)> {
        let timeout = self.job_timeout(client_timeout);
        let common = self.job_request_common(priority, args)?;
        let cache = optional_on_off("cache", cache)?;

        let req = commanderpb::QueryRunRequest {
            job_request_common: Some(common),
            r#ref: r#ref.map(|r| r.0),
            sql_query: query.to_owned(),
            cache: cache.unwrap_or_default().to_owned(),
            namespace: namespace.map(str::to_owned),
        };

        let resp = self
            .grpc
            .query_run(req)
            .await
            .map_err(query_err)?
            .into_inner();

        let Some(commanderpb::JobResponseCommon { job_id, .. }) = resp.job_response_common else {
            return Err(query_err("response missing job ID"));
        };

        info!(job_id, "succesfully planned query");

        let mut client_clone = self.grpc.clone();
        let mut req = tonic::Request::new(commanderpb::SubscribeLogsRequest {
            job_id: job_id.clone(),
        });
        req.set_timeout(timeout);
        let stream = client_clone.monitor_job(req);
        futures::pin_mut!(stream);

        let mut flight_event = None;
        loop {
            let event = match stream.try_next().await {
                Ok(Some(ev)) => ev,
                Ok(None) => break,
                Err(e)
                    if e.code() == tonic::Code::Cancelled
                        || e.code() == tonic::Code::DeadlineExceeded =>
                {
                    error!(job_id, "query timed out, cancelling execution");
                    self.cancel_query(&job_id).await?;
                    return Err(query_err("query execution timed out"));
                }
                Err(e) => return Err(query_err(e)),
            };

            match event {
                RunnerEvent::FlightServerStart(ev) => flight_event = Some(ev),
                RunnerEvent::JobCompletion(completion) => {
                    grpc::interpret_outcome(completion.outcome).map_err(query_err)?;
                    break;
                }
                _ => (),
            }
        }

        let Some(commanderpb::FlightServerStartEvent {
            endpoint,
            magic_token,
            ..
        }) = flight_event
        else {
            return Err(BauplanError::new_err(
                "query completed, but no results available",
            ));
        };

        let endpoint = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint
        } else {
            format!("https://{endpoint}")
        };

        let endpoint = endpoint
            .parse()
            .map_err(|_| BauplanError::new_err(format!("invalid flight endpoint: {endpoint}")))?;

        let (schema, batches) =
            flight::fetch_flight_results(endpoint, magic_token, timeout, max_rows, None)
                .await
                .map_err(|_| query_err("failed to fetch query results"))?;

        Ok((schema, batches.map_err(query_err)))
    }

    #[allow(clippy::too_many_arguments)]
    async fn query_to_file<T: RecordBatchWriter>(
        &mut self,
        query: &str,
        r#ref: Option<RefArg>,
        max_rows: Option<u64>,
        cache: Option<&str>,
        namespace: Option<&str>,
        args: HashMap<String, String>,
        priority: Option<u32>,
        client_timeout: Option<u64>,
        open: impl FnOnce(Arc<Schema>) -> arrow::error::Result<T>,
    ) -> PyResult<()> {
        let (schema, batches) = self
            .run_query(
                query,
                r#ref,
                max_rows,
                cache,
                namespace,
                args,
                priority,
                client_timeout,
            )
            .await?;

        futures::pin_mut!(batches);
        let mut writer = open(Arc::new(schema)).map_err(query_err)?;

        loop {
            let Some(batch) = batches.try_next().await? else {
                break;
            };

            writer.write(&batch).map_err(query_err)?;
        }

        writer.close().map_err(query_err)?;
        Ok(())
    }

    async fn cancel_query(&mut self, job_id: &str) -> PyResult<()> {
        let req = commanderpb::CancelJobRequest {
            job_id: Some(commanderpb::JobId {
                id: job_id.to_owned(),
                ..Default::default()
            }),
        };

        if let Err(err) = self.grpc.cancel(req).await {
            error!(?err, "failed to cancel timed out query");
            return Err(query_err(err));
        }

        Ok(())
    }
}

#[pymethods]
impl Client {
    /// Execute a SQL query and return the results as a pyarrow.Table.
    /// Note that this function uses Arrow also internally, resulting
    /// in a fast data transfer.
    ///
    /// If you prefer to return the results as a pandas DataFrame, use
    /// the `to_pandas` function of pyarrow.Table.
    ///
    /// ```python fixture:my_branch
    /// import bauplan
    ///
    /// client = bauplan.Client()
    ///
    /// # query the table and return result set as an arrow Table
    /// my_table = client.query(
    ///     query='SELECT avg(age) as average_age FROM bauplan.titanic',
    ///     ref='my_ref_or_branch_name',
    /// )
    ///
    /// # efficiently cast the table to a pandas DataFrame
    /// df = my_table.to_pandas()
    /// ```
    ///
    /// Parameters:
    ///     query: The Bauplan query to execute.
    ///     ref: The ref, branch name or tag name to query from.
    ///     max_rows: The maximum number of rows to return; default: `None` (no limit).
    ///     cache: Whether to enable or disable caching for the query.
    ///     namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
    ///     args: Additional arguments to pass to the query (default: None).
    ///     priority: Optional job priority (1-10, where 10 is highest priority).
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution.
    /// Returns:
    ///     The query results as a `pyarrow.Table`.
    #[pyo3(signature = (
        query: "str",
        *,
        r#ref: "str | Ref | None" = None,
        max_rows: "int | None" = None,
        cache: "str | None" = None,
        namespace: "str | None" = None,
        args: "dict[str, str] | None" = None,
        priority: "int | None" = None,
        client_timeout: "int | None" = None,
    ) -> "pyarrow.Table")]
    #[allow(clippy::too_many_arguments)]
    fn query(
        &mut self,
        py: Python<'_>,
        query: &str,
        r#ref: Option<RefArg>,
        max_rows: Option<u64>,
        cache: Option<&str>,
        namespace: Option<&str>,
        args: Option<HashMap<String, String>>,
        priority: Option<u32>,
        client_timeout: Option<u64>,
    ) -> Result<Py<PyAny>, PyErr> {
        rt().block_on(async {
            let (schema, stream) = self
                .run_query(
                    query,
                    r#ref,
                    max_rows,
                    cache,
                    namespace,
                    args.unwrap_or_default(),
                    priority,
                    client_timeout,
                )
                .await?;

            let batches: Vec<RecordBatch> = stream.try_collect().await?;
            let table = pyo3_arrow::PyTable::try_new(batches, Arc::new(schema))?;
            Ok(table.into_pyarrow(py)?.unbind())
        })
    }

    /// Execute a SQL query and return the results as a generator, where each row is
    /// a Python dictionary.
    ///
    /// ```python fixture:my_branch
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// # query the table and iterate through the results one row at a time
    /// res = client.query_to_generator(
    ///     query='SELECT name, age FROM bauplan.titanic LIMIT 100',
    ///     ref='my_ref_or_branch_name',
    /// )
    ///
    /// for row in res:
    ///     ... # handle results
    /// ```
    ///
    /// Parameters:
    ///     query: The Bauplan query to execute.
    ///     ref: The ref, branch name or tag name to query from.
    ///     max_rows: The maximum number of rows to return; default: `None` (no limit).
    ///     cache: Whether to enable or disable caching for the query.
    ///     namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
    ///     as_json: Whether to return the results as a JSON-compatible string (default: `False`).
    ///     args: Additional arguments to pass to the query (default: `None`).
    ///     priority: Optional job priority (1-10, where 10 is highest priority).
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution.
    ///
    /// Yields:
    ///     A dictionary representing a row of query results.
    #[pyo3(signature = (
        query: "str",
        *,
        r#ref: "str | Ref | None" = None,
        max_rows: "int | None" = None,
        cache: "str | None" = None,
        namespace: "str | None" = None,
        args: "dict[str, str] | None" = None,
        priority: "int | None" = None,
        client_timeout: "int | None" = None,
    ) -> "typing.Iterator[dict[str, typing.Any]]")]
    #[allow(clippy::too_many_arguments)]
    fn query_to_generator(
        &mut self,
        py: Python<'_>,
        query: &str,
        r#ref: Option<RefArg>,
        max_rows: Option<u64>,
        cache: Option<&str>,
        namespace: Option<&str>,
        args: Option<HashMap<String, String>>,
        priority: Option<u32>,
        client_timeout: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let (_schema, batches) = rt().block_on(self.run_query(
            query,
            r#ref,
            max_rows,
            cache,
            namespace,
            args.unwrap_or_default(),
            priority,
            client_timeout,
        ))?;

        BatchStreamRowIterator::new(Box::pin(batches)).into_py_any(py)
    }

    /// Export the results of a SQL query to a file in Parquet format.
    ///
    /// ```python fixture:my_branch
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// # query the table and iterate through the results one row at a time
    /// client.query_to_parquet_file(
    ///     path='/tmp/out.parquet',
    ///     query='SELECT name, age FROM bauplan.titanic LIMIT 100',
    ///     ref='my_ref_or_branch_name',
    /// )
    /// ```
    ///
    /// Parameters:
    ///     path: The name or path of the file parquet to write the results to.
    ///     query: The Bauplan query to execute.
    ///     ref: The ref, branch name or tag name to query from.
    ///     max_rows: The maximum number of rows to return; default: `None` (no limit).
    ///     cache: Whether to enable or disable caching for the query.
    ///     namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
    ///     args: Additional arguments to pass to the query (default: None).
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution.
    /// Returns:
    ///     The path of the file written.
    #[pyo3(signature = (
        path: "str",
        query: "str",
        *,
        r#ref: "str | Ref | None" = None,
        max_rows: "int | None" = None,
        cache: "str | None" = None,
        namespace: "str | None" = None,
        args: "dict[str, str] | None" = None,
        priority: "int | None" = None,
        client_timeout: "int | None" = None,
    ) -> "str")]
    #[allow(clippy::too_many_arguments)]
    fn query_to_parquet_file(
        &mut self,
        path: PathBuf,
        query: &str,
        r#ref: Option<RefArg>,
        max_rows: Option<u64>,
        cache: Option<&str>,
        namespace: Option<&str>,
        args: Option<HashMap<String, String>>,
        priority: Option<u32>,
        client_timeout: Option<u64>,
    ) -> PyResult<PathBuf> {
        use parquet::arrow::ArrowWriter;

        rt().block_on(self.query_to_file(
            query,
            r#ref,
            max_rows,
            cache,
            namespace,
            args.unwrap_or_default(),
            priority,
            client_timeout,
            |schema| {
                let file = File::create(&path)?;
                Ok(ArrowWriter::try_new(file, schema, None)?)
            },
        ))?;

        Ok(path)
    }

    /// Export the results of a SQL query to a file in CSV format.
    ///
    /// ```python fixture:my_branch
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// # query the table and iterate through the results one row at a time
    /// client.query_to_csv_file(
    ///     path='/tmp/out.csv',
    ///     query='SELECT name, age FROM bauplan.titanic LIMIT 100',
    ///     ref='my_ref_or_branch_name',
    /// )
    /// ```
    ///
    /// Parameters:
    ///     path: The name or path of the file csv to write the results to.
    ///     query: The Bauplan query to execute.
    ///     ref: The ref, branch name or tag name to query from.
    ///     max_rows: The maximum number of rows to return; default: `None` (no limit).
    ///     cache: Whether to enable or disable caching for the query.
    ///     namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
    ///     args: Additional arguments to pass to the query (default: None).
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution.
    /// Returns:
    ///     The path of the file written.
    #[pyo3(signature = (
        path: "str",
        query: "str",
        *,
        r#ref: "str | Ref | None" = None,
        max_rows: "int | None" = None,
        cache: "str | None" = None,
        namespace: "str | None" = None,
        args: "dict[str, str] | None" = None,
        priority: "int | None" = None,
        client_timeout: "int | None" = None,
    ) -> "str")]
    #[allow(clippy::too_many_arguments)]
    fn query_to_csv_file(
        &mut self,
        path: PathBuf,
        query: &str,
        r#ref: Option<RefArg>,
        max_rows: Option<u64>,
        cache: Option<&str>,
        namespace: Option<&str>,
        args: Option<HashMap<String, String>>,
        priority: Option<u32>,
        client_timeout: Option<u64>,
    ) -> PyResult<PathBuf> {
        use arrow_csv::WriterBuilder;

        rt().block_on(self.query_to_file(
            query,
            r#ref,
            max_rows,
            cache,
            namespace,
            args.unwrap_or_default(),
            priority,
            client_timeout,
            |_| {
                let file = File::create(&path)?;
                Ok(WriterBuilder::new().with_header(true).build(file))
            },
        ))?;

        Ok(path)
    }

    /// Export the results of a SQL query to a file in JSON format.
    ///
    /// ```python fixture:my_branch
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// # query the table and iterate through the results one row at a time
    /// client.query_to_json_file(
    ///     path='/tmp/out.json',
    ///     query='SELECT name, age FROM bauplan.titanic LIMIT 100',
    ///     ref='my_ref_or_branch_name',
    /// )
    /// ```
    ///
    /// Parameters:
    ///     path: The name or path of the file json to write the results to.
    ///     query: The Bauplan query to execute.
    ///     file_format: The format to write the results in; default: `json`. Allowed values are 'json' and 'jsonl'.
    ///     ref: The ref, branch name or tag name to query from.
    ///     max_rows: The maximum number of rows to return; default: `None` (no limit).
    ///     cache: Whether to enable or disable caching for the query.
    ///     namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
    ///     args: Additional arguments to pass to the query (default: None).
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution.
    /// Returns:
    ///     The path of the file written.
    #[pyo3(signature = (
        path: "str",
        query: "str",
        *,
        file_format: "str | None" = None,
        r#ref: "str | Ref | None" = None,
        max_rows: "int | None" = None,
        cache: "str | None" = None,
        namespace: "str | None" = None,
        args: "dict[str, str] | None" = None,
        priority: "int | None" = None,
        client_timeout: "int | None" = None,
    ) -> "str")]
    #[allow(clippy::too_many_arguments)]
    fn query_to_json_file(
        &mut self,
        path: PathBuf,
        query: &str,
        file_format: Option<&str>,
        r#ref: Option<RefArg>,
        max_rows: Option<u64>,
        cache: Option<&str>,
        namespace: Option<&str>,
        args: Option<HashMap<String, String>>,
        priority: Option<u32>,
        client_timeout: Option<u64>,
    ) -> PyResult<PathBuf> {
        use arrow::json::{ArrayWriter, LineDelimitedWriter};

        let jsonl = match file_format {
            None | Some("json") => false,
            Some("jsonl") => true,
            Some(other) => {
                return Err(PyValueError::new_err(format!(
                    "file_format must be 'json' or 'jsonl', got '{other}'"
                )));
            }
        };

        if jsonl {
            rt().block_on(self.query_to_file(
                query,
                r#ref,
                max_rows,
                cache,
                namespace,
                args.unwrap_or_default(),
                priority,
                client_timeout,
                |_| Ok(LineDelimitedWriter::new(File::create(&path)?)),
            ))?;
        } else {
            rt().block_on(self.query_to_file(
                query,
                r#ref,
                max_rows,
                cache,
                namespace,
                args.unwrap_or_default(),
                priority,
                client_timeout,
                |_| Ok(ArrayWriter::new(File::create(&path)?)),
            ))?;
        }

        Ok(path)
    }

    /// Execute a table scan (with optional filters) and return the results as an arrow Table.
    ///
    /// Note that this function uses SQLGlot to compose a safe SQL query,
    /// and then internally defer to the query_to_arrow function for the actual
    /// scan.
    /// ```python fixture:my_branch
    /// import bauplan
    /// client = bauplan.Client()
    ///
    /// # run a table scan over the data lake
    /// # filters are passed as a string
    /// my_table = client.scan(
    ///     table='titanic',
    ///     ref='my_ref_or_branch_name',
    ///     namespace='bauplan',
    ///     columns=['name'],
    ///     filters='age < 30',
    /// )
    /// ```
    ///
    /// Parameters:
    ///     table: The table to scan.
    ///     ref: The ref, branch name or tag name to scan from.
    ///     columns: The columns to return (default: `None`).
    ///     filters: The filters to apply (default: `None`).
    ///     limit: The maximum number of rows to return (default: `None`).
    ///     cache: Whether to enable or disable caching for the query.
    ///     namespace: The Namespace to run the scan in. If not set, the scan will be run in the default namespace for your account.
    ///     args: dict of arbitrary args to pass to the backend.
    ///     priority: Optional job priority (1-10, where 10 is highest priority).
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution.
    /// Returns:
    ///     The scan results as a `pyarrow.Table`.
    #[pyo3(signature = (
        table: "str | Table",
        *,
        r#ref: "str | Ref | None" = None,
        columns: "list[str] | None" = None,
        filters: "str | None" = None,
        limit: "int | None" = None,
        cache: "str | None" = None,
        namespace: "str | Namespace | None" = None,
        args: "dict[str, str] | None" = None,
        priority: "int | None" = None,
        client_timeout: "int | None" = None,
    ) -> "pyarrow.Table")]
    #[allow(clippy::too_many_arguments)]
    fn scan(
        &mut self,
        py: Python<'_>,
        table: &str,
        r#ref: Option<RefArg>,
        columns: Option<Vec<String>>,
        filters: Option<&str>,
        limit: Option<i64>,
        cache: Option<&str>,
        namespace: Option<&str>,
        args: Option<HashMap<String, String>>,
        priority: Option<u32>,
        client_timeout: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        use sql_query_builder as sql;

        let full_table = match namespace {
            Some(ns) => format!("{ns}.{table}"),
            None => table.to_owned(),
        };

        let mut query = sql::Select::new().from(&full_table);

        if let Some(cols) = &columns {
            for col in cols {
                query = query.select(col);
            }
        } else {
            query = query.select("*");
        }

        if let Some(f) = filters {
            query = query.where_clause(f);
        }

        if let Some(n) = limit {
            query = query.limit(&n.to_string());
        }

        let sql = query.to_string();

        rt().block_on(async {
            let (schema, stream) = self
                .run_query(
                    &sql,
                    r#ref,
                    None,
                    cache,
                    namespace,
                    args.unwrap_or_default(),
                    priority,
                    client_timeout,
                )
                .await?;

            let batches: Vec<RecordBatch> = stream.try_collect().await?;
            let table = pyo3_arrow::PyTable::try_new(batches, Arc::new(schema))?;
            Ok(table.into_pyarrow(py)?.unbind())
        })
    }
}
