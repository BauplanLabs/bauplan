//! Query operations.

#![allow(unused_imports)]

use pyo3::prelude::*;
use std::collections::HashMap;

use super::bauplan::Client;

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
    ///     connector: The connector type for the model (defaults to Bauplan). Allowed values are 'snowflake' and 'dremio'.
    ///     connector_config_key: The key name if the SSM key is custom with the pattern `bauplan/connectors/<connector_type>/<key>`.
    ///     connector_config_uri: Full SSM uri if completely custom path, e.g. `ssm://us-west-2/123456789012/baubau/dremio`.
    ///     namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
    ///     debug: Whether to enable or disable debug mode for the query.
    ///     args: Additional arguments to pass to the query (default: None).
    ///     priority: Optional job priority (1-10, where 10 is highest priority).
    ///     verbose: Whether to enable or disable verbose mode for the query.
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution.
    /// Returns:
    ///     The query results as a `pyarrow.Table`.
    #[pyo3(signature = (query, ref_=None, max_rows=None, cache=None, connector=None, connector_config_key=None, connector_config_uri=None, namespace=None, debug=None, args=None, priority=None, verbose=None, client_timeout=None))]
    #[allow(clippy::too_many_arguments)]
    fn query(
        &mut self,
        query: &str,
        ref_: Option<&str>,
        max_rows: Option<i64>,
        cache: Option<&str>,
        connector: Option<&str>,
        connector_config_key: Option<&str>,
        connector_config_uri: Option<&str>,
        namespace: Option<&str>,
        debug: Option<bool>,
        args: Option<std::collections::HashMap<String, String>>,
        priority: Option<i64>,
        verbose: Option<bool>,
        client_timeout: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (
            query,
            ref_,
            max_rows,
            cache,
            connector,
            connector_config_key,
            connector_config_uri,
            namespace,
            debug,
            args,
            priority,
            verbose,
            client_timeout,
        );
        todo!("query")
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
    ///     connector: The connector type for the model (defaults to Bauplan). Allowed values are 'snowflake' and 'dremio'.
    ///     connector_config_key: The key name if the SSM key is custom with the pattern `bauplan/connectors/<connector_type>/<key>`.
    ///     connector_config_uri: Full SSM uri if completely custom path, e.g. `ssm://us-west-2/123456789012/baubau/dremio`.
    ///     namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
    ///     debug: Whether to enable or disable debug mode for the query.
    ///     as_json: Whether to return the results as a JSON-compatible string (default: `False`).
    ///     args: Additional arguments to pass to the query (default: `None`).
    ///     priority: Optional job priority (1-10, where 10 is highest priority).
    ///     verbose: Whether to enable or disable verbose mode for the query.
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution.
    ///
    /// Yields:
    ///     A dictionary representing a row of query results.
    #[pyo3(signature = (query, ref_=None, max_rows=None, cache=None, connector=None, connector_config_key=None, connector_config_uri=None, namespace=None, debug=None, as_json=None, args=None, priority=None, verbose=None, client_timeout=None))]
    #[allow(clippy::too_many_arguments)]
    fn query_to_generator(
        &mut self,
        query: &str,
        ref_: Option<&str>,
        max_rows: Option<i64>,
        cache: Option<&str>,
        connector: Option<&str>,
        connector_config_key: Option<&str>,
        connector_config_uri: Option<&str>,
        namespace: Option<&str>,
        debug: Option<bool>,
        as_json: Option<bool>,
        args: Option<std::collections::HashMap<String, String>>,
        priority: Option<i64>,
        verbose: Option<bool>,
        client_timeout: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (
            query,
            ref_,
            max_rows,
            cache,
            connector,
            connector_config_key,
            connector_config_uri,
            namespace,
            debug,
            as_json,
            args,
            priority,
            verbose,
            client_timeout,
        );
        todo!("query_to_generator")
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
    ///     connector: The connector type for the model (defaults to Bauplan). Allowed values are 'snowflake' and 'dremio'.
    ///     connector_config_key: The key name if the SSM key is custom with the pattern `bauplan/connectors/<connector_type>/<key>`.
    ///     connector_config_uri: Full SSM uri if completely custom path, e.g. `ssm://us-west-2/123456789012/baubau/dremio`.
    ///     namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
    ///     debug: Whether to enable or disable debug mode for the query.
    ///     args: Additional arguments to pass to the query (default: None).
    ///     verbose: Whether to enable or disable verbose mode for the query.
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution.
    /// Returns:
    ///     The path of the file written.
    #[pyo3(signature = (path, query, ref_=None, max_rows=None, cache=None, connector=None, connector_config_key=None, connector_config_uri=None, namespace=None, debug=None, args=None, verbose=None, client_timeout=None))]
    #[allow(clippy::too_many_arguments)]
    fn query_to_parquet_file(
        &mut self,
        path: &str,
        query: &str,
        ref_: Option<&str>,
        max_rows: Option<i64>,
        cache: Option<&str>,
        connector: Option<&str>,
        connector_config_key: Option<&str>,
        connector_config_uri: Option<&str>,
        namespace: Option<&str>,
        debug: Option<bool>,
        args: Option<std::collections::HashMap<String, String>>,
        verbose: Option<bool>,
        client_timeout: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (
            path,
            query,
            ref_,
            max_rows,
            cache,
            connector,
            connector_config_key,
            connector_config_uri,
            namespace,
            debug,
            args,
            verbose,
            client_timeout,
        );
        todo!("query_to_parquet_file")
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
    ///     connector: The connector type for the model (defaults to Bauplan). Allowed values are 'snowflake' and 'dremio'.
    ///     connector_config_key: The key name if the SSM key is custom with the pattern `bauplan/connectors/<connector_type>/<key>`.
    ///     connector_config_uri: Full SSM uri if completely custom path, e.g. `ssm://us-west-2/123456789012/baubau/dremio`.
    ///     namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
    ///     debug: Whether to enable or disable debug mode for the query.
    ///     args: Additional arguments to pass to the query (default: None).
    ///     verbose: Whether to enable or disable verbose mode for the query.
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution.
    /// Returns:
    ///     The path of the file written.
    #[pyo3(signature = (path, query, ref_=None, max_rows=None, cache=None, connector=None, connector_config_key=None, connector_config_uri=None, namespace=None, debug=None, args=None, verbose=None, client_timeout=None))]
    #[allow(clippy::too_many_arguments)]
    fn query_to_csv_file(
        &mut self,
        path: &str,
        query: &str,
        ref_: Option<&str>,
        max_rows: Option<i64>,
        cache: Option<&str>,
        connector: Option<&str>,
        connector_config_key: Option<&str>,
        connector_config_uri: Option<&str>,
        namespace: Option<&str>,
        debug: Option<bool>,
        args: Option<std::collections::HashMap<String, String>>,
        verbose: Option<bool>,
        client_timeout: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (
            path,
            query,
            ref_,
            max_rows,
            cache,
            connector,
            connector_config_key,
            connector_config_uri,
            namespace,
            debug,
            args,
            verbose,
            client_timeout,
        );
        todo!("query_to_csv_file")
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
    ///     connector: The connector type for the model (defaults to Bauplan). Allowed values are 'snowflake' and 'dremio'.
    ///     connector_config_key: The key name if the SSM key is custom with the pattern `bauplan/connectors/<connector_type>/<key>`.
    ///     connector_config_uri: Full SSM uri if completely custom path, e.g. `ssm://us-west-2/123456789012/baubau/dremio`.
    ///     namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
    ///     debug: Whether to enable or disable debug mode for the query.
    ///     args: Additional arguments to pass to the query (default: None).
    ///     verbose: Whether to enable or disable verbose mode for the query.
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution.
    /// Returns:
    ///     The path of the file written.
    #[pyo3(signature = (path, query, file_format=None, ref_=None, max_rows=None, cache=None, connector=None, connector_config_key=None, connector_config_uri=None, namespace=None, debug=None, args=None, verbose=None, client_timeout=None))]
    #[allow(clippy::too_many_arguments)]
    fn query_to_json_file(
        &mut self,
        path: &str,
        query: &str,
        file_format: Option<&str>,
        ref_: Option<&str>,
        max_rows: Option<i64>,
        cache: Option<&str>,
        connector: Option<&str>,
        connector_config_key: Option<&str>,
        connector_config_uri: Option<&str>,
        namespace: Option<&str>,
        debug: Option<bool>,
        args: Option<std::collections::HashMap<String, String>>,
        verbose: Option<bool>,
        client_timeout: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (
            path,
            query,
            file_format,
            ref_,
            max_rows,
            cache,
            connector,
            connector_config_key,
            connector_config_uri,
            namespace,
            debug,
            args,
            verbose,
            client_timeout,
        );
        todo!("query_to_json_file")
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
    ///     connector: The connector type for the model (defaults to Bauplan). Allowed values are 'snowflake' and 'dremio'.
    ///     connector_config_key: The key name if the SSM key is custom with the pattern `bauplan/connectors/<connector_type>/<key>`.
    ///     connector_config_uri: Full SSM uri if completely custom path, e.g. `ssm://us-west-2/123456789012/baubau/dremio`.
    ///     namespace: The Namespace to run the scan in. If not set, the scan will be run in the default namespace for your account.
    ///     debug: Whether to enable or disable debug mode for the query.
    ///     args: dict of arbitrary args to pass to the backend.
    ///     priority: Optional job priority (1-10, where 10 is highest priority).
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution.
    /// Returns:
    ///     The scan results as a `pyarrow.Table`.
    #[pyo3(signature = (table, ref_=None, columns=None, filters=None, limit=None, cache=None, connector=None, connector_config_key=None, connector_config_uri=None, namespace=None, debug=None, args=None, priority=None, client_timeout=None))]
    #[allow(clippy::too_many_arguments)]
    fn scan(
        &mut self,
        table: &str,
        ref_: Option<&str>,
        columns: Option<Vec<String>>,
        filters: Option<&str>,
        limit: Option<i64>,
        cache: Option<&str>,
        connector: Option<&str>,
        connector_config_key: Option<&str>,
        connector_config_uri: Option<&str>,
        namespace: Option<&str>,
        debug: Option<bool>,
        args: Option<std::collections::HashMap<String, String>>,
        priority: Option<i64>,
        client_timeout: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (
            table,
            ref_,
            columns,
            filters,
            limit,
            cache,
            connector,
            connector_config_key,
            connector_config_uri,
            namespace,
            debug,
            args,
            priority,
            client_timeout,
        );
        todo!("scan")
    }
}
