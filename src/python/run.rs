//! Run operations.

#![allow(unused_imports)]

use pyo3::prelude::*;
use std::collections::HashMap;

use super::Client;

#[pymethods]
impl Client {
    /// Run a Bauplan project and return the state of the run. This is the equivalent of
    /// running through the CLI the `bauplan run` command. All parameters default to 'off'/false unless otherwise specified.
    ///
    /// ## Examples
    ///
    /// ```python notest
    /// # Run a daily sales pipeline on a dev branch, and if successful and data is good, merge to main
    /// run_state = client.run(
    ///     project_dir='./etl_pipelines/daily_sales',
    ///     ref="username.dev_branch",
    ///     namespace='sales_analytics',
    /// )
    ///
    /// if str(run_state.job_status).lower() != "success":
    ///     raise Exception(f"{run_state.job_id} failed: {run_state.job_status}")
    /// ```
    ///
    /// Parameters:
    ///     project_dir: The directory of the project (where the `bauplan_project.yml` or `bauplan_project.yaml` file is located).
    ///     ref: The ref, branch name or tag name from which to run the project.
    ///     namespace: The Namespace to run the job in. If not set, the job will be run in the default namespace.
    ///     parameters: Parameters for templating into SQL or Python models.
    ///     cache: Whether to enable or disable caching for the run. Defaults to 'on'.
    ///     transaction: Whether to enable or disable transaction mode for the run. Defaults to 'on'.
    ///     dry_run: Whether to enable or disable dry-run mode for the run; models are not materialized.
    ///     strict: Whether to enable or disable strict schema validation.
    ///     preview: Whether to enable or disable preview mode for the run.
    ///     debug: Whether to enable or disable debug mode for the run.
    ///     args: Additional arguments (optional).
    ///     priority: Optional job priority (1-10, where 10 is highest priority).
    ///     verbose: Whether to enable or disable verbose mode for the run.
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution.
    ///     detach: Whether to detach the run and return immediately instead of blocking on log streaming.
    /// Returns:
    ///     `bauplan.state.RunState`: The state of the run.
    #[pyo3(signature = (
        project_dir: "str | None" = None,
        r#ref: "str | None" = None,
        namespace: "str | None" = None,
        parameters: "dict[str, str] | None" = None,
        cache: "str | None" = None,
        transaction: "str | None" = None,
        dry_run: "bool | None" = None,
        strict: "str | None" = None,
        preview: "str | None" = None,
        debug: "bool | None" = None,
        args: "dict[str, str] | None" = None,
        priority: "int | None" = None,
        verbose: "bool | None" = None,
        client_timeout: "int | None" = None,
        detach: "bool | None" = None,
    ) -> "RunState")]
    #[allow(clippy::too_many_arguments)]
    fn run(
        &mut self,
        project_dir: Option<&str>,
        r#ref: Option<&str>,
        namespace: Option<&str>,
        parameters: Option<std::collections::HashMap<String, String>>,
        cache: Option<&str>,
        transaction: Option<&str>,
        dry_run: Option<bool>,
        strict: Option<&str>,
        preview: Option<&str>,
        debug: Option<bool>,
        args: Option<std::collections::HashMap<String, String>>,
        priority: Option<i64>,
        verbose: Option<bool>,
        client_timeout: Option<i64>,
        detach: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (
            project_dir,
            r#ref,
            namespace,
            parameters,
            cache,
            transaction,
            dry_run,
            strict,
            preview,
            debug,
            args,
            priority,
            verbose,
            client_timeout,
            detach,
        );
        todo!("run")
    }

    /// Re run a Bauplan job using its ID and return the state of the run.
    /// All on and off / bool parameters default to 'off' unless otherwise specified.
    ///
    /// ## Examples
    ///
    /// ```python notest
    /// rerun_state = client.rerun(
    ///     job_id=prod_job_id,
    ///     ref='feature-branch',
    ///     cache='off'
    /// )
    ///
    /// # Check if rerun succeeded (best practice)
    /// if str(rerun_state.job_status).lower() != "success":
    ///     raise Exception(f"Rerun failed with status: {rerun_state.job_status}")
    /// ```
    ///
    /// Parameters:
    ///     job_id: The Job ID of the previous run. This can be used to re-run a previous run, e.g., on a different branch.
    ///     ref: The ref, branch name or tag name from which to rerun the project.
    ///     namespace: The Namespace to run the job in. If not set, the job will be run in the default namespace.
    ///     cache: Whether to enable or disable caching for the run. Defaults to 'on'.
    ///     transaction: Whether to enable or disable transaction mode for the run. Defaults to 'on'.
    ///     dry_run: Whether to enable or disable dry-run mode for the run; models are not materialized.
    ///     strict: Whether to enable or disable strict schema validation.
    ///     preview: Whether to enable or disable preview mode for the run.
    ///     debug: Whether to enable or disable debug mode for the run.
    ///     args: Additional arguments (optional).
    ///     priority: Optional job priority (1-10, where 10 is highest priority).
    ///     verbose: Whether to enable or disable verbose mode for the run.
    ///     client_timeout: seconds to timeout; this also cancels the remote job execution.
    /// Returns:
    ///     `bauplan.state.ReRunState`: The state of the run.
    #[pyo3(signature = (
        job_id: "str",
        r#ref: "str | None" = None,
        namespace: "str | None" = None,
        cache: "str | None" = None,
        transaction: "str | None" = None,
        dry_run: "bool | None" = None,
        strict: "str | None" = None,
        preview: "str | None" = None,
        debug: "bool | None" = None,
        args: "dict[str, str] | None" = None,
        priority: "int | None" = None,
        verbose: "bool | None" = None,
        client_timeout: "int | None" = None,
    ) -> "ReRunState")]
    #[allow(clippy::too_many_arguments)]
    fn rerun(
        &mut self,
        job_id: &str,
        r#ref: Option<&str>,
        namespace: Option<&str>,
        cache: Option<&str>,
        transaction: Option<&str>,
        dry_run: Option<bool>,
        strict: Option<&str>,
        preview: Option<&str>,
        debug: Option<bool>,
        args: Option<std::collections::HashMap<String, String>>,
        priority: Option<i64>,
        verbose: Option<bool>,
        client_timeout: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (
            job_id,
            r#ref,
            namespace,
            cache,
            transaction,
            dry_run,
            strict,
            preview,
            debug,
            args,
            priority,
            verbose,
            client_timeout,
        );
        todo!("rerun")
    }
}
