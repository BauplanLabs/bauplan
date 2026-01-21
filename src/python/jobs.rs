//! Jobs operations.

#![allow(unused_imports)]

use pyo3::prelude::*;
use std::collections::HashMap;

use super::bauplan::Client;

#[pymethods]
impl Client {
    /// EXPERIMENTAL: Get a job by ID or ID prefix.
    ///
    /// Parameters:
    ///     job_id: A job ID
    #[pyo3(signature = (job_id))]
    fn get_job(&mut self, job_id: &str) -> PyResult<Py<PyAny>> {
        let _ = job_id;
        todo!("get_job")
    }

    /// DEPRECATED: List all jobs
    ///
    /// Parameters:
    ///     all_users: Optional[bool]:  (Default value = None)
    ///     filter_by_id: Optional[str]:  (Default value = None)
    ///     filter_by_status: Optional[Union[str, JobState]]:  (Default value = None)
    ///     filter_by_finish_time: Optional[DateRange]:  (Default value = None)
    ///
    /// A DateRange is an alias for `tuple[Optional[datetime], Optional[datetime]]`, where the
    /// first element is an "after" (start) filter and the second element is a "before" (end)
    /// filter.
    ///
    /// The `filter_by_finish_time` parameter takes a DateRange and allows jobs with a finish time
    /// later than "after" (if specified) and a finish time earlier than "before" (if specified),
    /// or between both. If neither is specified, for example `(None, None)`, then the behavior is
    /// the same as not specifying the filter itself, for example `filter_by_finish_time=None`.
    #[pyo3(signature = (all_users=None, filter_by_id=None, filter_by_status=None, filter_by_finish_time=None))]
    fn list_jobs(
        &mut self,
        all_users: Option<bool>,
        filter_by_id: Option<&str>,
        filter_by_status: Option<&str>,
        filter_by_finish_time: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (
            all_users,
            filter_by_id,
            filter_by_status,
            filter_by_finish_time,
        );
        todo!("list_jobs")
    }

    /// Get jobs with optional filtering.
    ///
    /// Parameters:
    ///     all_users: Optional[bool]: Whether to list jobs from all users or only the current user.
    ///     filter_by_ids: Optional[Union[str, List[str]]]: Optional, filter by job IDs.
    ///     filter_by_users: Optional[Union[str, List[str]]]: Optional, filter by job users.
    ///     filter_by_kinds: Optional[Union[str, JobKind, List[Union[str, JobKind]]]]: Optional, filter by job kinds.
    ///     filter_by_statuses: Optional[Union[str, JobState, List[Union[str, JobState]]]]: Optional, filter by job statuses.
    ///     filter_by_created_after: Optional[datetime]: Optional, filter jobs created after this datetime.
    ///     filter_by_created_before: Optional[datetime]: Optional, filter jobs created before this datetime.
    ///     limit: Optional[int]: Optional, max number of jobs to return.
    ///
    /// Returns:
    ///     A `bauplan.schema.ListJobsResponse` object.
    #[pyo3(signature = (all_users=None, filter_by_ids=None, filter_by_users=None, filter_by_kinds=None, filter_by_statuses=None, filter_by_created_after=None, filter_by_created_before=None, limit=None))]
    #[allow(clippy::too_many_arguments)]
    fn get_jobs(
        &mut self,
        all_users: Option<bool>,
        filter_by_ids: Option<&str>,
        filter_by_users: Option<&str>,
        filter_by_kinds: Option<&str>,
        filter_by_statuses: Option<&str>,
        filter_by_created_after: Option<chrono::NaiveDateTime>,
        filter_by_created_before: Option<chrono::NaiveDateTime>,
        limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (
            all_users,
            filter_by_ids,
            filter_by_users,
            filter_by_kinds,
            filter_by_statuses,
            filter_by_created_after,
            filter_by_created_before,
            limit,
        );
        todo!("get_jobs")
    }

    /// EXPERIMENTAL: Get logs for a job by ID prefix or from a specified `Job`.
    ///
    /// Parameters:
    ///     job: Union[str, Job]: A job ID, prefix of a job ID, a Job instance.
    ///     job_id_prefix: str: The prefix of a Job ID (deprecated in favor of `job`).
    #[pyo3(signature = (job_id_prefix=None, job=None))]
    fn get_job_logs(
        &mut self,
        job_id_prefix: Option<&str>,
        job: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (job_id_prefix, job);
        todo!("get_job_logs")
    }

    /// EXPERIMENTAL: Get logs for a job by ID prefix or from a specified `Job`.
    ///
    /// Parameters:
    ///     job: Union[str, Job]: A job ID, prefix of a job ID, a Job instance.
    ///     job_id_prefix: str: The prefix of a Job ID (deprecated in favor of `job`).
    #[pyo3(signature = (job, include_logs=None, include_snapshot=None))]
    fn get_job_context(
        &mut self,
        job: &str,
        include_logs: Option<bool>,
        include_snapshot: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (job, include_logs, include_snapshot);
        todo!("get_job_context")
    }

    /// EXPERIMENTAL: Get logs for a job by ID prefix or from a specified `Job`.
    ///
    /// Parameters:
    ///     job: Union[str, Job]: A job ID, prefix of a job ID, a Job instance.
    ///     job_id_prefix: str: The prefix of a Job ID (deprecated in favor of `job`).
    #[pyo3(signature = (jobs, include_logs=None, include_snapshot=None))]
    fn get_job_contexts(
        &mut self,
        jobs: Vec<String>,
        include_logs: Option<bool>,
        include_snapshot: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (jobs, include_logs, include_snapshot);
        todo!("get_job_contexts")
    }

    /// EXPERIMENTAL: Cancel a job by ID.
    ///
    /// Parameters:
    ///     job_id: A job ID
    #[pyo3(signature = (job_id))]
    fn cancel_job(&mut self, job_id: &str) -> PyResult<()> {
        let _ = job_id;
        todo!("cancel_job")
    }
}
