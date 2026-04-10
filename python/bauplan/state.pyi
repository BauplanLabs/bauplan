from bauplan.schema import JobLogEvent
from datetime import datetime
from typing import final

@final
class ExternalTableCreateContext:
    """
    The parameters that were passed to an external table creation job.
    """
    @property
    def branch_name(self, /) -> str:
        """
        Branch the external table is being created on.
        """
    @property
    def namespace(self, /) -> str:
        """
        Namespace of the external table.
        """
    @property
    def table_name(self, /) -> str:
        """
        Name of the external table to create.
        """

@final
class ExternalTableCreateState:
    """
    The state of a completed external table creation job.
    """
    def __repr__(self, /) -> str: ...
    @property
    def ctx(self, /) -> ExternalTableCreateContext:
        """
        The parameters that were used to launch the external table creation job.
        """
    @property
    def error(self, /) -> str | None:
        """
        Error message, if the job failed.
        """
    @property
    def job_id(self, /) -> str | None:
        """
        The job ID assigned by the server.
        """
    @property
    def job_status(self, /) -> str | None:
        """
        The final status string (e.g. `"SUCCESS"`, `"FAILED"`).
        """

@final
class RunExecutionContext:
    """
    The execution context for a run, capturing the parameters that were
    used to launch it.
    """
    def __repr__(self, /) -> str: ...
    @property
    def cache(self, /) -> str:
        """
        Cache mode used for the run (`"on"` / `"off"`).
        """
    @property
    def debug(self, /) -> bool:
        """
        Whether debug logging was enabled for the run.
        """
    @property
    def detach(self, /) -> bool:
        """
        Whether the run was submitted in detached (background) mode.
        """
    @property
    def dry_run(self, /) -> bool:
        """
        Whether the run was a dry run (no models materialized).
        """
    @property
    def namespace(self, /) -> str:
        """
        Namespace the run materialized models into.
        """
    @property
    def preview(self, /) -> str:
        """
        Preview mode used for the run (`"on"`, `"off"`, `"head"`, `"tail"`).
        """
    @property
    def project_dir(self, /) -> str:
        """
        Local project directory that was packaged and submitted.
        """
    @property
    def ref(self, /) -> str:
        """
        Ref (branch or tag) the run was executed against.
        """
    @property
    def snapshot_id(self, /) -> str:
        """
        Identifier of the immutable project snapshot that the server executed.
        """
    @property
    def snapshot_uri(self, /) -> str:
        """
        URI locating the project snapshot that the server executed.
        """
    @property
    def strict(self, /) -> str:
        """
        Strict mode (`"on"` / `"off"`). When on, runtime warnings such as
        failing expectations or invalid column outputs fail the run.
        """
    @property
    def transaction(self, /) -> str:
        """
        Transaction mode (`"on"` / `"off"`). When on, all models are
        materialized on a temporary branch and merged atomically on success.
        """

@final
class RunState:
    """
    The state of a completed (or failed) run, including logs, timing, and
    per-task lifecycle events.
    """
    def __repr__(self, /) -> str: ...
    @property
    def ctx(self, /) -> RunExecutionContext:
        """
        The execution context for the run.
        """
    @property
    def duration(self, /) -> float | None:
        """
        Duration in seconds, or None if the run hasn't ended.
        """
    @property
    def duration_ns(self, /) -> int | None:
        """
        Duration in nanoseconds, or None if the run hasn't ended.
        """
    @property
    def ended_at_ns(self, /) -> int | None:
        """
        Epoch nanoseconds when the run ended, if it has.
        """
    @property
    def error(self, /) -> str | None:
        """
        Error message, if the run failed.
        """
    @property
    def job_id(self, /) -> str | None:
        """
        The job ID assigned by the server.
        """
    @property
    def job_status(self, /) -> str | None:
        """
        The final status string (e.g. "SUCCESS", "FAILED").
        """
    @property
    def started_at_ns(self, /) -> int:
        """
        Epoch nanoseconds when the run started.
        """
    @property
    def tasks_started(self, /) -> dict[str, datetime]:
        """
        Per-task start times, keyed by task ID.
        """
    @property
    def tasks_stopped(self, /) -> dict[str, datetime]:
        """
        Per-task stop times, keyed by task ID.
        """
    @property
    def user_logs(self, /) -> list[JobLogEvent]:
        """
        User log messages emitted during the run.
        """

@final
class TableCreatePlanApplyState:
    """
    The state of a completed `Client.apply_table_creation_plan` job, which
    materializes a previously produced `bauplan.state.TableCreatePlanState` plan.
    """
    def __repr__(self, /) -> str: ...
    @property
    def error(self, /) -> str | None:
        """
        Error message, if the apply job failed.
        """
    @property
    def job_id(self, /) -> str | None:
        """
        The job ID assigned by the server.
        """
    @property
    def job_status(self, /) -> str | None:
        """
        The final status string (e.g. `"SUCCESS"`, `"FAILED"`).
        """

@final
class TableCreatePlanContext:
    """
    The parameters that were passed to a `Client.plan_table_creation` call.
    """
    @property
    def branch_name(self, /) -> str:
        """
        Branch the table is being created on.
        """
    @property
    def namespace(self, /) -> str:
        """
        Namespace the table will be created in.
        """
    @property
    def search_string(self, /) -> str:
        """
        URI pattern (e.g. `s3://bucket/path/*.parquet`) used to discover the
        source files to plan the table schema from.
        """
    @property
    def table_name(self, /) -> str:
        """
        Name of the table to create.
        """
    @property
    def table_partitioned_by(self, /) -> str | None:
        """
        Partitioning expression (e.g. a column name or transform) applied to
        the new table, or `None` if the table is not partitioned.
        """
    @property
    def table_replace(self, /) -> bool:
        """
        Whether an existing table with the same name should be replaced.
        """

@final
class TableCreatePlanState:
    """
    The result of a `Client.plan_table_creation` call.

    The `plan` field contains the schema plan as a YAML string. You can modify
    it before applying, for example to add partitioning:

    ```python
    import bauplan
    import yaml

    client = bauplan.Client()
    plan_state = client.plan_table_creation('my_table', 's3://bucket/path/*.parquet')
    plan = yaml.safe_load(plan_state.plan)
    plan['schema_info']['partitions'] = [
        {
            'from_column_name': 'datetime_column',
            'transform': {'name': 'year'},
        }
    ]
    modified_plan = yaml.dump(plan)
    ```
    """
    def __repr__(self, /) -> str: ...
    @property
    def can_auto_apply(self, /) -> bool:
        """
        Whether the plan has no schema conflicts and can be applied without
        manual intervention. If `False`, the caller must resolve conflicts in
        `plan` before applying.
        """
    @property
    def ctx(self, /) -> TableCreatePlanContext:
        """
        The parameters that were used to launch the planning job.
        """
    @property
    def error(self, /) -> str | None:
        """
        Error message, if the planning job failed.
        """
    @property
    def files_to_be_imported(self, /) -> list[str]:
        """
        The list of source files that the plan matched and will be imported
        when the plan is applied.
        """
    @property
    def job_id(self, /) -> str | None:
        """
        The job ID assigned by the server.
        """
    @property
    def job_status(self, /) -> str | None:
        """
        The final status string (e.g. `"SUCCESS"`, `"FAILED"`).
        """
    @property
    def plan(self, /) -> str | None:
        """
        The generated schema plan as a YAML string. You can edit this before
        calling `Client.apply_table_creation_plan` (for example to add partitioning).
        """

@final
class TableDataImportContext:
    """
    The parameters that were passed to a data import job.
    """
    @property
    def best_effort(self, /) -> bool:
        """
        If `True`, ignore source columns that do not exist on the destination
        table instead of failing the import.
        """
    @property
    def branch_name(self, /) -> str:
        """
        Branch the data is being imported into.
        """
    @property
    def continue_on_error(self, /) -> bool:
        """
        If `True`, do not fail the job when individual files fail to import.
        """
    @property
    def import_duplicate_files(self, /) -> bool:
        """
        If `True`, re-import files that have already been imported. This may
        result in duplicate rows.
        """
    @property
    def namespace(self, /) -> str:
        """
        Namespace of the destination table.
        """
    @property
    def preview(self, /) -> str:
        """
        Preview mode used for the import (`"on"`, `"off"`, `"head"`, `"tail"`).
        """
    @property
    def search_string(self, /) -> str:
        """
        URI pattern (e.g. `s3://bucket/path/*.parquet`) used to locate the
        source files to import.
        """
    @property
    def table_name(self, /) -> str:
        """
        Name of the destination table.
        """
    @property
    def transformation_query(self, /) -> str | None:
        """
        Optional SQL transformation applied to each file during import.
        """

@final
class TableDataImportState:
    """
    The state of a completed data import job.
    """
    def __repr__(self, /) -> str: ...
    @property
    def ctx(self, /) -> TableDataImportContext:
        """
        The parameters that were used to launch the import job.
        """
    @property
    def error(self, /) -> str | None:
        """
        Error message, if the import job failed.
        """
    @property
    def job_id(self, /) -> str | None:
        """
        The job ID assigned by the server.
        """
    @property
    def job_status(self, /) -> str | None:
        """
        The final status string (e.g. `"SUCCESS"`, `"FAILED"`).
        """
