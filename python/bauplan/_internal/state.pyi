from bauplan._internal.schema import JobLogEvent
from datetime import datetime
from typing import final

@final
class ExternalTableCreateContext:
    @property
    def branch_name(self, /) -> str: ...
    @property
    def namespace(self, /) -> str: ...
    @property
    def table_name(self, /) -> str: ...

@final
class ExternalTableCreateState:
    def __repr__(self, /) -> str: ...
    @property
    def ctx(self, /) -> ExternalTableCreateContext: ...
    @property
    def error(self, /) -> str |None: ...
    @property
    def job_id(self, /) -> str |None: ...
    @property
    def job_status(self, /) -> str |None: ...

@final
class RunExecutionContext:
    """
    The execution context for a run, capturing the parameters that were
    used to launch it.
    """
    def __repr__(self, /) -> str: ...
    @property
    def cache(self, /) -> str: ...
    @property
    def debug(self, /) -> bool: ...
    @property
    def detach(self, /) -> bool: ...
    @property
    def dry_run(self, /) -> bool: ...
    @property
    def namespace(self, /) -> str: ...
    @property
    def preview(self, /) -> str: ...
    @property
    def project_dir(self, /) -> str: ...
    @property
    def ref(self, /) -> str: ...
    @property
    def snapshot_id(self, /) -> str: ...
    @property
    def snapshot_uri(self, /) -> str: ...
    @property
    def strict(self, /) -> str: ...
    @property
    def transaction(self, /) -> str: ...

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
    def duration(self, /) -> float |None:
        """
        Duration in seconds, or None if the run hasn't ended.
        """
    @property
    def duration_ns(self, /) -> int |None:
        """
        Duration in nanoseconds, or None if the run hasn't ended.
        """
    @property
    def ended_at_ns(self, /) -> int |None:
        """
        Epoch nanoseconds when the run ended, if it has.
        """
    @property
    def error(self, /) -> str |None:
        """
        Error message, if the run failed.
        """
    @property
    def job_id(self, /) -> str |None:
        """
        The job ID assigned by the server.
        """
    @property
    def job_status(self, /) -> str |None:
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
    def __repr__(self, /) -> str: ...
    @property
    def error(self, /) -> str |None: ...
    @property
    def job_id(self, /) -> str |None: ...
    @property
    def job_status(self, /) -> str |None: ...

@final
class TableCreatePlanContext:
    @property
    def branch_name(self, /) -> str: ...
    @property
    def namespace(self, /) -> str: ...
    @property
    def search_string(self, /) -> str: ...
    @property
    def table_name(self, /) -> str: ...
    @property
    def table_partitioned_by(self, /) -> str |None: ...
    @property
    def table_replace(self, /) -> bool: ...

@final
class TableCreationPlanState:
    def __repr__(self, /) -> str: ...
    @property
    def can_auto_apply(self, /) -> bool: ...
    @property
    def ctx(self, /) -> TableCreatePlanContext: ...
    @property
    def error(self, /) -> str |None: ...
    @property
    def files_to_be_imported(self, /) -> list[str]: ...
    @property
    def job_id(self, /) -> str |None: ...
    @property
    def job_status(self, /) -> str |None: ...
    @property
    def plan(self, /) -> str |None: ...

@final
class TableDataImportContext:
    @property
    def best_effort(self, /) -> bool: ...
    @property
    def branch_name(self, /) -> str: ...
    @property
    def continue_on_error(self, /) -> bool: ...
    @property
    def import_duplicate_files(self, /) -> bool: ...
    @property
    def namespace(self, /) -> str: ...
    @property
    def preview(self, /) -> str: ...
    @property
    def search_string(self, /) -> str: ...
    @property
    def table_name(self, /) -> str: ...
    @property
    def transformation_query(self, /) -> str |None: ...

@final
class TableDataImportState:
    """
    The state of a completed data import job.
    """
    def __repr__(self, /) -> str: ...
    @property
    def ctx(self, /) -> TableDataImportContext: ...
    @property
    def error(self, /) -> str |None: ...
    @property
    def job_id(self, /) -> str |None: ...
    @property
    def job_status(self, /) -> str |None: ...
