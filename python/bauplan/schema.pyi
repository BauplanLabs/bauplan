from datetime import datetime
from typing import Final, final

import pyarrow
from typing_extensions import disjoint_base

__all__ = [
    "Branch",
    "Commit",
    "DAGEdge",
    "DAGNode",
    "DetachedRef",
    "Job",
    "JobContext",
    "JobKind",
    "JobLogEvent",
    "JobLogLevel",
    "JobLogStream",
    "JobState",
    "NamespaceIdentifier",
    "PartitionField",
    "Ref",
    "RefType",
    "Table",
    "TableIdentifier",
    "Tag",
]

@final
class Branch:
    """
    A data branch, used to isolate data changes before merging into main.
    """
    @property
    def commit_hash(self, /) -> str:
        """
        The hash of the commit the branch points to.
        """
    @property
    def created_at(self, /) -> datetime:
        """
        When the branch was created.
        """
    @property
    def name(self, /) -> str:
        """
        The name of the branch.
        """
    @property
    def updated_at(self, /) -> datetime:
        """
        When the branch last moved.
        """

@final
class Commit:
    """
    A commit in the catalog history.
    """
    @property
    def author_email(self, /) -> str | None:
        """
        The author's email, if known.
        """
    @property
    def author_name(self, /) -> str:
        """
        The author's display name.
        """
    @property
    def authored_at(self, /) -> datetime:
        """
        When the commit was authored.
        """
    @property
    def committed_at(self, /) -> datetime:
        """
        When the commit was committed.
        """
    @property
    def committer_email(self, /) -> str | None:
        """
        The committer's email, if known.
        """
    @property
    def committer_name(self, /) -> str:
        """
        The committer's display name.
        """
    @property
    def hash(self, /) -> str:
        """
        The commit hash.
        """
    @property
    def merge_parent_hash(self, /) -> str | None:
        """
        The hash of the commit merged in, only on merge commits.
        """
    @property
    def message(self, /) -> str:
        """
        The full commit message.
        """
    @property
    def parent_hash(self, /) -> str | None:
        """
        The parent commit hash, `None` only on the root commit.
        """
    @property
    def properties(self, /) -> dict[str, str]:
        """
        The properties of the commit.
        """
    @property
    def title(self, /) -> str:
        """
        The commit title, the first line of the message.
        """

@final
class DAGEdge:
    """
    A dependency between two `DAGNode` instances, representing dataflow.
    """
    @property
    def destination_model(self, /) -> str:
        """
        The destination model ID.
        """
    @property
    def source_model(self, /) -> str | None:
        """
        The source model ID. `None` when the data source is a table scan rather than another model's output.
        """

@final
class DAGNode:
    """
    A node in the job DAG (a model).
    """
    @property
    def id(self, /) -> str:
        """The unique identifier for this node (model)."""
    @property
    def name(self, /) -> str:
        """The model name."""

@final
class DetachedRef(Ref):
    """
    A ref not attached to a branch or tag, pointing directly to a commit hash.
    """

@final
class Job:
    """
    The record of running a pipeline, query, or an import (see `bauplan.schema.JobKind` for all job kinds).
    """
    @property
    def created_at(self, /) -> datetime | None:
        """
        When the job was created.
        """
    @property
    def error_message(self, /) -> str | None:
        """
        Error message for failed jobs, when available.
        """
    @property
    def finished_at(self, /) -> datetime | None:
        """
        When the job finished (successfully or not).
        """
    @property
    def id(self, /) -> str:
        """
        The unique identifier for this job.
        """
    @property
    def kind(self, /) -> JobKind:
        """
        The type of job (query, run, import, etc.).
        """
    @property
    def runner(self, /) -> str:
        """
        The runner instance assigned to execute this job.
        """
    @property
    def started_at(self, /) -> datetime | None:
        """
        When the job started executing.
        """
    @property
    def status(self, /) -> JobState:
        """
        The job's current state.
        """
    @property
    def human_readable_status(self, /) -> str:
        """
        A human-readable status string (e.g. "running", "complete").
        """
    @property
    def user(self, /) -> str:
        """
        The user who submitted the job.
        """

@final
class JobContext:
    """
    The working context of a job, including its ref, DAG, code snapshot, and logs.
    """
    @property
    def dag_edges(self, /) -> list[DAGEdge]: ...
    @property
    def dag_nodes(self, /) -> list[DAGNode]: ...
    @property
    def error_message(self, /) -> str | None: ...
    @property
    def id(self, /) -> str: ...
    @property
    def logs(self, /) -> list[JobLogEvent]: ...
    @property
    def project_id(self, /) -> str | None: ...
    @property
    def project_name(self, /) -> str | None: ...
    @property
    def ref(self, /) -> str | None: ...
    @property
    def snapshot_dict(self, /) -> dict[str, str]: ...
    @property
    def sql_query(self, /) -> str | None: ...
    @property
    def tx_ref(self, /) -> str | None: ...

@final
class JobKind:
    """
    The kind/type of a job.
    """

    EXTERNAL_TABLE_CREATE: Final[JobKind]
    IMPORT_PLAN_APPLY: Final[JobKind]
    IMPORT_PLAN_CREATE: Final[JobKind]
    QUERY: Final[JobKind]
    RUN: Final[JobKind]
    TABLE_IMPORT: Final[JobKind]
    TABLE_PLAN_CREATE: Final[JobKind]
    TABLE_PLAN_CREATE_APPLY: Final[JobKind]
    UNSPECIFIED: Final[JobKind]
    def __eq__(self, value: object, /) -> bool: ...
    def __int__(self, /) -> int: ...
    def __ne__(self, value: object, /) -> bool: ...

@final
class JobLogEvent:
    """
    A single log message from a job execution. When you output logs within a Python model, they are persisted as `JobLogEvent`s.
    """
    @property
    def level(self, /) -> JobLogLevel:
        """
        The log level (ERROR, WARN, DEBUG, INFO, TRACE).
        """
    @property
    def message(self, /) -> str:
        """
        The log message.
        """
    @property
    def stream(self, /) -> JobLogStream:
        """
        The output stream (STDOUT, STDERR).
        """

@final
class JobLogLevel:
    """
    The severity level of a log event.
    """

    DEBUG: Final[JobLogLevel]
    ERROR: Final[JobLogLevel]
    INFO: Final[JobLogLevel]
    TRACE: Final[JobLogLevel]
    WARN: Final[JobLogLevel]
    def __eq__(self, value: object, /) -> bool: ...
    def __int__(self, /) -> int: ...
    def __ne__(self, value: object, /) -> bool: ...

@final
class JobLogStream:
    """
    The output stream of a log event.
    """

    STDERR: Final[JobLogStream]
    STDOUT: Final[JobLogStream]
    def __eq__(self, value: object, /) -> bool: ...
    def __int__(self, /) -> int: ...
    def __ne__(self, value: object, /) -> bool: ...

@final
class JobState:
    """
    The execution state of a job.
    """

    ABORT: Final[JobState]
    COMPLETE: Final[JobState]
    FAIL: Final[JobState]
    NOT_STARTED: Final[JobState]
    OTHER: Final[JobState]
    RUNNING: Final[JobState]
    UNSPECIFIED: Final[JobState]
    def __eq__(self, value: object, /) -> bool: ...
    def __int__(self, /) -> int: ...
    def __ne__(self, value: object, /) -> bool: ...

@final
class NamespaceIdentifier:
    """
    A namespace in a list. A namespace is a container for organizing tables.
    """
    @property
    def name(self, /) -> str:
        """
        The name of the namespace.
        """

@final
class PartitionField:
    """
    A partition field of a table.
    """
    @property
    def name(self, /) -> str:
        """
        The field the table is partitioned by.
        """
    @property
    def transform(self, /) -> str:
        """
        The partition transform, like `day` or `bucket[16]`.
        """

@disjoint_base
class Ref:
    """
    A reference to a branch, tag, or commit, as returned by API operations.
    """
    @property
    def hash(self, /) -> str:
        """The hash of the branch or tag."""
    @property
    def name(self, /) -> str:
        """The name of the branch or tag."""
    @property
    def type(self, /) -> RefType:
        """The type of the ref, either 'BRANCH', 'TAG', or 'DETACHED'."""

@final
class RefType:
    """
    The type of a ref.
    """

    BRANCH: Final[RefType]
    DETACHED: Final[RefType]
    TAG: Final[RefType]
    def __eq__(self, value: object, /) -> bool: ...
    def __int__(self, /) -> int: ...
    def __ne__(self, value: object, /) -> bool: ...

@final
class Table:
    """
    A table in the lake.
    """
    @property
    def commit_hash(self, /) -> str:
        """
        The hash of the commit the table was read at.
        """
    @property
    def created_at(self, /) -> datetime | None:
        """
        When the table was created, only for the tables that record it.
        """
    @property
    def doc(self, /) -> str | None:
        """
        The documentation of the table.
        """
    @property
    def kind(self, /) -> str:
        """
        The kind of table: `iceberg_managed`, `iceberg_external` or
        `iceberg_external_parquet`.
        """
    @property
    def metadata_location(self, /) -> str | None:
        """
        The location of the current iceberg metadata file.
        """
    @property
    def name(self, /) -> str:
        """
        The full name of the table, `namespace.table`.
        """
    @property
    def partitions(self, /) -> list[PartitionField]:
        """
        The partition fields.
        """
    @property
    def records(self, /) -> int | None:
        """
        The number of records, if the current snapshot has the totals.
        """
    @property
    def schema(self, /) -> pyarrow.Schema:
        """
        The schema of the table, a `pyarrow.Schema`.
        """
    @property
    def size(self, /) -> int | None:
        """
        The size of the data files in bytes, if the current snapshot has the
        totals.
        """
    @property
    def updated_at(self, /) -> datetime:
        """
        When the table was last written.
        """

@final
class TableIdentifier:
    """
    A table in a list.
    """
    @property
    def name(self, /) -> str:
        """
        The full name of the table, `namespace.table`.
        """

@final
class Tag:
    """
    A human-readable name that points to a specific commit in the data lake.
    Tags are often used to mark important milestones in the data lake
    history, such as releases or experiments.
    """
    @property
    def commit_hash(self, /) -> str:
        """
        The hash of the commit the tag points to.
        """
    @property
    def created_at(self, /) -> datetime:
        """
        When the tag was created.
        """
    @property
    def name(self, /) -> str:
        """
        The name of the tag.
        """
    @property
    def updated_at(self, /) -> datetime:
        """
        When the tag was last updated.
        """
