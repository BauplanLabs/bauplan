from datetime import datetime
from typing import Final, Generic, TypeVar, final

import pyarrow
from typing_extensions import disjoint_base

__all__ = [
    "Branch",
    "Commit",
    "CreateTableKind",
    "DAGEdge",
    "DAGNode",
    "DiffChange",
    "DiffEntry",
    "Entity",
    "IteratorAtCommit",
    "IteratorBetweenCommits",
    "Job",
    "JobContext",
    "JobKind",
    "JobLogEvent",
    "JobLogLevel",
    "JobLogStream",
    "JobState",
    "MergeStrategy",
    "Namespace",
    "NamespaceIdentifier",
    "PartitionField",
    "PartitionTransform",
    "Table",
    "TableIdentifier",
    "TableKind",
    "Tag",
]

_T = TypeVar("_T")

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

@disjoint_base
class CreateTableKind:
    """
    The kind of a new table. It is one of `CreateTableKind.IcebergManaged` or
    `CreateTableKind.IcebergExternal`.
    """
    @final
    class IcebergExternal(CreateTableKind):
        """
        A read-only iceberg table whose metadata and data files live outside
        bauplan.
        """

        __match_args__: Final = ("metadata_location",)
        def __new__(
            cls, /, metadata_location: str
        ) -> CreateTableKind.IcebergExternal: ...
        @property
        def metadata_location(self, /) -> str:
            """
            The location of the metadata file, in object storage and outside
            the warehouse.
            """

    @final
    class IcebergManaged(CreateTableKind):
        """
        An empty iceberg table that bauplan manages.
        """

        __match_args__: Final = ("schema", "partitions", "doc")
        def __new__(
            cls,
            /,
            schema: pyarrow.Schema,
            partitions: list[PartitionField] = ...,
            doc: str | None = None,
        ) -> CreateTableKind.IcebergManaged: ...
        @property
        def doc(self, /) -> str | None:
            """
            The documentation of the table.
            """
        @property
        def partitions(self, /) -> list[PartitionField]:
            """
            The partition fields.
            """
        @property
        def schema(self, /) -> pyarrow.Schema:
            """
            The columns of the table, a `pyarrow.Schema`.
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

@disjoint_base
class DiffChange:
    """
    How an entity changes from `from_ref` to `to_ref`. It is one of
    `DiffChange.Added`, `DiffChange.Removed`, `DiffChange.Changed`,
    `DiffChange.Renamed` or `DiffChange.Unknown`.
    """
    def __eq__(self, value: object, /) -> bool: ...
    def __ne__(self, value: object, /) -> bool: ...
    @final
    class Added(DiffChange):
        """
        Only at `to_ref`.
        """

        __match_args__: Final = ()
        def __new__(cls, /) -> DiffChange.Added: ...

    @final
    class Changed(DiffChange):
        """
        At both refs, but it changed.
        """

        __match_args__: Final = ()
        def __new__(cls, /) -> DiffChange.Changed: ...

    @final
    class Removed(DiffChange):
        """
        Only at `from_ref`.
        """

        __match_args__: Final = ()
        def __new__(cls, /) -> DiffChange.Removed: ...

    @final
    class Renamed(DiffChange):
        """
        At `to_ref` with a new name.
        """

        __match_args__: Final = ("from_name",)
        def __new__(cls, /, from_name: str) -> DiffChange.Renamed: ...
        @property
        def from_name(self, /) -> str:
            """
            The name at `from_ref`.
            """

    @final
    class Unknown(DiffChange):
        """
        A kind of change this client does not know yet.
        """

        __match_args__: Final = ()
        def __new__(cls, /) -> DiffChange.Unknown: ...

@final
class DiffEntry:
    """
    A table or a namespace that differs between the two refs, and how.
    """
    @property
    def change(self, /) -> DiffChange:
        """
        How it changes from `from_ref` to `to_ref`.
        """
    @property
    def entity(self, /) -> Entity:
        """
        The table or the namespace.
        """

@disjoint_base
class Entity:
    """
    An entity of the catalog, like a table or a namespace.
    """
    def __eq__(self, value: object, /) -> bool: ...
    def __ne__(self, value: object, /) -> bool: ...
    @final
    class Namespace(Entity):
        """
        A namespace.
        """

        __match_args__: Final = ("name",)
        def __new__(cls, /, name: str) -> Entity.Namespace: ...
        @property
        def name(self, /) -> str:
            """
            The name of the namespace.
            """

    @final
    class Table(Entity):
        """
        A table.
        """

        __match_args__: Final = ("name",)
        def __new__(cls, /, name: str) -> Entity.Table: ...
        @property
        def name(self, /) -> str:
            """
            The full name of the table, `namespace.table`.
            """

    @final
    class Unknown(Entity):
        """
        A kind of entity this client does not know yet.
        """

        __match_args__: Final = ()
        def __new__(cls, /) -> Entity.Unknown: ...

@final
class IteratorAtCommit(Generic[_T]):
    """
    An iterator that reads all its pages at one commit, so the list does not
    change if the ref moves.
    """
    def __iter__(self, /) -> IteratorAtCommit[_T]: ...
    def __next__(self, /) -> _T: ...
    @property
    def commit_hash(self, /) -> str:
        """
        The hash of the commit the list is read at.
        """

@final
class IteratorBetweenCommits(Generic[_T]):
    """
    An iterator that reads all its pages at the same two commits, so the
    diff does not change if the refs move.
    """
    def __iter__(self, /) -> IteratorBetweenCommits[_T]: ...
    def __next__(self, /) -> _T: ...
    @property
    def from_commit_hash(self, /) -> str:
        """
        The hash of the commit at `from_ref`.
        """
    @property
    def to_commit_hash(self, /) -> str:
        """
        The hash of the commit at `to_ref`.
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

@disjoint_base
class MergeStrategy:
    """
    How a merge applies the changes of the source. `MergeStrategy.Squash` makes
    one commit with all the changes, and it takes an optional commit `message`.
    """
    @final
    class Squash(MergeStrategy):
        """
        One commit with all the changes of the source.
        """

        __match_args__: Final = ("message",)
        def __new__(cls, /, message: str | None = None) -> MergeStrategy.Squash: ...
        @property
        def message(self, /) -> str | None:
            """
            Optional, the commit message. Without it, the commit message is
            `Merge {from_ref} into {into_branch}`.
            """

@final
class Namespace:
    """
    A namespace, with its commit. A namespace is a container for organizing
    tables.
    """
    @property
    def commit(self, /) -> Commit:
        """
        The commit the namespace was read or written at.
        """
    @property
    def name(self, /) -> str:
        """
        The name of the namespace.
        """

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
    def __new__(cls, /, name: str, transform: PartitionTransform) -> PartitionField: ...
    @property
    def name(self, /) -> str:
        """
        The field the table is partitioned by.
        """
    @property
    def transform(self, /) -> PartitionTransform:
        """
        The partition transform.
        """

@disjoint_base
class PartitionTransform:
    """
    How a partition field derives its value from the column. It is one of
    `PartitionTransform.Identity`, `PartitionTransform.Year`,
    `PartitionTransform.Month`, `PartitionTransform.Day`,
    `PartitionTransform.Hour`, `PartitionTransform.Bucket`,
    `PartitionTransform.Truncate`, `PartitionTransform.Void` or
    `PartitionTransform.Unknown`.
    """
    @final
    class Bucket(PartitionTransform):
        """
        A hash of the value, in `num_buckets` buckets.
        """

        __match_args__: Final = ("num_buckets",)
        def __new__(cls, /, num_buckets: int) -> PartitionTransform.Bucket: ...
        @property
        def num_buckets(self, /) -> int:
            """
            The number of buckets.
            """

    @final
    class Day(PartitionTransform):
        """
        The day of a date or a timestamp.
        """

        __match_args__: Final = ()
        def __new__(cls, /) -> PartitionTransform.Day: ...

    @final
    class Hour(PartitionTransform):
        """
        The hour of a timestamp.
        """

        __match_args__: Final = ()
        def __new__(cls, /) -> PartitionTransform.Hour: ...

    @final
    class Identity(PartitionTransform):
        """
        The value of the column.
        """

        __match_args__: Final = ()
        def __new__(cls, /) -> PartitionTransform.Identity: ...

    @final
    class Month(PartitionTransform):
        """
        The month of a date or a timestamp.
        """

        __match_args__: Final = ()
        def __new__(cls, /) -> PartitionTransform.Month: ...

    @final
    class Truncate(PartitionTransform):
        """
        The value truncated to `width`.
        """

        __match_args__: Final = ("width",)
        def __new__(cls, /, width: int) -> PartitionTransform.Truncate: ...
        @property
        def width(self, /) -> int:
            """
            The width to truncate to.
            """

    @final
    class Unknown(PartitionTransform):
        """
        A transform this client does not know yet.
        """

        __match_args__: Final = ()
        def __new__(cls, /) -> PartitionTransform.Unknown: ...

    @final
    class Void(PartitionTransform):
        """
        Always null, for a partition field that is no longer used.
        """

        __match_args__: Final = ()
        def __new__(cls, /) -> PartitionTransform.Void: ...

    @final
    class Year(PartitionTransform):
        """
        The year of a date or a timestamp.
        """

        __match_args__: Final = ()
        def __new__(cls, /) -> PartitionTransform.Year: ...

@final
class Table:
    """
    A table in the lake.
    """
    @property
    def commit(self, /) -> Commit:
        """
        The commit the table was read or written at.
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
    def kind(self, /) -> TableKind:
        """
        The kind of table and, when known, the location of its metadata file.
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

@disjoint_base
class TableKind:
    """
    The kind of a table. It is one of `TableKind.IcebergManaged`,
    `TableKind.IcebergExternal`, `TableKind.IcebergExternalParquet` or
    `TableKind.Unknown`. All of them except `TableKind.Unknown` have the
    `metadata_location` of the table.
    """
    @final
    class IcebergExternal(TableKind):
        """
        A read-only iceberg table whose metadata and data files live outside
        bauplan.
        """

        __match_args__: Final = ("metadata_location",)
        def __new__(cls, /, metadata_location: str) -> TableKind.IcebergExternal: ...
        @property
        def metadata_location(self, /) -> str:
            """
            The location of the current metadata file.
            """

    @final
    class IcebergExternalParquet(TableKind):
        """
        A read-only iceberg table whose metadata bauplan writes, over parquet
        files that live outside bauplan.
        """

        __match_args__: Final = ("metadata_location",)
        def __new__(
            cls, /, metadata_location: str
        ) -> TableKind.IcebergExternalParquet: ...
        @property
        def metadata_location(self, /) -> str:
            """
            The location of the current metadata file.
            """

    @final
    class IcebergManaged(TableKind):
        """
        An iceberg table whose files bauplan writes and owns.
        """

        __match_args__: Final = ("metadata_location",)
        def __new__(cls, /, metadata_location: str) -> TableKind.IcebergManaged: ...
        @property
        def metadata_location(self, /) -> str:
            """
            The location of the current metadata file.
            """

    @final
    class Unknown(TableKind):
        """
        A kind this client does not know yet.
        """

        __match_args__: Final = ()
        def __new__(cls, /) -> TableKind.Unknown: ...

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
