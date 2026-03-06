from datetime import datetime
from typing import Final, final
from uuid import UUID

@final
class Actor:
    """
    An actor (author or committer) in a commit.
    """
    def __repr__(self, /) -> str: ...
    @property
    def email(self, /) -> str | None:
        """
        The actor's email address.
        """
    @property
    def name(self, /) -> str:
        """
        The actor's name.
        """

@final
class Branch(Ref):
    """
    A data branch, used to isolate data changes before merging into main.
    """

@final
class Commit:
    """
    A commit in the catalog.
    """
    def __repr__(self, /) -> str: ...
    @property
    def author(self, /) -> Actor |None: 
        """The first author of the commit."""
        ...
    @property
    def authored_date(self, /) -> datetime:
        """
        The date the commit was authored.
        """
    @property
    def authors(self, /) -> list[Actor]:
        """
        The authors of the commit.
        """
    @property
    def body(self, /) -> str |None:
        """The body of the commit message. ``None`` if the message has no body."""
        ...
    @property
    def committed_date(self, /) -> datetime:
        """
        The date the commit was committed.
        """
    @property
    def committer(self, /) -> Actor:
        """
        The committer of the commit.
        """
    @property
    def message(self, /) -> str | None:
        """
        The commit message.
        """
    @property
    def parent_hashes(self, /) -> list[str]:
        """
        The parent commit hashes.
        """
    @property
    def parent_merge_ref(self, /) -> Branch |None: 
        """For merge commits, the branch that was merged in (second parent). ``None`` for regular commits."""
        ...
    @property
    def parent_ref(self, /) -> Ref:
        """
        The parent ref.
        """
    @property
    def properties(self, /) -> dict[str, str]:
        """
        Custom properties on the commit.
        """
    @property
    def ref(self, /) -> Ref:
        """
        The ref (branch, tag, or detached) this commit is on.
        """
    @property
    def signed_off_by(self, /) -> list[Actor]:
        """
        Actors who signed off on the commit.
        """
    @property
    def subject(self, /) -> str |None: 
        """The subject line of the commit message."""
        ...

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
    def source_model(self, /) -> str |None:
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
        ...
    @property
    def name(self, /) -> str:
        """The model name."""
        ...

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
    def __repr__(self, /) -> str: ...
    @property
    def created_at(self, /) -> datetime | None:
        """
        When the job was created.
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
    def tx_ref(self, /) -> str | None: ...

@final
class JobKind:
    """
    The kind/type of a job.
    """

    IMPORT_PLAN_APPLY: Final[JobKind]
    IMPORT_PLAN_CREATE: Final[JobKind]
    QUERY: Final[JobKind]
    RUN: Final[JobKind]
    TABLE_IMPORT: Final[JobKind]
    TABLE_PLAN_CREATE: Final[JobKind]
    TABLE_PLAN_CREATE_APPLY: Final[JobKind]
    UNSPECIFIED: Final[JobKind]
    def __eq__(self, /, other: object) -> bool: ...
    def __int__(self, /) -> int: ...
    def __ne__(self, /, other: object) -> bool: ...
    def __repr__(self, /) -> str: ...
    def __str__(self, /) -> str: ...

@final
class JobLogEvent:
    """
    A single log message from a job execution. When you output logs within a Python model, they are persisted as `JobLogEvent`s.
    """
    def __repr__(self, /) -> str: ...
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
    def __eq__(self, /, other: object) -> bool: ...
    def __int__(self, /) -> int: ...
    def __ne__(self, /, other: object) -> bool: ...
    def __repr__(self, /) -> str: ...

@final
class JobLogStream:
    """
    The output stream of a log event.
    """

    STDERR: Final[JobLogStream]
    STDOUT: Final[JobLogStream]
    def __eq__(self, /, other: object) -> bool: ...
    def __int__(self, /) -> int: ...
    def __ne__(self, /, other: object) -> bool: ...
    def __repr__(self, /) -> str: ...

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
    def __eq__(self, /, other: object) -> bool: ...
    def __int__(self, /) -> int: ...
    def __ne__(self, /, other: object) -> bool: ...
    def __repr__(self, /) -> str: ...
    def __str__(self, /) -> str: ...

@final
class Namespace:
    """
    A container for organizing tables.
    """
    def __repr__(self, /) -> str: ...
    @property
    def name(self, /) -> str:
        """
        The namespace name.
        """

@final
class PartitionField:
    """
    A partition field on a table.
    """
    def __repr__(self, /) -> str: ...
    @property
    def name(self, /) -> str:
        """
        The partition field name.
        """
    @property
    def transform(self, /) -> str:
        """
        The partition transform (e.g. "day", "month", "identity").
        """

class Ref:
    """
    A reference to a branch, tag, or commit, as returned by API operations.
    """
    def __repr__(self, /) -> str: ...
    def __str__(self, /) -> str: ...
    @property
    def hash(self, /) -> str:
        """The hash of the branch or tag."""
        ...
    @property
    def name(self, /) -> str: 
        """The name of the branch or tag."""
        ...
    @property
    def type(self, /) -> RefType: 
        """The type of the ref, either 'BRANCH', 'TAG', or 'DETACHED'."""
        ...

@final
class RefType:
    """
    The type of a ref.
    """

    BRANCH: Final[RefType]
    DETACHED: Final[RefType]
    TAG: Final[RefType]
    def __eq__(self, /, other: object) -> bool: ...
    def __int__(self, /) -> int: ...
    def __ne__(self, /, other: object) -> bool: ...
    def __repr__(self, /) -> str: ...
    def __str__(self, /) -> str: ...

@final
class Table:
    """
    A table in the lake.
    """
    @property
    def fqn(self, /) -> str:
        """
        The fully qualified name: `namespace.name`.
        """
    def is_managed(self, /) -> bool:
        """
        Whether this is a managed table.
        """
    def is_external(self, /) -> bool:
        """
        Whether this is an external table.
        """
    def __repr__(self, /) -> str: ...
    @property
    def current_schema_id(self, /) -> int | None:
        """
        The current Iceberg schema ID.
        """
    @property
    def current_snapshot_id(self, /) -> int | None:
        """
        The current Iceberg snapshot ID.
        """
    @property
    def fields(self, /) -> list[TableField]:
        """
        The fields in the table schema.
        """
    @property
    def id(self, /) -> UUID:
        """
        The table ID.
        """
    @property
    def kind(self, /) -> TableKind:
        """
        The table type.
        """
    @property
    def last_updated_at(self, /) -> datetime:
        """
        The timestamp when the table was last updated.
        """
    @property
    def metadata_location(self, /) -> str:
        """
        The URI of the Iceberg metadata file.
        """
    @property
    def name(self, /) -> str:
        """
        The table name.
        """
    @property
    def namespace(self, /) -> str:
        """
        The table namespace.
        """
    @property
    def partitions(self, /) -> list[PartitionField]:
        """
        The partition fields on the table.
        """
    @property
    def properties(self, /) -> dict[str, str]:
        """
        Table properties.
        """
    @property
    def records(self, /) -> int | None:
        """
        The number of records in the table.
        """
    @property
    def size(self, /) -> int | None:
        """
        The size of the table.
        """
    @property
    def snapshots(self, /) -> int | None:
        """
        The number of snapshots.
        """

@final
class TableField:
    """
    A field in a table schema.
    """
    def __repr__(self, /) -> str: ...
    @property
    def id(self, /) -> int:
        """
        The field ID.
        """
    @property
    def name(self, /) -> str:
        """
        The field name.
        """
    @property
    def required(self, /) -> bool:
        """
        Whether the field is required.
        """
    @property
    def type(self, /) -> str:
        """
        The field type.
        """

@final
class TableKind:
    """
    The kind of table entry.
    """

    ExternalTable: Final[TableKind]
    """
    An external table.
    """
    Table: Final[TableKind]
    """
    A managed table.
    """
    def __eq__(self, /, other: object) -> bool: ...
    def __int__(self, /) -> int: ...
    def __ne__(self, /, other: object) -> bool: ...
    def __repr__(self, /) -> str: ...

@final
class Tag(Ref):
    """
    A human-readable name that points to a specific commit in the data lake. Tags are often used to mark important milestones in the data lake history, such as releases or experiments.
    """
