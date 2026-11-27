__version__: str

from bauplan_sdk_types import (
    Any,
    Binary,
    Bool,
    Date32,
    Date64,
    Float64,
    Int32,
    Int64,
    Model,
    ModelCacheStrategy,
    ModelMaterializationStrategy,
    Parameter,
    String,
    TableField,
    TableSchema,
    TimestampMicro,
    TimestampMicroUTC,
    TimestampNano,
    TimestampNanoUTC,
    expectation,
    model,
    python,
)

# Submodules.
from bauplan import exceptions, schema, standard_expectations, state
from bauplan.schema import (
    Branch,
    Commit,
    CreateTableKind,
    DiffEntry,
    IteratorAtCommit,
    IteratorBetweenCommits,
    Job,
    JobContext,
    JobKind,
    JobLogEvent,
    JobState,
    MergeStrategy,
    Namespace,
    NamespaceIdentifier,
    PartitionField,
    PartitionTransform,
    Table,
    TableIdentifier,
    Tag,
)
from bauplan.state import (
    ExternalTableCreateState,
    RunState,
    TableCreatePlanApplyState,
    TableCreatePlanState,
    TableDataImportState,
)

__all__ = [
    "Any",
    "Binary",
    "Bool",
    "Client",
    "CreateTableKind",
    "Date32",
    "Date64",
    "Float64",
    "InfoState",
    "Int32",
    "Int64",
    "JobKind",
    "JobState",
    "MergeStrategy",
    "Model",
    "ModelCacheStrategy",
    "ModelMaterializationStrategy",
    "OrganizationInfo",
    "Parameter",
    "PartitionField",
    "PartitionTransform",
    "RunnerNodeInfo",
    "String",
    "TableField",
    "TableSchema",
    "TimestampMicro",
    "TimestampMicroUTC",
    "TimestampNano",
    "TimestampNanoUTC",
    "UserInfo",
    "__version__",
    "exceptions",
    "expectation",
    "model",
    "python",
    "schema",
    "standard_expectations",
    "state",
]

import pathlib
import typing
from datetime import datetime
from typing import Literal, final, overload

import pyarrow

@final
class Client:
    """
    A client for the Bauplan API.

    #### Using the client

    ```python
    import bauplan
    client = bauplan.Client()

    # query the table and return result set as an arrow Table
    my_table = client.query('SELECT avg(Age) AS average_age FROM bauplan.titanic limit 1', ref='main')

    # efficiently cast the table to a pandas DataFrame
    df = my_table.to_pandas()
    ```

    #### Notes on authentication

    ```python
    # by default, authenticate from BAUPLAN_API_KEY >> BAUPLAN_PROFILE >> ~/.bauplan/config.yaml
    client = bauplan.Client()
    # client used ~/.bauplan/config.yaml profile 'default'

    import os
    os.environ['BAUPLAN_PROFILE'] = "someprofile"
    client = bauplan.Client()
    # >> client now uses profile 'someprofile'

    os.environ['BAUPLAN_API_KEY'] = "mykey"
    client = bauplan.Client()
    # >> client now authenticates with api_key value "mykey", because api key > profile

    # specify authentication directly - this supersedes BAUPLAN_API_KEY in the environment
    client = bauplan.Client(api_key='MY_KEY')

    # specify a profile from ~/.bauplan/config.yaml - this supersedes BAUPLAN_PROFILE in the environment
    client = bauplan.Client(profile='default')
    ```

    #### Handling Exceptions

    Catalog operations (branch, tag, namespace and table methods) raise a subclass of
    `bauplan.exceptions.BauplanAPIError`, one for each type of error, like
    `TableNotFoundError`. Each method lists the ones it raises, and every method can
    also raise `InternalError` and `ServiceUnavailableError`.

    Every HTTP error is a `bauplan.exceptions.BauplanStatusError`, with the `status` of
    the response. For an error without a type we know, like a 502 from a proxy or an
    error added after this release, we raise `BauplanStatusError` itself. To retry a
    temporary error, catch `BauplanStatusError` and check its `status`, like 502, 503
    or 504.

    Jobs like `run` and `import_data` return a state object instead. A failed job doesn't
    raise; check `state.job_status` and `state.error`. `bauplan.exceptions.BauplanJobError`
    is only raised if the job can't be submitted or times out.

    `query`, `scan` and `query_to_*` return the results directly and raise
    `bauplan.exceptions.BauplanQueryError` if the query fails.

    #### Logging

    You can use python's standard logging apparatus to tap logs from the client:

    ```python
    import logging

    # Enable debug logs from the Bauplan client.
    logging.basicConfig()
    logging.getLogger("bauplan").setLevel(logging.DEBUG)

    client = bauplan.Client()
    client.query("SELECT count(*) FROM titanic", ref="main")
    ```

    #### Pagination

    Methods like `list_branches` and `list_tables` return a lazy paginating iterator:

    ```python
    #! client = bauplan.Client()
    for branch in client.list_branches():
        ...
    ```

    As a general rule, you should try to avoid eagerly evaluating them, as the lists can be very large.

    ```python
    #! client = bauplan.Client()
    # Don't do this!
    branches = list(client.list_branches())
    ```

    ## Examples

    ```python
    client = bauplan.Client()
    state = client.run("./my_pipeline")
    if state.job_status != "SUCCESS":
        ...
    ```

    Parameters:
        profile: The Bauplan config profile name to use to determine api_key.
        api_key: Your unique Bauplan API key; mutually exclusive with `profile`. If not provided, fetch precedence is 1) environment `BAUPLAN_API_KEY` 2) ~/.bauplan/config.yaml
        client_timeout: The client timeout in seconds for all the requests.
        config_file_path: The path to the Bauplan config file to use. If not provided, ~/.bauplan/config.yaml will be used. Note that this disables any environment-based configuration.
        catalog: The name of the catalog to use. If not provided, fetch precedence is 1) environment `BAUPLAN_CATALOG` 2) ~/.bauplan/config.yaml 3) `default`
    """
    def __new__(
        cls,
        /,
        profile: str | None = None,
        api_key: str | None = None,
        client_timeout: int | None = None,
        config_file_path: str | None = None,
        catalog: str | None = None,
    ) -> Client: ...
    def apply_table_creation_plan(
        self,
        /,
        plan: TableCreatePlanState | str,
        *,
        args: dict[str, str] | None = None,
        priority: int | None = None,
        client_timeout: int | None = None,
    ) -> TableCreatePlanApplyState:
        """
        Apply a plan for creating a table. It is done automatically during the
        table plan creation if no schema conflicts exist. Otherwise, if schema
        conflicts exist, then this function is used to apply them after the
        schema conflicts are resolved. The most common schema conflict is two
        parquet files with the same column name but different datatypes.

        ```python
        import bauplan
        client = bauplan.Client()

        plan_state = client.plan_table_creation(
            table='my_table',
            search_uri='s3://my-bucket/data/*.parquet',
            branch='main',
            namespace='my_namespace',
        )
        if plan_state.error:
            raise RuntimeError(f"Planning failed: {plan_state.error}")

        if plan_state.can_auto_apply:
            # No schema conflicts - the table was already created automatically.
            print("Table created automatically (no conflicts)")
        else:
            # Schema conflicts detected (e.g. same column name, different types across files).
            # Inspect and resolve the plan YAML, then apply manually.
            print(plan_state.plan)  # review/edit the schema plan
            apply_state = client.apply_table_creation_plan(
                plan=plan_state,
                priority=5,
                client_timeout=30,
            )
            if apply_state.error:
                raise RuntimeError(f"Apply failed: {apply_state.error}")
            print(f"Table created after conflict resolution: {apply_state.job_status}")
        ```

        Parameters:
            plan: The plan to apply.
            args: dict of arbitrary args to pass to the backend.
            priority: Optional job priority (1-10, where 10 is highest priority).
            client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.
        Returns:
            A `bauplan.state.TableCreatePlanApplyState` object.

        Raises:
            `bauplan.exceptions.TableCreatePlanApplyStatusError`: if the table creation plan apply fails.
        """
    def cancel_job(self, job_id: str, /) -> None:
        """
        EXPERIMENTAL: Cancel a job by ID.

        ```python
        import bauplan
        client = bauplan.Client()

        # Kick off a pipeline without blocking, then cancel it if it's taking too long
        state = client.run(
            project_dir='./my_pipeline',
            ref='username.dev_branch',
            detach=True,
        )
        if state.job_id:
            client.cancel_job(state.job_id)
        ```

        Parameters:
            job_id: A job ID
        """
    @overload
    def create_branch(
        self, /, branch: str, from_ref: str, *, if_not_exists: Literal[False] = False
    ) -> Branch: ...
    @overload
    def create_branch(
        self, /, branch: str, from_ref: str, *, if_not_exists: bool
    ) -> Branch | None: ...
    def create_branch(
        self, /, branch: str, from_ref: str, *, if_not_exists: bool = False
    ) -> Branch | None:
        """
        Create a branch at a ref. The name of the branch should start with
        the username, `username.branch_name`, otherwise non-admin users can't
        create it.

        ```python
        import bauplan
        client = bauplan.Client()

        branch = client.create_branch('username.feature', 'main', if_not_exists=True)
        ```

        Parameters:
            branch: The name of the new branch.
            from_ref: The ref to start from, a branch, a tag or a commit hash.
            if_not_exists: If set to `True`, a branch with the same name is not an error.
        Returns:
            The created `bauplan.schema.Branch`, or `None` when a branch with the same name exists and `if_not_exists` is set.

        Raises:
            `bauplan.exceptions.BranchNameConflictError`: if a branch with the same name exists.
            `bauplan.exceptions.TagNameConflictError`: if a tag with the same name exists.
            `bauplan.exceptions.FromRefNotFoundError`: if the ref to start from does not exist.
            `bauplan.exceptions.ForbiddenError`: if the user can't create the branch.
            `bauplan.exceptions.BadRequestError`: if the name is not valid.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def create_external_table_from_parquet(
        self,
        /,
        table: str,
        search_patterns: list[str],
        *,
        branch: str | None = None,
        namespace: str | None = None,
        overwrite: bool = False,
        args: dict[str, str] | None = None,
        priority: int | None = None,
        client_timeout: int | None = None,
        detach: bool = False,
    ) -> ExternalTableCreateState:
        """
        Creates an external table from files in your object storage.

        ```python
        import bauplan
        client = bauplan.Client()

        # Create from S3 files
        state = client.create_external_table_from_parquet(
            table='my_external_table',
            search_patterns=['s3://path1/to/my/files/*.parquet', 's3://path2/to/my/file/f1.parquet'],
            branch='my_branch_name',
        )

        if state.error:
            print(f"Error: {state.error}")
        else:
            print(f"External table created: {state.ctx.table_name}")
        ```

        Parameters:
            table: The name of the external table to create.
            search_patterns: List of search_patterns for files to create the external table from. Must resolve to parquet files
            branch: Branch in which to create the table.
            namespace: Namespace of the table. If not specified, namespace will be inferred from table name or default settings.
            overwrite: Whether to delete and recreate the table if it already exists.
            args: dict of arbitrary args to pass to the backend.
            priority: Optional job priority (1-10, where 10 is highest priority).
            client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.
            detach: Whether to detach the job and return immediately without waiting for the job to finish.

        Returns:
            A `bauplan.state.ExternalTableCreateState` object.
        """
    @overload
    def create_namespace(
        self,
        /,
        namespace: str,
        branch: str,
        *,
        commit_message: str | None = None,
        expected_head: str | None = None,
        if_not_exists: Literal[False] = False,
    ) -> Namespace: ...
    @overload
    def create_namespace(
        self,
        /,
        namespace: str,
        branch: str,
        *,
        commit_message: str | None = None,
        expected_head: str | None = None,
        if_not_exists: bool,
    ) -> Namespace | None: ...
    def create_namespace(
        self,
        /,
        namespace: str,
        branch: str,
        *,
        commit_message: str | None = None,
        expected_head: str | None = None,
        if_not_exists: bool = False,
    ) -> Namespace | None:
        """
        Create a namespace on a branch.

        ```python
        import bauplan
        client = bauplan.Client()

        namespace = client.create_namespace('raw_data', 'username.feature', if_not_exists=True)
        ```

        Parameters:
            namespace: The name of the new namespace.
            branch: The branch to create the namespace on.
            commit_message: Optional, the commit message. Without it, the server writes one for the operation.
            expected_head: Optional, create only if the branch points to this commit hash.
            if_not_exists: If set to `True`, a namespace or a table with the same name is not an error.
        Returns:
            The new `bauplan.schema.Namespace`, with the commit that creates it, or `None` when the name is in use and `if_not_exists` is set.

        Raises:
            `bauplan.exceptions.NameConflictError`: if a namespace or a table with the same name exists.
            `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
            `bauplan.exceptions.NotABranchError`: if the ref is not a branch.
            `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `expected_head`.
            `bauplan.exceptions.ForbiddenError`: if the user can't create the namespace.
            `bauplan.exceptions.BadRequestError`: if the name is not valid.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def create_table(
        self,
        /,
        table: str,
        kind: CreateTableKind,
        branch: str,
        *,
        expected_head: str | None = None,
    ) -> Table:
        """
        Create a table.

        ```python
        import bauplan
        import pyarrow as pa
        client = bauplan.Client()

        table = client.create_table(
            'raw_data.orders',
            bauplan.CreateTableKind.IcebergManaged(
                pa.schema([('id', pa.int64()), ('created_at', pa.timestamp('us'))]),
                partitions=[bauplan.PartitionField('created_at', bauplan.PartitionTransform.Day())],
            ),
            'username.feature',
        )

        table = client.create_table(
            'raw_data.events',
            bauplan.CreateTableKind.IcebergExternal('s3://my-bucket/events/metadata/00001-abc123.metadata.json'),
            'username.feature',
        )
        ```

        Parameters:
            table: The full name of the table, `namespace.table`. The namespace must exist.
            kind: The kind of the table to create, a `bauplan.schema.CreateTableKind`.
            branch: The branch to create the table on.
            expected_head: Optional, create only if the branch points to this commit hash.
        Returns:
            The new `bauplan.schema.Table`. Its `commit` is the commit that creates it.

        Raises:
            `bauplan.exceptions.NameConflictError`: if a namespace or a table with the same name exists.
            `bauplan.exceptions.NamespaceNotFoundError`: if the namespace does not exist.
            `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
            `bauplan.exceptions.NotABranchError`: if the ref is not a branch.
            `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `expected_head`.
            `bauplan.exceptions.ForbiddenError`: if the user can't write on the branch.
            `bauplan.exceptions.BadRequestError`: if the name or the kind are not valid.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
            `ValueError`: if the schema is not an arrow schema, or a partition transform is `PartitionTransform.Unknown`.
        """
    def create_table_from_search_uri(
        self,
        /,
        table: str,
        search_uri: str,
        *,
        branch: str | None = None,
        namespace: str | None = None,
        partitioned_by: str | None = None,
        replace: bool | None = None,
        args: dict[str, str] | None = None,
        priority: int | None = None,
        client_timeout: int | None = None,
    ) -> Table:
        """
        Create a table from an object storage location.

        This operation will attempt to create a table based on schemas of N
        parquet files found by a given search uri. This is a two step operation using
        `Client.plan_table_creation` and `Client.apply_table_creation_plan`.

        ```python
        import bauplan
        client = bauplan.Client()

        table = client.create_table_from_search_uri(
            table='my_table_name',
            search_uri='s3://path/to/my/files/*.parquet',
            branch='my_branch_name',
        )
        ```

        To specify partitioning, use the `partitioned_by` parameter:

        ```python
        import bauplan

        client = bauplan.Client()

        # Create a partitioned table
        table = client.create_table_from_search_uri(
            table='my_partitioned_table',
            search_uri='s3://your-bucket/data/*.parquet',
            partitioned_by="hour(tpep_pickup_datetime), PULocationID",
            branch='my_branch'
        )
        ```

        Parameters:
            table: The table which will be created.
            search_uri: The location of the files to scan for schema.
            branch: The branch name in which to create the table.
            namespace: Optional argument specifying the namespace. If not specified, it will be inferred based on table location or the default.
            partitioned_by: Optional argument specifying the table partitioning.
            replace: Replace the table if it already exists.
            args: dict of arbitrary args to pass to the backend.
            priority: Optional job priority (1-10, where 10 is highest priority).
            client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.
        Returns:
            The created `bauplan.schema.Table`.

        Raises:
            `bauplan.exceptions.TableCreatePlanStatusError`: if the table creation plan fails.
            `bauplan.exceptions.TableCreatePlanApplyStatusError`: if the table creation plan apply fails.
        """
    @overload
    def create_tag(
        self, /, tag: str, from_ref: str, *, if_not_exists: Literal[False] = False
    ) -> Tag: ...
    @overload
    def create_tag(
        self, /, tag: str, from_ref: str, *, if_not_exists: bool
    ) -> Tag | None: ...
    def create_tag(
        self, /, tag: str, from_ref: str, *, if_not_exists: bool = False
    ) -> Tag | None:
        """
        Create a tag at a ref.

        ```python
        import bauplan
        client = bauplan.Client()

        tag = client.create_tag('v1.0', 'main', if_not_exists=True)
        ```

        Parameters:
            tag: The name of the new tag.
            from_ref: The ref to tag, a branch, a tag or a commit hash.
            if_not_exists: If set to `True`, a tag with the same name is not an error.
        Returns:
            The created `bauplan.schema.Tag`, or `None` when a tag with the same name exists and `if_not_exists` is set.

        Raises:
            `bauplan.exceptions.TagNameConflictError`: if a tag with the same name exists.
            `bauplan.exceptions.BranchNameConflictError`: if a branch with the same name exists.
            `bauplan.exceptions.FromRefNotFoundError`: if the ref to tag does not exist.
            `bauplan.exceptions.ForbiddenError`: if the user can't create the tag.
            `bauplan.exceptions.BadRequestError`: if the name is not valid.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def delete_branch(
        self,
        /,
        branch: str,
        *,
        expected_head: str | None = None,
        if_exists: bool = False,
    ) -> None:
        """
        Delete a branch.

        ```python
        import bauplan
        client = bauplan.Client()

        client.delete_branch('username.feature', if_exists=True)
        ```

        Parameters:
            branch: The name of the branch.
            expected_head: Optional, delete only if the branch points to this commit hash.
            if_exists: If set to `True`, a missing branch is not an error.

        Raises:
            `bauplan.exceptions.BranchNotFoundError`: if the branch does not exist.
            `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `expected_head`.
            `bauplan.exceptions.MainBranchError`: if the branch is main.
            `bauplan.exceptions.ForbiddenError`: if the user can't delete the branch.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    @overload
    def delete_namespace(
        self,
        /,
        namespace: str,
        branch: str,
        *,
        commit_message: str | None = None,
        expected_head: str | None = None,
        if_exists: Literal[False] = False,
    ) -> Commit: ...
    @overload
    def delete_namespace(
        self,
        /,
        namespace: str,
        branch: str,
        *,
        commit_message: str | None = None,
        expected_head: str | None = None,
        if_exists: bool,
    ) -> Commit | None: ...
    def delete_namespace(
        self,
        /,
        namespace: str,
        branch: str,
        *,
        commit_message: str | None = None,
        expected_head: str | None = None,
        if_exists: bool = False,
    ) -> Commit | None:
        """
        Delete an empty namespace from a branch.

        ```python
        import bauplan
        client = bauplan.Client()

        commit = client.delete_namespace('raw_data', 'username.feature', if_exists=True)
        ```

        Parameters:
            namespace: The name of the namespace.
            branch: The branch to delete the namespace from.
            commit_message: Optional, the commit message. Without it, the server writes one for the operation.
            expected_head: Optional, delete only if the branch points to this commit hash.
            if_exists: If set to `True`, a missing namespace is not an error.
        Returns:
            The `bauplan.schema.Commit` that deletes the namespace, or `None` when it does not exist and `if_exists` is set.

        Raises:
            `bauplan.exceptions.NamespaceNotFoundError`: if the namespace does not exist.
            `bauplan.exceptions.NamespaceNotEmptyError`: if the namespace still has tables.
            `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
            `bauplan.exceptions.NotABranchError`: if the ref is not a branch.
            `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `expected_head`.
            `bauplan.exceptions.ForbiddenError`: if the user can't delete the namespace.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    @overload
    def delete_table(
        self,
        /,
        table: str,
        branch: str,
        *,
        commit_message: str | None = None,
        expected_head: str | None = None,
        if_exists: Literal[False] = False,
    ) -> Commit: ...
    @overload
    def delete_table(
        self,
        /,
        table: str,
        branch: str,
        *,
        commit_message: str | None = None,
        expected_head: str | None = None,
        if_exists: bool,
    ) -> Commit | None: ...
    def delete_table(
        self,
        /,
        table: str,
        branch: str,
        *,
        commit_message: str | None = None,
        expected_head: str | None = None,
        if_exists: bool = False,
    ) -> Commit | None:
        """
        Delete a table from a branch. The data files stay in the storage.

        ```python
        import bauplan
        client = bauplan.Client()

        commit = client.delete_table('raw_data.orders', 'username.feature', if_exists=True)
        ```

        Parameters:
            table: The full name of the table, `namespace.table`.
            branch: The branch to delete the table from.
            commit_message: Optional, the commit message. Without it, the server writes one for the operation.
            expected_head: Optional, delete only if the branch points to this commit hash.
            if_exists: If set to `True`, a missing table is not an error.
        Returns:
            The `bauplan.schema.Commit` that deletes the table, or `None` when it does not exist and `if_exists` is set.

        Raises:
            `bauplan.exceptions.TableNotFoundError`: if the table does not exist.
            `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
            `bauplan.exceptions.NotABranchError`: if the ref is not a branch.
            `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `expected_head`.
            `bauplan.exceptions.ForbiddenError`: if the user can't delete the table.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def delete_tag(self, /, tag: str, *, if_exists: bool = False) -> None:
        """
        Delete a tag.

        ```python
        import bauplan
        client = bauplan.Client()

        client.delete_tag('v1.0', if_exists=True)
        ```

        Parameters:
            tag: The name of the tag.
            if_exists: If set to `True`, a missing tag is not an error.

        Raises:
            `bauplan.exceptions.TagNotFoundError`: if the tag does not exist.
            `bauplan.exceptions.TagChangedError`: if the tag changed during the delete.
            `bauplan.exceptions.ForbiddenError`: if the user can't delete the tag.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def diff_refs(
        self,
        /,
        from_ref: str,
        to_ref: str,
        *,
        limit: int | None = None,
    ) -> IteratorBetweenCommits[DiffEntry]:
        """
        List the tables and namespaces that differ between two refs, in name
        order.

        ```python
        import bauplan
        client = bauplan.Client()

        for entry in client.diff_refs('main', 'username.feature'):
            print(entry.change, entry.entity)
        ```

        Parameters:
            from_ref: The ref to compare from, a branch, a tag or a commit hash.
            to_ref: The ref to compare to, a branch, a tag or a commit hash.
            limit: Optional, max number of entries to get.
        Returns:
            A `bauplan.schema.IteratorBetweenCommits` over `bauplan.schema.DiffEntry` objects. Its `from_commit_hash` and `to_commit_hash` are the hashes of the commits the refs point to, and all the pages are read at those commits.

        Raises:
            `bauplan.exceptions.RefNotFoundError`: if a ref does not exist.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def get_branch(self, /, branch: str) -> Branch:
        """
        Get a branch.

        ```python
        import bauplan
        client = bauplan.Client()

        branch = client.get_branch('main')
        ```

        Parameters:
            branch: The name of the branch.
        Returns:
            A `bauplan.schema.Branch` object.

        Raises:
            `bauplan.exceptions.BranchNotFoundError`: if the branch does not exist.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def get_job(self, job_id: str, /) -> Job:
        """
        EXPERIMENTAL: Get a job by ID.

        ```python
        #! my_job: bauplan.schema.Job = ...  # ty: ignore[invalid-assignment]
        import bauplan
        client = bauplan.Client()

        job = client.get_job(my_job.id)
        print(f"{job.human_readable_status} ({job.kind})")
        ```

        Parameters:
            job_id: A job ID
        Returns:
            A `bauplan.schema.Job` object.
        """
    def get_job_context(
        self,
        /,
        job_id: str,
        *,
        include_logs: bool = False,
        include_snapshot: bool = False,
    ) -> JobContext:
        """
        EXPERIMENTAL: Get context for a job by ID.

        ```python
        #! my_job: bauplan.schema.Job = ...  # ty: ignore[invalid-assignment]
        import bauplan
        client = bauplan.Client()

        ctx = client.get_job_context(my_job.id, include_logs=True)
        print(f"ref: {ctx.ref}, project: {ctx.project_name}")
        for log in ctx.logs:
            print(f"[{log.level}] {log.message}")
        ```

        Parameters:
            job_id: A job ID.
            include_logs: Whether to include logs in the response.
            include_snapshot: Whether to include the code snapshot in the response.
        Returns:
            A `bauplan.schema.JobContext` object containing the job details, and optionally logs and snapshot.
        """
    def get_job_contexts(
        self,
        /,
        job_ids: str | list[str],
        *,
        include_logs: bool = False,
        include_snapshot: bool = False,
    ) -> list[JobContext]:
        """
        EXPERIMENTAL: Get context for multiple jobs.

        ```python
        #! my_job: bauplan.schema.Job = ...  # ty: ignore[invalid-assignment]
        import bauplan
        client = bauplan.Client()

        contexts = client.get_job_contexts([my_job.id], include_logs=True)
        for ctx in contexts:
            print(f"{ctx.id}: {ctx.project_name} on {ctx.ref}")
        ```

        Parameters:
            job_ids: A job ID or a list of job IDs.
            include_logs: Whether to include logs in the response.
            include_snapshot: Whether to include the code snapshot in the response.
        Returns:
            A list of `bauplan.schema.JobContext` objects containing the job details, and optionally logs and snapshot.
        """
    def get_job_logs(self, /, job_id: str) -> list[JobLogEvent]:
        """
        EXPERIMENTAL: Get logs for a job.

        ```python
        #! my_job: bauplan.schema.Job = ...  # ty: ignore[invalid-assignment]
        import bauplan
        client = bauplan.Client()

        for log in client.get_job_logs(my_job.id):
            print(f"[{log.level}] {log.message}")
        ```

        Parameters:
            job_id: A job ID.
        Returns:
            A list of `bauplan.schema.JobLogEvent` objects representing the log events for the job.
        """
    def get_namespace(self, /, namespace: str, ref: str) -> Namespace:
        """
        Get a namespace at a ref.

        ```python
        import bauplan
        client = bauplan.Client()

        namespace = client.get_namespace('raw_data', 'main')
        ```

        Parameters:
            namespace: The name of the namespace.
            ref: The ref, a branch, a tag or a commit hash.
        Returns:
            The `bauplan.schema.Namespace`, with the commit it was read at.

        Raises:
            `bauplan.exceptions.NamespaceNotFoundError`: if the namespace does not exist.
            `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def get_table(self, /, table: str, ref: str) -> Table:
        """
        Get a table at a ref.

        ```python
        import bauplan
        client = bauplan.Client()

        table = client.get_table('bauplan.titanic', 'main')
        print(table.records, table.schema.names)
        ```

        Parameters:
            table: The full name of the table, `namespace.table`.
            ref: The ref, a branch, a tag or a commit hash.
        Returns:
            A `bauplan.schema.Table` object.

        Raises:
            `bauplan.exceptions.TableNotFoundError`: if the table does not exist.
            `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def get_tag(self, /, tag: str) -> Tag:
        """
        Get a tag.

        ```python
        import bauplan
        client = bauplan.Client()

        tag = client.get_tag('v1.0')
        ```

        Parameters:
            tag: The name of the tag.
        Returns:
            A `bauplan.schema.Tag` object.

        Raises:
            `bauplan.exceptions.TagNotFoundError`: if the tag does not exist.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def has_branch(self, /, branch: str) -> bool:
        """
        Check if a branch exists.

        ```python
        import bauplan
        client = bauplan.Client()

        if client.has_branch('main'):
            ...
        ```

        Parameters:
            branch: The name of the branch.
        Returns:
            A boolean for if the branch exists.

        Raises:
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def has_namespace(self, /, namespace: str, ref: str) -> bool:
        """
        Check if a namespace exists at a ref.

        ```python
        import bauplan
        client = bauplan.Client()

        if client.has_namespace('raw_data', 'main'):
            ...
        ```

        Parameters:
            namespace: The name of the namespace.
            ref: The ref, a branch, a tag or a commit hash.
        Returns:
            A boolean for if the namespace exists.

        Raises:
            `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def has_table(self, /, table: str, ref: str) -> bool:
        """
        Check if a table exists at a ref.

        ```python
        import bauplan
        client = bauplan.Client()

        if client.has_table('bauplan.titanic', 'main'):
            ...
        ```

        Parameters:
            table: The full name of the table, `namespace.table`.
            ref: The ref, a branch, a tag or a commit hash.
        Returns:
            A boolean for if the table exists.

        Raises:
            `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def has_tag(self, /, tag: str) -> bool:
        """
        Check if a tag exists.

        ```python
        import bauplan
        client = bauplan.Client()

        if client.has_tag('v1.0'):
            ...
        ```

        Parameters:
            tag: The name of the tag.
        Returns:
            A boolean for if the tag exists.

        Raises:
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def import_data(
        self,
        /,
        table: str,
        search_uri: str,
        *,
        branch: str | None = None,
        namespace: str | None = None,
        continue_on_error: bool = False,
        import_duplicate_files: bool = False,
        best_effort: bool = False,
        preview: str | None = None,
        args: dict[str, str] | None = None,
        priority: int | None = None,
        client_timeout: int | None = None,
        detach: bool = False,
    ) -> TableDataImportState:
        """
        Imports data into an already existing table.

        ```python
        import bauplan
        client = bauplan.Client()

        state = client.import_data(
            table='my_table_name',
            search_uri='s3://path/to/my/files/*.parquet',
            branch='my_branch_name',
        )
        if state.error:
            print(f"Import failed: {state.error}")
        else:
            print(f"Import succeeded: {state.job_status}")
        ```

        Parameters:
            table: Previously created table into which data will be imported.
            search_uri: URI to scan for files to import.
            branch: Branch in which to import the table.
            namespace: Namespace of the table. If not specified, namespace will be inferred from table name or default settings.
            continue_on_error: Do not fail the import even if 1 data import fails.
            import_duplicate_files: Ignore prevention of importing files that were already imported.
            best_effort: Ignore source columns that are not in the table schema instead of failing the import.
            preview: Whether to enable or disable preview mode for the import.
            args: dict of arbitrary args to pass to the backend.
            priority: Optional job priority (1-10, where 10 is highest priority).
            client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.
            detach: Whether to detach the job and return immediately without waiting for the job to finish.
        Returns:
            A `bauplan.state.TableDataImportState` object.
        """
    def info(self, /, *, client_timeout: int | None = None) -> InfoState:
        """
        Fetch organization & account information.

        ```python
        import bauplan
        client = bauplan.Client()

        info = client.info()
        print(info.user.username)
        print(info.organization.name)
        ```

        Parameters:
            client_timeout: timeout in seconds.

        Returns:
            A `bauplan.InfoState` object containing organization, user, and runner information.
        """
    def list_branches(
        self,
        /,
        *,
        name: str | None = None,
        owned: bool = False,
        limit: int | None = None,
    ) -> typing.Iterator[Branch]:
        """
        List the branches in the Bauplan catalog.

        ```python
        import bauplan
        client = bauplan.Client()

        for branch in client.list_branches(name='^username.', owned=True):
            ...
        ```

        Parameters:
            name: Optional, filter the branches by name. Use `^` and `$` to anchor it to the start or the end.
            owned: If set to `True`, only the branches of the caller.
            limit: Optional, max number of branches to get.
        Returns:
            An iterator over `bauplan.schema.Branch` objects.

        Raises:
            `bauplan.exceptions.BadRequestError`: if the name filter is not valid.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def list_commits(
        self, /, ref: str, *, limit: int | None = None
    ) -> IteratorAtCommit[Commit]:
        """
        List the commit history at a ref, newest first.

        ```python
        import bauplan
        client = bauplan.Client()

        for commit in client.list_commits('main', limit=50):
            ...
        ```

        Parameters:
            ref: The ref, a branch, a tag or a commit hash.
            limit: Optional, max number of commits to get.
        Returns:
            A `bauplan.schema.IteratorAtCommit` over `bauplan.schema.Commit` objects. Its `commit_hash` is the hash of the commit `ref` points to, and all the pages are read at that commit.

        Raises:
            `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def list_jobs(
        self,
        /,
        *,
        filter_by_current_user: bool = True,
        filter_by_ids: str | list[str] | None = None,
        filter_by_users: str | list[str] | None = None,
        filter_by_kinds: str | JobKind | list[str] | list[JobKind] | None = None,
        filter_by_statuses: str | JobState | list[str] | list[JobState] | None = None,
        filter_by_created_after: datetime | None = None,
        filter_by_created_before: datetime | None = None,
        limit: int | None = None,
    ) -> typing.Iterator[Job]:
        """
        List the jobs, with optional filters.

        ```python
        import bauplan
        client = bauplan.Client()

        for job in client.list_jobs():
            print(f"{job.id}: {job.status} ({job.kind})")
        ```

        Parameters:
            filter_by_current_user: Optional[bool]: If True (the default), only return jobs
                belonging to the current user. Mutually exclusive with filter_by_users.
            filter_by_ids: Optional[Union[str, List[str]]]: Optional, filter by job IDs.
            filter_by_users: Optional[Union[str, List[str]]]: Optional, filter by job users.
            filter_by_kinds: Optional[Union[str, JobKind, List[Union[str, JobKind]]]]: Optional, filter by job kinds.
            filter_by_statuses: Optional[Union[str, JobState, List[Union[str, JobState]]]]: Optional, filter by job statuses.
            filter_by_created_after: Optional[datetime]: Optional, filter jobs created after this datetime.
            filter_by_created_before: Optional[datetime]: Optional, filter jobs created before this datetime.
            limit: Optional[int]: Optional, max number of jobs to return.

        Returns:
            An iterator over `bauplan.schema.Job` objects.
        """
    def list_namespaces(
        self, /, ref: str, *, name: str | None = None, limit: int | None = None
    ) -> IteratorAtCommit[NamespaceIdentifier]:
        """
        List the namespaces at a ref.

        ```python
        import bauplan
        client = bauplan.Client()

        for namespace in client.list_namespaces('main', name='^raw_'):
            ...
        ```

        Parameters:
            ref: The ref, a branch, a tag or a commit hash.
            name: Optional, filter the namespaces by name. Use `^` and `$` to anchor it to the start or the end.
            limit: Optional, max number of namespaces to get.
        Returns:
            A `bauplan.schema.IteratorAtCommit` over `bauplan.schema.NamespaceIdentifier` objects. Its `commit_hash` is the hash of the commit `ref` points to, and all the pages are read at that commit.

        Raises:
            `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
            `bauplan.exceptions.BadRequestError`: if the name filter is not valid.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def list_tables(
        self,
        /,
        ref: str,
        *,
        name: str | None = None,
        namespace: str | None = None,
        limit: int | None = None,
    ) -> IteratorAtCommit[TableIdentifier]:
        """
        List the tables at a ref.

        ```python
        import bauplan
        client = bauplan.Client()

        for table in client.list_tables('main', namespace='bauplan'):
            ...
        ```

        Parameters:
            ref: The ref, a branch, a tag or a commit hash.
            name: Optional, filter the tables by their full name, `namespace.table`. Use `^` and `$` to anchor it to the start or the end.
            namespace: Optional, filter the tables by namespace, together with `name`. Use `^` and `$` to anchor it to the start or the end.
            limit: Optional, max number of tables to get.
        Returns:
            A `bauplan.schema.IteratorAtCommit` over `bauplan.schema.TableIdentifier` objects. Its `commit_hash` is the hash of the commit `ref` points to, and all the pages are read at that commit.

        Raises:
            `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
            `bauplan.exceptions.BadRequestError`: if a filter is not valid.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def list_tags(
        self, /, *, name: str | None = None, limit: int | None = None
    ) -> typing.Iterator[Tag]:
        """
        List the tags in the Bauplan catalog.

        ```python
        import bauplan
        client = bauplan.Client()

        for tag in client.list_tags(name='^v1.'):
            ...
        ```

        Parameters:
            name: Optional, filter the tags by name. Use `^` and `$` to anchor it to the start or the end.
            limit: Optional, max number of tags to get.
        Returns:
            An iterator over `bauplan.schema.Tag` objects.

        Raises:
            `bauplan.exceptions.BadRequestError`: if the name filter is not valid.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def merge_branch(
        self,
        /,
        from_ref: str,
        into_branch: str,
        *,
        strategy: MergeStrategy | None = None,
        expected_head: str | None = None,
    ) -> Commit | None:
        """
        Merge a ref into a branch.

        ```python
        import bauplan
        client = bauplan.Client()

        commit = client.merge_branch('username.feature', 'main')
        ```

        To set the message of the squash commit:

        ```python
        import bauplan
        client = bauplan.Client()

        commit = client.merge_branch(
            'username.feature',
            'main',
            strategy=bauplan.MergeStrategy.Squash(message='Merge the feature'),
        )
        ```

        Parameters:
            from_ref: The ref to merge, a branch, a tag or a commit hash.
            into_branch: The branch to merge into.
            strategy: Optional, how to merge, a `bauplan.schema.MergeStrategy`. It is `MergeStrategy.Squash()` by default.
            expected_head: Optional, merge only if the branch points to this commit hash.
        Returns:
            The `bauplan.schema.Commit` of the merge, or `None` when there is nothing to merge.

        Raises:
            `bauplan.exceptions.BranchNotFoundError`: if the branch does not exist.
            `bauplan.exceptions.FromRefNotFoundError`: if the ref to merge does not exist.
            `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `expected_head`.
            `bauplan.exceptions.MergeConflictError`: if the merge has conflicts.
            `bauplan.exceptions.ForbiddenError`: if the user can't merge into the branch.
            `bauplan.exceptions.BadRequestError`: if `from_ref` is not a valid ref.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def plan_table_creation(
        self,
        /,
        table: str,
        search_uri: str,
        *,
        branch: str | None = None,
        namespace: str | None = None,
        partitioned_by: str | None = None,
        replace: bool | None = None,
        args: dict[str, str] | None = None,
        priority: int | None = None,
        client_timeout: int | None = None,
    ) -> TableCreatePlanState:
        """
        Create a table import plan from an object storage location.

        This operation will attempt to create a table based on schemas of N
        parquet files found by a given search uri. A YAML file containing the
        schema and plan is returned and if there are no conflicts, it is
        automatically applied.

        ```python
        import bauplan
        client = bauplan.Client()

        plan_state = client.plan_table_creation(
            table='my_table_name',
            search_uri='s3://path/to/my/files/*.parquet',
            branch='my_branch_name',
        )
        if plan_state.error:
            print(f"Plan failed: {plan_state.error}")
        else:
            print(plan_state.plan)
        ```

        To manually add partitioning to a plan before applying it:

        ```python
        import yaml
        import bauplan

        client = bauplan.Client()

        plan_state = client.plan_table_creation(
            table='my_partitioned_table',
            search_uri='s3://your-data/*.parquet',
            branch='my_branch',
        )

        # Modify plan to add partitioning
        plan = yaml.safe_load(plan_state.plan)
        plan['schema_info']['partitions'] = [
            {
                'from_column_name': 'datetime_column',
                'transform': {'name': 'year'},
            }
        ]

        # Apply the modified plan
        client.apply_table_creation_plan(yaml.dump(plan))
        ```

        Parameters:
            table: The table which will be created.
            search_uri: The location of the files to scan for schema.
            branch: The branch name in which to create the table.
            namespace: Optional argument specifying the namespace. If not specified, it will be inferred based on table location or the default.
            partitioned_by: Optional argument specifying the table partitioning.
            replace: Replace the table if it already exists.
            args: dict of arbitrary args to pass to the backend.
            priority: Optional job priority (1-10, where 10 is highest priority).
            client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.

        Returns:
            A `bauplan.state.TableCreatePlanState` object.

        Raises:
            `bauplan.exceptions.TableCreatePlanStatusError`: if the table creation plan fails.
        """
    def query(
        self,
        /,
        query: str,
        *,
        ref: str | None = None,
        max_rows: int | None = None,
        cache: Literal["on", "off"] | None = None,
        namespace: str | None = None,
        args: dict[str, str] | None = None,
        priority: int | None = None,
        client_timeout: int | None = None,
    ) -> pyarrow.Table:
        """
        Execute a SQL query and return the results as a pyarrow.Table.
        Note that this function uses Arrow also internally, resulting
        in a fast data transfer.

        If you prefer to return the results as a pandas DataFrame, use
        the `to_pandas` function of pyarrow.Table.

        ```python
        import bauplan

        client = bauplan.Client()

        # query the table and return result set as an arrow Table
        my_table = client.query(
            query='SELECT avg(Age) as average_age FROM bauplan.titanic',
            ref='my_ref_or_branch_name',
        )

        # efficiently cast the table to a pandas DataFrame
        df = my_table.to_pandas()
        ```

        Parameters:
            query: The Bauplan query to execute. Column and table names are case-sensitive.
            ref: The ref, branch name or tag name to query from.
            max_rows: The maximum number of rows to return; default: `None` (no limit).
            cache: Whether to enable or disable caching for the query.
            namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
            args: Additional arguments to pass to the query (default: None).
            priority: Optional job priority (1-10, where 10 is highest priority).
            client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.
        Returns:
            The query results as a `pyarrow.Table`.
        """
    def query_to_csv_file(
        self,
        /,
        path: str | pathlib.Path,
        query: str,
        *,
        ref: str | None = None,
        max_rows: int | None = None,
        cache: Literal["on", "off"] | None = None,
        namespace: str | None = None,
        args: dict[str, str] | None = None,
        priority: int | None = None,
        client_timeout: int | None = None,
    ) -> pathlib.Path:
        """
        Export the results of a SQL query to a file in CSV format.

        ```python
        import bauplan
        client = bauplan.Client()

        # query the table and iterate through the results one row at a time
        client.query_to_csv_file(
            path='/tmp/out.csv',
            query='SELECT Name, Age FROM bauplan.titanic LIMIT 100',
            ref='my_ref_or_branch_name',
        )
        ```

        Parameters:
            path: The name or path of the file csv to write the results to.
            query: The Bauplan query to execute. Column and table names are case-sensitive.
            ref: The ref, branch name or tag name to query from.
            max_rows: The maximum number of rows to return; default: `None` (no limit).
            cache: Whether to enable or disable caching for the query.
            namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
            args: Additional arguments to pass to the query (default: None).
            client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.
        Returns:
            The path of the file written.
        """
    def query_to_generator(
        self,
        /,
        query: str,
        *,
        ref: str | None = None,
        max_rows: int | None = None,
        cache: Literal["on", "off"] | None = None,
        namespace: str | None = None,
        args: dict[str, str] | None = None,
        priority: int | None = None,
        client_timeout: int | None = None,
    ) -> typing.Iterator[dict[str, typing.Any]]:
        """
        Execute a SQL query and return the results as a generator, where each row is
        a Python dictionary.

        ```python
        import bauplan
        client = bauplan.Client()

        # query the table and iterate through the results one row at a time
        res = client.query_to_generator(
            query='SELECT Name, Age FROM bauplan.titanic LIMIT 100',
            ref='my_ref_or_branch_name',
        )

        for row in res:
            ... # handle results
        ```

        Parameters:
            query: The Bauplan query to execute. Column and table names are case-sensitive.
            ref: The ref, branch name or tag name to query from.
            max_rows: The maximum number of rows to return; default: `None` (no limit).
            cache: Whether to enable or disable caching for the query.
            namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
            args: Additional arguments to pass to the query (default: `None`).
            priority: Optional job priority (1-10, where 10 is highest priority).
            client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.

        Yields:
            A dictionary representing a row of query results.
        """
    def query_to_json_file(
        self,
        /,
        path: str | pathlib.Path,
        query: str,
        *,
        file_format: Literal["json", "jsonl"] = "json",
        ref: str | None = None,
        max_rows: int | None = None,
        cache: Literal["on", "off"] | None = None,
        namespace: str | None = None,
        args: dict[str, str] | None = None,
        priority: int | None = None,
        client_timeout: int | None = None,
    ) -> pathlib.Path:
        """
        Export the results of a SQL query to a file in JSON format.

        ```python
        import bauplan
        client = bauplan.Client()

        # query the table and iterate through the results one row at a time
        client.query_to_json_file(
            path='/tmp/out.json',
            query='SELECT Name, Age FROM bauplan.titanic LIMIT 100',
            ref='my_ref_or_branch_name',
        )
        ```

        Parameters:
            path: The name or path of the file json to write the results to.
            query: The Bauplan query to execute. Column and table names are case-sensitive.
            file_format: The format to write the results in; default: `json`. Allowed values are 'json' and 'jsonl'.
            ref: The ref, branch name or tag name to query from.
            max_rows: The maximum number of rows to return; default: `None` (no limit).
            cache: Whether to enable or disable caching for the query.
            namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
            args: Additional arguments to pass to the query (default: None).
            client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.
        Returns:
            The path of the file written.
        """
    def query_to_parquet_file(
        self,
        /,
        path: str | pathlib.Path,
        query: str,
        *,
        ref: str | None = None,
        max_rows: int | None = None,
        cache: Literal["on", "off"] | None = None,
        namespace: str | None = None,
        args: dict[str, str] | None = None,
        priority: int | None = None,
        client_timeout: int | None = None,
    ) -> pathlib.Path:
        """
        Export the results of a SQL query to a file in Parquet format.

        ```python
        import bauplan
        client = bauplan.Client()

        # query the table and iterate through the results one row at a time
        client.query_to_parquet_file(
            path='/tmp/out.parquet',
            query='SELECT Name, Age FROM bauplan.titanic LIMIT 100',
            ref='my_ref_or_branch_name',
        )
        ```

        Parameters:
            path: The name or path of the file parquet to write the results to.
            query: The Bauplan query to execute. Column and table names are case-sensitive.
            ref: The ref, branch name or tag name to query from.
            max_rows: The maximum number of rows to return; default: `None` (no limit).
            cache: Whether to enable or disable caching for the query.
            namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
            args: Additional arguments to pass to the query (default: None).
            client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.
        Returns:
            The path of the file written.
        """
    def rename_branch(self, /, branch: str, new_branch: str) -> Branch:
        """
        Rename a branch. The new name should start with the username,
        `username.branch_name`, otherwise non-admin users can't rename it.

        ```python
        import bauplan
        client = bauplan.Client()

        branch = client.rename_branch('username.old_name', 'username.new_name')
        ```

        Parameters:
            branch: The name of the branch.
            new_branch: The new name of the branch.
        Returns:
            The renamed `bauplan.schema.Branch`.

        Raises:
            `bauplan.exceptions.BranchNotFoundError`: if the branch does not exist.
            `bauplan.exceptions.BranchNameConflictError`: if a branch with the new name exists.
            `bauplan.exceptions.TagNameConflictError`: if a tag with the new name exists.
            `bauplan.exceptions.MainBranchError`: if the branch is main.
            `bauplan.exceptions.BranchHeadChangedError`: if the branch moves during the rename.
            `bauplan.exceptions.ForbiddenError`: if the user can't rename the branch.
            `bauplan.exceptions.BadRequestError`: if the new name is not valid.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def rename_table(
        self,
        /,
        table: str,
        new_table: str,
        branch: str,
        *,
        commit_message: str | None = None,
        expected_head: str | None = None,
    ) -> Table:
        """
        Rename a table on a branch, or move it to another namespace.

        ```python
        import bauplan
        client = bauplan.Client()

        table = client.rename_table('raw_data.orders', 'raw_data.orders_v1', 'username.feature')
        ```

        Parameters:
            table: The full name of the table, `namespace.table`.
            new_table: The new full name of the table, `namespace.table`. The namespace must exist.
            branch: The branch to rename the table on.
            commit_message: Optional, the commit message. Without it, the server writes one for the operation.
            expected_head: Optional, rename only if the branch points to this commit hash.
        Returns:
            The renamed `bauplan.schema.Table`. Its `commit` is the commit that renames it, or the head of the branch when the name does not change.

        Raises:
            `bauplan.exceptions.TableNotFoundError`: if the table does not exist.
            `bauplan.exceptions.NameConflictError`: if a namespace or a table with the new name exists.
            `bauplan.exceptions.NamespaceNotFoundError`: if the namespace of the new name does not exist.
            `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
            `bauplan.exceptions.NotABranchError`: if the ref is not a branch.
            `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `expected_head`.
            `bauplan.exceptions.ForbiddenError`: if the user can't rename the table.
            `bauplan.exceptions.BadRequestError`: if the new name is not valid.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def rename_tag(self, /, tag: str, new_tag: str) -> Tag:
        """
        Rename a tag.

        ```python
        import bauplan
        client = bauplan.Client()

        tag = client.rename_tag('v1.0', 'v1.0.0')
        ```

        Parameters:
            tag: The name of the tag.
            new_tag: The new name of the tag.
        Returns:
            The renamed `bauplan.schema.Tag`.

        Raises:
            `bauplan.exceptions.TagNotFoundError`: if the tag does not exist.
            `bauplan.exceptions.TagNameConflictError`: if a tag with the new name exists.
            `bauplan.exceptions.BranchNameConflictError`: if a branch with the new name exists.
            `bauplan.exceptions.TagChangedError`: if the tag changed during the rename.
            `bauplan.exceptions.ForbiddenError`: if the user can't rename the tag.
            `bauplan.exceptions.BadRequestError`: if the new name is not valid.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def revert_table(
        self,
        /,
        table: str,
        from_ref: str,
        into_branch: str,
        *,
        replace: bool = False,
        commit_message: str | None = None,
        expected_head: str | None = None,
    ) -> Table:
        """
        Copy the state of a table from a ref to a branch.

        ```python
        import bauplan
        client = bauplan.Client()

        table = client.revert_table('raw_data.orders', 'main', 'username.feature', replace=True)
        ```

        Parameters:
            table: The full name of the table, `namespace.table`.
            from_ref: The ref to take the table from, a branch, a tag or a commit hash.
            into_branch: The branch to put the table on.
            replace: If set to `True`, the table replaces the one with the same name on the branch.
            commit_message: Optional, the commit message. Without it, the server writes one for the operation.
            expected_head: Optional, revert only if the branch points to this commit hash.
        Returns:
            The reverted `bauplan.schema.Table`. Its `commit` is the commit that reverts it, or the head of the branch when the table is already as it is at `from_ref`.

        Raises:
            `bauplan.exceptions.FromTableNotFoundError`: if the table does not exist at `from_ref`.
            `bauplan.exceptions.FromRefNotFoundError`: if `from_ref` does not exist.
            `bauplan.exceptions.NameConflictError`: if the table exists on the branch and `replace` is not set.
            `bauplan.exceptions.NamespaceNotFoundError`: if the namespace of the table does not exist on the branch.
            `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
            `bauplan.exceptions.NotABranchError`: if the ref is not a branch.
            `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `expected_head`.
            `bauplan.exceptions.ForbiddenError`: if the user can't write on the branch.
            `bauplan.exceptions.BadRequestError`: if the table name is not valid.
            `bauplan.exceptions.UnsupportedTableKindError`: if the table is not an Iceberg table.
            `bauplan.exceptions.CatalogNotFoundError`: if the catalog does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.BauplanError`: the base of every error above, also raised when the request does not reach the server.
        """
    def run(
        self,
        /,
        project_dir: str,
        *,
        ref: str | None = None,
        namespace: str | None = None,
        parameters: dict[str, str | int | float | bool | None] | None = None,
        cache: bool = True,
        transaction: bool = True,
        dry_run: bool | None = None,
        strict: bool = True,
        preview: str | None = None,
        args: dict[str, str] | None = None,
        priority: int | None = None,
        client_timeout: int | None = None,
        detach: bool = False,
    ) -> RunState:
        """
        Run a Bauplan project and return the state of the run. This is the equivalent of
        running through the CLI the `bauplan run` command. Caching, transaction mode, and strict mode are enabled by default.

        ## Examples

        ```python
        #! client = bauplan.Client()
        # Run a daily sales pipeline on a dev branch, and if successful and data is good, merge to main
        run_state = client.run(
            project_dir='./etl_pipelines/daily_sales',
            ref="username.dev_branch",
            namespace='sales_analytics',
        )

        if str(run_state.job_status).lower() != "success":
            raise RuntimeError(f"{run_state.job_id} failed: {run_state.job_status} - {run_state.error}")
        ```

        Parameters:
            project_dir: The directory of the project (where the `bauplan_project.yml` or `bauplan_project.yaml` file is located).
            ref: The ref, branch name or tag name from which to run the project.
            namespace: The Namespace to run the job in. If not set, the job will be run in the default namespace.
            parameters: Parameters for templating into SQL or Python models.
            cache: Whether to enable caching for the run. Defaults to True. Set to False to disable caching.
            transaction: Whether to enable transaction mode for the run. Defaults to True. Set to False to disable transaction mode.
            dry_run: Whether to enable or disable dry-run mode for the run; models are not materialized.
            strict: Whether runtime warnings, including failing expectations and invalid column outputs, fail the run. Defaults to True. Set to False to disable strict mode.
            preview: Whether to enable or disable preview mode for the run.
            args: Additional arguments (optional).
            priority: Optional job priority (1-10, where 10 is highest priority).
            client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.
            detach: Whether to detach the run and return immediately instead of blocking on log streaming.
        Returns:
            `bauplan.state.RunState`: The state of the run.
        """
    def scan(
        self,
        /,
        table: str,
        *,
        ref: str | None = None,
        columns: list[str] | None = None,
        filters: str | None = None,
        limit: int | None = None,
        cache: Literal["on", "off"] | None = None,
        namespace: str | None = None,
        args: dict[str, str] | None = None,
        priority: int | None = None,
        client_timeout: int | None = None,
    ) -> pyarrow.Table:
        """
        Execute a table scan (with optional filters) and return the results as an arrow Table.

        Note that this function uses SQLGlot to compose a safe SQL query,
        and then internally defer to the `query` function for the actual
        scan.
        ```python
        import bauplan
        client = bauplan.Client()

        # run a table scan over the data lake
        # filters are passed as a string
        my_table = client.scan(
            table='titanic',
            ref='my_ref_or_branch_name',
            namespace='bauplan',
            columns=['Name'],
            filters='Age < 30',
        )
        ```

        Parameters:
            table: The table to scan.
            ref: The ref, branch name or tag name to scan from.
            columns: The columns to return (default: `None`).
            filters: The filters to apply (default: `None`).
            limit: The maximum number of rows to return (default: `None`).
            cache: Whether to enable or disable caching for the query.
            namespace: The Namespace to run the scan in. If not set, the scan will be run in the default namespace for your account.
            args: dict of arbitrary args to pass to the backend.
            priority: Optional job priority (1-10, where 10 is highest priority).
            client_timeout: seconds to timeout; this also cancels the remote job execution. Defaults to 1800 seconds.
        Returns:
            The scan results as a `pyarrow.Table`.
        """

@final
class InfoState:
    @property
    def client_version(self, /) -> str: ...
    @property
    def organization(self, /) -> OrganizationInfo: ...
    @property
    def runners(self, /) -> list[RunnerNodeInfo]: ...
    @property
    def user(self, /) -> UserInfo: ...

@final
class OrganizationInfo:
    @property
    def default_parameter_secret_key(self, /) -> str | None: ...
    @property
    def default_parameter_secret_public_key(self, /) -> str | None: ...
    @property
    def id(self, /) -> str: ...
    @property
    def name(self, /) -> str: ...
    @property
    def slug(self, /) -> str: ...

@final
class RunnerNodeInfo:
    @property
    def hostname(self, /) -> str: ...

@final
class UserInfo:
    @property
    def first_name(self, /) -> str: ...
    @property
    def full_name(self, /) -> str: ...
    @property
    def id(self, /) -> str: ...
    @property
    def last_name(self, /) -> str: ...
    @property
    def username(self, /) -> str: ...
