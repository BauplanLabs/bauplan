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
    Job,
    JobContext,
    JobKind,
    JobLogEvent,
    JobState,
    NamespaceIdentifier,
    Ref,
    RefType,
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
    "Date32",
    "Date64",
    "Float64",
    "InfoState",
    "Int32",
    "Int64",
    "JobKind",
    "JobState",
    "Model",
    "ModelCacheStrategy",
    "ModelMaterializationStrategy",
    "OrganizationInfo",
    "Parameter",
    "RefType",
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
from typing import Literal, final

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
    `bauplan.exceptions.BauplanHTTPError` matching the HTTP status code:

    - 400: `bauplan.exceptions.BadRequestError` (e.g. `InvalidRefError`)
    - 401: `bauplan.exceptions.UnauthorizedError`
    - 403: `bauplan.exceptions.ForbiddenError`
    - 404: `bauplan.exceptions.NotFoundError` (e.g. `TableNotFoundError`)
    - 405: `bauplan.exceptions.MethodNotAllowedError`
    - 409: `bauplan.exceptions.ConflictError` (e.g. `MergeConflictError`)
    - 429: `bauplan.exceptions.TooManyRequestsError`
    - 500: `bauplan.exceptions.InternalError`
    - 502: `bauplan.exceptions.BadGatewayError`
    - 503: `bauplan.exceptions.ServiceUnavailableError`
    - 504: `bauplan.exceptions.GatewayTimeoutError`

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

    Methods like `get_branches` and `get_tables` return a lazy paginating iterator:

    ```python
    #! client = bauplan.Client()
    for branch in client.get_branches():
        ...
    ```

    As a general rule, you should try to avoid eagerly evaluating them, as the lists can be very large.

    ```python
    #! client = bauplan.Client()
    # Don't do this!
    branches = list(client.get_branches())
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
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
        """
    def create_external_table_from_metadata(
        self,
        /,
        table: str,
        metadata_json_uri: str,
        *,
        namespace: str,
        branch: str | None = None,
        overwrite: bool = False,
    ) -> Table:
        """
        Create an external table from an Iceberg metadata.json file.

        This operation creates an external table by pointing to an existing Iceberg table's
        metadata.json file. This is useful for importing external Iceberg tables into Bauplan
        without copying the data.

        ```python
        import bauplan
        client = bauplan.Client()

        # Create an external table from metadata
        result = client.create_external_table_from_metadata(
            table='my_external_table',
            metadata_json_uri='s3://my-bucket/path/to/metadata/00001-abc123.metadata.json',
            namespace='my_namespace',
            branch='my_branch_name',
        )
        ```

        Parameters:
            table: The name of the table to create.
            metadata_json_uri: The URI pointing to the Iceberg table's metadata.json file.
            namespace: The namespace for the table (required).
            branch: The branch name in which to create the table. Defaults to the active branch, or 'main'.
            overwrite: Whether to overwrite an existing table with the same name (default: False).

        Returns:
            The registered `bauplan.schema.Table` with full metadata.

        Raises:
            `ValueError`: if metadata_json_uri is empty or invalid, or if table parameter is invalid.
            `bauplan.exceptions.BranchNotFoundError`: if the branch does not exist.
            `bauplan.exceptions.NamespaceNotFoundError`: if the namespace does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `bauplan.exceptions.InvalidDataError`: if the metadata location is within the warehouse directory.
            `bauplan.exceptions.UpdateConflictError`: if a table with the same name already exists and overwrite=False.
            `bauplan.exceptions.BauplanError`: for other API errors during registration or retrieval.
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
    def create_namespace(
        self,
        /,
        namespace: str,
        branch: str,
        *,
        commit_body: str | None = None,
        if_head: str | None = None,
        if_not_exists: bool = False,
    ) -> Commit | None:
        """
        Create a namespace on a branch.

        ```python
        import bauplan
        client = bauplan.Client()

        commit = client.create_namespace('raw_data', 'username.feature', if_not_exists=True)
        ```

        Parameters:
            namespace: The name of the new namespace.
            branch: The branch to create the namespace on.
            commit_body: Optional, the body of the commit message.
            if_head: Optional, create only if the branch points to this commit hash.
            if_not_exists: If set to `True`, a namespace or a table with the same name is not an error.
        Returns:
            The `bauplan.schema.Commit` that creates the namespace, or `None` when the name is in use and `if_not_exists` is set.

        Raises:
            `bauplan.exceptions.NameConflictError`: if a namespace or a table with the same name exists.
            `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
            `bauplan.exceptions.NotABranchRefError`: if the ref is not a branch.
            `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `if_head`.
            `bauplan.exceptions.ForbiddenError`: if the user can't create the namespace.
            `bauplan.exceptions.BadRequestError`: if the name is not valid.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
        """
    def create_table(
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

        table = client.create_table(
            table='my_table_name',
            search_uri='s3://path/to/my/files/*.parquet',
            branch='my_branch_name',
        )
        ```

        To specify partitioning, use the `create_table` method with the `partitioned_by` parameter:

        ```python
        import bauplan

        client = bauplan.Client()

        # Create a partitioned table
        table = client.create_table(
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
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
        """
    def delete_branch(
        self, /, branch: str, *, if_exists: bool = False, if_head: str | None = None
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
            if_exists: If set to `True`, a missing branch is not an error.
            if_head: Optional, delete only if the branch points to this commit hash.

        Raises:
            `bauplan.exceptions.BranchNotFoundError`: if the branch does not exist.
            `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `if_head`.
            `bauplan.exceptions.MainBranchError`: if the branch is main.
            `bauplan.exceptions.ForbiddenError`: if the user can't delete the branch.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
        """
    def delete_namespace(
        self,
        /,
        namespace: str,
        branch: str,
        *,
        commit_body: str | None = None,
        if_head: str | None = None,
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
            commit_body: Optional, the body of the commit message.
            if_head: Optional, delete only if the branch points to this commit hash.
            if_exists: If set to `True`, a missing namespace is not an error.
        Returns:
            The `bauplan.schema.Commit` that deletes the namespace, or `None` when it does not exist and `if_exists` is set.

        Raises:
            `bauplan.exceptions.NamespaceNotFoundError`: if the namespace does not exist.
            `bauplan.exceptions.NamespaceIsNotEmptyError`: if the namespace still has tables.
            `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
            `bauplan.exceptions.NotABranchRefError`: if the ref is not a branch.
            `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `if_head`.
            `bauplan.exceptions.ForbiddenError`: if the user can't delete the namespace.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
        """
    def delete_table(
        self,
        /,
        table: str,
        branch: str,
        *,
        commit_body: str | None = None,
        if_head: str | None = None,
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
            commit_body: Optional, the body of the commit message.
            if_head: Optional, delete only if the branch points to this commit hash.
            if_exists: If set to `True`, a missing table is not an error.
        Returns:
            The `bauplan.schema.Commit` that deletes the table, or `None` when it does not exist and `if_exists` is set.

        Raises:
            `bauplan.exceptions.TableNotFoundError`: if the table does not exist.
            `bauplan.exceptions.RefNotFoundError`: if the branch does not exist.
            `bauplan.exceptions.NotABranchRefError`: if the ref is not a branch.
            `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `if_head`.
            `bauplan.exceptions.ForbiddenError`: if the user can't delete the table.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
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
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
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
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
        """
    def get_branches(
        self,
        /,
        *,
        name: str | None = None,
        owned: bool = False,
        limit: int | None = None,
    ) -> typing.Iterator[Branch]:
        """
        Get the branches in the Bauplan catalog.

        ```python
        import bauplan
        client = bauplan.Client()

        for branch in client.get_branches(name='^username.', owned=True):
            ...
        ```

        Parameters:
            name: Filter the branches by name, `^` and `$` anchor it to the start or the end.
            owned: Only the branches of the caller.
            limit: Optional, max number of branches to get.
        Returns:
            An iterator over `bauplan.schema.Branch` objects.

        Raises:
            `bauplan.exceptions.BadRequestError`: if the name filter is not valid.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
        """
    def get_commits(
        self, /, ref: str, *, limit: int | None = None
    ) -> typing.Iterator[Commit]:
        """
        Get the commit history at a ref, newest first.

        ```python
        import bauplan
        client = bauplan.Client()

        for commit in client.get_commits('main', limit=50):
            ...
        ```

        Parameters:
            ref: The ref, a branch, a tag or a commit hash.
            limit: Optional, max number of commits to get.
        Returns:
            An iterator over `bauplan.schema.Commit` objects.

        Raises:
            `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
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
        job: str,
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
            job: A job ID.
            include_logs: bool: Whether to include logs in the response.
            include_snapshot: bool: Whether to include the code snapshot in the response.
        Returns:
            A `bauplan.schema.JobContext` object containing the job details, and optionally logs and snapshot.
        """
    def get_job_contexts(
        self,
        /,
        jobs: str | list[str],
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
            jobs: A job ID or a list of job IDs.
            include_logs: bool: Whether to include logs in the response.
            include_snapshot: bool: Whether to include the code snapshot in the response.
        Returns:
            A list of `bauplan.schema.JobContext` objects containing the job details, and optionally logs and snapshot.
        """
    def get_job_logs(self, /, job: str) -> list[JobLogEvent]:
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
            job: A job ID.
        Returns:
            A list of `bauplan.schema.JobLogEvent` objects representing the log events for the job.
        """
    def get_jobs(
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
        Get jobs with optional filtering.

        ```python
        import bauplan
        client = bauplan.Client()

        for job in client.get_jobs():
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
    def get_namespaces(
        self, /, ref: str, *, name: str | None = None, limit: int | None = None
    ) -> typing.Iterator[NamespaceIdentifier]:
        """
        Get the namespaces at a ref.

        ```python
        import bauplan
        client = bauplan.Client()

        for namespace in client.get_namespaces('main', name='^raw_'):
            ...
        ```

        Parameters:
            ref: The ref, a branch, a tag or a commit hash.
            name: Filter the namespaces by name, `^` and `$` anchor it to the start or the end.
            limit: Optional, max number of namespaces to get.
        Returns:
            An iterator over `bauplan.schema.NamespaceIdentifier` objects.

        Raises:
            `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
            `bauplan.exceptions.BadRequestError`: if the name filter is not valid.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
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
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
        """
    def get_tables(
        self,
        /,
        ref: str,
        *,
        name: str | None = None,
        namespace: str | None = None,
        limit: int | None = None,
    ) -> typing.Iterator[TableIdentifier]:
        """
        Get the tables at a ref.

        ```python
        import bauplan
        client = bauplan.Client()

        for table in client.get_tables('main', namespace='bauplan'):
            ...
        ```

        Parameters:
            ref: The ref, a branch, a tag or a commit hash.
            name: Filter the tables by their full name, `^` and `$` anchor it to the start or the end.
            namespace: Filter the tables by namespace, `^` and `$` anchor it to the start or the end.
            limit: Optional, max number of tables to get.
        Returns:
            An iterator over `bauplan.schema.TableIdentifier` objects.

        Raises:
            `bauplan.exceptions.RefNotFoundError`: if the ref does not exist.
            `bauplan.exceptions.BadRequestError`: if a filter is not valid.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
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
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
        """
    def get_tags(
        self, /, *, name: str | None = None, limit: int | None = None
    ) -> typing.Iterator[Tag]:
        """
        Get the tags in the Bauplan catalog.

        ```python
        import bauplan
        client = bauplan.Client()

        for tag in client.get_tags(name='^v1.'):
            ...
        ```

        Parameters:
            name: Filter the tags by name, `^` and `$` anchor it to the start or the end.
            limit: Optional, max number of tags to get.
        Returns:
            An iterator over `bauplan.schema.Tag` objects.

        Raises:
            `bauplan.exceptions.BadRequestError`: if the name filter is not valid.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
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
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
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
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
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
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
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
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
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
    def merge_branch(
        self,
        /,
        source_ref: str,
        into_branch: str,
        *,
        strategy: Literal["squash", "rebase", "fast_forward"] = "squash",
        commit_message: str | None = None,
        if_head: str | None = None,
    ) -> Commit | None:
        """
        Merge a ref into a branch.

        ```python
        import bauplan
        client = bauplan.Client()

        commit = client.merge_branch('username.feature', 'main')
        ```

        Parameters:
            source_ref: The ref to merge, a branch, a tag or a commit hash.
            into_branch: The branch to merge into.
            strategy: `squash` makes one commit with all the changes, `rebase` replays the commits one by one, `fast_forward` moves the branch to the ref.
            commit_message: Optional, the message of the `squash` commit.
            if_head: Optional, merge only if the branch points to this commit hash.
        Returns:
            The `bauplan.schema.Commit` of the merge, or `None` when there is nothing to merge.

        Raises:
            `bauplan.exceptions.BranchNotFoundError`: if the branch does not exist.
            `bauplan.exceptions.FromRefNotFoundError`: if the ref to merge does not exist.
            `bauplan.exceptions.BranchHeadChangedError`: if the branch does not point to `if_head`.
            `bauplan.exceptions.MergeConflictError`: if the merge has conflicts.
            `bauplan.exceptions.ForbiddenError`: if the user can't merge into the branch.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
            `ValueError`: if the strategy is not valid, or there is a message without `squash`.
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
            `bauplan.exceptions.ForbiddenError`: if the user can't rename the branch.
            `bauplan.exceptions.BadRequestError`: if the new name is not valid.
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
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
            `bauplan.exceptions.UnauthorizedError`: if the user's credentials are invalid.
        """
    def revert_table(
        self,
        /,
        table: str,
        *,
        namespace: str | None = None,
        source_ref: str,
        into_branch: str,
        replace: bool | None = None,
        commit_body: str | None = None,
        commit_properties: dict[str, str] | None = None,
    ) -> Ref:
        """
        Revert a table to a previous state.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python
        import bauplan
        client = bauplan.Client()

        assert client.revert_table(
            table='my_table_name',
            namespace='my_namespace',
            source_ref='my_ref_or_branch_name',
            into_branch='main',
        )
        ```

        Parameters:
            table: The table to revert.
            namespace: The namespace of the table to revert.
            source_ref: The name of the source ref; either a branch like "main" or ref like "main@[sha]".
            into_branch: The name of the target branch where the table will be reverted.
            replace: Optional, whether to replace the table if it already exists.
            commit_body: Optional, the commit body message to attach to the operation.
            commit_properties: Optional, a list of properties to attach to the operation.
        Returns:
            The `bauplan.schema.Ref` of the branch where the revert was made.

        Raises:
            `ValueError`: if the table identifier is invalid, source_ref or branch_name is blank, the source entry is not Iceberg, a snapshot_id is missing, or a path validation error occurs.
            `bauplan.exceptions.InvalidRefError`: if the ref format from Nessie is invalid.
            `bauplan.exceptions.NotAWriteBranchRefError`: if the destination ref is not a branch.
            `bauplan.exceptions.SameRefError`: if the source and destination have the same hash.
            `bauplan.exceptions.UnauthorizedError`: if the JWT is invalid or the auth session is missing.
            `bauplan.exceptions.ForbiddenError`: if the user lacks writer role.
            `bauplan.exceptions.RevertTableForbiddenError`: if there is a zone/workspace restriction on the destination branch.
            `bauplan.exceptions.RefNotFoundError`: if the source ref doesn't exist.
            `bauplan.exceptions.TableNotFoundError`: if the source table is not found on the source ref.
            `bauplan.exceptions.BranchNotFoundError`: if the destination branch doesn't exist.
            `bauplan.exceptions.NamespaceUnresolvedError`: if the namespace cannot be resolved.
            `bauplan.exceptions.BranchHeadChangedError`: if a concurrent modification changed the branch head.
            `bauplan.exceptions.RevertDestinationTableExistsError`: if the destination table exists and the replace flag is not set.
            `bauplan.exceptions.RevertIdenticalTableError`: if the source and destination have the same snapshot.
            `bauplan.exceptions.MergeConflictError`: if there is a merge conflict during the transactional revert.
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
