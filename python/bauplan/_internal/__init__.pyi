import typing
from datetime import datetime
from typing import Literal, final

import pyarrow

from bauplan._internal.schema import (
    Branch, Commit, Job, JobContext, JobKind, JobLogEvent, JobState,
    Namespace, Ref, Table, Tag,
)
from bauplan._internal.state import (
    ExternalTableCreateState, RunState,
    TableCreatePlanApplyState, TableCreatePlanState,
    TableDataImportState,
)

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

    ```python notest fixture:bauplan
    # by default, authenticate from BAUPLAN_API_KEY >> BAUPLAN_PROFILE >> ~/.bauplan/config.yml
    client = bauplan.Client()
    # client used ~/.bauplan/config.yml profile 'default'

    import os
    os.environ['BAUPLAN_PROFILE'] = "someprofile"
    client = bauplan.Client()
    # >> client now uses profile 'someprofile'

    os.environ['BAUPLAN_API_KEY'] = "mykey"
    client = bauplan.Client()
    # >> client now authenticates with api_key value "mykey", because api key > profile

    # specify authentication directly - this supercedes BAUPLAN_API_KEY in the environment
    client = bauplan.Client(api_key='MY_KEY')

    # specify a profile from ~/.bauplan/config.yml - this supercedes BAUPLAN_PROFILE in the environment
    client = bauplan.Client(profile='default')
    ```

    #### Handling Exceptions

    Catalog operations (branch/table methods) raise a subclass of `bauplan.exceptions.BauplanError` that mirror HTTP status codes.
        - 400: `bauplan.exceptions.InvalidDataError`
        - 401: `bauplan.exceptions.UnauthorizedError`
        - 403: `bauplan.exceptions.ForbiddenError`
        - 404: `bauplan.exceptions.ResourceNotFoundError` e.g .ID doesn't match any records
        - 404: `bauplan.exceptions.ApiRouteError` e.g. the given route doesn't exist
        - 405: `bauplan.exceptions.ApiMethodError` e.g. POST on a route with only GET defined
        - 409: `bauplan.exceptions.UpdateConflictError` e.g. creating a record with a name that already exists
        - 429: `bauplan.exceptions.TooManyRequestsError`

    Run/Query/Scan/Import operations raise a subclass of `bauplan.exceptions.BauplanError` that represents, and also return a `bauplan.state.RunState` object containing details and logs:
        - `bauplan.exceptions.JobError` e.g. something went wrong in a run/query/import/scan; includes error details

    Run/import operations also return a state object that includes a `job_status` and other details.
    There are two ways to check status for run/import operations:
        1. try/except `bauplan.exceptions.JobError`
        2. check the `state.job_status` attribute

    ## Examples

    ```python notest fixture:client
    state = client.run(...)
    if state.job_status != "SUCCESS":
        ...
    ```

    Parameters:
        profile: The Bauplan config profile name to use to determine api_key.
        api_key: Your unique Bauplan API key; mutually exclusive with `profile`. If not provided, fetch precedence is 1) environment `BAUPLAN_API_KEY` 2) .bauplan/config.yml
        client_timeout: The client timeout in seconds for all the requests.
        config_file_path: The path to the Bauplan config file to use. If not provided, ~/.bauplan/config.yaml will be used. Note that this disables any environment-based configuration.
    """
    def __new__(cls, /, profile: str |None = None, api_key: str |None = None, client_timeout: int |None = None, config_file_path: str |None = None) -> Client: ...
    def apply_table_creation_plan(self, /, plan: "TableCreatePlanState | str", *, args: "dict[str, str] | None" = None, priority: "int | None" = None, client_timeout: "int | None" = None) -> "TableCreatePlanApplyState":
        """
        Apply a plan for creating a table. It is done automaticaly during th
        table plan creation if no schema conflicts exist. Otherwise, if schema
        conflicts exist, then this function is used to apply them after the
        schema conflicts are resolved. Most common schema conflict is a two
        parquet files with the same column name but different datatype

        Parameters:
            plan: The plan to apply.
            args: dict of arbitrary args to pass to the backend.
            priority: Optional job priority (1-10, where 10 is highest priority).
            client_timeout: seconds to timeout; this also cancels the remote job execution.
        Returns:
            The plan state.

        Raises:
            TableCreatePlanApplyStatusError: if the table creation plan apply fails.
        """
    def cancel_job(self, job_id: str, /) -> "None":
        """
        EXPERIMENTAL: Cancel a job by ID.

        Parameters:
            id: A job ID
        """
    def create_branch(self, /, branch: "str | Branch", from_ref: "str | Ref", *, if_not_exists: "bool" = False) -> "Branch":
        """
        Create a new branch at a given ref.
        The branch name should follow the convention of `username.branch_name`,
        otherwise non-admin users won't be able to complete the operation.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python notest
        import bauplan

        client = bauplan.Client()
        user = client.info().user
        assert user is not None
        username = user.username

        branch = client.create_branch(
            branch = username+'.feature_branch',
            from_ref = 'branch_name@abcd1234',
            if_not_exists = True,
        )
        ```

        Parameters:
            branch: The name of the new branch.
            from_ref: The name of the base branch; either a branch like "main" or ref like "main@[sha]".
            if_not_exists: If set to `True`, the branch will not be created if it already exists.
        Returns:
            The created branch object.

        Raises:
            CreateBranchForbiddenError: if the user does not have access to create the branch.
            BranchExistsError: if the branch already exists.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def create_external_table_from_metadata(self, /, table: "str | Table", metadata_json_uri: "str", *, namespace: "str | Namespace", branch: "str | Branch | None" = None, overwrite: "bool" = False) -> "Table":
        """
        Create an external table from an Iceberg metadata.json file.

        This operation creates an external table by pointing to an existing Iceberg table's
        metadata.json file. This is useful for importing external Iceberg tables into Bauplan
        without copying the data.

        ```python notest
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
            metadata_json_uri: The S3 URI pointing to the Iceberg table's metadata.json file.
            namespace: The namespace for the table (required).
            branch: The branch name in which to create the table. Defaults to the active branch, or 'main'.
            overwrite: Whether to overwrite an existing table with the same name (default: False).

        Returns:
            Table: The registered table with full metadata.

        Raises:
            ValueError: if metadata_json_uri is empty or invalid, or if table parameter is invalid.
            BranchNotFoundError: if the branch does not exist.
            NamespaceNotFoundError: if the namespace does not exist.
            UnauthorizedError: if the user's credentials are invalid.
            InvalidDataError: if the metadata location is within the warehouse directory.
            UpdateConflictError: if a table with the same name already exists and overwrite=False.
            BauplanError: for other API errors during registration or retrieval.
        """
    def create_external_table_from_parquet(self, /, table: "str | Table", search_patterns: "list[str]", *, branch: "str | Branch | None" = None, namespace: "str | Namespace | None" = None, overwrite: "bool" = False, args: "dict[str, str] | None" = None, priority: "int | None" = None, client_timeout: "int | None" = None, detach: "bool" = False) -> "ExternalTableCreateState":
        """
        Creates an external table from S3 files.

        ```python notest
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
            client_timeout: seconds to timeout; this also cancels the remote job execution.
            detach: Whether to detach the job and return immediately without waiting for the job to finish.

        Returns:
            The external table create state.
        """
    def create_namespace(self, /, namespace: "str | Namespace", branch: "str | Branch", *, commit_body: "str | None" = None, commit_properties: "dict[str, str] | None" = None, if_not_exists: "bool" = False) -> "Namespace":
        """
        Create a new namespace at a given branch.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python fixture:my_branch
        import bauplan
        client = bauplan.Client()

        assert client.create_namespace(
            namespace='my_namespace_name',
            branch='my_branch_name',
            if_not_exists=True,
        )
        ```

        Parameters:
            namespace: The name of the namespace.
            branch: The name of the branch to create the namespace on.
            commit_body: Optional, the commit body to attach to the operation.
            commit_properties: Optional, a list of properties to attach to the commit.
            if_not_exists: If set to `True`, the namespace will not be created if it already exists.
        Returns:
            The created `bauplan.schema.Namespace` object.

        Raises:
            CreateNamespaceForbiddenError: if the user does not have access to create the namespace.
            BranchNotFoundError: if the branch does not exist.
            NotAWriteBranchError: if the destination branch is not a writable ref.
            BranchHeadChangedError: if the branch head hash has changed.
            NamespaceExistsError: if the namespace already exists.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def create_table(self, /, table: "str | Table", search_uri: "str", *, branch: "str | Branch | None" = None, namespace: "str | Namespace | None" = None, partitioned_by: "str | None" = None, replace: "bool | None" = None, args: "dict[str, str] | None" = None, priority: "int | None" = None, client_timeout: "int | None" = None) -> "Table":
        """
        Create a table from an S3 location.

        This operation will attempt to create a table based of schemas of N
        parquet files found by a given search uri. This is a two step operation using
        `plan_table_creation ` and  `apply_table_creation_plan`.

        ```python notest
        import bauplan
        client = bauplan.Client()

        table = client.create_table(
            table='my_table_name',
            search_uri='s3://path/to/my/files/*.parquet',
            branch='my_branch_name',
        )
        ```

        Parameters:
            table: The table which will be created.
            search_uri: The location of the files to scan for schema.
            branch: The branch name in which to create the table in.
            namespace: Optional argument specifying the namespace. If not specified, it will be inferred based on table location or the default.
            partitioned_by: Optional argument specifying the table partitioning.
            replace: Replace the table if it already exists.
            args: dict of arbitrary args to pass to the backend.
            priority: Optional job priority (1-10, where 10 is highest priority).
            client_timeout: seconds to timeout; this also cancels the remote job execution.
        Returns:
            Table

        Raises:
            TableCreatePlanStatusError: if the table creation plan fails.
            TableCreatePlanApplyStatusError: if the table creation plan apply fails.
        """
    def create_tag(self, /, tag: "str | Tag", from_ref: "str | Ref", *, if_not_exists: "bool" = False) -> "Tag":
        """
        Create a new tag at a given ref.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python notest
        import bauplan
        client = bauplan.Client()

        assert client.create_tag(
            tag='my_tag',
            from_ref='my_ref_or_branch_name',
        )
        ```

        Parameters:
            tag: The name of the new tag.
            from_ref: The name of the base branch; either a branch like "main" or ref like "main@[sha]".
            if_not_exists: If set to `True`, the tag will not be created if it already exists.
        Returns:
            The created `bauplan.schema.Tag` object.

        Raises:
            CreateTagForbiddenError: if the user does not have access to create the tag.
            RefNotFoundError: if the ref does not exist.
            TagExistsError: if the tag already exists.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def delete_branch(self, /, branch: "str | Branch", *, if_exists: "bool" = False) -> "bool":
        """
        Delete a branch.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python fixture:my_branch
        import bauplan
        client = bauplan.Client()

        if client.delete_branch('my_branch_name'):
            ...
        ```

        Parameters:
            branch: The name of the branch to delete.
            if_exists: If set to `True`, the branch will not raise an error if it does not exist.
        Returns:
            A boolean for if the branch was deleted.

        Raises:
            DeleteBranchForbiddenError: if the user does not have access to delete the branch.
            BranchNotFoundError: if the branch does not exist.
            BranchHeadChangedError: if the branch head hash has changed.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def delete_namespace(self, /, namespace: "str | Namespace", branch: "str | Branch", *, if_exists: "bool" = False, commit_body: "str | None" = None, commit_properties: "dict[str, str] | None" = None) -> "Branch":
        """
        Delete a namespace.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python fixture:my_branch fixture:my_namespace
        import bauplan
        client = bauplan.Client()

        assert client.delete_namespace(
            namespace='my_namespace_name',
            branch='my_branch_name',
        )
        ```

        Parameters:
            namespace: The name of the namespace to delete.
            branch: The name of the branch to delete the namespace from.
            commit_body: Optional, the commit body to attach to the operation.
            commit_properties: Optional, a list of properties to attach to the commit.
            if_exists: If set to `True`, the namespace will not raise an error if it does not exist.
        Returns:
            A `bauplan.schema.Branch` object pointing to head.

        Raises:
            DeleteNamespaceForbiddenError: if the user does not have access to delete the namespace.
            BranchNotFoundError: if the branch does not exist.
            NotAWriteBranchError: if the destination branch is not a writable ref.
            BranchHeadChangedError: if the branch head hash has changed.
            NamespaceNotFoundError: if the namespace does not exist.
            NamespaceIsNotEmptyError: if the namespace is not empty.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def delete_table(self, /, table: "str | Table", branch: "str | Branch", *, namespace: "str | Namespace | None" = None, if_exists: "bool" = False, commit_body: "str | None" = None, commit_properties: "dict[str, str] | None" = None) -> "Branch":
        """
        Drop a table.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python notest
        import bauplan
        client = bauplan.Client()

        assert client.delete_table(
            table='my_table_name',
            branch='my_branch_name',
            namespace='my_namespace',
        )
        ```

        Parameters:
            table: The table to delete.
            branch: The branch on which the table is stored.
            namespace: The namespace of the table to delete.
            commit_body: Optional, the commit body message to attach to the commit.
            commit_properties: Optional, a list of properties to attach to the commit.
            if_exists: If set to `True`, the table will not raise an error if it does not exist.
        Returns:
            A `bauplan.schema.Branch` object pointing to the new head.

        Raises:
            DeleteTableForbiddenError: if the user does not have access to delete the table.
            BranchNotFoundError: if the branch does not exist.
            NotAWriteBranchError: if the destination branch is not a writable ref.
            BranchHeadChangedError: if the branch head hash has changed.
            TableNotFoundError: if the table does not exist.
            NamespaceConflictsError: if conflicting namespaces names are specified.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def delete_tag(self, /, tag: "str | Tag", *, if_exists: "bool" = False) -> "bool":
        """
        Delete a tag.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python fixture:my_tag
        import bauplan
        client = bauplan.Client()

        assert client.delete_tag('my_tag_name')
        ```

        Parameters:
            tag: The name of the tag to delete.
            if_exists: If set to `True`, the tag will not raise an error if it does not exist.
        Returns:
            A boolean for if the tag was deleted.

        Raises:
            DeleteTagForbiddenError: if the user does not have access to delete the tag.
            TagNotFoundError: if the tag does not exist.
            NotATagRefError: if the object is not a tag.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def get_branch(self, /, branch: "str | Branch") -> "Branch":
        """
        Get the branch.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python fixture:my_branch
        import bauplan
        client = bauplan.Client()

        # retrieve only the tables as tuples of (name, kind)
        branch = client.get_branch('my_branch_name')
        ```

        Parameters:
            branch: The name of the branch to retrieve.
        Returns:
            A `Branch` object.

        Raises:
            BranchNotFoundError: if the branch does not exist.
            NotABranchRefError: if the object is not a branch.
            ForbiddenError: if the user does not have access to the branch.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def get_branches(self, /, *, name: "str | None" = None, user: "str | None" = None, limit: "int | None" = None) -> "typing.Iterator[Branch]":
        """
        Get the available data branches in the Bauplan catalog.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python
        import bauplan
        client = bauplan.Client()

        for branch in client.get_branches():
            ...
        ```

        Parameters:
            name: Filter the branches by name.
            user: Filter the branches by user.
            limit: Optional, max number of branches to get.
        Returns:
            An iterator over `Branch` objects.
        """
    def get_commits(self, /, ref: "str | Ref", *, filter_by_message: "str | None" = None, filter_by_author_username: "str | None" = None, filter_by_author_name: "str | None" = None, filter_by_author_email: "str | None" = None, filter_by_authored_date: "str | datetime | None" = None, filter_by_authored_date_start_at: "str | datetime | None" = None, filter_by_authored_date_end_at: "str | datetime | None" = None, filter_by_parent_hash: "str | None" = None, filter_by_properties: "dict[str, str] | None" = None, filter: "str | None" = None, limit: "int | None" = None) -> "typing.Iterator[Commit]":
        """
        Get the commits for the target branch or ref.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        Parameters:
            ref: The ref or branch to get the commits from.
            filter_by_message: Optional, filter the commits by message (can be a string or a regex like '^abc.*$')
            filter_by_author_username: Optional, filter the commits by author username (can be a string or a regex like '^abc.*$')
            filter_by_author_name: Optional, filter the commits by author name (can be a string or a regex like '^abc.*$')
            filter_by_author_email: Optional, filter the commits by author email (can be a string or a regex like '^abc.*$')
            filter_by_authored_date: Optional, filter the commits by the exact authored date.
            filter_by_authored_date_start_at: Optional, filter the commits by authored date start at.
            filter_by_authored_date_end_at: Optional, filter the commits by authored date end at.
            filter_by_parent_hash: Optional, filter the commits by parent hash.
            filter_by_properties: Optional, filter the commits by commit properties.
            filter: Optional, a CEL filter expression to filter the commits.
            limit: Optional, max number of commits to get.
        Returns:
            An iterator over `Commit` objects.

        Raises:
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def get_job(self, job_id: str, /) -> "Job":
        """
        EXPERIMENTAL: Get a job by ID or ID prefix.

        Parameters:
            job_id: A job ID
        """
    def get_job_context(self, /, job: str | Job, *, include_logs: bool = False, include_snapshot: bool = False) -> "JobContext":
        """
        EXPERIMENTAL: Get context for a job by ID.

        Parameters:
            job: Union[str, Job]: A job ID, prefix of a job ID, a Job instance.
            include_logs: bool: Whether to include logs in the response.
            include_snapshot: bool: Whether to include the code snapshot in the response.
        """
    def get_job_contexts(self, /, jobs: str | list[str] | list[Job], *, include_logs: bool = False, include_snapshot: bool = False) -> "list[JobContext]":
        """
        EXPERIMENTAL: Get context for multiple jobs.

        Parameters:
            jobs: list[Union[str, Job]]: A list of job IDs or Job instances.
            include_logs: bool: Whether to include logs in the response.
            include_snapshot: bool: Whether to include the code snapshot in the response.
        """
    def get_job_logs(self, /, job: str | Job) -> "list[JobLogEvent]":
        """
        EXPERIMENTAL: Get logs for a job.

        Parameters:
            job: Union[str, Job]: A job ID, prefix of a job ID, or a Job instance.
        """
    def get_jobs(self, /, *, all_users: bool = False, filter_by_ids: str | list[str] | list[Job] |None = None, filter_by_users: str | list[str] |None = None, filter_by_kinds: str | JobKind | list[str] | list[JobKind] |None = None, filter_by_statuses: str | JobState | list[str] | list[JobState] |None = None, filter_by_created_after: datetime |None = None, filter_by_created_before: datetime |None = None, limit: int |None = None) -> "typing.Iterator[Job]":
        """
        Get jobs with optional filtering.

        Parameters:
            all_users: Optional[bool]: Whether to list jobs from all users or only the current user.
            filter_by_ids: Optional[Union[str, List[str]]]: Optional, filter by job IDs.
            filter_by_users: Optional[Union[str, List[str]]]: Optional, filter by job users.
            filter_by_kinds: Optional[Union[str, JobKind, List[Union[str, JobKind]]]]: Optional, filter by job kinds.
            filter_by_statuses: Optional[Union[str, JobState, List[Union[str, JobState]]]]: Optional, filter by job statuses.
            filter_by_created_after: Optional[datetime]: Optional, filter jobs created after this datetime.
            filter_by_created_before: Optional[datetime]: Optional, filter jobs created before this datetime.
            limit: Optional[int]: Optional, max number of jobs to return.

        Returns:
            An iterator over `Job` objects.
        """
    def get_namespace(self, /, namespace: "str | Namespace", ref: "str | Ref") -> "Namespace":
        """
        Get a namespace.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python fixture:my_namespace
        import bauplan
        client = bauplan.Client()

        namespace =  client.get_namespace(
            namespace='my_namespace_name',
            ref='my_branch_name',
        )
        ```

        Parameters:
            namespace: The name of the namespace to get.
            ref: The ref, branch name or tag name to check the namespace on.
        Returns:
            A `bauplan.schema.Namespace` object.

        Raises:
            NamespaceNotFoundError: if the namespace does not exist.
            RefNotFoundError: if the ref does not exist.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def get_namespaces(self, /, ref: "str | Ref", *, filter_by_name: "str | None" = None, limit: "int | None" = None) -> "typing.Iterator[Namespace]":
        """
        Get the available data namespaces in the Bauplan catalog branch.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python fixture:my_namespace
        import bauplan
        client = bauplan.Client()

        for namespace in client.get_namespaces('my_ref_or_branch_name'):
            ...
        ```

        Parameters:
            ref: The ref, branch name or tag name to retrieve the namespaces from.
            filter_by_name: Optional, filter the namespaces by name.
            limit: Optional, max number of namespaces to get.

        Raises:
            RefNotFoundError: if the ref does not exist.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.

        Yields:
            A Namespace object.
        """
    def get_table(self, /, table: "str | Table", ref: "str | Ref", *, namespace: "str | Namespace | None" = None) -> "Table":
        """
        Get the table data and metadata for a table in the target branch.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python fixture:my_branch fixture:my_namespace
        import bauplan
        client = bauplan.Client()

        # get the fields and metadata for a table
        table = client.get_table(
            table='titanic',
            ref='my_ref_or_branch_name',
            namespace='bauplan',
        )

        # You can get the total number of rows this way.
        num_records = table.records

        # Or access the schema.
        for c in table.fields:
            ...
        ```

        Parameters:
            ref: The ref, branch name or tag name to get the table from.
            table: The table to retrieve.
            namespace: The namespace of the table to retrieve.
        Returns:
            a `bauplan.schema.Table` object

        Raises:
            RefNotFoundError: if the ref does not exist.
            NamespaceNotFoundError: if the namespace does not exist.
            NamespaceConflictsError: if conflicting namespaces names are specified.
            TableNotFoundError: if the table does not exist.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def get_tables(self, /, ref: "str | Ref", *, filter_by_name: "str | None" = None, filter_by_namespace: "str | Namespace | None" = None, limit: "int | None" = None) -> "typing.Iterator[Table]":
        """
        Get the tables and views in the target branch.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python fixture:my_branch
        import bauplan
        client = bauplan.Client()

        for table in client.get_tables('my_branch_name'):
            ...
        ```

        Parameters:
            ref: The ref or branch to get the tables from.
            filter_by_name: Optional, the table name to filter by.
            filter_by_namespace: Optional, the namespace to get filtered tables from.
            limit: Optional, max number of tables to get.
        Returns:
            An iterator over `Table` objects.
        """
    def get_tag(self, /, tag: "str | Tag") -> "Tag":
        """
        Get the tag.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python fixture:my_tag
        import bauplan
        client = bauplan.Client()

        # retrieve only the tables as tuples of (name, kind)
        tag = client.get_tag('my_tag_name')
        ```

        Parameters:
            tag: The name of the tag to retrieve.
        Returns:
            A `bauplan.schema.Tag` object.

        Raises:
            TagNotFoundError: if the tag does not exist.
            NotATagRefError: if the object is not a tag.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def get_tags(self, /, *, filter_by_name: "str | None" = None, limit: "int | None" = None) -> "typing.Iterator[Tag]":
        """
        Get all the tags.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        Parameters:
            filter_by_name: Optional, filter the tags by name.
            limit: Optional, max number of tags to get.
        Returns:
            An iterator over `Tag` objects.

        Raises:
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def has_branch(self, /, branch: "str | Branch") -> "bool":
        """
        Check if a branch exists.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python fixture:my_branch
        import bauplan
        client = bauplan.Client()

        if client.has_branch('my_branch_name'):
            ...
        ```

        Parameters:
            branch: The name of the branch to check.
        Returns:
            A boolean for if the branch exists.

        Raises:
            ForbiddenError: if the user does not have access to the branch.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def has_namespace(self, /, namespace: "str | Namespace", ref: "str | Ref") -> "bool":
        """
        Check if a namespace exists.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python fixture:my_namespace
        import bauplan
        client = bauplan.Client()

        assert client.has_namespace(
            namespace='my_namespace_name',
            ref='my_branch_name',
        )
        ```

        Parameters:
            namespace: The name of the namespace to check.
            ref: The ref, branch name or tag name to check the namespace on.

        Returns:
            A boolean for if the namespace exists.

        Raises:
            RefNotFoundError: if the ref does not exist.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def has_table(self, /, table: "str | Table", ref: "str | Ref", *, namespace: "str | Namespace | None" = None) -> "bool":
        """
        Check if a table exists.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python fixture:my_branch
        import bauplan
        client = bauplan.Client()

        assert client.has_table(
            table='titanic',
            ref='my_ref_or_branch_name',
            namespace='bauplan',
        )
        ```

        Parameters:
            ref: The ref, branch name or tag name to get the table from.
            table: The table to retrieve.
            namespace: The namespace of the table to check.
        Returns:
            A boolean for if the table exists.

        Raises:
            RefNotFoundError: if the ref does not exist.
            NamespaceNotFoundError: if the namespace does not exist.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def has_tag(self, /, tag: "str | Tag") -> "bool":
        """
        Check if a tag exists.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python fixture:my_tag
        import bauplan
        client = bauplan.Client()

        assert client.has_tag(
            tag='my_tag_name',
        )
        ```

        Parameters:
            tag: The tag to retrieve.
        Returns:
            A boolean for if the tag exists.

        Raises:
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def import_data(self, /, table: "str | Table", search_uri: "str", *, branch: "str | Branch | None" = None, namespace: "str | Namespace | None" = None, continue_on_error: "bool" = False, import_duplicate_files: "bool" = False, best_effort: "bool" = False, preview: "str | None" = None, args: "dict[str, str] | None" = None, priority: "int | None" = None, client_timeout: "int | None" = None, detach: "bool" = False) -> "TableDataImportState":
        """
        Imports data into an already existing table.

        ```python notest
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
            table: Previously created table in into which data will be imported.
            search_uri: Uri which to scan for files to import.
            branch: Branch in which to import the table.
            namespace: Namespace of the table. If not specified, namespace will be infered from table name or default settings.
            continue_on_error: Do not fail the import even if 1 data import fails.
            import_duplicate_files: Ignore prevention of importing s3 files that were already imported.
            best_effort: Don't fail if schema of table does not match.
            preview: Whether to enable or disable preview mode for the import.
            args: dict of arbitrary args to pass to the backend.
            priority: Optional job priority (1-10, where 10 is highest priority).
            client_timeout: seconds to timeout; this also cancels the remote job execution.
            detach: Whether to detach the job and return immediately without waiting for the job to finish.
        Returns:
            A `bauplan.state.TableDataImportState` object.
        """
    def info(self, /, *, client_timeout: "int | None" = None) -> "InfoState":
        """
        Fetch organization & account information.

        ```python
        import bauplan
        client = bauplan.Client()

        info = client.info()
        if info.user:
            print(info.user.username)
        if info.organization:
            print(info.organization.name)
        ```

        Parameters:
            client_timeout: timeout in seconds.

        Returns:
            An `InfoState` object containing organization, user, and runner information.
        """
    def merge_branch(self, /, source_ref: "str | Ref", into_branch: "str | Branch", *, commit_message: "str | None" = None, commit_body: "str | None" = None, commit_properties: "dict[str, str] | None" = None) -> "Branch":
        """
        Merge one branch into another.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python notest
        import bauplan
        client = bauplan.Client()

        assert client.merge_branch(
            source_ref='my_ref_or_branch_name',
            into_branch='main',
        )
        ```

        Parameters:
            source_ref: The name of the merge source; either a branch like "main" or ref like "main@[sha]".
            into_branch: The name of the merge target.
            commit_message: Optional, the commit message.
            commit_body: Optional, the commit body.
            commit_properties: Optional, a list of properties to attach to the merge.
        Returns:
            the `Branch` where the merge was made.

        Raises:
            MergeForbiddenError: if the user does not have access to merge the branch.
            BranchNotFoundError: if the destination branch does not exist.
            NotAWriteBranchError: if the destination branch is not a writable ref.
            MergeConflictError: if the merge operation results in a conflict.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def plan_table_creation(self, /, table: "str | Table", search_uri: "str", *, branch: "str | Branch | None" = None, namespace: "str | Namespace | None" = None, partitioned_by: "str | None" = None, replace: "bool | None" = None, args: "dict[str, str] | None" = None, priority: "int | None" = None, client_timeout: "int | None" = None) -> "TableCreatePlanState":
        """
        Create a table import plan from an S3 location.

        This operation will attempt to create a table based of schemas of N
        parquet files found by a given search uri. A YAML file containing the
        schema and plan is returns and if there are no conflicts, it is
        automatically applied.

        ```python notest
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

        Parameters:
            table: The table which will be created.
            search_uri: The location of the files to scan for schema.
            branch: The branch name in which to create the table in.
            namespace: Optional argument specifying the namespace. If not specified, it will be inferred based on table location or the default.
            partitioned_by: Optional argument specifying the table partitioning.
            replace: Replace the table if it already exists.
            args: dict of arbitrary args to pass to the backend.
            priority: Optional job priority (1-10, where 10 is highest priority).
            client_timeout: seconds to timeout; this also cancels the remote job execution.

        Returns:
            The plan state.

        Raises:
            TableCreatePlanStatusError: if the table creation plan fails.
        """
    def query(self, /, query: "str", *, ref: "str | Ref | None" = None, max_rows: "int | None" = None, cache: "Literal['on', 'off'] | None" = None, namespace: "str | Namespace | None" = None, args: "dict[str, str] | None" = None, priority: "int | None" = None, client_timeout: "int | None" = None) -> "pyarrow.Table":
        """
        Execute a SQL query and return the results as a pyarrow.Table.
        Note that this function uses Arrow also internally, resulting
        in a fast data transfer.

        If you prefer to return the results as a pandas DataFrame, use
        the `to_pandas` function of pyarrow.Table.

        ```python fixture:my_branch
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
            query: The Bauplan query to execute.
            ref: The ref, branch name or tag name to query from.
            max_rows: The maximum number of rows to return; default: `None` (no limit).
            cache: Whether to enable or disable caching for the query.
            namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
            args: Additional arguments to pass to the query (default: None).
            priority: Optional job priority (1-10, where 10 is highest priority).
            client_timeout: seconds to timeout; this also cancels the remote job execution.
        Returns:
            The query results as a `pyarrow.Table`.
        """
    def query_to_csv_file(self, /, path: "str", query: "str", *, ref: "str | Ref | None" = None, max_rows: "int | None" = None, cache: "Literal['on', 'off'] | None" = None, namespace: "str | Namespace | None" = None, args: "dict[str, str] | None" = None, priority: "int | None" = None, client_timeout: "int | None" = None) -> "str":
        """
        Export the results of a SQL query to a file in CSV format.

        ```python fixture:my_branch
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
            query: The Bauplan query to execute.
            ref: The ref, branch name or tag name to query from.
            max_rows: The maximum number of rows to return; default: `None` (no limit).
            cache: Whether to enable or disable caching for the query.
            namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
            args: Additional arguments to pass to the query (default: None).
            client_timeout: seconds to timeout; this also cancels the remote job execution.
        Returns:
            The path of the file written.
        """
    def query_to_generator(self, /, query: "str", *, ref: "str | Ref | None" = None, max_rows: "int | None" = None, cache: "Literal['on', 'off'] | None" = None, namespace: "str | Namespace | None" = None, args: "dict[str, str] | None" = None, priority: "int | None" = None, client_timeout: "int | None" = None) -> "typing.Iterator[dict[str, typing.Any]]":
        """
        Execute a SQL query and return the results as a generator, where each row is
        a Python dictionary.

        ```python fixture:my_branch
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
            query: The Bauplan query to execute.
            ref: The ref, branch name or tag name to query from.
            max_rows: The maximum number of rows to return; default: `None` (no limit).
            cache: Whether to enable or disable caching for the query.
            namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
            args: Additional arguments to pass to the query (default: `None`).
            priority: Optional job priority (1-10, where 10 is highest priority).
            client_timeout: seconds to timeout; this also cancels the remote job execution.

        Yields:
            A dictionary representing a row of query results.
        """
    def query_to_json_file(self, /, path: "str", query: "str", *, file_format: "Literal['json', 'jsonl']" = "json", ref: "str | Ref | None" = None, max_rows: "int | None" = None, cache: "Literal['on', 'off'] | None" = None, namespace: "str | Namespace | None" = None, args: "dict[str, str] | None" = None, priority: "int | None" = None, client_timeout: "int | None" = None) -> "str":
        """
        Export the results of a SQL query to a file in JSON format.

        ```python fixture:my_branch
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
            query: The Bauplan query to execute.
            file_format: The format to write the results in; default: `json`. Allowed values are 'json' and 'jsonl'.
            ref: The ref, branch name or tag name to query from.
            max_rows: The maximum number of rows to return; default: `None` (no limit).
            cache: Whether to enable or disable caching for the query.
            namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
            args: Additional arguments to pass to the query (default: None).
            client_timeout: seconds to timeout; this also cancels the remote job execution.
        Returns:
            The path of the file written.
        """
    def query_to_parquet_file(self, /, path: "str", query: "str", *, ref: "str | Ref | None" = None, max_rows: "int | None" = None, cache: "Literal['on', 'off'] | None" = None, namespace: "str | Namespace | None" = None, args: "dict[str, str] | None" = None, priority: "int | None" = None, client_timeout: "int | None" = None) -> "str":
        """
        Export the results of a SQL query to a file in Parquet format.

        ```python fixture:my_branch
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
            query: The Bauplan query to execute.
            ref: The ref, branch name or tag name to query from.
            max_rows: The maximum number of rows to return; default: `None` (no limit).
            cache: Whether to enable or disable caching for the query.
            namespace: The Namespace to run the query in. If not set, the query will be run in the default namespace for your account.
            args: Additional arguments to pass to the query (default: None).
            client_timeout: seconds to timeout; this also cancels the remote job execution.
        Returns:
            The path of the file written.
        """
    def rename_branch(self, /, branch: "str | Branch", new_branch: "str | Branch") -> "Branch":
        """
        Rename an existing branch.
        The branch name should follow the convention of "username.branch_name",
        otherwise non-admin users won't be able to complete the operation.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python notest
        import bauplan
        client = bauplan.Client()

        assert client.rename_branch(
            branch='username.old_name',
            new_branch='username.new_name',
        )
        ```

        Parameters:
            branch: The name of the branch to rename.
            new_branch: The name of the new branch.
        Returns:
            The renamed `Branch` object.

        Raises:
            `RenameBranchForbiddenError`: if the user does not have access to create the branch.
            `UnauthorizedError`: if the user's credentials are invalid.
            `ValueError`: if one or more parameters are invalid.
        """
    def rename_tag(self, /, tag: "str | Tag", new_tag: "str | Tag") -> "Tag":
        """
        Rename an existing tag.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python notest
        import bauplan
        client = bauplan.Client()

        assert client.rename_tag(
            tag='old_tag_name',
            new_tag='new_tag_name',
        )
        ```

        Parameters:
            tag: The name of the tag to rename.
            new_tag: The name of the new tag.
        Returns:
            The renamed tag object.

        Raises:
            RenameTagForbiddenError: if the user does not have access to create the tag.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def revert_table(self, /, table: "str | Table", *, namespace: "str | Namespace | None" = None, source_ref: "str | Ref", into_branch: "str | Branch", replace: "bool | None" = None, commit_body: "str | None" = None, commit_properties: "dict[str, str] | None" = None) -> "Branch":
        """
        Revert a table to a previous state.

        Upon failure, raises `bauplan.exceptions.BauplanError`

        ```python notest
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
            The `bauplan.schema.Branch` where the revert was made.

        Raises:
            RevertTableForbiddenError: if the user does not have access to revert the table.
            RefNotFoundError: if the ref does not exist.
            BranchNotFoundError: if the destination branch does not exist.
            NotAWriteBranchError: if the destination branch is not a writable ref.
            BranchHeadChangedError: if the branch head hash has changed.
            MergeConflictError: if the merge operation results in a conflict.
            NamespaceConflictsError: if conflicting namespaces names are specified.
            UnauthorizedError: if the user's credentials are invalid.
            ValueError: if one or more parameters are invalid.
        """
    def run(self, /, project_dir: "str", *, ref: "str | Ref | None" = None, namespace: "str | Namespace | None" = None, parameters: "dict[str, str | int | float | bool | None] | None" = None, cache: "Literal['on', 'off'] | None" = None, transaction: "Literal['on', 'off'] | None" = None, dry_run: "bool | None" = None, strict: "Literal['on', 'off'] | None" = None, preview: "str | None" = None, args: "dict[str, str] | None" = None, priority: "int | None" = None, client_timeout: "int | None" = None, detach: "bool" = False) -> "RunState":
        """
        Run a Bauplan project and return the state of the run. This is the equivalent of
        running through the CLI the `bauplan run` command. All parameters default to 'off'/false unless otherwise specified.

        ## Examples

        ```python notest
        import bauplan
        client = bauplan.Client()

        # Run a daily sales pipeline on a dev branch, and if successful and data is good, merge to main
        run_state = client.run(
            project_dir='./etl_pipelines/daily_sales',
            ref="username.dev_branch",
            namespace='sales_analytics',
        )

        if str(run_state.job_status).lower() != "success":
            raise Exception(f"{run_state.job_id} failed: {run_state.job_status}")
        ```

        Parameters:
            project_dir: The directory of the project (where the `bauplan_project.yml` or `bauplan_project.yaml` file is located).
            ref: The ref, branch name or tag name from which to run the project.
            namespace: The Namespace to run the job in. If not set, the job will be run in the default namespace.
            parameters: Parameters for templating into SQL or Python models.
            cache: Whether to enable or disable caching for the run. Defaults to 'on'.
            transaction: Whether to enable or disable transaction mode for the run. Defaults to 'on'.
            dry_run: Whether to enable or disable dry-run mode for the run; models are not materialized.
            strict: Whether to enable or disable strict schema validation.
            preview: Whether to enable or disable preview mode for the run.
            args: Additional arguments (optional).
            priority: Optional job priority (1-10, where 10 is highest priority).
            client_timeout: seconds to timeout; this also cancels the remote job execution.
            detach: Whether to detach the run and return immediately instead of blocking on log streaming.
        Returns:
            `bauplan.state.RunState`: The state of the run.
        """
    def scan(self, /, table: "str | Table", *, ref: "str | Ref | None" = None, columns: "list[str] | None" = None, filters: "str | None" = None, limit: "int | None" = None, cache: "Literal['on', 'off'] | None" = None, namespace: "str | Namespace | None" = None, args: "dict[str, str] | None" = None, priority: "int | None" = None, client_timeout: "int | None" = None) -> "pyarrow.Table":
        """
        Execute a table scan (with optional filters) and return the results as an arrow Table.

        Note that this function uses SQLGlot to compose a safe SQL query,
        and then internally defer to the query_to_arrow function for the actual
        scan.
        ```python fixture:my_branch
        import bauplan
        client = bauplan.Client()

        # run a table scan over the data lake
        # filters are passed as a string
        my_table = client.scan(
            table='titanic',
            ref='my_ref_or_branch_name',
            namespace='bauplan',
            columns=['name'],
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
            client_timeout: seconds to timeout; this also cancels the remote job execution.
        Returns:
            The scan results as a `pyarrow.Table`.
        """

@final
class InfoState:
    def __repr__(self, /) -> str: ...
    @property
    def client_version(self, /) -> str: ...
    @property
    def organization(self, /) -> OrganizationInfo |None: ...
    @property
    def runners(self, /) -> list[RunnerNodeInfo]: ...
    @property
    def user(self, /) -> UserInfo |None: ...

@final
class OrganizationInfo:
    def __repr__(self, /) -> str: ...
    @property
    def default_parameter_secret_key(self, /) -> str |None: ...
    @property
    def default_parameter_secret_public_key(self, /) -> str |None: ...
    @property
    def id(self, /) -> str: ...
    @property
    def name(self, /) -> str: ...
    @property
    def slug(self, /) -> str: ...

@final
class RunnerNodeInfo:
    def __repr__(self, /) -> str: ...
    @property
    def hostname(self, /) -> str: ...

@final
class UserInfo:
    def __repr__(self, /) -> str: ...
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
