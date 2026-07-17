import os
from uuid import uuid4

import bauplan

from utils import wait_for_job


def import_data(
    bpln_client: bauplan.Client,
    username: str,
    table: str,
    year: str,
    month: str,
    day: str,
    dt_partition_column: str,
    base_branch: str,
    namespace: str,
) -> dict:
    """Importing data into Bauplan is a two-step procedure:
    1. First, the target table is created;
    2. Second, data is imported.
    If the table already exists, it is not necessary to recreate it and we simply import data.
    Notice that Bauplan imports are stateful, so already imported files will not be re-added.
    """

    # Fetch variables from environment
    BUCKET = os.environ["BUCKET_NAME"]
    PREFIX = os.environ["PREFIX"]

    # Format search URI
    uri = (
        f"s3://{BUCKET}/{PREFIX}/{table}/year={year}/month={month}/day={day}/*.parquet"
    )

    # Create (and truncate for simplicity) a uuid v4 to disambiguate branches
    id = str(uuid4())[:8]

    # Format ingestion branch name
    ingestion_branch = (
        f"{username}.workshop-ingestion-{table}-{year}-{month}-{day}-{id}"
    )

    # Create dedicated branch for data ingestion and namespace
    bpln_client.create_branch(branch=ingestion_branch, from_ref=base_branch)
    bpln_client.create_namespace(
        namespace=namespace, branch=ingestion_branch, if_not_exists=True
    )

    # If table is not there, we create it first
    if not bpln_client.has_table(
        table=table, namespace=namespace, ref=ingestion_branch
    ):
        bpln_client.create_table(
            table=table,
            namespace=namespace,
            search_uri=uri,
            branch=ingestion_branch,
            partitioned_by=f"day({dt_partition_column})",
        )

    # Import detached and poll to completion so the branch is ready before the audit
    state = bpln_client.import_data(
        table=table,
        namespace=namespace,
        search_uri=uri,
        branch=ingestion_branch,
        detach=True,
    )
    wait_for_job(
        bpln_client, state.job_id, f"import of {table} into {ingestion_branch}"
    )

    return {
        "branch": ingestion_branch,
        "namespace": namespace,
        "uri": uri,
        "job_id": state.job_id,
    }
