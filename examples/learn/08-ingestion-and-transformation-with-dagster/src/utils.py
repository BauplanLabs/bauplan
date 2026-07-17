import time
from typing import Any

import bauplan
import dagster as dg
from bauplan.schema import JobState

# Polling cadence and upper bound for detached bauplan jobs
POLL_INTERVAL_SECONDS = 2
POLL_TIMEOUT_SECONDS = 600

# Job states that mean the job has stopped running (JobState is unhashable, so a tuple)
TERMINAL_STATES = (JobState.COMPLETE, JobState.FAIL, JobState.ABORT)


def wait_for_job(client: bauplan.Client, job_id: str | None, label: str) -> None:
    """Poll a detached bauplan job until it stops, raising unless it completed cleanly"""
    if job_id is None:
        raise RuntimeError(f"{label} returned no job id; cannot poll it")

    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    while True:
        job = client.get_job(job_id)
        if job.status in TERMINAL_STATES:
            break
        if time.monotonic() > deadline:
            client.cancel_job(job_id)
            raise TimeoutError(
                f"{label} job {job_id} still {job.human_readable_status} after {POLL_TIMEOUT_SECONDS}s"
            )
        time.sleep(POLL_INTERVAL_SECONDS)

    if job.status != JobState.COMPLETE:
        raise RuntimeError(
            f"{label} job {job_id} failed: {job.human_readable_status} - {job.error_message}"
        )


def get_table_metadata(
    client: bauplan.Client,
    table: str,
    namespace: str,
    ref: str,
    partition_predicate: str,
    extra: dict[str, Any],
) -> dict[str, Any]:
    """Assemble the Dagster metadata for a materialized table: total and
    partition-scoped row counts, the column schema, plus caller-provided entries.
    The dagster/ keys are standard ones that get dedicated rendering in the UI"""
    partition_rows = (
        client.query(
            f"SELECT COUNT(*) AS n FROM {table} WHERE {partition_predicate}",
            ref=ref,
            namespace=namespace,
        )
        .column("n")
        .to_pylist()[0]
    )
    table_info = client.get_table(table=table, ref=ref, namespace=namespace)
    return {
        "dagster/row_count": int(table_info.records or 0),
        "dagster/partition_row_count": int(partition_rows or 0),
        "dagster/column_schema": dg.TableSchema(
            columns=[
                dg.TableColumn(name=field.name, type=field.type)
                for field in table_info.fields
            ]
        ),
        **extra,
    }
