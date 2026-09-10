import os
from collections.abc import Iterator
from datetime import datetime
from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import Annotated
from uuid import uuid4

import bauplan
import dagster as dg
import typer
from bauplan.exceptions import BauplanError

from ingestion.import_data import import_data
from utils import get_table_metadata

app = typer.Typer()


class SourceTable(str, Enum):
    transactions = "transactions"
    account_events = "account_events"


# Lakehouse coordinates, overridable via the environment to point the example
# at a different namespace or base branch
NAMESPACE = os.environ.get("NAMESPACE", "workshop")
BASE_BRANCH = os.environ.get("BASE_BRANCH", "workshop.main")

AUDIT_DIR = Path(__file__).parent / "ingestion" / "pipelines" / "audit"
TRANSFORM_DIR = (
    Path(__file__).parent / "transformation" / "pipelines" / "account_summary"
)

# One Dagster partition per hive-style day prefix on S3; the sample data covers
# 2026-06-15 through 2026-07-14, so end_date (exclusive) is the day after the
# last populated prefix. Regenerating the data means moving these dates too
daily_partitions = dg.DailyPartitionsDefinition(
    start_date="2026-06-15", end_date="2026-07-15"
)


class BauplanResource(dg.ConfigurableResource):
    """Allocate a Dagster resource for the Bauplan client to
    be reused across invocations of Bauplan commands. Requires a valid
    Bauplan API key to work.

    Username is returned as it determines the branch prefix."""

    api_key: str

    @cached_property
    def client(self) -> bauplan.Client:
        return bauplan.Client(api_key=self.api_key)

    @cached_property
    def username(self) -> str:
        user = self.client.info().user
        if user is None:
            raise ValueError("Bauplan client returned no user info; check API key")
        return user.username


def build_ingestion_asset(table: str, dt_partition_column: str) -> dg.AssetsDefinition:
    """Build the daily-partitioned asset that ingests one source table through a
    write-audit-publish cycle: import the partition on an ingestion branch, audit
    it with the table's Bauplan expectations, and merge into the base branch only
    on a clean audit. The audit outcome surfaces as the audit_expectations check."""

    @dg.asset(
        name=table,
        partitions_def=daily_partitions,
        kinds={"bauplan"},
        check_specs=[
            dg.AssetCheckSpec(
                name="audit_expectations",
                asset=table,
                description=f"Bauplan expectations for {table}, audited on the ingestion branch before publishing",
            )
        ],
        description=f"Daily partition of {table} imported from S3 with write-audit-publish",
    )
    def ingest(
        context: dg.AssetExecutionContext, bauplan_client: BauplanResource
    ) -> Iterator[dg.AssetCheckResult | dg.MaterializeResult]:
        """Import one daily partition, audit it, publish it on success."""
        client = bauplan_client.client
        day = datetime.strptime(context.partition_key, "%Y-%m-%d")

        # Write: import data in a dedicated branch
        imported = import_data(
            bpln_client=client,
            username=bauplan_client.username,
            table=table,
            year=f"{day:%Y}",
            month=f"{day:%m}",
            day=f"{day:%d}",
            dt_partition_column=dt_partition_column,
            base_branch=BASE_BRANCH,
            namespace=NAMESPACE,
        )
        branch = imported["branch"]

        # Audit: a synchronous run so the returned state carries status and duration
        state = client.run(
            project_dir=str((AUDIT_DIR / table).resolve()),
            ref=branch,
            namespace=NAMESPACE,
            cache="off",
            strict="on",
        )
        passed = str(state.job_status).lower() == "success"
        check_metadata = {
            "bauplan_job_id": str(state.job_id),
            "bauplan_branch": branch,
            "duration_seconds": round(state.duration or 0.0, 2),
            "error": str(state.error or ""),
        }
        yield dg.AssetCheckResult(
            check_name="audit_expectations", passed=passed, metadata=check_metadata
        )
        if not passed:
            raise dg.Failure(
                description=f"Audit failed on branch {branch} (branch kept for debugging)",
                metadata=check_metadata,
            )

        # Publish: merge only after a clean audit, then drop the ingestion branch
        try:
            client.merge_branch(source_ref=branch, into_branch=BASE_BRANCH)
        except BauplanError as merge_error:
            raise dg.Failure(
                description=f"Merge of branch {branch} into {BASE_BRANCH} failed; branch kept for debugging",
                metadata={"bauplan_branch": branch},
            ) from merge_error

        try:
            client.delete_branch(branch=branch)
        except BauplanError as delete_error:
            context.log.warning(
                f"Deletion of branch {branch} into {BASE_BRANCH} failed"
            )

        window = context.partition_time_window
        yield dg.MaterializeResult(
            metadata=get_table_metadata(
                client=client,
                table=table,
                namespace=NAMESPACE,
                ref=BASE_BRANCH,
                partition_predicate=(
                    f"{dt_partition_column} >= TIMESTAMP '{window.start:%Y-%m-%d} 00:00:00' "
                    f"AND {dt_partition_column} < TIMESTAMP '{window.end:%Y-%m-%d} 00:00:00'"
                ),
                extra={
                    "bauplan_branch": branch,
                    "bauplan_import_job_id": str(imported["job_id"]),
                    "dagster/uri": imported["uri"],
                },
            )
        )

    return ingest


transactions = build_ingestion_asset("transactions", "txn_ts")
account_events = build_ingestion_asset("account_events", "event_ts")


@dg.asset(
    name="account_activity_summary",
    partitions_def=daily_partitions,
    deps=[transactions, account_events],
    kinds={"bauplan"},
    description="Daily per-account summary of settled spend joined with event and login counts",
)
def account_activity_summary(
    context: dg.AssetExecutionContext, bauplan_client: BauplanResource
) -> dg.MaterializeResult:
    """Run the account_summary pipeline for one daily partition on a dev branch
    forked from the base branch, then publish the result by merging it back."""
    client = bauplan_client.client
    window = context.partition_time_window
    start_date = f"{window.start:%Y-%m-%d}"
    end_date = f"{window.end:%Y-%m-%d}"

    branch = f"{bauplan_client.username}.workshop-transform-{context.partition_key}-{str(uuid4())[:8]}"
    client.create_branch(branch=branch, from_ref=BASE_BRANCH)

    state = client.run(
        project_dir=str(TRANSFORM_DIR.resolve()),
        ref=branch,
        namespace=NAMESPACE,
        cache="off",
        strict="on",
        parameters={"start_date": start_date, "end_date": end_date},
    )
    if str(state.job_status).lower() != "success":
        raise dg.Failure(
            description=f"transform on {branch} failed; branch kept for debugging",
            metadata={
                "bauplan_branch": branch,
                "bauplan_job_id": str(state.job_id),
                "error": str(state.error or ""),
            },
        )

    try:
        client.merge_branch(source_ref=branch, into_branch=BASE_BRANCH)
        client.delete_branch(branch=branch)
    except BauplanError as merge_error:
        raise dg.Failure(
            description=f"merge of {branch} into {BASE_BRANCH} failed; branch kept for debugging",
            metadata={"bauplan_branch": branch},
        ) from merge_error

    return dg.MaterializeResult(
        metadata=get_table_metadata(
            client=client,
            table="account_activity_summary",
            namespace=NAMESPACE,
            ref=BASE_BRANCH,
            partition_predicate=(
                f"date >= DATE '{start_date}' AND date < DATE '{end_date}'"
            ),
            extra={
                "bauplan_branch": branch,
                "bauplan_job_id": str(state.job_id),
                "bauplan_run_duration_seconds": round(state.duration or 0.0, 2),
            },
        )
    )


@dg.asset_check(
    asset=transactions,
    description="Auditing pipeline for 'transactions' table. Replicates the one run at ingestion time.",
)
def transactions_audit(bauplan_client: BauplanResource) -> dg.AssetCheckResult:
    """Post-publish check: run expectations."""
    state = bauplan_client.client.run(
        project_dir=str((AUDIT_DIR / "transactions").resolve()),
        ref=BASE_BRANCH,
        namespace=NAMESPACE,
        dry_run=True,
        cache="off",
        strict="on",
    )

    successful = str(state.job_status).lower() == "success"

    return dg.AssetCheckResult(
        passed=successful,
        metadata={"error": str(state.error or ""), "ref": BASE_BRANCH},
    )


@dg.asset_check(
    asset=transactions,
    description="No duplicate txn_id may survive on the published branch",
)
def transactions_txn_id_unique(bauplan_client: BauplanResource) -> dg.AssetCheckResult:
    """Post-publish check: count duplicate transaction ids on the base branch."""
    duplicates = (
        bauplan_client.client.query(
            "SELECT COUNT(*) - COUNT(DISTINCT txn_id) AS n FROM transactions",
            ref=BASE_BRANCH,
            namespace=NAMESPACE,
        )
        .column("n")
        .to_pylist()[0]
    )
    return dg.AssetCheckResult(
        passed=duplicates == 0,
        metadata={"duplicate_txn_ids": int(duplicates or 0), "ref": BASE_BRANCH},
    )


@dg.asset_check(
    asset=account_events,
    description="account_id must be fully populated on the published branch",
)
def account_events_account_id_no_nulls(
    bauplan_client: BauplanResource,
) -> dg.AssetCheckResult:
    """Post-publish check: count events without an account id on the base branch."""
    nulls = (
        bauplan_client.client.query(
            "SELECT COUNT(*) AS n FROM account_events WHERE account_id IS NULL",
            ref=BASE_BRANCH,
            namespace=NAMESPACE,
        )
        .column("n")
        .to_pylist()[0]
    )
    return dg.AssetCheckResult(
        passed=nulls == 0,
        metadata={"null_account_ids": int(nulls or 0), "ref": BASE_BRANCH},
    )


@dg.asset_check(
    asset=account_events,
    description="Auditing pipeline for 'account_events' table. Replicates the one run at ingestion time.",
)
def account_events_audit(bauplan_client: BauplanResource) -> dg.AssetCheckResult:
    """Post-publish check: run expectations."""
    state = bauplan_client.client.run(
        project_dir=str((AUDIT_DIR / "account_events").resolve()),
        ref=BASE_BRANCH,
        namespace=NAMESPACE,
        dry_run=True,
        cache="off",
        strict="on",
    )

    successful = str(state.job_status).lower() == "success"

    return dg.AssetCheckResult(
        passed=successful,
        metadata={"error": str(state.error or ""), "ref": BASE_BRANCH},
    )


ingestion = dg.define_asset_job(
    name="ingestion",
    selection=dg.AssetSelection.assets("transactions", "account_events"),
)

transformation = dg.define_asset_job(
    name="transformation",
    selection=dg.AssetSelection.assets("account_activity_summary"),
)

RESOURCES = {"bauplan_client": BauplanResource(api_key=dg.EnvVar("BAUPLAN_API_KEY"))}

defs = dg.Definitions(
    assets=[transactions, account_events, account_activity_summary],
    asset_checks=[
        transactions_txn_id_unique,
        transactions_audit,
        account_events_account_id_no_nulls,
        account_events_audit,
    ],
    jobs=[ingestion, transformation],
    resources=RESOURCES,
)

ALL_DEFINITIONS = [
    transactions,
    account_events,
    account_activity_summary,
    transactions_txn_id_unique,
    transactions_audit,
    account_events_account_id_no_nulls,
    account_events_audit,
]


@app.command("ingest")
def ingestion_command(
    table: Annotated[SourceTable, typer.Argument(help="Source table to ingest")],
    date: Annotated[
        datetime,
        typer.Argument(formats=["%Y-%m-%d"], help="Partition date, YYYY-MM-DD"),
    ],
) -> None:
    """Materialize one daily partition of a source table, running its WAP audit
    and post-publish checks."""
    result = dg.materialize(
        assets=ALL_DEFINITIONS,
        selection=dg.AssetSelection.assets(table.value)
        | dg.AssetSelection.checks_for_assets(table.value),
        partition_key=date.strftime("%Y-%m-%d"),
        resources=RESOURCES,
        raise_on_error=False,
    )
    if not result.success:
        raise typer.Exit(code=1)


@app.command("transform")
def transformation_command(
    date: Annotated[
        datetime,
        typer.Argument(formats=["%Y-%m-%d"], help="Partition date, YYYY-MM-DD"),
    ],
) -> None:
    """Materialize one daily partition of the account activity summary; use
    Dagster backfills to cover date ranges."""
    result = dg.materialize(
        assets=ALL_DEFINITIONS,
        selection=dg.AssetSelection.assets("account_activity_summary"),
        partition_key=date.strftime("%Y-%m-%d"),
        resources=RESOURCES,
        raise_on_error=False,
    )
    if not result.success:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
