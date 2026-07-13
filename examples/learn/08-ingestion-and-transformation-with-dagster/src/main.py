from datetime import datetime
from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import Annotated
from uuid import uuid4

import bauplan
import dagster as dg
import typer

from ingestion.import_data import import_data
from utils import wait_for_job

app = typer.Typer()


class SourceTable(str, Enum):
    transactions = "transactions"
    account_events = "account_events"


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


@dg.op(
    name="import_data",
    config_schema={
        "table": str,
        "year": str,
        "month": str,
        "day": str,
        "dt_partition_column": str,
        "base_branch": str,
        "namespace": str,
    },
)
def run_import_data(
    context: dg.OpExecutionContext, bauplan_client: BauplanResource
) -> dict:
    """DAG node responsible for importing new parquet files into
    an ingestion branch. This branch is separate from the base branch as quality
    of new data needs to be ascertained."""
    cfg = context.op_config
    result = import_data(
        bpln_client=bauplan_client.client,
        username=bauplan_client.username,
        table=cfg["table"],
        year=cfg["year"],
        month=cfg["month"],
        day=cfg["day"],
        dt_partition_column=cfg["dt_partition_column"],
        base_branch=cfg["base_branch"],
        namespace=cfg["namespace"],
    )
    # Carry results over to next step
    result["base_branch"] = cfg["base_branch"]
    result["table"] = cfg["table"]
    return result


@dg.op(name="wap")
def run_wap(imported: dict, bauplan_client: BauplanResource) -> dict:
    """DAG node responsible for running a WRITE-AUDIT-PUBLISH step.
    It runs expectations on imported data to ascertain that it meets
    requirements. Early exit in case of corrupted data."""
    client = bauplan_client.client
    project_dir = (
        Path(__file__).parent / "ingestion" / "pipelines" / "wap" / imported["table"]
    ).resolve()

    # Detach and poll
    state = client.run(
        project_dir=str(project_dir),
        ref=imported["branch"],
        namespace=imported["namespace"],
        cache="off",
        strict="on",
        detach=True,
    )
    wait_for_job(client, state.job_id, f"WAP audit on {imported['branch']}")

    # Pass-through so merge depends on wap and runs only after a clean audit
    return imported


@dg.op(name="merge")
def run_merge(audited: dict, bauplan_client: BauplanResource) -> None:
    """DAG node responsible for merging a branch into the main branch,
    thus making new data available to downstream consumers. Deletes the import
    branch afterwards."""
    bauplan_client.client.merge_branch(
        source_ref=audited["branch"],
        into_branch=audited["base_branch"],
    )

    bauplan_client.client.delete_branch(branch=audited["branch"])


@dg.op(
    name="transform",
    config_schema={
        "start_date": str,
        "end_date": str,
        "base_branch": str,
        "namespace": str,
    },
)
def run_transform(
    context: dg.OpExecutionContext, bauplan_client: BauplanResource
) -> dict:
    """DAG node responsible for running the transformation pipeline on a dev
    branch forked from the base branch, materializing the derived tables."""
    cfg = context.op_config
    client = bauplan_client.client

    branch = f"{bauplan_client.username}.workshop-transform-{str(uuid4())[:8]}"
    client.create_branch(branch=branch, from_ref=cfg["base_branch"])

    project_dir = (
        Path(__file__).parent / "transformation" / "pipelines" / "account_summary"
    ).resolve()

    # Detach and poll
    state = client.run(
        project_dir=str(project_dir),
        ref=branch,
        namespace=cfg["namespace"],
        cache="off",
        detach=True,
        parameters={"start_date": cfg["start_date"], "end_date": cfg["end_date"]},
    )
    wait_for_job(client, state.job_id, f"transform on {branch}")

    return {"branch": branch, "base_branch": cfg["base_branch"]}


@dg.job(name="ingestion")
def ingestion():
    """Dagster job to run the full ingestion pipeline."""
    imported = run_import_data()
    audited = run_wap(imported)
    run_merge(audited)


@dg.job(name="transformation")
def transformation():
    """Dagster job to run the transformation pipeline"""
    transformed = run_transform()
    run_merge(transformed)


defs = dg.Definitions(
    jobs=[ingestion, transformation],
    resources={"bauplan_client": BauplanResource(api_key=dg.EnvVar("BAUPLAN_API_KEY"))},
)


@app.command("ingest")
def ingestion_command(
    table: Annotated[SourceTable, typer.Argument(help="Source table to ingest")],
    date: Annotated[
        datetime,
        typer.Argument(formats=["%Y-%m-%d"], help="Partition date, YYYY-MM-DD"),
    ],
    dt_partition_column: Annotated[
        str, typer.Argument(help="Datetime column to use for daily partition")
    ],
    namespace: Annotated[str, typer.Option(help="Target namespace")] = "workshop",
    base_branch: Annotated[
        str, typer.Option(help="Branch to fork from and merge into")
    ] = "workshop.main",
) -> None:
    """Launch the ingestion WAP job in-process for one table partition."""
    run_config = {
        "ops": {
            "import_data": {
                "config": {
                    "table": table.value,
                    "year": str(date.year),
                    "month": f"{date.month:02d}",
                    "day": f"{date.day:02d}",
                    "dt_partition_column": dt_partition_column,
                    "base_branch": base_branch,
                    "namespace": namespace,
                }
            }
        }
    }
    result = ingestion.execute_in_process(
        run_config=run_config,
        resources={
            "bauplan_client": BauplanResource(api_key=dg.EnvVar("BAUPLAN_API_KEY"))
        },
    )
    if not result.success:
        raise typer.Exit(code=1)


@app.command("transform")
def transformation_command(
    start_date: Annotated[
        datetime,
        typer.Argument(formats=["%Y-%m-%d"], help="Start date, YYYY-MM-DD"),
    ],
    end_date: Annotated[
        datetime,
        typer.Argument(formats=["%Y-%m-%d"], help="End date, YYYY-MM-DD"),
    ],
    namespace: Annotated[
        str, typer.Option(help="Namespace of the source tables")
    ] = "workshop",
    base_branch: Annotated[
        str, typer.Option(help="Branch to fork from and merge into")
    ] = "workshop.main",
) -> None:
    """Run the transformation pipeline on a dev branch and merge it into the base branch."""

    run_config = {
        "ops": {
            "transform": {
                "config": {
                    "start_date": str(start_date.date()),
                    "end_date": str(end_date.date()),
                    "base_branch": base_branch,
                    "namespace": namespace,
                }
            }
        }
    }
    result = transformation.execute_in_process(
        run_config=run_config,
        resources={
            "bauplan_client": BauplanResource(api_key=dg.EnvVar("BAUPLAN_API_KEY"))
        },
    )
    if not result.success:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
