import bauplan
import typer
from typing import Annotated

TABLE_NAME = "workshop_average_fares"


def print_schema(client: bauplan.Client, ref: str) -> None:
    """Print the schema of the target table at `ref`."""
    for field in client.get_table(table=TABLE_NAME, ref=ref).fields:
        print(f"    {field.name:<20} {field.type}")


def main(
    profile: Annotated[str, typer.Option(help="Bauplan profile to use")] = "default",
) -> None:
    """
    Example that demonstrates how Bauplan expectations and type contracts catch, and
    prevent, a subtle schema drift when an old data pipeline is extended to address a
    new need. An output schema is defined to guarantee the shape of a model's outputs
    and an expectation is defined to catch data violations.

    Both pipelines run sequentially on the same branch with strict mode enabled.
    In order:
      1. The old pipeline runs on the branch. It computes the mean Titanic fare per
         passenger class, over standard fares only, and publishes `Fare` as a
         Decimal128[4, 2] so the money is exact rather than binary floating point.
         The output schema specifies the decimal data type for the `Fare` column and
         the expectation checks that the fares being averaged fit that precision.
         The run succeeds.
      2. The business asks for a broader, finer-grained view: cover every passenger
         class, split by `Sex`, and include the number of passengers. A new pipeline
         is built for that.
      3. The pipeline is modified and the filter on `Pclass` is dropped, so first
         class fares now enter the analysis. Those reach 512.33, and the mean fare
         for first class women is 106.13, so neither fits a Decimal128[4, 2].
      4. The expectation fires on the widened input and fails the run, so no drift
         happens on the branch. The fix is a deliberate one: widen the column to
         Decimal128[5, 2] in the output schema and raise the bound the expectation
         checks. Here the schema and the expectation each do a job the other cannot,
         since the declared type is what the expectation validates the data against.
    """
    client = bauplan.Client(profile=profile)

    username = client.info().user.username
    branch = f"{username}.schema_conflict"

    # Start from fresh branch so previous runs don't interfere
    client.delete_branch(branch=branch, if_exists=True)
    client.create_branch(branch=branch, from_ref="main")

    # --- Step 1: ship the old pipeline ---
    print(
        "\n=== Step 1: ship the old pipeline "
        "(mean standard fare per Pclass, Fare: Decimal128[4, 2]) ===\n"
    )

    # Setting strict="on" tells Bauplan to run the expectations associated with the DAG,
    # which prevents tables that fail validation from being merged.
    run_state = client.run(
        project_dir="pipelines/old_pipeline", ref=branch, strict="on"
    )

    if str(run_state.job_status).lower() != "success":
        raise Exception(
            f"Old pipeline failed unexpectedly: {run_state.job_status} - {run_state.error}"
        )
    print("Old pipeline succeeded; expectation passed (fares fit Decimal128[4, 2])")
    print(f"{TABLE_NAME} is now published")
    assert client.has_table(table=TABLE_NAME, ref=branch)

    print(f"\nSchema of {TABLE_NAME}:")
    print_schema(client, ref=branch)

    # --- Step 2: run the new pipeline; the expectation catches the precision drift ---
    # Dropping the filter on Pclass pulls first class fares into the analysis, which
    # reach 512.33 and average 106.13 for first class women. The pipeline still declares
    # Fare as a Decimal128[4, 2] and still carries the expectation that fares must fit
    # that precision, so the expectation fails the run.
    print(
        "\n=== Step 2: build the new pipeline (all classes, adds Sex, n_passengers) ===\n"
    )

    print("Running the new pipeline...")
    run_state = client.run(
        project_dir="pipelines/new_pipeline", ref=branch, strict="on"
    )

    if str(run_state.job_status).lower() == "success":
        # Should not happen given the drift above; keep the guard so a regression is loud.
        print("New pipeline succeeded (unexpected: the drift slipped through)")
    else:
        # Expected outcome: the expectation halted the run before anyone could merge.
        print(
            f"New pipeline run blocked by the expectation, as expected. "
            f"Status: {run_state.job_status}. "
            f"Reason: {run_state.error}."
        )

    # --- Step 3: because the run failed, we never attempt a merge; main is intact ---
    # Conditioning the merge on run success is what turns the failed expectation into
    # a blocked merge. No try/except around merge_branch is needed because we simply
    # never call it when the run does not succeed.
    print(f"\nSchema of {TABLE_NAME} after the new pipeline was blocked:")
    print_schema(client, ref=branch)
    print()

    # Cleanup afterwards
    client.delete_branch(branch=branch, if_exists=True)


if __name__ == "__main__":
    typer.run(main)
