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
    Demonstrate how a Bauplan expectation catches a subtle schema drift between two
    versions of the same pipeline and prevents the drift.

    Both pipelines run sequentially on the same branch with strict mode enabled.
    In order:
      1. The old pipeline runs on the branch. It computes the mean Titanic fare per
         passenger class, with `Fare` cast to Float32. An expectation that ships with
         the pipeline asserts `Fare` must be Float32: downstream dashboards rely on
         that type. The run succeeds.
      2. The business asks for a finer-grained view: group by `Pclass` AND `Sex`, and
         include the number of passengers. A new pipeline is built for that.
      3. A subtle change slips in while rewriting: `Fare` is cast with Python's `float`
         (Float64 in polars) instead of `pl.Float32`.
      4. The new pipeline runs on the same branch. The expectation fires on the new
         output, the run fails, and no drift happens on the branch.
    """
    client = bauplan.Client(profile=profile)

    username = client.info().user.username
    branch = f"{username}.schema_conflict"

    # Start from fresh branch so previous runs don't interfere
    client.delete_branch(branch=branch, if_exists=True)
    client.create_branch(branch=branch, from_ref="main")

    # --- Step 1: ship the old pipeline ---
    print(
        "\n=== Step 1: ship the old pipeline (mean fare per Pclass, Fare: Float32) ===\n"
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
    print("Old pipeline succeeded; expectation passed (Fare is Float32)")
    print(f"{TABLE_NAME} is now published")
    assert client.has_table(table=TABLE_NAME, ref=branch)

    print(f"\nSchema of {TABLE_NAME}:")
    print_schema(client, ref=branch)

    # --- Step 2: run the new pipeline; the expectation catches the type drift ---
    # `cast(float)` inside the new pipeline promotes Fare to Float64 (polars' default
    # for Python's `float`). The pipeline still carries the expectation that Fare must
    # be Float32, so it fires after the model materializes and fails the run.
    print(
        "\n=== Step 2: build the new pipeline (adds Sex, n_passengers; subtle Fare drift) ===\n"
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
