import bauplan
import typer
from typing import Annotated

TABLE_NAME = "workshop_fare_table"


def run_pipeline(
    client: bauplan.Client, branch: str, pipeline_path: str, parameters: dict = None
) -> None:
    """Run a Bauplan pipeline on `branch` and raise if the job does not succeed."""
    run_state = client.run(project_dir=pipeline_path, ref=branch, parameters=parameters)

    if str(run_state.job_status).lower() != "success":
        raise Exception(
            f"{run_state.job_id} failed: {run_state.job_status} - {run_state.error}."
        )


def print_avg_fare_by_year(client: bauplan.Client, ref: str) -> None:
    """
    Print the average fare per year on `ref` in ascending year order.
    """
    result = client.query(
        f"SELECT fare_calculated_in, ROUND(AVG(Fare), 4) AS avg_fare "
        f"FROM {TABLE_NAME} "
        f"GROUP BY fare_calculated_in "
        f"ORDER BY fare_calculated_in",
        ref=ref,
    )

    years = result.column("fare_calculated_in").to_pylist()
    fares = result.column("avg_fare").to_pylist()
    print("  avg fare by year:")
    for year, fare in zip(years, fares):
        print(f"    {year}: ${fare:.4f}")


def fix_and_backfill(
    client: bauplan.Client,
    init_ref: str,
    main_branch: str,
    fix_branch: str,
) -> None:
    """
    Revert the fare table to its pre-inflation snapshot on an isolated `fix_branch`,
    then re-run the correct pipeline for 1913 and 1914.

    This is the remediation step once the bug is discovered: the corrupted history is
    rolled back and rebuilt year-by-year with the fixed formula.
    """
    client.delete_branch(branch=fix_branch, if_exists=True)
    client.create_branch(branch=fix_branch, from_ref=main_branch, if_not_exists=True)

    client.revert_table(
        table=TABLE_NAME,
        source_ref=init_ref,
        into_branch=fix_branch,
        replace=True,
    )
    print(f"  [fix] Table reverted to initial snapshot ({init_ref})")

    print("  [fix] Backfilling 1913-1914 with correct formula...")
    for year in [1913, 1914]:
        run_pipeline(
            client=client,
            branch=fix_branch,
            pipeline_path="pipelines/update_table_fixed",
            parameters={"year": year},
        )
        print(f"  [fix] Backfilled {year}")

def main(
    profile: Annotated[str, typer.Option(help="Bauplan profile to use")] = "default",
) -> None:
    """
    Demonstrate how to fix and backfill a corrupted table.

    The scenario is a scheduled job that appends inflation-adjusted fares year-by-year
    to a shared table. The update pipeline carries a bug (a missing '+ 1.0' in the
    exponentiation) that causes fares to collapse toward zero. When the bug is found,
    we must repair the historical rows.

      1. Build the initial fare table at the 1912 baseline.
      2. The scheduler runs: it appends 1913-1914 data with the buggy formula. Fares
         collapse instead of growing.
      3. On 1914, the bug is spotted. Kick off a fix-and-backfill on an isolated branch:
         revert to the 1912 snapshot, replay every past year with the correct formula,
         then merge the corrected table back into the main.

    """
    client = bauplan.Client(profile=profile)

    username = client.info().user.username

    # Set up branches
    main_branch = f"{username}.reprocessing_race_main"
    fix_branch = f"{username}.reprocessing_race_fix"

    # Start from fresh branches so previous runs don't interfere
    for b in (main_branch, fix_branch):
        client.delete_branch(branch=b, if_exists=True)

    # Start by creating a (fake) main branch
    client.create_branch(branch=main_branch, from_ref="main", if_not_exists=True)
    print(f"Working on branch: {main_branch}")

    # --- Step 1: build the initial fare table at the 1912 baseline ---
    print(f"\n=== Step 1: initialise '{TABLE_NAME}' at the 1912 baseline ===\n")
    run_pipeline(
        client=client, branch=main_branch, pipeline_path="pipelines/setup_table"
    )

    # Snapshot the current hash so the fix can revert the table to exactly this state,
    # regardless of any commits that land on main_branch in the meantime.
    init_hash = client.get_branch(main_branch).hash
    init_ref = f"{main_branch}@{init_hash}"
    print(f"Initial state available at: {init_ref}")

    # --- Step 2: append years using the wrong formula ---
    print("\n=== Step 2: append 1913-1914 with the buggy formula ===\n")

    for year in [1913, 1914]:
        # Append inflation-adjusted fares using the wrong formula
        run_pipeline(
            client=client,
            branch=main_branch,
            pipeline_path="pipelines/update_table_wrong",
            parameters={"year": year},
        )
        print(f"[{year}] Appended with wrong inflation formula.")
        print_avg_fare_by_year(client=client, ref=main_branch)

    # --- Step 3: bug spotted; fix-and-backfill ---
    print(
        "\n[!!!] Bug discovered: inflation formula is missing "
        "'+ 1.0', fares are collapsing instead of growing."
    )

    print("\n[fix] Starting fix and backfill in background...")

    fix_and_backfill(
        client=client,
        init_ref=init_ref,
        main_branch=main_branch,
        fix_branch=fix_branch,
    )
    client.merge_branch(source_ref=fix_branch, into_branch=main_branch)
    print("[fix] Fixed table merged back into main branch.\n")

    print(f"\n=== Final state of '{TABLE_NAME}' on {main_branch} ===\n")
    print_avg_fare_by_year(client=client, ref=main_branch)

    # Cleanup afterwards
    for b in (main_branch, fix_branch):
        client.delete_branch(branch=b, if_exists=True)


if __name__ == "__main__":
    typer.run(main)
