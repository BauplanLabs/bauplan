import bauplan
import typer
from typing import Annotated

TABLE_NAME = "workshop_average_fares"
PIPELINE_PATH = "pipelines/avg_fare"


def run_pipeline(client: bauplan.Client, branch: str) -> None:
    """Run the pipeline on `branch` and raise if the job does not succeed."""
    run_state = client.run(project_dir=PIPELINE_PATH, ref=branch)
    if str(run_state.job_status).lower() != "success":
        raise Exception(
            f"{run_state.job_id} failed: {run_state.job_status} - {run_state.error}."
        )


def main(
    profile: Annotated[str, typer.Option(help="Bauplan profile to use")] = "default",
) -> None:
    """
    Demonstrate a concurrency conflict when two users independently create the same table.

    The story, in order:
      1. User A and user B both open their own branch off main at roughly the same time,
         when `workshop_average_fares` does not yet exist.
      2. Each user runs the same pipeline on their own branch.
      3. User A merges first, publishing the table on main.
      4. User B then tries to merge, but Bauplan rejects the merge because main has
         moved on since user B branched off.
    """
    client = bauplan.Client(profile=profile)

    username = client.info().user.username

    # Use a "fake" main branch to avoid polluting main
    example_main_branch = f"{username}.main"
    client.delete_branch(branch=example_main_branch, if_exists=True)
    client.create_branch(
        branch=example_main_branch, from_ref="main", if_not_exists=True
    )

    # Reset the target table on the example main so the story always starts from a clean state
    client.delete_table(table=TABLE_NAME, branch=example_main_branch, if_exists=True)

    user_A_branch = f"{username}.user_A"
    user_B_branch = f"{username}.user_B"

    # Start from fresh branches so previous runs don't interfere
    for b in (user_A_branch, user_B_branch):
        client.delete_branch(branch=b, if_exists=True)

    # --- Step 1: both users open a branch off main, roughly at the same time ---
    # User A is about to start working on `workshop_average_fares`, which is not yet on main
    client.create_branch(branch=user_A_branch, from_ref=example_main_branch)
    print(f"User A opened branch {user_A_branch}")

    # Meanwhile, user B independently starts working on the same table
    client.create_branch(branch=user_B_branch, from_ref=example_main_branch)
    print(f"User B opened branch {user_B_branch}")

    # Nothing has been materialised yet, so the table does not exist anywhere
    assert not client.has_table(table=TABLE_NAME, ref=example_main_branch)
    assert not client.has_table(table=TABLE_NAME, ref=user_A_branch)
    assert not client.has_table(table=TABLE_NAME, ref=user_B_branch)

    # --- Step 2: user A runs the pipeline on his branch ---
    run_pipeline(client, user_A_branch)
    print(f"User A pipeline succeeded on {user_A_branch}")

    # The table now exists on user A's branch, but main is untouched: user A hasn't merged yet
    assert client.has_table(table=TABLE_NAME, ref=user_A_branch)
    assert not client.has_table(table=TABLE_NAME, ref=example_main_branch)

    # --- Step 3: user B runs the same pipeline on his own branch ---
    run_pipeline(client, user_B_branch)
    print(f"User B pipeline succeeded on {user_B_branch}")

    # Both users now have their own copy of the table; main is still unchanged because nobody has merged yet
    assert client.has_table(table=TABLE_NAME, ref=user_B_branch)
    assert not client.has_table(table=TABLE_NAME, ref=example_main_branch)

    # --- Step 4: user A merges first; main picks up the table ---
    client.merge_branch(source_ref=user_A_branch, into_branch=example_main_branch)
    print("User A merged into main")
    assert client.has_table(table=TABLE_NAME, ref=example_main_branch)

    # --- Step 5: user B tries to merge, but main has moved on ---
    # User B branched when the table did not exist on main. Since then user A has
    # published a table with that exact name, so main and user B's branch both independently
    # introduce `workshop_average_fares` from different histories. Bauplan detects
    # the divergence and rejects the merge rather than letting user B silently
    # overwrite user A's work.
    try:
        client.merge_branch(source_ref=user_B_branch, into_branch=example_main_branch)
        print("User B merged into main (unexpected)")
    except Exception as e:
        print(f"User B failed to merge into main, as expected. Error: {e}")

    # Cleanup afterwards
    for b in (user_A_branch, user_B_branch, example_main_branch):
        client.delete_branch(branch=b, if_exists=True)


if __name__ == "__main__":
    typer.run(main)
