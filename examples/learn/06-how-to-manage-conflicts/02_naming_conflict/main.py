import bauplan
import typer
from typing import Annotated

TABLE_NAME = "workshop_average_fares"


def run_pipeline(
    client: bauplan.Client,
    username: str,
    main_branch: str,
    branch: str,
    pipeline_path: str,
    namespace: str | None = None,
) -> str:
    """
    Open a fresh workspace for one user and run their pipeline on it.

    Concretely: delete any leftover branch from a previous run, create a new branch off the
    current state of `main`, optionally create the target namespace on that branch, then run
    the pipeline. Raises if the job does not succeed. Returns the branch name so the caller
    can merge it into `main` when the story calls for it.
    """
    branch_name = f"{username}.{branch}"

    # Always start from a clean branch so previous runs don't interfere
    client.delete_branch(branch=branch_name, if_exists=True)
    client.create_branch(branch=branch_name, from_ref=main_branch, if_not_exists=True)
    print(f"  -> opened branch {branch_name} off main")

    # Create the user's namespace on the branch (only in the namespaced scenario)
    if namespace:
        client.create_namespace(
            namespace=namespace, branch=branch_name, if_not_exists=True
        )
        print(f"  ->  using namespace {namespace} on {branch_name}")

    run_state = client.run(
        project_dir=pipeline_path, ref=branch_name, namespace=namespace
    )

    if str(run_state.job_status).lower() != "success":
        raise Exception(
            f"{run_state.job_id} failed: {run_state.job_status} - {run_state.error}."
        )
    else:
        print(f"  -> pipeline succeeded on {branch_name}")

    return branch_name


def run_scenario(
    client: bauplan.Client,
    username: str,
    main_branch: str,
    user_A_namespace: str | None,
    user_B_namespace: str | None,
):
    """
    Play out the naming-conflict story end-to-end, either without or with namespace isolation.

    The two users act sequentially, not concurrently:
      1. User A opens a fresh branch off main, runs their pipeline, and merges into main.
         `workshop_average_fares` is now on main (with user A's schema).
      2. User B then opens their own fresh branch off main (which now already contains
         user A's table) runs their pipeline, and merges.

    Without namespaces, both users write to the same fully qualified name, so step 2 silently
    overwrites user A's table on main. With namespaces, each user writes into their own
    namespace, so both tables coexist on main under different fully qualified names.
    """

    # --- User A goes first: branch off main, run pipeline, merge back ---
    # User A produces average Titanic fares grouped by passenger class.
    print("User A starts working...")
    user_A_branch = run_pipeline(
        client=client,
        username=username,
        main_branch=main_branch,
        branch="user_A",
        pipeline_path="pipelines/user_A_pipeline",
        namespace=user_A_namespace,
    )

    client.merge_branch(source_ref=user_A_branch, into_branch=main_branch)
    print(f"  -> user A merged into main; {TABLE_NAME} is now on main")

    # Inspect what is now on main after user A's merge
    print()
    print(
        f"Schema of {TABLE_NAME} on main after user A's merge (namespace: {user_A_namespace or 'bauplan'}):"
    )
    for fields in client.get_table(
        table=TABLE_NAME, ref=main_branch, namespace=user_A_namespace
    ).fields:
        print(f"    {fields.name:<30} {fields.type}")
    print()

    # --- User B goes second: branch off main (which now already contains user A's table), run, merge back ---
    # User B produces average NYC taxi fares grouped by license number.
    print(f"User B starts working (main already contains user A's {TABLE_NAME})...")
    user_B_branch = run_pipeline(
        client=client,
        username=username,
        main_branch=main_branch,
        branch="user_B",
        pipeline_path="pipelines/user_B_pipeline",
        namespace=user_B_namespace,
    )

    client.merge_branch(source_ref=user_B_branch, into_branch=main_branch)
    if user_B_namespace:
        print(
            "  -> user B merged into main; both tables now coexist on main under separate namespaces"
        )
    else:
        print(
            f"  -> user B merged into main; user A's {TABLE_NAME} has been silently overwritten"
        )

    # Inspect what is on main after user B's merge:
    #   - without namespaces: user A's table has been silently overwritten by user B's
    #   - with namespaces: user A's and user B's tables coexist under different namespaces
    print()
    print(
        f"Schema of {TABLE_NAME} on main after user B's merge (namespace: {user_B_namespace or 'bauplan'}):"
    )
    for fields in client.get_table(
        table=TABLE_NAME, ref=main_branch, namespace=user_B_namespace
    ).fields:
        print(f"    {fields.name:<30} {fields.type}")
    print()


def main(
    profile: Annotated[str, typer.Option(help="Bauplan profile to use")] = "default",
) -> None:
    """
    Demonstrate the naming conflict problem and one way to resolve it with namespaces.

    The script plays two scenarios back to back, each one a full story where user A merges
    first and user B merges second on top of user A's result:

      - Scenario #1 (no namespace): both users write 'workshop_average_fares' in the default
        namespace. Both merges succeed, but user B silently overwrites user A's table on main;
        the schema and data from user A are lost.
      - Scenario #2 (user namespaces): each user writes to their own namespace, so both tables
        coexist on main under 'user_A_namespace' and 'user_B_namespace' respectively.

    The table is wiped from main before the scenarios run so the demo always starts from a
    clean state.
    """
    client = bauplan.Client(profile=profile)

    username = client.info().user.username

    # Use a "fake" main branch to avoid polluting main
    example_main_branch = f"{username}.main"
    client.delete_branch(branch=example_main_branch, if_exists=True)
    client.create_branch(
        branch=example_main_branch, from_ref="main", if_not_exists=True
    )

    # Reset the target table on main so the conflict is reproducible across runs
    client.delete_table(table=TABLE_NAME, branch=example_main_branch, if_exists=True)

    print(
        "\n=== Scenario #1: no namespace; user B's merge silently overwrites user A's table ===\n"
    )

    # In scenario #1, these will be set to None and Bauplan will resort to the default namespace, i.e. 'bauplan'
    run_scenario(
        client=client,
        username=username,
        main_branch=example_main_branch,
        user_A_namespace=None,
        user_B_namespace=None,
    )

    print("\n=== Scenario #2: per-user namespaces; both tables coexist on main ===\n")

    # In scenario #2, the namespaces are instead specified, avoiding naming conflict
    run_scenario(
        client=client,
        username=username,
        main_branch=example_main_branch,
        user_A_namespace="user_A_namespace",
        user_B_namespace="user_B_namespace",
    )


if __name__ == "__main__":
    typer.run(main)
