import bauplan
from datetime import datetime, timezone
from typing import Annotated
from collections import Counter

import re
import typer


def prune_branches(
    profile: Annotated[str, typer.Option(help="Bauplan profile to use")] = "default",
    jobs_failed_after: Annotated[
        datetime | None,
        typer.Option(
            help="Only consider jobs that failed after this date (UTC). Leave empty to apply no lower bound"
        ),
    ] = None,
    jobs_failed_before: Annotated[
        datetime | None,
        typer.Option(
            help="Only consider jobs that failed before this date (UTC). Leave empty to apply no upper bound"
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(help="Print branches that would be pruned without deleting them"),
    ] = True,
) -> None:
    """
    Prune dead transactional branches to clean up the lakehouse.

    Bauplan creates a transactional branch for every command that alters lake state. These branches
    remain open when job execution fails. This script identifies branches whose job has failed and
    marks them for deletion, leaving branches with running jobs untouched.

    An optional date range (--jobs-failed-after / --jobs-failed-before) narrows which failed jobs
    are considered. Runs in dry-run mode by default; pass --no-dry-run to actually delete branches.
    """
    client = bauplan.Client(profile=profile)

    # Enforce UTC timezone on date filters
    if jobs_failed_after:
        jobs_failed_after = jobs_failed_after.replace(tzinfo=timezone.utc)
    if jobs_failed_before:
        jobs_failed_before = jobs_failed_before.replace(tzinfo=timezone.utc)

    if jobs_failed_after and jobs_failed_before:
        if jobs_failed_before < jobs_failed_after:
            raise ValueError("jobs_failed_before must be later than jobs_failed_after")

    # Transactional branches have a distinct pattern following <user>.<branch_name>-bpln-tx-<command>-<timestamp>-<job_id>
    pattern_for_transactional = re.compile(
        r"^(?P<user>[^.]+)\.(?P<branch>[^-]+)-bpln-tx-(?P<command>.+)-(?P<timestamp>\d{14})-(?P<job_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$"
    )

    transactional_branches_data = []

    # Transactional branches contain 'bpln-tx' in their name; pre-filter on that substring to 'push-down' filtering
    for branch in client.get_branches(name="bpln-tx"):
        match = pattern_for_transactional.fullmatch(branch.name)

        if match is not None:
            transactional_branches_data.append(
                {
                    **match.groupdict(),
                    "name": branch.name,
                }
            )

    # Retrieve IDs for all jobs which failed in a transactional branch and are within the required temporal window
    failed_job_ids = [
        failed_job.id
        for failed_job in (
            client.get_jobs(
                all_users=True,
                filter_by_ids=[
                    datum.get("job_id") for datum in transactional_branches_data
                ],
                filter_by_statuses="fail",
                filter_by_created_after=jobs_failed_after,
                filter_by_created_before=jobs_failed_before,
            )
        )
    ]

    # Restrict to transactional branches that can actually be pruned (in window and whose job failed)
    transactional_branches_to_prune = [
        branch
        for branch in transactional_branches_data
        if branch.get("job_id") in failed_job_ids
    ]

    command_counter = Counter(
        [branch.get("command") for branch in transactional_branches_to_prune]
    )

    print(
        f"Found {len(transactional_branches_to_prune)} 'dead' transactional branches. These can be pruned.\n"
    )
    print("Breakdown by command:")
    for k, v in command_counter.most_common():
        print(f" {k:<20} {v}")

    if dry_run:
        print("\nDRY RUN ON. The job would have pruned the following branches:\n")

    for branch in transactional_branches_to_prune:
        branch_name = branch.get("name")
        # In dry run, just print the branches that would have been pruned
        if dry_run:
            print(branch_name)
        else:
            try:
                client.delete_branch(branch_name)
                print(f"Delete branch {branch_name}")
            except Exception as e:
                print(f"Failed to delete branch {branch_name}. Error: {e}")


if __name__ == "__main__":
    typer.run(prune_branches)
