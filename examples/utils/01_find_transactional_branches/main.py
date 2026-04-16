import bauplan

import re
import typer
from typing import Annotated
from collections import Counter


def find_transactional_branches(
    profile: Annotated[str, typer.Option(help="Bauplan profile to use")] = "default",
    verbose: Annotated[
        bool, typer.Option(help="Print all transactional branches")
    ] = False,
) -> None:
    """
    Retrieve transactional branches that can be pruned to clean up the lakehouse.

    A transactional branch is created for any Bauplan command altering the state of the lake and remains open if job execution fails for some reason.

    Given a transactional branch, its corresponding job should be in one of two states:
        - 'failed': branch can be pruned or inspected for debugging
        - 'running': branch cannot be pruned, job still in flight

    Prints a breakdown of transactional branches by command, sorted from most to least frequent,
    optionally followed by the full list of branch names.
    """
    client = bauplan.Client(profile=profile)

    # Counter for commands
    counter = Counter()

    # Buffer to be used to print out branch names in verbose mode
    buffer = []

    # Transactional branches have a distinct pattern following <user>.<branch_name>-bpln-tx-<command>-<timestamp>-<job_id>
    pattern_for_transactional = re.compile(
        r"^(?P<user>[^.]+)\.(?P<branch>[^-]+)-bpln-tx-(?P<command>.+)-(?P<timestamp>\d{14})-(?P<job_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$"
    )

    # Pre-filter by branches whose name contain 'bpln-tx' which indicates transactional branches
    for branch in client.get_branches(name="bpln-tx"):
        # If branch matches pattern, then it's transactional.
        match = pattern_for_transactional.fullmatch(branch.name)

        if match is not None:
            counter.update([match.groupdict().get("command")])
            if verbose:
                buffer.append(branch.name)

    print(f"Found {counter.total()} transactional branches in profile '{profile}'.\n")

    print("Command breakdown:")
    for command, count in counter.most_common():
        print(f"  {command:<30} {count}")

    print()
    if verbose:
        for branch_name in buffer:
            print(branch_name)


if __name__ == "__main__":
    typer.run(find_transactional_branches)
