# Prune transactional branches

Identifies dead transactional branches to clean up the lakehouse.

Bauplan creates a transactional branch for every command that alters lake state. These branches remain open if job execution fails. This script finds branches whose corresponding job has failed in a user-provided time window, leaving branches with jobs still running untouched. If not in `dry-run` mode, these branches are pruned.

## Usage

```sh
uv run main.py [OPTIONS]
```

Run `uv run main.py --help` to see all available options.

### Options

| Option | Default | Description |
|---|---|---|
| `--profile` | `default` | Bauplan profile to use. |
| `--jobs-failed-after` | `None` | Only consider jobs that failed after this date (UTC). Leave empty to apply no lower bound. |
| `--jobs-failed-before` | `None` | Only consider jobs that failed before this date (UTC). Leave empty to apply no upper bound. |
| `--dry-run` / `--no-dry-run` | `True` | Print branches that would be pruned without deleting them. Pass `--no-dry-run` to actually delete. |

## Output

Prints the total count of dead transactional branches eligible for pruning and a breakdown by command type. In dry-run mode, also lists the branch names that would be deleted.
