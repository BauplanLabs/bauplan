# Find transactional branches

Lists all transactional branches for a given Bauplan profile.

Bauplan creates a transactional branch for every command that alters the state of the lake. These branches remain open if job execution fails, and should be either pruned or inspected for debugging.

## Usage

```sh
uv run main.py [OPTIONS]
```

Run `uv run main.py --help` to see all available options.

### Options

| Option | Default | Description |
|---|---|---|
| `--profile` | `default` | Bauplan profile to use. |
| `--verbose` | `False` | Print the full list of transactional branch names after the summary. |

## Output

Always prints the total count of transactional branches found and a breakdown by command, sorted from most to least frequent. With `--verbose`, also prints the full list of branch names.

A transactional branch follows the naming pattern:

```
<user>.<branch>-bpln-tx-<command>-<timestamp>-<job_id>
```
