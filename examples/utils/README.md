# Bauplan utilities

A (growing) collection of utilities to efficiently manage Bauplan.

Each utility lives in its own directory with a `main.py` entry point and a `README.md` that explains its aim, the expected output, and why it matters.

## Utilities

| # | Directory | What it does |
|---|---|---|
| 01 | `00_find_transactional_branches` | List all transactional branches and how many each command left behind |
| 02 | `01_prune_transactional_branches` | Identify and delete dead transactional branches left by failed jobs |

## Running an example

Every example uses `uv` for dependency management. From the example directory:

```sh
uv run main.py
# or, to pick a non-default Bauplan profile:
uv run main.py --profile <profile>
```

Run `uv run main.py --help` in any directory to see all available options.
