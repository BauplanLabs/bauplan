# Conflicts in Bauplan

A collection of self-contained examples that show how Bauplan's Git-like branching handles common data engineering problems such as conflicts, corruption, and lakehouse hygiene.

Each example lives in its own directory with a `main.py` entry point and a `README.md` that explains its aim, the expected output, and why the behaviour matters.

## Examples

| # | Directory | What it shows |
|---|---|---|
| 01 | `01_concurrency_conflict` | Two users independently create the same table; Bauplan rejects the second merge |
| 02 | `02_naming_conflict` | Two pipelines write to the same table name; silent overwrite vs. namespace isolation |
| 03 | `03_schema_conflict` | An expectation catches a subtle type drift before it can reach the branch |
| 04 | `04_corruption_conflict` | A corrupted table must revert to a healthy state and be repopulated |

## Running an example

Every example uses `uv` for dependency management. From the example directory:

```sh
uv run main.py
# or, to pick a non-default Bauplan profile:
uv run main.py --profile <profile>
```

Run `uv run main.py --help` in any directory to see all available options.
