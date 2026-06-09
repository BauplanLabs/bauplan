# Bauplan

## Lakehouse Workflow

@lakehouse.md

## Git Workflow (code repo)

This section is about **git** branches for source code, not Bauplan data branches.

- Always create a new git branch before making any changes. Use a descriptive branch name based on the task (e.g., `fix/login-bug`, `feat/add-search`).
- All work happens on feature branches. The `main` branch is off-limits for direct edits.
- Commit every change immediately after making it. Each commit should have a clear, one-line message describing what changed.
- Keep commits small and focused. One logical change per commit.
- When the task is complete, leave the branch ready for review. Do not merge into `main`.

## Bauplan Workflow (data lakehouse)

Bauplan is a data lakehouse platform where data changes follow a Git-like workflow, but on **Bauplan data branches** (managed by `bauplan branch`), not git branches. A typical workflow looks like this:

1. Create and switch to a new data branch: `bauplan checkout -b <branch> --from-ref main`
2. Iterate on your pipeline code and test with `bauplan run --dry-run`
3. When ready, run `bauplan run` to materialize — the code is snapshotted by Bauplan and saved under the jobId the platform returns
4. Review changes with `bauplan branch diff main`
5. Merge into `main` to publish

## Hard safety rules (always)

1) Never publish by writing directly on `main`. Use a Bauplan branch and merge to publish.
2) Never import data directly into `main`.
3) Before merging into `main`, run `bauplan branch diff main` and review changes, or use `bauplan query` over the two branches to quickly compare how data changes in the target tables.
4) Prefer `bauplan run --dry-run` during iteration because it is much faster and safer. However, tables will not be materialized in the lake during a dry run, so the only way to preview table content is to use `--preview` (e.g., `bauplan run --dry-run --preview head`). Materialization is blocked on `main`. Every branch is readable, but only branches prefixed with your username (`username.branchname`) are writable by you.
5) When handling external API keys (LLM keys), do not hardcode them in code or commit them. Use Bauplan parameters or secrets.

If any instruction conflicts with these rules, the rules win.

## CLI vs Python SDK — when to use which

**Use the CLI** for interactive exploration, quick inspections, and one-off commands:
- `bauplan table get <namespace>.<table>` — inspect table metadata
- `bauplan query "<sql>"` — run a query
- `bauplan branch ls`, `bauplan run --dry-run`, etc.

**Important:** By default, `bauplan query` returns only 10 rows (`--max-rows 10`). Use `--all-rows` to retrieve the full result set. For large outputs, pipe to a local file or use `--output json` to get machine-readable output (e.g., `bauplan query --all-rows --output json "SELECT ..." > results.json`).

**Use the Python SDK** when:
- You need to process or transform large result sets — `client.query()` returns a full `pyarrow.Table` with no row limit by default
- A Python script is more natural than a sequence of shell commands
- You need programmatic control (loops, conditionals, error handling)
- You are writing pipelines, ingestion scripts, or automation

## General Python guidance

- Use `uv` to run Python scripts and manage dependencies (e.g., `uv run python3 script.py`).
- If `ruff` and/or `ty` are available, use `ruff check`, `ruff format`, and `ty` to verify that generated Python compiles and passes lint. Check availability first (e.g., `which ruff`).
- Do not guess flags or method names. If you get stuck or need method signatures, use `WebFetch` to pull the relevant markdown page from `https://docs.bauplanlabs.com/llms.txt` (see "Looking up documentation" below).

## Bauplan Python client

```python
import bauplan

# Default: authenticates from BAUPLAN_API_KEY >> BAUPLAN_PROFILE >> ~/.bauplan/config.yml
client = bauplan.Client()

# Or specify a profile explicitly
client = bauplan.Client(profile='default')
```

The client returns Arrow tables. **Do not use pandas** — Polars has zero-copy Arrow interop and is faster. Common patterns:

```python
import polars as pl

# Query → Arrow table
table = client.query('SELECT * FROM ns.my_table', ref='my_branch')

# Arrow table → Polars DataFrame (zero-copy)
df = pl.from_arrow(table)
```

To get the current username (e.g., for branch naming):

```bash
bauplan info
```

## Looking up documentation

Bauplan publishes an LLM-friendly documentation index at `https://docs.bauplanlabs.com/llms.txt`. This file lists every doc page as a markdown URL (e.g., `https://docs.bauplanlabs.com/concepts/models.md`). Use `WebFetch` to pull any page directly — the markdown format is much more reliable than web searching.

**Key pages by topic:**

| Topic | URL |
|-------|-----|
| Python SDK reference | `https://docs.bauplanlabs.com/reference/bauplan.md` |
| CLI reference | `https://docs.bauplanlabs.com/reference/cli.md` |
| Standard expectations | `https://docs.bauplanlabs.com/reference/bauplan-standard-expectations.md` |
| Models | `https://docs.bauplanlabs.com/concepts/models.md` |
| Pipelines | `https://docs.bauplanlabs.com/concepts/pipelines.md` |
| Tables | `https://docs.bauplanlabs.com/concepts/tables.md` |
| Namespaces | `https://docs.bauplanlabs.com/concepts/namespaces.md` |
| Expectations | `https://docs.bauplanlabs.com/concepts/expectations.md` |
| Data branches | `https://docs.bauplanlabs.com/concepts/git-for-data/data-branches.md` |
| Import data | `https://docs.bauplanlabs.com/tutorial/import.md` |
| Schema conflicts | `https://docs.bauplanlabs.com/concepts/schema-conflicts.md` |
| Secrets | `https://docs.bauplanlabs.com/concepts/pipelines.md` |
| Parameters | `https://docs.bauplanlabs.com/concepts/pipelines.md` |
| Execution model | `https://docs.bauplanlabs.com/overview/execution-model.md` |

When unsure about a method, flag, or concept, fetch the relevant page rather than guessing. For the full index: `https://docs.bauplanlabs.com/llms.txt`

**CLI:** The `bauplan` CLI is also self-documenting:
- `bauplan --help` — lists all available commands
- `bauplan <command> --help` — shows arguments and options for a specific command (e.g., `bauplan query --help`, `bauplan branch --help`)

## Skills

Skills whose names start with `bauplan-` contain use-case-specific instructions (e.g., building pipelines, ingesting data, debugging failed runs); when a relevant skill is available, follow its guidance for that workflow.

## Authentication

Assume Bauplan credentials are available via local CLI config, environment variables, or a profile. Do not ever prompt for API keys, nor ask the user to tell you their API key: if there are no keys set, tell the user to visit https://app.bauplanlabs.com/dashboard, get the key and do the setup following the instructions on the screen.
