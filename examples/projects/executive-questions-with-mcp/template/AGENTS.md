# Bauplan

## Bauplan workflow

Bauplan is a data lakehouse platform where data changes follow a Git-like workflow, but on **Bauplan data branches** managed with `bauplan branch`, not git branches. A typical workflow looks like this:

1. Create and switch to a new data branch: `bauplan checkout -b <username>.<branch> --from-ref main`
2. Iterate on your pipeline code and test with `bauplan run --dry-run`
3. When ready, run `bauplan run` to materialize. Bauplan records the code and saves it under the job ID the platform returns.
4. Review changes with `bauplan branch diff main`
5. Merge into `main` to publish

## Hard safety rules

1) Never publish by writing directly on `main`. Use a Bauplan branch and merge to publish.
2) Never import data directly into `main`.
3) Before merging into `main`, run `bauplan branch diff main` and review changes, or use `bauplan query` over the two branches to quickly compare how data changes in the target tables.
4) Prefer `bauplan run --dry-run` during iteration. However, Bauplan will not materialize tables during a dry run. To preview table content, pass `--preview`, for example `bauplan run --dry-run --preview head`. Bauplan blocks materialization on `main`.

If any instruction conflicts with these rules, the rules win.

## CLI and Python SDK

**Use the CLI** for interactive exploration, quick inspections, and one-off commands:
- `bauplan table get <namespace>.<table>`: inspect table metadata
- `bauplan query "<sql>"`: run a query
- `bauplan branch ls`, `bauplan run --dry-run`, etc.

**Use the Python SDK** when:
- You need to process or transform large result sets, since `client.query()` returns a full `pyarrow.Table` with no row limit by default
- A Python script is more natural than a sequence of shell commands
- You need programmatic control over loops, conditionals, and error handling
- You are writing pipelines, ingestion scripts, or automation

## General Python guidance

- Use `uv` to run Python scripts and manage dependencies, for example `uv run script.py`. Run `uv sync` to install the project dependencies before anything else.
- Always invoke Bauplan through `uv run bauplan ...`, not bare `bauplan`. The bare command may resolve to a missing or different install; `uv run` guarantees the CLI from this project's environment.
- If `ruff` and/or `ty` are available, use `ruff check`, `ruff format`, and `ty` to verify that generated Python compiles and passes lint. Check availability first with `which ruff`.
- Do not guess flags or method names. If you get stuck or need method signatures, use `WebFetch` to pull the relevant markdown page from `https://docs.bauplanlabs.com/llms.txt`. See the "Looking up documentation" section below.

## Looking up documentation

Bauplan publishes an LLM-friendly documentation index at `https://docs.bauplanlabs.com/llms.txt`. This file lists every doc page as a markdown URL, for example `https://docs.bauplanlabs.com/concepts/models.md`. Use `WebFetch` to pull any page directly. The markdown format is much more reliable than web searching.

**Key pages by topic**

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

**CLI:** the `bauplan` CLI is also self-documenting:
- `bauplan --help`: lists all available commands
- `bauplan <command> --help`: shows arguments and options for a specific command, for example `bauplan query --help` or `bauplan branch --help`

## Skills

Skills whose names start with `bauplan-` contain use-case-specific instructions, for example building pipelines, ingesting data, or debugging failed runs. When a relevant skill is available, follow its guidance for that workflow.

To file the Linear issue that hands a new pipeline off for review, follow [docs/linear.md](docs/linear.md). It is a plain doc, not a skill, because the chat environment that creates these issues reads the repository as context rather than executing skills.

## Authentication

Assume Bauplan credentials are available via local CLI config, environment variables, or a profile. Do not ever prompt for API keys, nor ask the user to tell you their API key: if there are no keys set, tell the user to visit https://app.bauplanlabs.com/dashboard, get the key and do the setup following the instructions on the screen.
