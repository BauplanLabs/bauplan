# Filing the handoff Linear issue

When no existing table answers a question, you answer the executive with a query and then file a Linear issue that hands the work of turning that query into a permanent table off to an automated agent. This document is the concrete recipe for that issue. The two paths and the reasoning are in [workflow.md](workflow.md). This document is the concrete how-to.

You are in the chat/MCP environment: you have the Bauplan and Linear MCP tools, no terminal and no git. You create the issue, complete, in a single step. Its creation triggers the GitHub Action that converts the query into a pipeline and opens the pull request. You do not write or run the pipeline here.

## 1. Gather context with the Bauplan MCP

There is no terminal here, so do not shell out to the `bauplan` CLI. Use the MCP tools:

- Username: `get_user_info`.
- Tables: `list_tables` (and `get_namespaces` to scope by namespace).
- Schemas: `get_table` or `get_schema` for each relevant table. Keep the full schema verbatim.
- The answering query: keep the exact SQL you ran through `run_query` to answer the executive, and the result it returned. Both go into the issue.

## 2. Create the issue

Create it with `Linear:save_issue` as a plain issue without a Linear template, targeting:

- Project: `ceo`
- Team: `linear-integration-demo`
- Assignee: `giacomo.piccinini`
- Priority: medium
- Status: `Todo`

Title: a concise description of the question.

The body must contain these four things, explicitly. Do not improvise around them and do not drop any:

1. **Question and interpretation.** The question asked, and any choice you made.
2. **Source query.** The exact SQL you ran through the Bauplan MCP, verbatim, in a fenced `sql` block.
3. **Result.** What the query returned, that is, what you showed the executive: a small table or the key figures.
4. **Involved tables.** For each table, its schema from step 1, verbatim. If a schema fetch failed, say so.

Then append the implementation instructions below.

The issue's creation is the whole handoff. Do not edit it afterward, and do not post any trigger comment. Linear syncs it to GitHub, and that creation starts the Action.

## 3. Implementation instructions to append

Paste this block at the end of the issue body. It is the brief for the agent that picks the issue up. It reads the repository, so it also has `CLAUDE.md`, `docs/lakehouse.md`, and `docs/semantics.md`. This block makes the non-negotiable points explicit.

> Reimplement the source query above as a proper, reviewable pipeline whose gold output matches the result above. Do not ship the query as a one-off.
>
> - Follow `docs/lakehouse.md`. This is a medallion project: gold reads only from silver, silver from bronze. Add a silver base model for every source table the query touches, an enriched silver model for any shared lookup join, and a gold model for the question, in `src/pipelines/silver/` and `src/pipelines/gold/`. Do not read bronze from gold.
> - Follow `docs/semantics.md` for what the data means; pin the grain and the metric before writing the models.
> - Write the models in Python with Polars, as native DataFrame and expression operations (`select`, `filter`, `group_by`, `join`, `with_columns`). Polars is mandatory unless an extreme case genuinely rules it out. Do not use Polars' SQL interface (`SQLContext` / `.sql()`), and never use pandas.
> - Set `materialization_strategy='REPLACE'` on the gold model.
> - Add expectations for every new or changed table, and validate with `uv run bauplan run --dry-run --strict` (`--strict` is required, otherwise expectations are skipped). Create your own Bauplan data branch prefixed with your username; never run on `main`.

## 4. Report to the user

Report the issue identifier, title, and URL. Keep it concise. The executive does not need a recap of the body.
