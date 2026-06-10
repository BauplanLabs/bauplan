# Executive-chat workflow

How a question from a non-technical executive becomes an answer, and how an answer that does not exist yet becomes a permanent, reviewed table.

The audience for the answer is an executive with no technical context. They see the content of the answer and nothing else: no branch names, no pipeline code, no materialization strategy. Everything below is the machinery that produces that answer.

## The two paths

Every question resolves to one of two cases.

**The answer already exists.** A table in the lakehouse already holds the data the question asks for. Query it and return the result. The agent writes nothing and reviews nothing.

**The answer does not exist yet.** No existing table answers the question. This does not mean the question is unanswerable: the data may be one query away. The agent answers it now with a query, and files a Linear issue recording that query so it can be reimplemented as a reviewed pipeline and the table made permanent.

The goal of the second path is consistency. If two executives ask the same question a month apart, they should get the same answer computed the same way. Promoting a validated pipeline to a reviewed table is how a one-off answer becomes a standard one.

## Deciding which path

Before building anything, check whether the data is already there.

1. **Map the question to the semantics.** Use [semantics.md](semantics.md) to confirm the question is one the dataset can actually answer, and to pin down the grain and the metric such as net revenue at the line-item grain or customer region rather than supplier region. A question that does not map onto the data meaning is not answerable by either path.
2. **Look for an existing table.** Audit the silver and gold models per [lakehouse.md](lakehouse.md). The table you need may already exist, or be one small join or column away from a model that does.
3. **If a table answers the question, query it** and return the result. Done.
4. **If no table answers it, build a new pipeline** as described in the next section.

## Building a new table

When the data does not exist, you still answer the executive now, with a query, and hand off the work of turning that query into a permanent table. You do not write the pipeline here: the chat environment has no repository and no git, only the Bauplan and Linear tools.

1. **Answer with a query.** Write the SQL needed to produce the answer and run it through the Bauplan MCP. Return the result to the executive. This query is a one-off: it answers the question, but it is not a reviewed, reusable artifact.
2. **File a Linear issue.** Follow [linear.md](linear.md) for the exact recipe. The issue records the question and any interpretation you made, such as customer region versus supplier region, the exact query you ran, the result it returned, and the schemas of the tables involved.
3. **Let the automation close the loop.** Creating the issue is the whole handoff. Linear syncs it to GitHub, where an automated agent converts the query into a proper medallion pipeline (silver and gold models, Python + Polars only), sets `materialization_strategy='REPLACE'` on the gold model, validates it, and opens the pull request. A human reviews and merges. On merge the table becomes canonical, and from then on the same question takes the first path: query the existing table.

The executive never participates in steps 2 and 3. Those steps exist for everyone who asks the question after them.

## Why the query is not the table

The query that answers the executive and the table that will answer everyone after them are not the same thing, and the gap between them is the point.

A one-off query is fast and unreviewed. It may read straight from bronze, it may encode an interpretation nobody has checked, and nothing stops two people from writing it two different ways. Returning its result to the executive is fine. Promoting it to a permanent table is not, until someone has looked at it.

The Linear issue is that promotion request. Downstream, the agent converts the query into a medallion pipeline against the rules in `lakehouse.md` and the meaning in `semantics.md`, writes the gold model with `materialization_strategy='REPLACE'`, and a human reviews the pull request. Merging is the act of accepting the table as canonical. Until then, nothing permanent has entered the lakehouse.

## Boundaries

This workflow sits on top of the Bauplan safety rules in [../CLAUDE.md](../CLAUDE.md), which always win. In particular: never write or materialize directly on `main`, and publish data only by running on a Bauplan branch and merging. From the chat environment you have the Bauplan and Linear tools but no git: a downstream agent writes, validates, and materializes the pipeline, then opens a pull request for a human to review and merge.
