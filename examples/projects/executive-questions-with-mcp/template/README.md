# Executive questions with MCP

A base project for answering executive questions about company data through a chat interface (for example Claude Code). A non-technical executive asks a question in natural language and gets back a concrete answer: a number, a table, or a chart. They never see the lakehouse, the pipelines, or the SQL underneath.

Every question resolves to one of two paths:

1. **The data already exists.** A table answers the question, so the agent queries it and returns the result.
2. **The data does not exist yet.** No table answers the question. The agent answers it with a query, returns the result, and files a Linear issue recording that query. Downstream automation converts it into a reviewed pipeline, and once the pull request merges the table is permanent.

The second path is what keeps answers consistent across executives over time: once a reviewed pipeline answers a question, the table it produces becomes the canonical source for that question.

## Documentation

`workflow.md` is the spine. The others are references you reach for while following it.

| Document | When to read it |
|----------|-----------------|
| [docs/workflow.md](docs/workflow.md) | First. The end-to-end flow from a question to an answer: the query-versus-build decision, and how human review gates each new table. |
| [docs/answering.md](docs/answering.md) | How to talk to the executive: answer directly, prefer charts, never invent an answer the data does not support. This is the doc to point a chat assistant's project instructions at. |
| [docs/linear.md](docs/linear.md) | How to file the handoff Linear issue when no table answers a question: what the issue must contain and the instructions it carries to the implementing agent. |
| [docs/semantics.md](docs/semantics.md) | What the data means: the benchmark's business concepts, the metrics that matter, and the vocabulary to agree on before writing a query. Consult it whenever a question needs mapping to the data. |
| [docs/lakehouse.md](docs/lakehouse.md) | Where a new table belongs: the medallion architecture, bronze, silver, and gold layers, naming conventions, and the rules a pipeline must follow when you build one. |

Operational rules for agents working in this repository (git workflow, Bauplan safety rules, `uv` usage, CLI versus SDK) live in [CLAUDE.md](CLAUDE.md).
