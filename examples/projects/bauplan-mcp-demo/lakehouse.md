# Lakehouse Architecture

This project follows the **medallion architecture**: three layers of increasing refinement, each with its own namespace, Bauplan models, and expectations.

---

## Bronze — raw source data (`tpch_1`)

Raw tables ingested as-is from the source. No transformations, no joins.  
In this project the bronze layer is the `tpch_1` namespace (TPCH dataset), read-only.

**What goes here:** source tables exactly as they arrive  
**What does not go here:** any join, rename, cast, or business logic.

---

## Silver — cleaned, conformed data (`silver/`)

The trusted single source of truth. Silver makes bronze data reliable and queryable without encoding any business interpretation. It is a clean, close-to-source reflection of operational reality — not an opinion about it.

Models live in `silver/models.py`; quality tests live in `silver/expectations.py`.

**What goes here:**
- **Deduplication** — remove duplicate records from upstream ingestion
- **Type casting** — parse strings into proper timestamps, numbers, booleans
- **Schema normalization** — consistent column names (snake_case), dropped junk columns
- **Light validation** — null checks, range guards, referential integrity assertions (enforced via expectations)
- **PII masking/tokenization** — anonymize sensitive fields before they propagate downstream
- **Stable reference enrichment** — attach low-cardinality lookup tables (e.g., nation, region, country codes, product categories) directly to the entity they describe. **Detection trigger:** if two or more gold models join the same lookup table independently, that join belongs in a silver enriched entity instead. **Important:** every source table used in an enriched silver model must first have its own silver base model — enriched models join silver-to-silver, never silver-to-bronze.

**What does not go here:** aggregations, metric definitions, business logic, or anything that encodes a decision a team could reasonably disagree on.

---

## Gold — business-ready, use-case-specific tables (`gold/`)

Tables shaped for consumers: analysts, dashboards, ML models, and APIs. Each gold table serves a specific, named purpose. It is expected and encouraged to have multiple gold tables built from the same silver data for different teams or use cases.

Models live in `gold/models.py`; quality tests live in `gold/expectations.py`.

**What goes here:**
- **Granular consumption tables** — entity or event-grain tables (e.g., order-level, session-level) that give BI tools and analysts full flexibility to aggregate, filter, and drill down dynamically.
- **Pre-aggregated summary tables** — daily revenue, weekly active users, churn rates; metrics whose definitions must be consistent and locked across all consumers
- **Business metric definitions** — e.g., "an active user is someone who logged in within 30 days"
- **Wide denormalized tables** — pre-joined fact + dimension tables so no further lookups are needed
- **Feature tables for ML** — engineered features ready for model training or scoring
- **Mart-level segmentation** — finance mart, product mart, marketing mart, etc.

**What does not go here:** logic that belongs in silver (cleaning, normalization) — gold reads exclusively from silver. Reading any table directly from bronze is forbidden, no exceptions.

---

## Naming conventions

- **Never prefix table names with the layer.** The layer is already conveyed by the namespace or folder (`silver/`, `gold/`). Use `orders`, not `silver_orders`; use `orders_by_region`, not `gold_orders_by_region`.
- Name tables after the **entity or concept** they represent, not the transformation applied to them.
- Silver base models use the singular or plural noun of the source entity.
- Silver enriched models describe the enriched entity.
- Gold models describe the **business question or consumer use case**.
- Use `snake_case` for all table and column names.

---

## Common mistakes to avoid

1. **Business logic in silver** — every consumer inherits your assumptions; keep silver neutral
2. **Gold reading from bronze** — gold must only consume silver tables. Every bronze table needed downstream must have a corresponding silver base model first. There are no exceptions, even for "simple" fact tables.
3. **Silver enriched models reading from bronze** — when building an enriched silver model that joins multiple tables, each of those tables must already exist as its own silver base model. Never join a bronze table directly inside a silver enriched model.
4. **One monolithic gold table** — gold should be consumer-specific, not a catch-all
5. **Re-implementing silver logic in gold** — if something needs to be reused, it belongs in silver
6. **Duplicate joins across gold models** — if `gold/models.py` has two functions that join the same lookup table, that join belongs in a single silver enriched model. Gold should only aggregate or filter, never re-derive the same relationship from scratch.

---

## Adding a new table

| Step | Action |
|------|--------|
| 1. **Audit existing models** | Check `silver/models.py` and `gold/models.py` — the table you need may already exist or be one small join/column away from an existing model. Extending an existing model is always cheaper than building from scratch. |
| 1b. **Scan for duplicate joins** | Before writing any join in a gold model, grep `gold/models.py` for the same table name. If it appears in another model's signature, extract the join into a silver enriched entity first. |
| 2. **Decide: extend or create** | If an existing model covers 80%+ of the need, add the missing columns or join to it. If the grain or purpose is fundamentally different, create a new model function in the right layer. |
| 3. **Place it in the right layer** | Cleaned/conformed data reusable across use cases → `silver/`. Aggregated, metric-defined, or consumer-specific table → `gold/`. Gold reads only from silver — if a bronze table has no silver model yet, create one before building on it. |
| 4. **Add or update expectations** | `silver/expectations.py` or `gold/expectations.py` — cover nulls, uniqueness, and accepted values for any new or changed columns. |
| 5. **Audit layers**| Add checks in the audit bodies in `wap_flow.py` for the new generated tables.
| 6. **Run the pipeline** | Use `uv run wap_flow.py` to run the pipeline and verify the table is being built correctly. |
