# Architecture

This project follows the **medallion architecture**: three layers of increasing refinement, each with its own namespace, Bauplan models, and expectations.

## Bronze

Raw tables ingested as-is from the source. No transformations, no joins.  
In this project the bronze layer is the `tpch_1` namespace, the example benchmark dataset, read-only.

**What goes here:** source tables exactly as they arrive  
**What does not go here:** any join, rename, cast, or business logic.

## Silver

The trusted single source of truth. Silver makes bronze data reliable and queryable without encoding any business interpretation. It is a clean, close-to-source reflection of operational reality.

Models live in `src/pipelines/silver/models.py`. Quality tests live in `src/pipelines/silver/expectations.py`.

**What goes here:**
- **Deduplication**: remove duplicate records from upstream ingestion
- **Type casting**: parse strings into proper timestamps, numbers, booleans
- **Schema normalization**: consistent column names (snake_case), dropped junk columns
- **Light validation**: null checks, range guards, and referential integrity assertions, enforced via expectations
- **Personally identifiable information masking**: anonymize and redact sensitive fields before they propagate downstream
- **Stable reference enrichment**: attach low-cardinality lookup tables, such as nation, region, country codes, or product categories, directly to the entity they describe. 
- **Detection trigger:** if two or more gold models join the same lookup table independently, that join belongs in a silver enriched entity instead. 

**Important:** every source table used in an enriched silver model must first have its own silver base model. Enriched models join silver-to-silver, never silver-to-bronze.

**What does not go here:** aggregations, metric definitions, business logic, or anything that encodes a decision a team could reasonably disagree on.

## Gold

Tables shaped for consumers: analysts, dashboards, ML models, and APIs. Each gold table serves a specific, named purpose. You can and should build multiple gold tables from the same silver data, one per team or use case.

Models live in `src/pipelines/gold/models.py`. Quality tests live in `src/pipelines/gold/expectations.py`.

**What goes here:**
- **Granular consumption tables**: entity or event-grain tables, such as order-level or session-level, that give BI tools and analysts full flexibility to aggregate, filter, and drill down dynamically.
- **Pre-aggregated summary tables**: daily revenue, weekly active users, churn rates; metrics whose definitions must be consistent and locked across all consumers
- **Business metric definitions**: for example, "an active user is someone who logged in within 30 days"
- **Wide denormalized tables**: pre-joined fact + dimension tables, removing the need for further lookups
- **Feature tables for ML**: engineered features ready for model training or scoring
- **Mart-level segmentation**: finance mart, product mart, marketing mart, etc.

**What does not go here:** cleaning and normalization logic, which belongs in silver. Gold reads exclusively from silver. Never read any table directly from bronze.

## Naming conventions

- **Never prefix table names with the layer.** The layer is already conveyed by the namespace or folder path, for example `src/pipelines/silver/` or `src/pipelines/gold/`. Use `orders`, not `silver_orders`; use `orders_by_region`, not `gold_orders_by_region`.
- Name tables after the **entity or concept** they represent, not the transformation applied to them.
- Silver base models use the singular or plural noun of the source entity.
- Silver enriched models describe the enriched entity.
- Gold models describe the **business question or consumer use case**.
- Use `snake_case` for all table and column names.

## Common mistakes to avoid

1. **Business logic in silver**: every consumer inherits your assumptions; keep silver neutral
2. **Gold reading from bronze**: gold must only consume silver tables. Every bronze table needed downstream must have a corresponding silver base model first. There are no exceptions, even for "simple" fact tables.
3. **Silver enriched models reading from bronze**: when building an enriched silver model that joins multiple tables, each of those tables must already exist as its own silver base model. Never join a bronze table directly inside a silver enriched model.
4. **One monolithic gold table**: gold should be consumer-specific, not a catch-all
5. **Re-implementing silver logic in gold**: if something is shareable across gold models, it belongs in silver
6. **Duplicate joins across gold models**: if `src/pipelines/gold/models.py` has two functions that join the same lookup table, that join belongs in a single silver enriched model. Gold should only aggregate or filter, never re-derive the same relationship from scratch.

## Adding a new table

| Step | Action |
|------|--------|
| 1. **Audit existing models** | Check `src/pipelines/silver/models.py` and `src/pipelines/gold/models.py`. The table you need may already exist or be one small join/column away from an existing model. Extending an existing model is always cheaper than building from scratch. |
| 1b. **Scan for duplicate joins** | Before writing any join in a gold model, grep `src/pipelines/gold/models.py` for the same table name. If it appears in another model's signature, extract the join into a silver enriched entity first. |
| 2. **Decide: extend or create** | If an existing model covers 80%+ of the need, add the missing columns or join to it. If the grain or purpose is fundamentally different, create a new model function in the right layer. |
| 3. **Place it in the right layer** | Cleaned/conformed data reusable across use cases goes in `src/pipelines/silver/`. Aggregated, metric-defined, or consumer-specific table goes in `src/pipelines/gold/`. Gold reads only from silver. If a bronze table has no silver model yet, create one before building on it. |
| 4. **Add or update expectations** | `src/pipelines/silver/expectations.py` or `src/pipelines/gold/expectations.py` cover nulls, uniqueness, and accepted values for any new or changed columns. |
| 5. **Run and validate** | Run `uv run bauplan run --dry-run --strict on` against the project to build the tables and execute the expectations from step 4. |
