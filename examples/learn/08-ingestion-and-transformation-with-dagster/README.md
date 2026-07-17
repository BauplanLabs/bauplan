# Ingestion and Transformation with Bauplan and Dagster

## Table of contents

- [Goal](#goal)
- [Set up](#set-up)
- [Usage](#usage)
- [Data quality checks in the Dagster UI](#data-quality-checks-in-the-dagster-ui)
- [Materialization metadata](#materialization-metadata)
- [When things fail](#when-things-fail)

## Goal

This example shows how to run an ingestion pipeline (with a write-audit-publish step) followed by a transformation pipeline, using Dagster as the orchestrator and Bauplan as the lakehouse and compute engine.

The setup mimics a scenario built on two synthetic tables that share an `account_id`:

- `transactions`: one row per card payment, carrying the account that made it, the amount and currency, the merchant and its category, a settlement status, and a timestamp.
- `account_events`: one row per account activity event (login, KYC update, limit change, password reset, card added), carrying the channel it came through and a timestamp.

The synthetic data has already been generated and uploaded to S3 for you; the code that produces it lives in `data/generate_data.py`. The data is partitioned by year, month, and day, so on S3 each file sits under `<prefix>/<table>/year=<year>/month=<month>/day=<day>`.

The core idea is that Dagster triggers the import of a single partition into Bauplan. Rather than importing the new data blindly, we first check that it meets our quality requirements with a write-audit-publish (WAP) pattern, backed by [Bauplan's expectations](https://docs.bauplanlabs.com/concepts/expectations): the partition lands on an isolated ingestion branch, the expectations run against it, and only a branch that passes is merged into the base branch.

Once both tables are in the lakehouse, a transformation pipeline builds a _daily_ summary. It keeps only settled transactions, aggregates spend per account (total, count, and average amount), and joins the result with per-account event and login counts derived from `account_events`.

On the Dagster side, everything is modeled with software-defined assets: the two source tables and the summary table appear in the asset lineage view, the Bauplan expectations surface as Dagster asset checks, and each materialization carries Bauplan metadata (row counts, column schema, branch and job identifiers) into the Dagster asset catalog.

## Set up

Use `src/.env.example` as a reference for the fields required to run this example. You can generate a new Bauplan API key (if you don't already have one) from [https://app.bauplanlabs.com/api-keys](https://app.bauplanlabs.com/api-keys). A bucket and prefix have already been provided; `NAMESPACE` and `BASE_BRANCH` default to `workshop` and `workshop.main` and only need changing if you want to publish elsewhere. When you launch the Dagster UI, these variables are loaded automatically from `src/.env`.

Ensure [`uv`](https://docs.astral.sh/uv/) is installed by following the [official documentation](https://docs.astral.sh/uv/getting-started/installation/), then install the dependencies:

```sh
uv sync
```

## Usage

For a fast turnaround you can use the CLI to materialize partitions without spinning up the Dagster UI:

```sh
set -a && source src/.env && set +a
uv run src/main.py ingest transactions 2026-06-16
uv run src/main.py ingest account_events 2026-06-16
uv run src/main.py transform 2026-06-16
```

`ingest` materializes one daily partition of one source table, including its checks; `transform` materializes one daily partition of the summary. Note that each CLI invocation runs on an ephemeral Dagster instance, so its run history will not appear in a later UI session.

For the full experience, open the Dagster UI on your localhost:

```sh
cd src && uv run dg dev -m main
```

Open the `Assets` page (or the lineage tab) to see the graph `transactions`, `account_events` into `account_activity_summary` with the daily partition bar under each asset. Select an asset, click `Materialize`, and pick a partition in the dialog. The `ingestion` and `transformation` jobs group the same assets if you prefer launching from the `Jobs` page.

To load a date range, use a backfill from the partition dialog. Keep the backfill concurrency at 1: parallel partition runs of the same table would race on the merge into the base branch.

## Data quality checks in the Dagster UI

The example demonstrates the two ways of coupling Bauplan data quality with Dagster asset checks.

The `audit_expectations` check on each source table is the audit step of the WAP cycle. The asset body runs the table's expectations project on the ingestion branch and reports the outcome as an in-asset check result, so the audit that gates the merge is the same event you see in the `Checks` tab. Its metadata carries the Bauplan job id, the audit duration, and, on failure, the error and the name of the ingestion branch that was kept for debugging. The expectations themselves stay in the Bauplan project (`src/ingestion/pipelines/audit/<table>/expectations.py`).

The post-publish checks (`transactions_txn_id_unique`, `transactions_audit`, `account_events_account_id_no_nulls`, `account_events_audit`) are standalone `@asset_check` definitions. They run automatically on the base branch after each materialization of their asset. This is the pattern to follow when you want additional checks defined and versioned on the orchestrator side. Notice that these checks can be manually re-run, whereas the expectations embedded in the ingestion job cannot. This is why we have included `transactions_audit` and `account_events_audit` which replicate the expectations verbatim. 

One evaluation per check is shown in the `Checks` tab, the latest one; the per-partition history remains available in the run logs of each materialization.

## Materialization metadata

Every successful materialization attaches Bauplan metadata to the Dagster event: total row count (`dagster/row_count`), rows in the materialized partition (`dagster/partition_row_count`), the column schema (`dagster/column_schema`), the imported files (`dagster/uri`, ingestion assets only), plus the Bauplan branch and job identifiers and, for the transform, the run duration.

## When things fail

If an audit fails, the run stops before the merge: the `audit_expectations` check turns red, the asset is not marked materialized, and the ingestion branch is left in place so you can inspect the rejected data. The branch name is in the check metadata and in the run failure; delete the branch once you are done debugging.

If the import itself fails, the run errors before any audit, so the check shows no evaluation for that run; the failing branch name is part of the exception message. If the merge fails after a clean audit, the check stays green (the audit did pass) while the run fails, and the branch is again kept for inspection.
