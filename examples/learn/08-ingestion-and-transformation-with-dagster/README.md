# Ingestion and Transformation with Bauplan and Dagster

## Table of contents

- [Goal](#goal)
- [Set up](#set-up)
- [Usage](#usage)

## Goal

This example shows how to run an ingestion pipeline (with a write-audit-publish step) followed by a transformation pipeline, using Dagster as the orchestrator and Bauplan as the lakehouse and compute engine.

The setup mimics a scenario built on two synthetic tables that share an `account_id`:

- `transactions`: one row per card payment, carrying the account that made it, the amount and currency, the merchant and its category, a settlement status, and a timestamp.
- `account_events`: one row per account activity event (login, KYC update, limit change, password reset, card added), carrying the channel it came through and a timestamp.

The synthetic data has already been generated and uploaded to S3 for you; the code that produces it lives in `data/generate_data.py`. The data is partitioned by year, month, and day, so on S3 each file sits under `<prefix>/<table>/year=<year>/month=<month>/day=<day>`.

The core idea is that Dagster triggers the import of a single partition into Bauplan. Rather than importing the new data blindly, we first check that it meets our quality requirements with a write-audit-publish (WAP) pattern, backed by [Bauplan's expectations](https://docs.bauplanlabs.com/concepts/expectations): the partition lands on an isolated ingestion branch, the expectations run against it, and only a branch that passes is merged into the base branch.

Once both tables are in the lakehouse, a transformation pipeline builds a _daily_ summary. It keeps only settled transactions, aggregates spend per account (total, count, and average amount), and joins the result with per-account event and login counts derived from `account_events`.

## Set up

Use `src/.env.example` as a reference for the fields required to run this example. You can generate a new Bauplan API key (if you don't already have one) from [https://app.bauplanlabs.com/api-keys](https://app.bauplanlabs.com/api-keys). A bucket and prefix have already been provided. When you launch the Dagster UI, these variables are loaded automatically from `src/.env`.

Ensure [`uv`](https://docs.astral.sh/uv/) is installed by following the [official documentation](https://docs.astral.sh/uv/getting-started/installation/), then install the dependencies:

```sh
uv sync
```

## Usage

For a fast turnaround you can use the CLI to launch jobs without spinning up the Dagster UI:

```sh
set -a && source src/.env && set +a && uv run src/main.py --help
```

The CLI exposes two commands, `ingest` and `transform`; pass `--help` to either to see the available parameters.

For the full experience, open the Dagster UI on your localhost. Run:

```sh
cd src && uv run dg dev -m main
```

Open `Jobs > ingestion > Launchpad` and set up a config such as

```yaml
ops:
  import_data:
    config:
      base_branch: 'workshop.main'
      day: '16'
      month: '06'
      namespace: 'workshop'
      table: 'account_events'
      year: '2026'
      dt_partition_column: 'event_ts'
resources:
  bauplan_client:
    config:
      api_key:
        env: 'BAUPLAN_API_KEY'
```

then run the job. Each ingestion run imports a single table, so you need two runs to have all the data a transformation needs. Pair the config above with

```yaml
ops:
  import_data:
    config:
      base_branch: 'workshop.main'
      day: '16'
      month: '06'
      namespace: 'workshop'
      table: 'transactions'
      year: '2026'
      dt_partition_column: 'txn_ts'
resources:
  bauplan_client:
    config:
      api_key:
        env: 'BAUPLAN_API_KEY'
```

to load all the data. Once both tables are loaded, open `Jobs > transformation > Launchpad` and set

```yaml
ops:
  transform:
    config:
      base_branch: 'workshop.main'
      namespace: 'workshop'
      start_date: '2026-06-16'
      end_date: '2026-06-18'
resources:
  bauplan_client:
    config:
      api_key:
        env: 'BAUPLAN_API_KEY'
```

then run the job. The `transformation` job forks a fresh branch from the base branch and runs the `account_summary` pipeline on it. The two upstream models, `settled_transactions` and `daily_account_spend`, are computed on the fly and never persisted; only the final model is materialized, as the `account_activity_summary` table, using a `OVERWRITE_PARTITIONS` strategy that overwrites any previous contents for a given partition. When the run succeeds, the branch is merged back into the base branch, so `account_activity_summary` is published to `workshop.main` next to the source tables.
