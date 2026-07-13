import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated

import polars as pl
import typer
from faker import Faker

# Shared categorical domains for the two fact tables
CURRENCIES = ["EUR", "USD", "GBP"]
MERCHANT_CATEGORIES = [
    "groceries",
    "travel",
    "electronics",
    "restaurants",
    "utilities",
    "entertainment",
]
TRANSACTION_STATUSES = ["settled", "pending", "declined"]
EVENT_TYPES = ["login", "kyc_update", "limit_change", "password_reset", "card_added"]
CHANNELS = ["mobile", "web", "atm", "branch"]

SECONDS_PER_DAY = 24 * 60 * 60


def _instant_within(day: datetime) -> datetime:
    # a wall-clock instant somewhere inside the given calendar day
    return day + timedelta(seconds=random.randint(0, SECONDS_PER_DAY - 1))


def _write_partition(
    df: pl.DataFrame, output_dir: Path, table: str, day: datetime
) -> None:
    # hive layout: partition values live in the path, not inside the parquet file
    partition_dir = (
        output_dir
        / table
        / f"year={day.year}"
        / f"month={day.month:02d}"
        / f"day={day.day:02d}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(partition_dir / f"{table}-000.parquet")


def main(
    output_dir: Annotated[
        Path, typer.Option(help="Root folder for the generated hive-partitioned tables")
    ] = Path("."),
    start_date: Annotated[
        str, typer.Option(help="First day to generate, format YYYY-MM-DD")
    ] = "2026-06-15",
    days: Annotated[
        int, typer.Option(help="Number of consecutive days to generate")
    ] = 30,
    accounts: Annotated[
        int, typer.Option(help="Size of the shared account pool joining the two tables")
    ] = 200,
    transactions_per_day: Annotated[
        int, typer.Option(help="Rows written to transactions per day")
    ] = 500,
    events_per_day: Annotated[
        int, typer.Option(help="Rows written to account_events per day")
    ] = 150,
    seed: Annotated[int, typer.Option(help="Seed for reproducible output")] = 1184,
) -> None:
    """Generate two synthetic fintech fact tables (transactions and account_events) as hive-partitioned parquet.

    The tables share a pool of account_id values so they can be joined on account and day when building
    downstream ingestion and transformation pipelines. Values are synthetic and carry no real meaning.
    """
    random.seed(f"{seed}-{start_date}")
    Faker.seed(f"{seed}-{start_date}")
    fake = Faker()

    # Stable pool of accounts shared by both tables so joins produce matches
    account_ids = [fake.unique.iban() for _ in range(accounts)]

    start = datetime.strptime(start_date, "%Y-%m-%d")
    for offset in range(days):
        day = start + timedelta(days=offset)

        transactions = pl.DataFrame(
            [
                {
                    "txn_id": fake.uuid4(),
                    "account_id": random.choice(account_ids),
                    "amount": round(random.uniform(1.0, 2500.0), 2),
                    "currency": random.choice(CURRENCIES),
                    "merchant": fake.company(),
                    "merchant_category": random.choice(MERCHANT_CATEGORIES),
                    # Skew towards settled so the data reads like a healthy ledger
                    "status": random.choices(
                        TRANSACTION_STATUSES, weights=[0.85, 0.10, 0.05]
                    )[0],
                    "txn_ts": _instant_within(day),
                }
                for _ in range(transactions_per_day)
            ]
        )
        _write_partition(transactions, output_dir, "transactions", day)

        account_events = pl.DataFrame(
            [
                {
                    "event_id": fake.uuid4(),
                    "account_id": random.choice(account_ids),
                    "event_type": random.choice(EVENT_TYPES),
                    "channel": random.choice(CHANNELS),
                    "event_ts": _instant_within(day),
                }
                for _ in range(events_per_day)
            ]
        )
        _write_partition(account_events, output_dir, "account_events", day)


if __name__ == "__main__":
    typer.run(main)
