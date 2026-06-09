"""
WAP (Write-Audit-Publish) pipeline: bronze → silver → gold.

Runs the full medallion pipeline on a dev branch, audits each layer,
and optionally merges to main on success.

Usage:
    uv run python wap_flow.py
    uv run python wap_flow.py --merge   # auto-merge to main after audit
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import bauplan
import polars as pl

SILVER_DIR = Path(__file__).parent / "silver"
GOLD_DIR = Path(__file__).parent / "gold"

EXPECTED_REGIONS = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}


def make_branch_name(username: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"{username}.wap-{ts}"


def ensure_namespace(client: bauplan.Client, namespace: str, branch: str) -> None:
    if not client.has_namespace(namespace, ref=branch):
        client.create_namespace(namespace, branch=branch)
        print(f"  created namespace '{namespace}'")
    else:
        print(f"  namespace '{namespace}' already exists")


def audit_silver(client: bauplan.Client, branch: str) -> None:
    print("\n[AUDIT] silver")
    for table in ("orders", "customers", "nations", "regions", "customers_enriched"):
        result = client.query(f"SELECT COUNT(*) AS n FROM silver.{table}", ref=branch)
        n = pl.from_arrow(result)["n"][0]
        print(f"  silver.{table}: {n:,} rows")
        if n == 0:
            raise RuntimeError(f"silver.{table} is empty")


def audit_gold(client: bauplan.Client, branch: str) -> None:
    print("\n[AUDIT] gold")
    result = client.query(
        "SELECT region_name, total_orders, total_revenue, avg_order_value "
        "FROM gold.orders_by_region ORDER BY total_orders DESC",
        ref=branch,
    )
    df = pl.from_arrow(result)
    print(f"  gold.orders_by_region: {len(df)} rows")
    if len(df) == 0:
        raise RuntimeError("gold.orders_by_region is empty")

    actual_regions = set(df["region_name"].to_list())
    missing = EXPECTED_REGIONS - actual_regions
    if missing:
        raise RuntimeError(f"gold.orders_by_region missing regions: {missing}")

    if (df["total_orders"] <= 0).any() or (df["total_revenue"] <= 0).any():
        raise RuntimeError("gold.orders_by_region has non-positive metrics")

    print(df)

    result2 = client.query(
        "SELECT region_name, nation_name, customer_count "
        "FROM gold.customers_by_region_and_nation ORDER BY region_name, nation_name",
        ref=branch,
    )
    df2 = pl.from_arrow(result2)
    print(f"\n  gold.customers_by_region_and_nation: {len(df2)} rows")
    if len(df2) == 0:
        raise RuntimeError("gold.customers_by_region_and_nation is empty")

    actual_regions2 = set(df2["region_name"].to_list())
    missing2 = EXPECTED_REGIONS - actual_regions2
    if missing2:
        raise RuntimeError(f"gold.customers_by_region_and_nation missing regions: {missing2}")

    if (df2["customer_count"] <= 0).any():
        raise RuntimeError("gold.customers_by_region_and_nation has non-positive customer counts")

    print(df2)


def run_wap(branch: str, on_success: str = "inspect") -> None:
    client = bauplan.Client()

    # ── WRITE ──────────────────────────────────────────────────────────────
    print(f"\n[WRITE] branch: {branch}")
    if not client.has_branch(branch):
        client.create_branch(branch, from_ref="main")
        print(f"  created branch '{branch}'")
    else:
        print(f"  branch '{branch}' already exists")

    print("\n[WRITE] silver namespace")
    ensure_namespace(client, "silver", branch)

    print("\n[WRITE] running silver pipeline …")
    silver_run = client.run(project_dir=str(SILVER_DIR), ref=branch, namespace="silver")
    if silver_run.job_status != "SUCCESS":
        raise RuntimeError(f"Silver pipeline failed: {silver_run.job_id}")
    print(f"  job {silver_run.job_id} → {silver_run.job_status}")

    print("\n[WRITE] gold namespace")
    ensure_namespace(client, "gold", branch)

    print("\n[WRITE] running gold pipeline …")
    gold_run = client.run(project_dir=str(GOLD_DIR), ref=branch, namespace="gold")
    if gold_run.job_status != "SUCCESS":
        raise RuntimeError(f"Gold pipeline failed: {gold_run.job_id}")
    print(f"  job {gold_run.job_id} → {gold_run.job_status}")

    # ── AUDIT ──────────────────────────────────────────────────────────────
    audit_silver(client, branch)
    audit_gold(client, branch)

    # ── PUBLISH ────────────────────────────────────────────────────────────
    print("\n[PUBLISH]")
    if on_success == "merge":
        client.merge_branch(source_ref=branch, into_branch="main")
        print(f"  merged '{branch}' → main ✓")
    else:
        print(f"  branch '{branch}' ready for inspection.")
        print(f"  To publish: bauplan branch merge {branch}")


def main() -> None:
    parser = argparse.ArgumentParser(description="WAP pipeline: bronze → silver → gold")
    parser.add_argument(
        "--merge", action="store_true", help="auto-merge to main after audit"
    )
    parser.add_argument(
        "--branch",
        default=None,
        help="data branch name (default: <user>.customers-by-region-nation)",
    )
    args = parser.parse_args()

    client = bauplan.Client()
    user_info = client.info().user
    if user_info is None or user_info.username is None:
        raise RuntimeError("Could not retrieve user info from Bauplan")
    username = user_info.username
    branch = args.branch or make_branch_name(username)
    on_success = "merge" if args.merge else "inspect"

    try:
        run_wap(branch, on_success=on_success)
    except Exception as exc:
        print(f"\n[FAIL] {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
