import csv
import os
from datetime import datetime
from pathlib import Path

import clickhouse_connect
from dagster import MaterializeResult, asset


_COLUMNS = [
    "branch_id",
    "branch_name",
    "region",
    "branch_status",
    "branch_type",
    "opening_date",
    "manager_name",
    "city",
]


def _read_branches() -> list[tuple[object, ...]]:
    csv_path = Path(__file__).resolve().parents[4] / "data" / "lake" / "branches.csv"
    with csv_path.open(newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames != _COLUMNS:
            raise ValueError(f"Unexpected columns in {csv_path}: {reader.fieldnames!r}")

        rows = []
        for row in reader:
            rows.append(
                (
                    row["branch_id"],
                    row["branch_name"],
                    row["region"],
                    row["branch_status"],
                    row["branch_type"],
                    datetime.strptime(row["opening_date"], "%Y-%m-%d").date(),
                    row["manager_name"],
                    row["city"],
                )
            )
    return rows


@asset
def branches_staging() -> MaterializeResult:
    """Load the branches CSV into the ClickHouse branches staging table."""
    rows = _read_branches()
    client = clickhouse_connect.get_client(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME_STAGING"),
    )
    try:
        client.command(
            """
            CREATE TABLE IF NOT EXISTS stg_branches (
                branch_id String,
                branch_name String,
                region String,
                branch_status String,
                branch_type String,
                opening_date Date,
                manager_name String,
                city String
            ) ENGINE = MergeTree
            ORDER BY branch_id
            """
        )
        client.command("TRUNCATE TABLE stg_branches")
        client.insert("stg_branches", rows, column_names=_COLUMNS)
    finally:
        client.close()

    return MaterializeResult(metadata={"table": "branches", "row_count": len(rows)})


@asset(deps=["branches_staging"])
def branches_dwh() -> None:
    """Load the branches staging table into the ClickHouse data warehouse."""
    client = clickhouse_connect.get_client(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME_DWH"),
    )
    try:
        client.command(
            """
            CREATE TABLE IF NOT EXISTS BRANCHES (
                branch_id String,
                branch_name String,
                region String,
                branch_status String,
                branch_type String,
                opening_date Date,
                manager_name String,
                city String
            ) ENGINE = MergeTree
            ORDER BY branch_id
            """
        )
        client.command("TRUNCATE TABLE BRANCHES")
        client.command(
            """
            INSERT INTO BRANCHES
            SELECT
                branch_id,
                branch_name,
                region,
                branch_status,
                branch_type,
                opening_date,
                manager_name,
                city
            FROM FIN_STG.stg_branches
            """
        )
    finally:
        client.close()
