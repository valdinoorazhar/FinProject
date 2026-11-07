import csv
import os
from datetime import datetime
from pathlib import Path

import clickhouse_connect
from dagster import MaterializeResult, asset
from FinProject.defs.partitions import daily_partitions


_COLUMNS = [
    "customer_id",
    "customer_name",
    "city",
    "registration_date",
    "customer_status",
    "customer_segment",
    "email",
    "phone_number",
    "birth_date",
    "occupation",
    "income_band",
    "kyc_status",
]


def _read_customers() -> list[tuple[object, ...]]:
    csv_path = Path(__file__).resolve().parents[4] / "data" / "lake" / "customers.csv"
    with csv_path.open(newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames != _COLUMNS:
            raise ValueError(f"Unexpected columns in {csv_path}: {reader.fieldnames!r}")

        rows = []
        for row in reader:
            rows.append(
                (
                    row["customer_id"],
                    row["customer_name"],
                    row["city"],
                    datetime.strptime(row["registration_date"], "%Y-%m-%d").date(),
                    row["customer_status"],
                    row["customer_segment"],
                    row["email"],
                    row["phone_number"],
                    datetime.strptime(row["birth_date"], "%Y-%m-%d").date(),
                    row["occupation"],
                    row["income_band"],
                    row["kyc_status"],
                )
            )
    return rows


@asset
def customers_staging() -> MaterializeResult:
    """Load the customers CSV into the ClickHouse customers staging table."""
    rows = _read_customers()
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
            CREATE TABLE IF NOT EXISTS stg_customers (
                customer_id String,
                customer_name String,
                city String,
                registration_date Date,
                customer_status String,
                customer_segment String,
                email String,
                phone_number String,
                birth_date Date32,
                occupation String,
                income_band String,
                kyc_status String
            ) ENGINE = MergeTree
            ORDER BY customer_id
            """
        )
        client.command("TRUNCATE TABLE stg_customers")
        client.insert("stg_customers", rows, column_names=_COLUMNS)
    finally:
        client.close()

    return MaterializeResult(metadata={"table": "customers", "row_count": len(rows)})


@asset(deps=["customers_staging"], partitions_def = daily_partitions)
def customers_dwh(context) -> None:
    """Load the customers staging table into the ClickHouse data warehouse."""
    partition_date = context.partition_key
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
            CREATE TABLE IF NOT EXISTS CUSTOMERS (
                customer_id String,
                customer_name String,
                city String,
                registration_date Date,
                customer_status String,
                customer_segment String,
                email String,
                phone_number String,
                birth_date Date,
                occupation String,
                income_band String,
                kyc_status String
            ) ENGINE = MergeTree
            ORDER BY customer_id
            """
        )
        client.command(f"DELETE FROM CUSTOMERS WHERE toDate(registration_date) = '{partition_date}'")
        client.command(
            f"""
            INSERT INTO CUSTOMERS
            SELECT
                customer_id,
                customer_name,
                city,
                registration_date,
                customer_status,
                customer_segment,
                email,
                phone_number,
                birth_date,
                occupation,
                income_band,
                kyc_status
            FROM(
                SELECT
                    sc.*,
                    row_number() OVER (
                        PARTITION BY customer_id 
                        ORDER BY 
                            CASE 
                                WHEN kyc_status = 'VERIFIED' THEN 1
                                WHEN kyc_status = 'PENDING' THEN 2
                                ELSE 3
                            END ASC
                            , CASE 
                                WHEN customer_status = 'ACTIVE' THEN 1
                                ELSE 2
                            END ASC
                            , registration_date DESC
                    ) AS rn
                FROM FIN_STG.stg_customers sc
                WHERE toDate(registration_date) = '{partition_date}'
            )
            WHERE rn  = 1
            """
        )
    finally:
        client.close()
