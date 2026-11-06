import csv
import os
from datetime import datetime
from pathlib import Path

import clickhouse_connect
from dagster import MaterializeResult, asset


_COLUMNS = [
    "transaction_id",
    "customer_id",
    "branch_id",
    "transaction_date",
    "amount",
    "payment_method",
    "transaction_status",
    "channel",
    "updated_at",
    "merchant_category",
    "device_type",
    "currency",
    "fraud_flag",
    "promo_code",
]


def _read_transactions() -> list[tuple[object, ...]]:
    csv_path = Path(__file__).resolve().parents[4] / "data" / "lake" / "transactions.csv"
    with csv_path.open(newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames != _COLUMNS:
            raise ValueError(
                f"Unexpected columns in {csv_path}: {reader.fieldnames!r}"
            )

        rows = []
        for row in reader:
            rows.append(
                (
                    row["transaction_id"],
                    row["customer_id"],
                    row["branch_id"],
                    datetime.strptime(row["transaction_date"], "%Y-%m-%d %H:%M:%S"),
                    int(row["amount"]) if row["amount"] else None,
                    row["payment_method"],
                    row["transaction_status"],
                    row["channel"],
                    datetime.strptime(row["updated_at"], "%Y-%m-%d %H:%M:%S"),
                    row["merchant_category"],
                    row["device_type"],
                    row["currency"],
                    row["fraud_flag"],
                    row["promo_code"] or None,
                )
            )
    return rows


@asset
def transactions() -> MaterializeResult:
    """Load the transactions CSV into the ClickHouse transactions table."""
    rows = _read_transactions()
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
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id String,
                customer_id String,
                branch_id String,
                transaction_date DateTime,
                amount Nullable(Int64),
                payment_method String,
                transaction_status String,
                channel String,
                updated_at DateTime,
                merchant_category String,
                device_type String,
                currency String,
                fraud_flag String,
                promo_code Nullable(String)
            ) ENGINE = MergeTree
            ORDER BY (transaction_date, transaction_id)
            """
        )
        client.command("TRUNCATE TABLE transactions")
        client.insert("transactions", rows, column_names=_COLUMNS)
    finally:
        client.close()

    return MaterializeResult(
        metadata={"table": "transactions", "row_count": len(rows)}
    )