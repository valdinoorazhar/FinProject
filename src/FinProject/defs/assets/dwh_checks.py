import os

import clickhouse_connect
import dagster as dg


def _duplicate_primary_key_count(table: str, column: str) -> int:
    client = clickhouse_connect.get_client(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME_DWH"),
    )
    try:
        result = client.query(
            f"""
            SELECT count()
            FROM (
                SELECT {column}
                FROM {table}
                GROUP BY {column}
                HAVING count() > 1
            )
            """
        )
        return int(result.result_rows[0][0])
    finally:
        client.close()


@dg.asset_check(asset=dg.AssetKey("branches_dwh"))
def branches_dwh_primary_key_is_unique() -> dg.AssetCheckResult:
    duplicate_count = _duplicate_primary_key_count("BRANCHES", "branch_id")
    return dg.AssetCheckResult(
        passed=duplicate_count == 0,
        metadata={"duplicate_key_count": duplicate_count},
    )


@dg.asset_check(asset=dg.AssetKey("customers_dwh"))
def customers_dwh_primary_key_is_unique() -> dg.AssetCheckResult:
    duplicate_count = _duplicate_primary_key_count("CUSTOMERS", "customer_id")
    return dg.AssetCheckResult(
        passed=duplicate_count == 0,
        metadata={"duplicate_key_count": duplicate_count},
    )


@dg.asset_check(asset=dg.AssetKey("transactions_dwh"))
def transactions_dwh_primary_key_is_unique() -> dg.AssetCheckResult:
    duplicate_count = _duplicate_primary_key_count("TRANSACTIONS", "transaction_id")
    return dg.AssetCheckResult(
        passed=duplicate_count == 0,
        metadata={"duplicate_key_count": duplicate_count},
    )
