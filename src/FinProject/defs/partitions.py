import dagster as dg

daily_partitions = dg.DailyPartitionsDefinition(
    start_date="2023-01-01",
    timezone="Asia/Jakarta",
)