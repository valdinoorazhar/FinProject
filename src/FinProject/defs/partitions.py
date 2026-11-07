import dagster as dg

#Define daily partition 

daily_partitions = dg.DailyPartitionsDefinition(
    start_date="2023-01-01",
    timezone="Asia/Jakarta",
)