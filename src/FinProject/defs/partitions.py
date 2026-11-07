import dagster as dg

daily_partitions = dg.DailyPartitionsDefinition(start_date="2025-01-01")