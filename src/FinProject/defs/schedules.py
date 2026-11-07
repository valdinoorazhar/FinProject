import dagster as dg


daily_asset_job = dg.define_asset_job(
    name="daily_asset_job",
    selection=dg.AssetSelection.all(),
    op_retry_policy=dg.RetryPolicy(max_retries=5, delay=300) #job retry policy: 5 retries with 5 minutes delay between each retry
)

#schedule for daily asset job, runs at 6am every day
daily_asset_schedule = dg.build_schedule_from_partitioned_job(
    daily_asset_job,
    name="daily_asset_schedule",
    hour_of_day=6
)
