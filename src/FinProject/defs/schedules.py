import dagster as dg


daily_asset_job = dg.define_asset_job(
    name="daily_asset_job",
    selection=dg.AssetSelection.all(),
)

daily_asset_schedule = dg.build_schedule_from_partitioned_job(
    daily_asset_job,
    name="daily_asset_schedule",
    hour_of_day=6,
)
