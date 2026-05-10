from dagster import Definitions, define_asset_job
from dagster_project.assets import bronze_load, silver_transform, gold_metrics, quality_checks, queue_sensor

etl_job = define_asset_job("etl_job")

defs = Definitions(
    assets=[bronze_load, silver_transform, gold_metrics, quality_checks],
    sensors=[queue_sensor],
    jobs=[etl_job]
)